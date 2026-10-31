# 로컬 개발

## 준비

- JDK 21 (이 프로젝트 전용으로 지정, 기본 Java를 바꾸지 않아도 됨)
- Docker Desktop (실행 중이어야 함)
- Gradle은 래퍼(`./gradlew`)를 사용

```bash
# JDK 21 위치를 환경변수로 지정 (Homebrew 예시)
export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
export JAVA_HOME_21=$JAVA_HOME

# 빌드 + 테스트 (Docker 불필요)
./gradlew test bootJar

# 인프라 기동 (Kafka, PostgreSQL)
cp .env.example .env        # 값 채우기 (.env는 커밋되지 않음)
docker compose -f infra/compose/docker-compose.yml --env-file .env up -d

# 서비스 실행 (저장소 루트에서). 시작 순서는 상관없음(토픽을 각 서비스가 선언함)
java -jar services/collector/build/libs/collector-0.0.1-SNAPSHOT.jar
java -jar services/market/build/libs/market-0.0.1-SNAPSHOT.jar
java -jar services/news/build/libs/news-0.0.1-SNAPSHOT.jar
# 브리핑, 발송기는 아래 참고
```

시세처리기(market)는 `market.snapshot`을 `price_snapshot`(`as_of` 기준 날짜별 파티션, 응답 원문은 jsonb `raw`)에, `market.snapshot.failed`를 `quote_failure`에 저장합니다. 파티션은 저장 직전에 없으면 만들며(Asia/Seoul 자정 경계), 테이블 구조는 `services/market/src/main/resources/db/migration/`의 마이그레이션이 원본입니다. 원문 보존 기간은 아직 정하지 않았습니다.

뉴스 저장(news)은 `news.collected`와 함께 공시 이벤트 `filing.collected`를 별도 consumer group(`news-filings`)으로 구독해 `filing`(접수번호 유니크)과 `filing_ticker`에 저장합니다. 같은 공시가 다른 이벤트로 다시 오면 새로 연결된 티커만 더하고, 재시도 후에도 실패한 이벤트는 뉴스와 같은 `failed_event`에 남깁니다. 종목·기간 조회는 `FilingQuery`(티커, 시작·끝 시각, 기준 시각)이며 기준 시각보다 늦게 접수된 공시는 돌려주지 않습니다(같으면 포함). 해외 종목의 직전 미국 정규장 마감 등락률은 시세처리기의 `OverseasCloseRateQuery`(티커, 기준 시각)가 저장된 해외 일봉 원문에서 읽어 고른 일자와 등락율 원문(`up_dwn_r_p2`)을 돌려줍니다. 고르는 규칙과 이유는 `OverseasCloseRateQuery`의 주석에 있습니다. 공시 조회는 news의 `news.filings.requested` 응답이, 해외 등락률은 시세처리기의 `market.context.requested` 응답이 씁니다(아래 브리핑 서비스).

시세처리기는 스냅샷마다 급변동을 감지해 `market.move`를 발행합니다. 유형 1(`window`)은 최근 N분 이력의 가장 오래된 스냅샷 대비, 유형 2(`prevClose`)는 응답 원문의 전일 종가 필드 대비 변동률이 임계값 이상일 때이며, 한 번 발행한 뒤에는 마지막 발행 가격에서 임계값만큼 더 움직일 때만 다시 발행합니다(상태는 `move_state` 테이블에 둡니다). 설정은 `services/market/src/main/resources/application.yml`의 `market.*`이고 환경변수로 덮어쓸 수 있습니다.

기본값은 `application.yml`이 원본입니다.

| 설정 키 | 설명 |
|---|---|
| `market.window-minutes` | 유형 1이 비교할 최근 이력의 길이(분) |
| `market.window-threshold-percent` | 유형 1 임계값(%) |
| `market.prev-close-field` | 유형 2가 원문에서 읽을 전일 종가 필드(점으로 중첩 경로). 제공자 고유 이름이라 공개 파일에 쓰지 않고 공유 `.env`의 `MARKET_PREV_CLOSE_FIELD`로만 넣습니다. 비어 있으면 유형 2를 끄고 시작할 때 경고를 한 번 남깁니다. 원문에 필드가 없거나 숫자가 아니면 유형 2는 그 스냅샷을 판단하지 않습니다 |
| `market.prev-close-threshold-percent` | 유형 2 임계값(%) |

시세처리기는 브리핑의 배경 자료 요청(`market.context.requested`)에 답합니다. 종류마다(환율종합, 시장종합, 세계지수 대륙별, 그 종목의 국내 일봉) 받은 시각이 기준 시각 이전인 것 중 최신 원문 1건을 주고, ETF에 담긴 미국 종목 목록과 종목마다 직전 정규장 등락률을 함께 줍니다. 세계지수는 장이 끝난 지수의 응답 등락률이 0.00으로 오므로 쓰지 않고, 같은 지수의 영업일이 더 이른 직전 저장 행 종가와 비교해 계산한 지수별 등락률(`indexRates`)을 함께 줍니다. 아래 값은 안전 상한이며 요청값의 원본은 브리핑 `config.py`입니다. 요청이 더 크면 이 값까지만 줍니다.

| 설정 키 | 설명 |
|---|---|
| `market.holdings.us-by-etf` | ETF별 미국 종목 대응표. 수집기 `ETF_US_HOLDINGS`와 같은 값과 형식이며 `.env`에만 둡니다. 형식이 틀리면 시작 단계에서 실패하고, 비어 있으면 미국 종목이 없는 것으로 봅니다 |
| `market.context.max-age` | 기준 시각보다 이 기간 넘게 이른 자료는 없음으로 봅니다 |
| `market.context.max-response-bytes` | 원문 1건의 최대 크기(UTF-8 바이트). 넘으면 원문을 빼고 너무 큼으로 표시합니다 |
| `market.context.max-tickers` | 응답에 담을 미국 종목의 최대 개수 |
| `market.context.world-index-continents` | 배경 자료로 줄 세계지수 대륙구분. 수집기의 마감 후 수집 대륙과 맞춥니다 |

이전 키 `market.move-threshold-percent`는 없어졌습니다. 마이그레이션(`services/market/src/main/resources/db/migration/`)은 서비스 기동 때 Flyway가 적용하며, 단위 테스트는 저장소를 대역으로 쓰므로 SQL은 로컬 Postgres에서 따로 확인해야 합니다.

## 브리핑 서비스 (Python)

`market.move` 한 건마다 근거를 모아 리포트를 만들고 `briefing.generated`로 발행합니다.

1. 그 변동에 쓸 수 있는 뉴스 수집 완료(`collection.completed`)를 기다립니다. 쓸 수 있는 완료는 `jobType`이 `collect-news`이고, `startedAt`이 변동 시각 T + Δ 이후이며, 그 종목과 `*`가 `incompleteSymbols`에 없는 것입니다. 변동보다 먼저 도착한 최근 완료도 메모리에 보관했다가 확인합니다.
2. 세 요청을 함께 보냅니다: 시세처리기에 시세 이력(`market.history.requested`, T - 5분 ~ T + Δ를 일정 간격으로 솎아서)과 배경 자료(`market.context.requested`)를, news 서비스에 기사(`news.articles.requested`, T - 24시간 ~ T + Δ, 양끝 포함)를 요청합니다. 기사는 그 종목과 시장 전체(`*`) 태그만 돌아오며, 다른 종목 태그 기사를 빼는 것은 news 조회가 보장합니다(브리핑은 다시 검사하지 않음).
3. 배경 자료 응답에 미국 종목 목록이 있으면 그 종목들의 공시(`news.filings.requested`, 접수 시각 T - 72시간 ~ T)를 요청합니다. 공시는 하루 한 번 받으므로 + Δ를 두지 않습니다. 미국 종목이 없으면 공시를 요청하지 않고 해당 없음으로 둡니다.
4. 응답이 제한 시간 안에 없으면 새 순번(`seq`)으로 다시 요청하는 규칙은 모든 요청에 같습니다. 기사는 일정 간격으로 다시 요청해, 연속 두 번 같은 기사 목록(순서 무시)이 오면 안정된 것으로 봅니다. 모든 응답을 받아야 다음으로 넘어갑니다.
5. 받은 자료의 시각을 다시 검사합니다. 발행·접수 시각이 범위 밖인 기사·공시와, 받은 시각이 T 뒤이거나 기준 기간보다 오래된 배경 자료·해외 등락률은 버리고 경고 로그를 남깁니다(테스트로 검증). 변동마다 종목·티커 없이 작업 번호와 자료 종류별 건수·제외·잘림을 한 줄 수집 내역 로그(`collection job=...`)로 남깁니다.
6. 기사(`articleId`), 공시(접수번호), 해외 등락률(`U1`…), 배경 자료(`B1`…)에 근거 번호를 붙여 LLM에 줍니다. 배경 자료에는 받은 시각과 변동보다 얼마나 전 값인지를 붙이고, 원인으로 단정하지 말라고 지시합니다. LLM이 근거 번호(`refs`)로 인용한 문장을 만들면 코드가 형식과 인용 번호를 검증하고, 목록 밖 번호가 있으면 거부합니다. 머리말과 "투자 조언이 아님" 문구는 코드가 넣습니다. `generatedBy`는 `llm`입니다. LLM 키가 없으면 기사와 공시를 나열하는 규칙 기반 리포트를 만듭니다. `generatedBy`는 `rule-based`입니다.
7. `briefing.generated`의 근거에는 자료 종류(`kind`)와 근거 번호(`ref`)를 담습니다. 출처는 제공자 이름 대신 중립 표현을 쓰고, 공시와 해외 등락률의 근거 제목에는 어느 회사 자료인지 알 수 있게 미국 티커를 넣습니다(로그에는 넣지 않습니다).

같은 종목의 변동이 여러 번 와도 이벤트마다 리포트를 만듭니다. 같은 `eventId`는 한 번만 처리합니다.

### 설정

근거 조회 설정(Δ, 자료별 시간 범위와 기준 기간, 시세 이력 간격, 자료별 최대 건수·크기, 응답 제한 시간, 재시도 횟수, 재확인 간격, 대기 상한 등)은 `services/briefing/src/briefing/config.py`의 `CoreSettings` 기본값이며 환경변수로 바꿀 수 없습니다. 값과 뜻은 그 파일에 있습니다.

환경변수(`.env`)는 다음과 같습니다.

환경변수가 없을 때 쓰는 값은 `main.py`와 `report.py`가 원본입니다.

| 환경변수 | 설명 |
|---|---|
| `KAFKA_BOOTSTRAP` | Kafka 주소 |
| `ANTHROPIC_API_KEY` | LLM 키. 비어 있거나 자리표시 값(`change-me`)이면 시작할 때 경고를 남기고 규칙 기반 리포트를 만듭니다. 아래 Parameter Store의 `briefing` 그룹에서 가져옵니다 |
| `BRIEFING_LLM_MODEL` | 리포트를 쓰는 모델 |
| `POSTGRES_DB` | 실패 기록 DB 이름 |
| `BRIEFING_DB_USER` / `BRIEFING_DB_PASSWORD` | 브리핑 전용 DB 계정. 비밀번호는 `.env`에만 둡니다 |

### 리포트를 못 만든 변동 기록

리포트를 만들지 못하면 `briefing` 스키마의 `briefing.failed_jobs` 테이블에 한 줄을 남깁니다: `job_id`(변동 이벤트의 `eventId`, 같은 값은 한 번만), 종목, 사유, 짧은 설명(500자까지), 시도 횟수, 기록 시각. 기사 내용과 개인 정보는 저장하지 않습니다.

| 사유 | 뜻 |
|---|---|
| `collection_not_covered` | 쓸 수 있는 수집 완료를 대기 상한 안에 받지 못함 |
| `market_no_response` | 시세 이력 응답을 총 시도 횟수 안에 받지 못함 |
| `news_no_response` | 기사 응답을 총 시도 횟수 안에 받지 못함 |
| `news_unstable` | 최대 라운드 수 안에 기사 목록이 안정되지 않음 |
| `context_no_response` | 배경 자료 응답을 총 시도 횟수 안에 받지 못함 |
| `filings_no_response` | 공시 응답을 총 시도 횟수 안에 받지 못함 |
| `llm_error` | LLM 출력이 3번 모두 형식·인용 검증을 통과하지 못하거나 호출이 실패함 |

기록 자체가 실패하면 짧은 간격으로 3번까지 시도하고, 그래도 안 되면 `job_id`, 종목, 사유만 ERROR 로그에 남기고 넘어갑니다.

### DB 준비와 다시 만들기

브리핑 전용 계정과 `briefing` 스키마는 `infra/compose/postgres/init/03-roles-briefing.sql`이 만듭니다. 테이블은 마이그레이션 도구 없이 브리핑이 시작할 때 `CREATE TABLE IF NOT EXISTS`로 만듭니다. DB에 연결할 수 없으면 브리핑은 시작하지 않습니다.

- 초기화 스크립트는 DB 볼륨을 처음 만들 때만 실행됩니다. `.env`에 `BRIEFING_DB_USER`, `BRIEFING_DB_PASSWORD`를 넣고, 이미 볼륨이 있으면 볼륨을 지우고 다시 띄워야 브리핑 계정이 생깁니다. 비어 있으면 초기화 스크립트가 실패합니다.
- 사유 목록 제약은 시작할 때마다 다시 걸어, 이전에 만든 테이블도 새 사유를 받습니다.
- 테이블 모양이 바뀌면 `CREATE TABLE IF NOT EXISTS`는 기존 테이블을 고치지 않습니다. 배포 전까지는 DB를 지워도 되는 전제이므로 로컬 DB를 지우고 다시 만듭니다.

```bash
# 로컬 DB를 지우고 다시 만든다 (모든 서비스의 DB 데이터가 사라진다)
docker compose -f infra/compose/docker-compose.yml --env-file .env down -v
docker compose -f infra/compose/docker-compose.yml --env-file .env up -d
```

### 실행

```bash
# 최초 1회: 프로젝트 전용 가상환경 (시스템 파이썬은 건드리지 않음)
python3 -m venv services/briefing/.venv
services/briefing/.venv/bin/python -m pip install -r services/briefing/requirements.txt
# 의존성이 바뀌면(예: DB 드라이버, LLM SDK 추가) 위 pip install 을 다시 실행한다

# 테스트 (실제 Kafka·DB·LLM을 호출하지 않음)
services/briefing/.venv/bin/python -m pytest services/briefing

# 실행 (저장소 루트에서). 브리핑은 .env 파일을 읽지 않고 환경변수만 읽으므로 필요한 키만 넘긴다
# (.env 전체를 셸로 읽으면 ';'가 들어간 값이 명령으로 해석될 수 있다)
export $(grep -E '^(KAFKA_BOOTSTRAP|ANTHROPIC_API_KEY|BRIEFING_LLM_MODEL|POSTGRES_DB|BRIEFING_DB_USER|BRIEFING_DB_PASSWORD)=' .env | xargs)
PYTHONPATH=services/briefing/src services/briefing/.venv/bin/python -m briefing.main
```

브리핑은 시작할 때 자기가 쓰는 토픽을 선언합니다(목록과 파티션 수는 `main.py`의 `TOPIC_PARTITIONS`). 데일리 브리핑 결과 토픽 `briefing.daily.generated`(아침 판·마감 후 판 한 통씩, 형식은 `contracts/events/briefing.daily.generated.v1.schema.json`)도 선언하지만, 이 토픽에 발행하는 처리는 아직 없습니다. `Ctrl+C`(또는 종료 신호)를 받으면 처리 중인 루프를 멈추고 끝납니다.

변동 알림(`market.move`)은 읽는 즉시 읽은 위치(오프셋)를 확정합니다. 진행 중인 작업은 메모리에만 있으므로 **브리핑이 재시작되면 처리 중이던 변동은 리포트도 실패 기록도 남지 않고 사라집니다.**

흐름을 사람이 직접 확인할 때는 `scripts/manual-briefing-move.sh [종목]`을 씁니다. 로컬 Kafka에 가짜 `market.move`와 그 변동에 쓸 수 있는 `collection.completed`를 한 건씩 넣고, 브리핑이 낸 요청과 `briefing.generated`를 보여 줍니다. 미리 인프라, 브리핑, 시세처리기, news 서비스를 띄워 둡니다.

## 이메일 발송기 (`notifier`)

`briefing.generated`(급변동 건별)와 `briefing.daily.generated`(데일리 브리핑 판) → 이메일. **기본은 드라이런**(로그만 남기고 아무것도 보내지 않음)이며, 켜려면 `.env`에 아래를 채웁니다. 기본값은 `application.yml`이 원본입니다.

| 설정 | 설명 |
|---|---|
| `NOTIFIER_SEND_ENABLED` | `true`일 때만 실제 발송 |
| `NOTIFIER_SENDER` / `NOTIFIER_RECIPIENT` | 발신/수신 주소. **개인정보이므로 `.env`에만** 둡니다. SES 샌드박스에서는 둘 다 SES에서 인증된 주소여야 함 |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USERNAME` / `SMTP_PASSWORD` | SES 콘솔의 "SMTP settings"에서 만든 전용 자격증명(STARTTLS) |
| `NOTIFIER_MAX_PER_HOUR` | 시간당 발송 한도. 건별 메일의 초과분은 보내지 않고 로그만 남김. 데일리 브리핑 메일은 한도에 막히지 않고 항상 보내되 보낸 수는 한도에 더하므로, 그 뒤 건별 메일이 막힐 수 있음 |
| `NOTIFIER_SUBJECT_PREFIX` | 제목 머리말. 데이터가 가짜인 동안 진짜 투자 정보로 오해하지 않도록 |

발송기는 데일리 브리핑 결과 토픽 `briefing.daily.generated`도 브리핑과 같은 파티션 수로 선언합니다(`Topics`). 이 토픽의 메시지 한 건(판 하나)마다 메일 한 통을 만듭니다(`DailyBriefingListener`). 제목은 머리말 뒤에 판 종류와 날짜(이번 발송 시각의 한국 날짜)를 붙인 `[시뮬레이션] 아침 판 데일리 브리핑 2026-10-20` 꼴이고, 본문은 받은 글 뒤에 자료 없음 항목, 다룬 범위, 추적 ID, 생성 방식을 붙입니다. 이 토픽에 메시지를 내는 브리핑 쪽 처리는 아직 없습니다.

안전 규칙(모두 테스트로 검증): 설정이 없으면 드라이런, 발송을 켰는데 주소/호스트가 비어 있으면 **시작 단계에서 실패**, 같은 브리핑은 한 번만 발송(건별·데일리 모두 `eventId` 기준), 필수 값이 빠지거나 형식이 잘못된 메시지는 보내지 않고 건너뜀, 로그에는 주소를 **마스킹**해서만 출력.

> **`.env`에는 ASCII 문자만 쓰세요.** 스프링이 이 파일을 Latin-1로 읽어 한글 값이 깨집니다. 한글 기본값은 `application.yml`에 있습니다.

```bash
java -jar services/notifier/build/libs/notifier-0.0.1-SNAPSHOT.jar   # 저장소 루트에서
```

## 수집기의 외부 데이터 설정 (`collector`)

수집기에는 공시, 시세, 뉴스 검색 클라이언트가 있습니다. 시세 기본은 가짜 데이터(시뮬레이터)이며, `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 외부 API를 씁니다. 뉴스는 `collect-news` tick마다 검색해 기사마다 `news.collected`를 발행하고, 기사 전송이 모두 확정된 뒤 `collection.completed`를 한 건 발행합니다([ADR-010](adr/010-event-format.md)). DART 공시는 아직 조회 클라이언트까지만 구현되어 있고 이벤트로 발행하지 않습니다. 미국 종목 공시(SEC EDGAR)는 아래 장 시작 전 수집에서 `filing.collected`로 발행합니다. 기본값은 `application.yml`의 `collector.*`가 원본입니다.

| 설정 | 설명 |
|---|---|
| `COLLECTOR_MARKET_DATA_SOURCE` | `simulator` 또는 `broker` |
| `BROKER_BASE_URL` / `BROKER_APP_KEY` / `BROKER_APP_SECRET` | 시세 제공자의 주소와 키. `broker`일 때 필요 |
| `COLLECTOR_TICK_INTERVAL_MS` | `schedule.tick`(수집) 발행 간격(밀리초). 타이머(`COLLECTOR_SCHEDULE_ENABLED=true`)가 켜져 있을 때만 쓰입니다. `scheduledFor`가 초 단위라 1000보다 작게 두면 같은 초의 tick이 중복으로 버려집니다 |
| `COLLECTOR_MAX_TICK_AGE_MS` | 현재보다 이만큼 이상 오래된 tick은 조회하지 않고 종목마다 `TICK_SKIPPED` 실패 이벤트로 기록합니다 |
| `BROKER_MAX_CALLS_PER_SECOND` | 초당 시세 호출 상한. 종목 수가 이보다 많으면 시작할 때 경고하고, 넘친 종목은 그 tick에서 호출하지 않고 `TICK_SKIPPED` 실패 이벤트로 기록합니다. 상한 때문임은 `exceptionType=RateLimited`로 구분하며, 나중에 계약에 정식 실패 종류가 생기면 바꿀 수 있습니다. **종목 수와 같게 두면 오래 돌 때 일부 호출을 놓치고 처리도 느려지므로 종목 수의 1.2배 이상으로 두세요**([측정 기록](test-history/S03-quote-fixed-load/results.md), 가짜 서버로 쟀을 때. 1.1배까지는 가짜 서버 시험에서 놓침이 없었지만 처리 시간이 길었습니다, [R012](test-history/S03-quote-fixed-load/results.md)). 값을 올릴 때는 실제 서버가 5분을 버틴 가장 높은 속도가 초당 12회이고 13회부터 걸렸다는 점을 넘지 않게 합니다([S04](test-history/S04-quote-real-api/results.md)). 제공자의 공식 한도는 앱 전체 초당 40회, API별 초당 20회입니다 |
| `NEWS_SEARCH_BASE_URL` / `NEWS_SEARCH_CLIENT_ID` / `NEWS_SEARCH_CLIENT_SECRET` | 뉴스 검색 제공자의 주소와 키 |
| `NEWS_SEARCH_ID_HEADER` / `NEWS_SEARCH_SECRET_HEADER` | 제공자가 정한 인증 헤더 이름 |
| `NEWS_SEARCH_QUERIES` | `검색어=종목코드,종목코드;검색어2=*` 형식. 관심 종목이 드러나므로 `.env`에만 둡니다 |
| `NEWS_SEARCH_CALL_INTERVAL_MS` | 검색어를 연달아 호출할 때 호출 사이의 대기(밀리초). 0이면 기다리지 않으며, 짧으면 제공자가 429로 거절할 수 있습니다([측정 기록](test-history/S01-real-api/call-interval.md)) |
| `DART_API_KEY` | 공시 API 키 |
| `ETF_US_HOLDINGS` | 관심 ETF별 미국 상위 종목 목록. `ETF코드=티커,티커;ETF코드2=티커` 형식이며 ETF 코드는 6자리 숫자·대문자, 티커는 영문 대문자·숫자 1~6자(종류 구분이 있으면 `.` 또는 `-` 뒤 1~2자)입니다. 형식이 틀리면 시작 단계에서 실패하고, 오류 메시지에는 값 대신 몇 번째 항목인지만 적습니다. 종류 구분은 `-`로 맞춰 씁니다(`.`로 적어도 같은 종목). 비어 있으면 보유 종목이 없는 것으로 봅니다. 보유 종목이 드러나므로 `.env`에만 둡니다 |
| `EDGAR_USER_AGENT` | SEC EDGAR 요청의 User-Agent. SEC가 요구하는 `회사명 연락이메일` 형식입니다. 연락 이메일은 개인 정보이므로 `.env`에만 둡니다. 비어 있으면 공시를 조회하지 않습니다 |
| `EDGAR_FORMS` | 수집할 공시 형식. 쉼표로 구분합니다. 비어 있거나 형식이 틀리면 시작 단계에서 실패합니다 |
| `COLLECTOR_INSIGHT_SCHEDULE_ENABLED` | `true`일 때만 장 마감 후 조회 tick(`collect-post-close`)을 발행합니다. 휴장일은 판정하지 않아 휴장일에도 발행됩니다 |
| `COLLECTOR_INSIGHT_CRON` | 마감 후 조회 시각. 한국 시간 기준 cron(초 분 시 일 월 요일) |
| `COLLECTOR_PRE_OPEN_ENABLED` | `true`일 때만 장 시작 전 수집 tick(`collect-pre-open`)을 발행합니다. 휴장일은 판정하지 않아 휴장일에도 발행됩니다. 공시는 이 tick에서만 받으므로 꺼 두면 공시를 수집하지 않습니다 |
| `COLLECTOR_PRE_OPEN_CRON` | 장 시작 전 수집 시각. 한국 시간 기준 cron |
| `COLLECTOR_DAILY_BRIEFING_ENABLED` | `true`일 때만 데일리 브리핑 시작 tick(아침 판 `daily-briefing-morning`, 마감 후 판 `daily-briefing-post-close`)을 발행합니다. 수집기는 신호만 보내고 자료를 새로 모으지 않습니다. 휴장일은 판정하지 않아 휴장일에도 발행됩니다 |
| `COLLECTOR_DAILY_BRIEFING_MORNING_CRON` | 아침 판 시작 시각. 한국 시간 기준 cron이며 장 시작 전 수집 뒤로 둡니다. `-`를 주면 이 판만 발행하지 않습니다 |
| `COLLECTOR_DAILY_BRIEFING_POST_CLOSE_CRON` | 마감 후 판 시작 시각. 한국 시간 기준 cron이며 장 마감 후 조회 뒤로 둡니다. `-`를 주면 이 판만 발행하지 않습니다 |
| `COLLECTOR_INTRADAY_ENABLED` | `true`일 때만 장중 수집 tick(`collect-intraday`)을 발행합니다. 기본은 꺼짐이며, 켜기 전에 장중 실호출 확인이 필요합니다 |
| `COLLECTOR_INTRADAY_CRON` | 장중 수집 주기. 한국 시간 기준 cron |
| `COLLECTOR_INTRADAY_START` / `COLLECTOR_INTRADAY_END` | 장중 수집 시작·끝 시각(`HH:mm`, 양끝 포함). 이 밖의 tick은 조회하지 않습니다 |
| `COLLECTOR_INTRADAY_STAGGER_MS` | 장중 조회 사이 대기(밀리초). 같은 초에 몰려 시세 조회가 호출 상한에 걸리지 않게 나눠 부릅니다 |

장중 수집은 환율종합, 시장종합, 세계지수(아시아, 아메리카)를 시차를 두고 조회해 마감 후 조회와 같은 `market.insight`로 보냅니다. 시세 조회와 같은 호출 상한을 거치며, 휴장일은 판정하지 않습니다. 기본값은 `application.yml`의 `collector.intraday.*`입니다.

마감 후 조회(일봉, 세계지수, 환율종합, 시장종합)는 `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 동작하며, 응답 원문을 `market.insight` 이벤트로 발행합니다. 일봉의 시장구분과 조회 건수, 세계지수의 대륙 목록은 `application.yml`의 `collector.insight.*`에 기본값이 있습니다.

장 시작 전 수집(`COLLECTOR_PRE_OPEN_CRON`, 평일 아침)은 국내 장중 변동의 근거로 쓸 직전 미국 장 결과와 밤사이 공시를 미리 받습니다. 하나의 tick(`collect-pre-open`)을 두 리스너가 받습니다.

| 받는 것 | 조건 | 결과 |
|---|---|---|
| 세계지수 아메리카(`C`) 1건, 보유 미국 종목(`ETF_US_HOLDINGS`) 해외 일봉 | `COLLECTOR_MARKET_DATA_SOURCE=broker` | 마감 후 조회와 같은 `market.insight`(`world-index`, `overseas-daily-price`)와 같은 테이블. 시세 조회와 같은 호출 상한을 거칩니다 |
| SEC EDGAR 공시 | `EDGAR_USER_AGENT`가 있을 때 | `filing.collected`. 접수 시각이 `collector.edgar.lookback-hours` 안인 공시만 발행합니다. 하루 한 번이라 주말과 연휴를 넘겨도 빠지지 않게 넉넉히 둡니다 |

공시 15분 주기 수집(`collect-filings`)은 없어졌습니다. SEC 공시 이력(submissions)은 조건 조회를 지원하지 않아 매번 최근 이력을 통째로 주므로(회사당 약 167KB, 압축 시 약 22KB, 2026-10-29 실측) 하루 한 번만 받고 gzip 압축을 요청합니다. 서버가 압축하지 않고 주면 그대로 읽습니다. 장 마감 후 수집(16:30)은 국내 일봉·세계지수·환율종합·시장종합만 받습니다. 국내 장 마감 무렵에는 미국 장이 열리지 않아 해외 일봉은 장 시작 전에만 받습니다.

해외 일봉의 거래소 코드는 SEC 티커·거래소 매핑 파일(`collector.edgar.ticker-map-url`)로 고릅니다. 이 파일은 하루 한 번 받고, 새로 받기에 실패하면 전에 받은 파일을 계속 씁니다. 처음 받기부터 실패하면 잠시 다시 받지 않고 해외 일봉 종목은 거래소 조회 실패로 기록됩니다.

해외 일봉 응답에서 확인한 것(2026-10-29, 공개 미국 종목 실호출): 등락율 필드(`up_dwn_r_p2`)는 퍼센트 값이고 소수 둘째 자리까지이며 앞을 공백으로 채운 문자열입니다(하락은 `-`). 레코드 배열의 맨 앞은 미국 정규장이 끝난 뒤 열리는 장외 거래의 진행 중 막대일 수 있고(거래량이 매우 적음), 응답 최상위의 등락율도 그 막대 기준입니다. 그래서 등락률은 `OverseasCloseRateQuery`가 레코드의 정규장 마감 시각으로 고릅니다. 미국 장이 끝난 뒤의 해외 일봉 응답에서는 맨 앞 레코드가 직전 정규장의 완성 막대였습니다(2026-10-31 확인). 세계지수 응답에서 장이 끝난 지수는 시각이 `9999`로 오고, 평일에도 전일대비와 등락률이 0으로 옵니다(2026-10-31, 11-02 확인). 응답에 전일 종가가 없어 미국 지수 등락률은 이 응답만으로 구할 수 없습니다.

시세 조회는 종목마다 병렬로 호출하고 실패해도 재시도하지 않으며 다음 tick에 맡깁니다. 성공하면 응답 원문을 `market.snapshot`의 `raw`에, 실패하면 종목마다 `market.snapshot.failed`를 발행합니다(실패 원문 포함, 키·시크릿·접근토큰 값은 가려서 담습니다). 로그와 예외 메시지에는 서버가 보낸 문구 대신 형식을 확인한 처리 코드(영숫자 8자 이하, 아니면 `invalid`)만 남습니다. 테스트는 로컬 가짜 HTTP 서버(`FakeBrokerServer`)를 쓰며 실제 서버를 호출하지 않습니다.

키 값(`*_KEY`, `*_SECRET`, `DART_API_KEY`)은 아래 Parameter Store에서 가져옵니다. 주소와 헤더 이름은 비밀이 아니지만 코드에 두지 않고 `.env`에 직접 적습니다.

### 시세 API 연동 확인

시세는 조회만 씁니다(ADR-007). `.env`에는 다음을 둡니다.

- `BROKER_BASE_URL`: 시세 제공자가 안내한 운영 주소
- `BROKER_APP_KEY`, `BROKER_APP_SECRET`: Parameter Store에서 가져옵니다.
- `COLLECTOR_MARKET_DATA_SOURCE=broker`

실제 서버 확인용 수동 테스트: `BrokerLiveProbeTest`는 평소 건너뛰며 `BROKER_PROBE=1`일 때만 토큰 1회와 종목별 현재가를 호출합니다. 요약은 `BROKER_PROBE_OUT`, 응답 원문은 `BROKER_PROBE_RAW_DIR`에 쓰고 키와 토큰 값은 남기지 않습니다. 종목은 `BROKER_PROBE_SYMBOLS`(쉼표 구분)로 지정하며 코드에는 두지 않습니다. 실제 시세 응답은 저장소에 올리지 않으므로 `BROKER_PROBE_RAW_DIR`은 `data/`처럼 gitignore 대상 경로로 지정합니다. 실제 운영 서버를 호출하므로 필요할 때 최소한으로 실행합니다.

장 마감 후 조회 API(일봉, 세계지수, 환율종합, 시장종합)의 응답 형식을 보는 `PostCloseLiveProbeTest`는 `BROKER_POSTCLOSE_PROBE=1`일 때만 토큰 1회와 API당 1회씩을 호출합니다. 종목은 `BROKER_PROBE_SYMBOLS`의 첫 번째만 쓰고, 조회 건수(`BROKER_POSTCLOSE_PROBE_COUNT`, 기본 30), 시장구분(`BROKER_POSTCLOSE_PROBE_MARKET`, 기본 `0`), 세계지수 대륙구분(`BROKER_POSTCLOSE_PROBE_CONTINENT`, 기본 `C`, 쉼표로 여러 개 가능하며 대륙마다 한 번씩 호출)은 바꿔 볼 수 있습니다. 호출할 API는 `BROKER_POSTCLOSE_PROBE_APIS`(`daily`, `world`, `exchange`, `overview`를 쉼표로, 기본 전부)로 고를 수 있고, `price`를 고르면 현재가를 거래소 구분(`BROKER_POSTCLOSE_PROBE_EXCHANGES`, 기본 `0,1,2`)마다 한 번씩 호출합니다(일봉 거래량이 어느 거래소 기준인지 맞춰 볼 때 씁니다, 기본 대상에는 포함되지 않음). 요약과 원문 저장 위치는 위 `BrokerLiveProbeTest`와 같습니다.

호출 속도 실험용 `BrokerRateProbeTest`는 한 종목의 현재가를 초당 호출 수(`BROKER_RATE_PROBE_RATES`, 쉼표 구분, 기본 `1,2,3`)로 `BROKER_RATE_PROBE_PHASE_SECONDS`(기본 30)초씩 단계별로 올리며 호출합니다. 정상 응답의 `processCode`를 `BROKER_RATE_PROBE_OK_CODE`로 받아, 이와 다른 응답이 하나라도 나오면 즉시 멈추고, 단계별 횟수·지연과 처음 이상 응답의 헤더만 `BROKER_PROBE_OUT`에 씁니다.

이 시험은 **`scripts/live_probe.py`로만 실행합니다.** 시험 클래스는 이 도구가 켜 주는 `LIVE_PROBE_GUARD`가 없으면 건너뜁니다. `--probe smoke`를 주면 속도 시험 대신 `BrokerClientLiveSmokeTest`가 실행되어, 하나의 `BrokerClient`가 토큰 1회로 현재가와 마감 후 조회를 처리하는지 확인합니다(호출 3회, `--ok-code` 불필요).

```bash
python3 scripts/live_probe.py --profile <AWS 프로필> --base-url <시세 제공자 운영 주소> \
  --symbols <종목 코드> --ok-code <정상 응답 코드> --rates 5,9 --phase-seconds 60
```

- 키는 **`.env`가 아니라 Parameter Store에서 직접** 읽고 시험 프로세스의 환경변수로만 넘깁니다(명령줄과 출력에는 남기지 않음).
- 직전 시험에서 토큰 발급이 거절됐으면 `data/.live-probe-last.json`을 사람이 지우기 전에는 다시 실행하지 않습니다(틀린 키 반복 방지).
- 직전 시험이 이상 응답(한도 초과 등)으로 멈췄으면 쉬는 시간(기본 10분, `--cooldown-minutes`)이 지나기 전에는 거부하고, `--force`로 넘길 수 있습니다.
- 실행할 때 `JAVA_HOME_21`이 필요하고, 결과는 `data/` 아래(gitignore 대상)에 씁니다.

## 설정 값 가져오기 (Parameter Store, 선택)

메일 설정(SMTP 호스트·포트·계정, 발신·수신 주소) 6개를 AWS Systems Manager Parameter Store(시드니, `ap-southeast-2`)에 두고 `.env`로 가져옵니다. 수집기의 키 5개(`/stock-reading/dev/collector/` 아래)와 브리핑의 LLM 키(`/stock-reading/dev/briefing/anthropic-api-key` → `ANTHROPIC_API_KEY`)도 같은 방식으로 가져옵니다. 두 그룹은 선택이라 없어도 메일 설정 반영은 막지 않으며, LLM 키가 없으면 브리핑은 규칙 기반 리포트를 만듭니다. 서비스는 계속 환경변수만 읽습니다.

```bash
# 읽을 값 확인 (파일은 쓰지 않고 키 이름만 출력)
python3 scripts/load_config.py --dry-run
# .env 반영 (대상 6개 키만 갱신, 나머지는 그대로)
python3 scripts/load_config.py
# 한 그룹만 가져오기 (예: 뉴스 검증에 필요한 collector 키만, 메일 설정은 가져오지 않음)
python3 scripts/load_config.py --only collector
# .env 가 Parameter Store 와 같은지 비교 (파일은 쓰지 않고 값도 출력하지 않음)
python3 scripts/load_config.py --only collector --check
# 브리핑의 LLM 키만 가져오기
python3 scripts/load_config.py --only briefing
```

- `--check`는 이름별로 `같음`, `다름`, `없음`과 양쪽 길이만 보여 주고, 어긋남이 있으면 종료 코드 1을 돌려줍니다. 로더가 관리하지 않는 앱키·시크릿 이름이 `.env`에 남아 있으면 경고합니다. `.env`는 복사본이라 원본과 어긋날 수 있으니 실서버를 호출하기 전에 확인합니다.

- 값은 어디에도 출력하지 않고, 6개가 모두 있을 때만 `.env`를 씁니다(`--only collector`일 때는 메일 설정을 확인하지 않습니다).
- `NOTIFIER_SEND_ENABLED`는 가져오지 않습니다. 실수로 발송이 켜지지 않도록 사람이 `.env`에서만 바꿉니다.
- 자격증명은 `AWS_PROFILE` 환경변수나 `--profile` 옵션으로 지정합니다. 지역은 기본이 시드니이고 `--region`으로 바꿀 수 있습니다.
- 읽기 전용 IAM 자격증명이 필요합니다: [`infra/iam/README.md`](../infra/iam/README.md)

## 테스트

| 대상 | 명령 | 개수 |
|---|---|---|
| Java (수집기, 시세처리기, 발송기, 뉴스 저장) | `./gradlew cleanTest test --no-build-cache` | 764 (실서버 확인·실험용 5개, 부하 시험 1개, 시세처리기 DB 통합 16개, 뉴스 저장 DB 통합 7개는 건너뜀) |
| Python (브리핑) | `services/briefing/.venv/bin/python -m pytest services/briefing` | 223 |
| Python (설정 로더, 가설 검증 도구) | `services/briefing/.venv/bin/python -m pytest -c scripts/pytest.ini scripts/tests` | 72 |

`--no-build-cache`가 필요한 이유: 빌드 캐시가 켜져 있으면 입력이 같을 때 테스트를 실행하지 않고 이전 결과를 복원합니다.

시세처리기의 DB 통합 테스트(`*IntegrationTest`)는 `MARKET_IT_DB_URL`, `MARKET_IT_DB_USER`, `MARKET_IT_DB_PASSWORD`가 모두 있을 때만 실행됩니다. 마이그레이션을 적용할 비어 있는 새 DB가 필요하므로 개발용 DB와 분리된 임시 컨테이너를 쓰고, 끝나면 지웁니다.

```bash
# 임시 Postgres (호스트 포트는 비어 있는 값을 자동으로 받고, 비밀번호는 일회용 값을 쓴다)
docker run -d --name market-it-pg -e POSTGRES_PASSWORD=change-me -e POSTGRES_DB=demo \
  -p 127.0.0.1::5432 pgvector/pgvector:0.8.7-pg17-trixie
PORT="$(docker port market-it-pg 5432/tcp | head -1 | sed 's/.*://')"   # 준비될 때까지 몇 초 기다린다
MARKET_IT_DB_URL="jdbc:postgresql://127.0.0.1:$PORT/demo" MARKET_IT_DB_USER=postgres \
  MARKET_IT_DB_PASSWORD=change-me \
  ./gradlew :services:market:cleanTest :services:market:test --tests '*IntegrationTest' --no-build-cache
docker rm -f market-it-pg
```

뉴스 저장의 DB 통합 테스트도 같은 방식이며 환경변수 이름만 `NEWS_IT_DB_URL`, `NEWS_IT_DB_USER`, `NEWS_IT_DB_PASSWORD`이고 대상은 `:services:news`입니다. 비어 있는 새 DB가 필요하므로 시세처리기와 같은 컨테이너를 이어 쓰지 말고 따로 띄웁니다.

## 실행 기록 (테스트 기록)

여러 검색어를 반복 수집하는 실행의 사양과 결과는 [`docs/test-history/`](test-history/README.md)에 남깁니다. 고정 입력 시험에는 가짜 검색 서버 `scripts/fake-news-search.py`를 씁니다. 시세 조회의 종목 수별 부하는 `QuoteFixedLoadTest`(`QUOTE_LOAD=1`일 때만 실행, 실행 방법은 [S03 사양](test-history/S03-quote-fixed-load/scenario.md))로 쟀습니다.

## 배포 전에 정할 것

지금은 로컬 실행을 전제로 미뤄 둔 결정입니다. 배포 전에 정합니다.

- **브리핑 DB 마이그레이션 방식:** 실패 기록 테이블은 마이그레이션 도구 없이 시작할 때 `CREATE TABLE IF NOT EXISTS`로 만들고, 모양이 바뀌면 로컬 DB를 지우고 다시 만듭니다. 데이터를 지울 수 없게 되기 전에 Java 서비스처럼 버전 관리되는 마이그레이션을 쓸지, Python 쪽 도구를 쓸지 정합니다.
- **변동 알림의 읽은 위치를 언제 확정할지:** 지금은 `market.move`를 읽는 즉시 확정해, 브리핑이 재시작되면 처리 중이던 변동이 기록 없이 사라집니다. 처리를 끝낸 뒤 확정하도록 바꿀지(재시작 후 다시 처리하므로 중복 리포트를 막는 방법도 함께 필요) 정합니다.
- **장중 수집을 켤지:** 장중 수집은 기본 꺼짐입니다. 켜기 전에 장중에 실제 호출로 응답 모양과 호출 상한 여유를 확인합니다.
- **휴장일 판정:** 마감 후·장 시작 전·장중 수집 모두 휴장일을 판정하지 않아 휴장일에도 tick을 발행합니다. 판정할지와 휴장일 목록을 어디서 받을지 정합니다.
- **같은 종목의 중복 변동 리포트를 줄이는 방식:** 지금은 같은 종목의 변동이 짧은 간격으로 여러 번 와도 이벤트마다 리포트를 만듭니다. 묶어서 한 번만 보낼지, 일정 시간 안의 변동은 건너뛸지 정합니다.

## 후속

- **보유 종목과 뉴스 검색어 테이블:** 지금은 ETF별 미국 종목과 뉴스 검색어를 설정(`ETF_US_HOLDINGS`, `NEWS_SEARCH_QUERIES`)으로 줍니다. 보유 종목 테이블(시세처리기)과 종목에 연결된 검색어 테이블(news)로 옮깁니다.
- **검색어와 태그 개선:** 태그는 검색어 설정에서 정해져 저장 때 고정됩니다. 다른 종목 태그지만 관련 있는 기사를 근거에 넣는 방법과 미국 종목 검색어를 정합니다. 당장은 검색어 설정에 관련 종목 태그를 함께 적어 운영합니다.
- **LLM 2단계:** 1단계 LLM이 모은 자료의 중복을 없애고 거른 뒤, 2단계 LLM이 분석해 리포트를 씁니다.

- **외부 접속을 받는 기능:** 브리핑의 Kafka 수신·발행은 "시작"과 "멈추기"로 부를 수 있게 나뉘어 있어 다른 프로그램(예: 어드민 화면)이 같은 실행부를 부를 수 있습니다. 어드민 화면이나 웹 서버는 아직 없습니다. 이런 화면처럼 외부 접속을 받는 기능을 만들 때는 [ADR-007](adr/007-outbound-only.md)(외부 접속을 받지 않는다)을 먼저 갱신하고, 접속 경로와 인증 방식을 정해야 합니다.

## 보안

- 비밀값(`.env`, 토큰, 키)과 개인 정보(보유 종목·금액·계좌)는 저장소에 올리지 않습니다.
- 커밋 전 `gitleaks`가 자동으로 검사합니다: `pre-commit install`
