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
# 브리핑, 발송기는 아래 참고
```

## 브리핑 서비스 (Python)

규칙 기반이며 LLM을 쓰지 않습니다. 샘플 뉴스(파일)에서 근거 후보를 고르고, **변동 시각 이후에 나온 뉴스와 24시간보다 오래된 뉴스는 쓰지 않습니다**(테스트로 검증). 원인을 단정하지 않고 "근거 후보"를 나열합니다.

```bash
# 최초 1회: 프로젝트 전용 가상환경 (시스템 파이썬은 건드리지 않음)
python3 -m venv services/briefing/.venv
services/briefing/.venv/bin/python -m pip install -r services/briefing/requirements.txt

# 테스트
services/briefing/.venv/bin/python -m pytest services/briefing

# 실행 (저장소 루트에서)
PYTHONPATH=services/briefing/src services/briefing/.venv/bin/python -m briefing.main
```

## 이메일 발송기 (`notifier`)

`briefing.generated` → 이메일. **기본은 드라이런**(로그만 남기고 아무것도 보내지 않음)이며, 켜려면 `.env`에 아래를 채웁니다.

| 설정 | 설명 |
|---|---|
| `NOTIFIER_SEND_ENABLED` | `true`일 때만 실제 발송. 기본 `false` |
| `NOTIFIER_SENDER` / `NOTIFIER_RECIPIENT` | 발신/수신 주소. **개인정보이므로 `.env`에만** 둡니다. SES 샌드박스에서는 둘 다 SES에서 인증된 주소여야 함 |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USERNAME` / `SMTP_PASSWORD` | SES 콘솔의 "SMTP settings"에서 만든 전용 자격증명 (포트 587, STARTTLS) |
| `NOTIFIER_MAX_PER_HOUR` | 시간당 발송 한도(기본 6, 코드 기본값만 사용). 초과분은 보내지 않고 로그만 남김 |
| `NOTIFIER_SUBJECT_PREFIX` | 제목 머리말(기본 `[시뮬레이션]`). 데이터가 가짜인 동안 진짜 투자 정보로 오해하지 않도록 |

안전 규칙(모두 테스트로 검증): 설정이 없으면 드라이런, 발송을 켰는데 주소/호스트가 비어 있으면 **시작 단계에서 실패**, 같은 브리핑은 한 번만 발송, 로그에는 주소를 **마스킹**해서만 출력.

> **`.env`에는 ASCII 문자만 쓰세요.** 스프링이 이 파일을 Latin-1로 읽어 한글 값이 깨집니다. 한글 기본값은 `application.yml`에 있습니다.

```bash
java -jar services/notifier/build/libs/notifier-0.0.1-SNAPSHOT.jar   # 저장소 루트에서
```

## 수집기의 외부 데이터 설정 (`collector`)

수집기에는 공시, 시세, 뉴스 검색 클라이언트가 있습니다. 기본은 가짜 데이터(시뮬레이터)이며, 시세는 `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 외부 API를 씁니다. 공시와 뉴스는 아직 클라이언트까지만 구현되어 있고 이벤트로 발행하지 않습니다.

| 설정 | 설명 |
|---|---|
| `COLLECTOR_MARKET_DATA_SOURCE` | `simulator`(기본) 또는 `broker` |
| `BROKER_BASE_URL` / `BROKER_APP_KEY` / `BROKER_APP_SECRET` | 시세 제공자의 주소와 키. `broker`일 때 필요 |
| `NEWS_SEARCH_BASE_URL` / `NEWS_SEARCH_CLIENT_ID` / `NEWS_SEARCH_CLIENT_SECRET` | 뉴스 검색 제공자의 주소와 키 |
| `NEWS_SEARCH_ID_HEADER` / `NEWS_SEARCH_SECRET_HEADER` | 제공자가 정한 인증 헤더 이름 |
| `NEWS_SEARCH_QUERIES` | `검색어=종목코드,종목코드;검색어2=*` 형식. 관심 종목이 드러나므로 `.env`에만 둡니다 |
| `NEWS_SEARCH_CALL_INTERVAL_MS` | 검색어를 연달아 호출할 때 호출 사이의 대기(밀리초, 기본 60). 0이면 기다리지 않으며, 짧으면 제공자가 429로 거절할 수 있습니다([측정 기록](test-history/S01-real-api/call-interval.md)) |
| `DART_API_KEY` | 공시 API 키 |
| `COLLECTOR_INSIGHT_SCHEDULE_ENABLED` | `true`일 때만 장 마감 후 조회 tick(`collect-post-close`)을 발행합니다(기본 `false`). 휴장일은 판정하지 않아 휴장일에도 발행됩니다 |
| `COLLECTOR_INSIGHT_CRON` | 마감 후 조회 시각. 한국 시간 기준 cron(초 분 시 일 월 요일, 기본 `0 30 16 * * MON-FRI`) |

마감 후 조회(일봉, 세계지수, 환율종합, 시장종합)는 `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 동작하며, 응답 원문을 `market.insight` 이벤트로 발행합니다([ADR-018](adr/018-post-close-insight-event.md)). 일봉의 시장구분과 조회 건수, 세계지수의 대륙 목록은 `application.yml`의 `collector.insight.*`에 기본값이 있습니다.

키 값(`*_KEY`, `*_SECRET`, `DART_API_KEY`)은 아래 Parameter Store에서 가져옵니다. 주소와 헤더 이름은 비밀이 아니지만 코드에 두지 않고 `.env`에 직접 적습니다.

### 시세 API 연동 확인

시세는 조회만 씁니다(ADR-007). `.env`에는 다음을 둡니다.

- `BROKER_BASE_URL`: 시세 제공자가 안내한 운영 주소
- `BROKER_APP_KEY`, `BROKER_APP_SECRET`: Parameter Store에서 가져옵니다.
- `COLLECTOR_MARKET_DATA_SOURCE=broker`

실제 서버 확인용 수동 테스트: `BrokerLiveProbeTest`는 평소 건너뛰며 `BROKER_PROBE=1`일 때만 토큰 1회와 종목별 현재가를 호출합니다. 요약은 `BROKER_PROBE_OUT`, 응답 원문은 `BROKER_PROBE_RAW_DIR`에 쓰고 키와 토큰 값은 남기지 않습니다. 종목은 `BROKER_PROBE_SYMBOLS`(쉼표 구분)로 지정하며 코드에는 두지 않습니다. 실제 시세 응답은 저장소에 올리지 않으므로 `BROKER_PROBE_RAW_DIR`은 `data/`처럼 gitignore 대상 경로로 지정합니다. 실제 운영 서버를 호출하므로 필요할 때 최소한으로 실행합니다.

장 마감 후 조회 API(일봉, 세계지수, 환율종합, 시장종합)의 응답 형식을 보는 `PostCloseLiveProbeTest`는 `BROKER_POSTCLOSE_PROBE=1`일 때만 토큰 1회와 API당 1회씩을 호출합니다. 종목은 `BROKER_PROBE_SYMBOLS`의 첫 번째만 쓰고, 조회 건수(`BROKER_POSTCLOSE_PROBE_COUNT`, 기본 30), 시장구분(`BROKER_POSTCLOSE_PROBE_MARKET`, 기본 `0`), 세계지수 대륙구분(`BROKER_POSTCLOSE_PROBE_CONTINENT`, 기본 `C`, 쉼표로 여러 개 가능하며 대륙마다 한 번씩 호출)은 바꿔 볼 수 있습니다. 호출할 API는 `BROKER_POSTCLOSE_PROBE_APIS`(`daily`, `world`, `exchange`, `overview`를 쉼표로, 기본 전부)로 고를 수 있고, `price`를 고르면 현재가를 거래소 구분(`BROKER_POSTCLOSE_PROBE_EXCHANGES`, 기본 `0,1,2`)마다 한 번씩 호출합니다(일봉 거래량이 어느 거래소 기준인지 맞춰 볼 때 씁니다, 기본 대상에는 포함되지 않음). 요약과 원문 저장 위치는 위 `BrokerLiveProbeTest`와 같습니다.

호출 속도 실험용 `BrokerRateProbeTest`는 `BROKER_RATE_PROBE=1`일 때만 한 종목의 현재가를 `BROKER_RATE_PROBE_RATES`(초당 호출 수, 쉼표 구분, 기본 `1,2,3`)로 `BROKER_RATE_PROBE_PHASE_SECONDS`(기본 30)초씩 단계별로 올리며 호출합니다. 정상 응답의 `processCode`를 `BROKER_RATE_PROBE_OK_CODE`로 받아, 이와 다른 응답이 하나라도 나오면 즉시 멈추고, 단계별 횟수·지연과 처음 이상 응답의 헤더만 `BROKER_PROBE_OUT`에 씁니다.

## 설정 값 가져오기 (Parameter Store, 선택)

메일 설정(SMTP 호스트·포트·계정, 발신·수신 주소) 6개를 AWS Systems Manager Parameter Store(시드니, `ap-southeast-2`)에 두고 `.env`로 가져옵니다. 수집기의 키 5개(`/stock-reading/dev/collector/` 아래)도 같은 방식으로 가져오며, 이 그룹은 선택이라 없어도 메일 설정 반영은 막지 않습니다. 서비스는 계속 환경변수만 읽습니다([ADR-015](adr/015-parameter-store.md)).

```bash
# 읽을 값 확인 (파일은 쓰지 않고 키 이름만 출력)
python3 scripts/load_config.py --dry-run
# .env 반영 (대상 6개 키만 갱신, 나머지는 그대로)
python3 scripts/load_config.py
# 한 그룹만 가져오기 (예: 뉴스 검증에 필요한 collector 키만, 메일 설정은 가져오지 않음)
python3 scripts/load_config.py --only collector
```

- 값은 어디에도 출력하지 않고, 6개가 모두 있을 때만 `.env`를 씁니다(`--only collector`일 때는 메일 설정을 확인하지 않습니다).
- `NOTIFIER_SEND_ENABLED`는 가져오지 않습니다. 실수로 발송이 켜지지 않도록 사람이 `.env`에서만 바꿉니다.
- 자격증명은 `AWS_PROFILE` 환경변수나 `--profile` 옵션으로 지정합니다. 지역은 기본이 시드니이고 `--region`으로 바꿀 수 있습니다.
- 읽기 전용 IAM 자격증명이 필요합니다: [`infra/iam/README.md`](../infra/iam/README.md)

## 테스트

| 대상 | 명령 | 개수 |
|---|---|---|
| Java (수집기, 시세처리기, 발송기, 뉴스 저장) | `./gradlew cleanTest test --no-build-cache` | 314 (실서버 확인·실험용 3개와 시세처리기 DB 통합 6개는 건너뜀) |
| Python (브리핑) | `services/briefing/.venv/bin/python -m pytest services/briefing` | 26 |
| Python (설정 로더, 가설 검증 도구) | `services/briefing/.venv/bin/python -m pytest -c scripts/pytest.ini scripts/tests` | 44 |

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

## 실행 기록 (테스트 기록)

여러 검색어를 반복 수집하는 실행의 사양과 결과는 [`docs/test-history/`](test-history/README.md)에 남깁니다. 고정 입력 시험에는 가짜 검색 서버 `scripts/fake-news-search.py`를 씁니다.

## 보안

- 비밀값(`.env`, 토큰, 키)과 개인 정보(보유 종목·금액·계좌)는 저장소에 올리지 않습니다.
- 커밋 전 `gitleaks`가 자동으로 검사합니다: `pre-commit install`
