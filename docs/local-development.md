# 로컬 개발

## 준비

- JDK 21 (이 프로젝트 전용으로 지정, 기본 Java를 바꾸지 않아도 됨)
- Docker Desktop (실행 중이어야 함)
- Gradle은 래퍼(`./gradlew`)를 사용
- 새로 클론했거나 새 작업 폴더를 만들었으면 `scripts/setup-dev.sh`를 먼저 돌립니다. `.env`와 `JAVA_HOME_21`(JDK 21)을 확인해 알려 주고, 브리핑 venv 만들기·의존성 설치와 차트 글꼴 내려받기는 직접 합니다. `--check`는 아무것도 만들지 않고 상태만 봅니다. 아래 명령은 이 스크립트가 하는 일을 하나씩 한 것입니다.

```bash
# JDK 21 위치를 환경변수로 지정 (Homebrew 예시)
export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
export JAVA_HOME_21=$JAVA_HOME

# 빌드 + 테스트 (Docker 불필요)
./gradlew test bootJar

# 인프라 기동 (Kafka, PostgreSQL)
cp .env.example .env        # 값 채우기 (.env는 커밋되지 않음)
docker compose -f infra/compose/docker-compose.yml --env-file .env up -d

# 서비스 실행 (저장소 루트에서). 시작 순서는 상관없음(토픽을 각 서비스가 선언함). 다만 새 버전을 배포할 때는 뉴스를 먼저(V6 마이그레이션 적용) 올리고 수집기를 나중에 올림(수집기가 먼저면 미국 티커 태그 기사를 뉴스가 저장하지 못해 유실)
java -jar services/collector/build/libs/collector-0.0.1-SNAPSHOT.jar
java -jar services/market/build/libs/market-0.0.1-SNAPSHOT.jar
java -jar services/news/build/libs/news-0.0.1-SNAPSHOT.jar
# 브리핑, 발송기는 아래 참고
```

시세처리기(market)는 `market.snapshot`을 `price_snapshot`(`as_of` 기준 날짜별 파티션, 응답 원문은 jsonb `raw`)에, `market.snapshot.failed`를 `quote_failure`에 저장합니다. 파티션은 저장 직전에 없으면 만들며(Asia/Seoul 자정 경계), 테이블 구조는 `services/market/src/main/resources/db/migration/`의 마이그레이션이 원본입니다. 원문 보존 기간은 아직 정하지 않았습니다.

뉴스 저장(news)은 `news.collected`와 함께 공시 이벤트 `filing.collected`를 별도 consumer group(`news-filings`)으로 구독해 `filing`(접수번호 유니크)과 `filing_ticker`에 저장합니다. 같은 공시가 다른 이벤트로 다시 오면 새로 연결된 티커만 더하고, 재시도 후에도 실패한 이벤트는 뉴스와 같은 `failed_event`에 남깁니다. 종목·기간 조회는 `FilingQuery`(티커, 시작·끝 시각, 기준 시각)이며 기준 시각보다 늦게 접수된 공시는 돌려주지 않습니다(같으면 포함). 해외 종목의 직전 미국 정규장 마감 등락률은 시세처리기의 `OverseasCloseRateQuery`(티커, 기준 시각)가 저장된 해외 일봉 원문에서 읽어 고른 일자와 등락율 원문(`up_dwn_r_p2`)을 돌려줍니다. 고르는 규칙과 이유는 `OverseasCloseRateQuery`의 주석에 있습니다. 공시 조회는 news의 `news.filings.requested` 응답이, 해외 등락률은 시세처리기의 `market.context.requested` 응답이 씁니다(아래 브리핑 서비스).

시세처리기는 스냅샷마다 급변동을 감지해 `market.move`를 발행합니다. 유형 1(`window`)은 최근 N분 이력의 가장 오래된 스냅샷 대비, 유형 2(`prevClose`)는 응답 원문의 전일 종가(후보 세 칸의 다수결) 대비 변동률이 임계값 이상일 때이며, 발행하려면 조건이 받은 스냅샷 기준으로 연속 `market.confirm-count`건(기본 3) 이어져야 합니다(두 유형 공통, 같은 방향만 이어서 세고 빠진 초는 세지 않으며, 판단할 수 없는 스냅샷이 끼면 그 유형은 처음부터 다시 세고, 재시작해도 처음부터 셉니다). 연속 확인을 받지 못한 튄 점(앞뒤 값과 모두 다른 짧은 구간)과 첫 체결 전 응답은 유형 1의 기준값으로 쓰지 않습니다. 유형별로는 한 번 발행한 뒤 마지막 발행 가격에서 임계값만큼 더 움직일 때만 다시 발행하고, 종목 단위로는 유형과 상관없이 마지막 발행 뒤 `market.republish-interval-minutes`(기본 30분) 안에는 발행하지 않습니다. 단 마지막 발행 가격 대비 `market.republish-exception-percent`(기본 2%) 이상 더 움직이면 발행합니다. 간격 때문에 내지 않은 변동은 로그에 한 번 남기고 그 시점의 가격으로 그 유형의 상태에 남겨, 조건이 풀리기 전에는 간격이 끝나도 새 움직임(그 유형 임계값 이상) 없이는 발행하지 않습니다. 간격은 `market.move` 발행 기준이라 브리핑이 메일을 보냈는지와 상관없습니다(상태는 `move_state` 테이블에 둡니다. 종목 단위 마지막 발행은 `detection_type`이 `lastPublish`인 행입니다). 설정은 `services/market/src/main/resources/application.yml`의 `market.*`이고 환경변수로 덮어쓸 수 있습니다.

기본값은 `application.yml`이 원본입니다.

| 설정 키 | 설명 |
|---|---|
| `market.window-minutes` | 유형 1이 비교할 최근 이력의 길이(분) |
| `market.window-threshold-percent` | 유형 1 임계값(%) |
| `market.prev-close-fields` | 유형 2가 원문에서 읽을 전일 종가 후보 필드들(점으로 중첩 경로, 쉼표로 구분). 기본 칸은 `services/market/src/main/resources/application.yml`에 있습니다. 응답의 한 칸이 깨져 올 수 있어 스냅샷마다 모든 칸을 읽고, 읽을 수 있는 값 중 설정한 칸 수의 절반을 넘는 칸이 같은 값일 때만 그 값을 전일 종가로 씁니다. 다수가 없으면 그 스냅샷은 유형 2를 판단하지 않습니다. 바꾸려면 `MARKET_PREV_CLOSE_FIELDS` 환경변수에 칸 경로를 쉼표로 주고, 빈 값으로 주면 유형 2를 끄며 시작할 때 경고를 한 번 남깁니다. 유형 1과 유형 2는 따로 평가하고 연속 확인도 따로 세지만, 종목 단위 재발행 간격 때문에 같은 움직임에서는 먼저 확인된 한 유형만 발행되고, 다른 유형은 조건이 풀리기 전까지 새 움직임이 없으면 간격이 끝나도 발행되지 않습니다 |
| `market.open-price-fields` | 아침 판의 개장 흐름이 시세 응답 원문에서 시가를 읽을 칸(점으로 중첩 경로, 쉼표로 구분, 앞에서부터 처음 읽히는 값). 기본 칸은 `application.yml`에 있습니다. 읽히는 값이 없거나(없음·숫자 아님·0·소수) 비어 있으면 장 시작 뒤 첫 스냅샷 가격을 시가로 쓰고 `openSource`에 `first-snapshot`으로 표시합니다. 바꾸려면 `MARKET_OPEN_PRICE_FIELDS` 환경변수에 칸 경로를 쉼표로 줍니다. 시가가 장 시작 뒤 첫 스냅샷 가격과 30% 넘게 다르면(가격제한폭, 깨진 칸) 시가를 버리고 `first-snapshot`으로 처리합니다. 비율은 응답의 등락률 칸을 믿지 않고 코드가 가격으로 계산합니다 |
| `market.daily.open-flow-max-window` | 개장 흐름이 판 기준 시각부터 거슬러 읽는 시세 구간의 안전 상한(기본 30분). 요청 구간이 이보다 길면 이 기간까지만 읽습니다 |
| `market.prev-close-threshold-percent` | 유형 2 임계값(%) |
| `market.confirm-count` | 두 유형 모두 조건이 받은 스냅샷 기준으로 이 건수만큼 연속 이어질 때만 발행합니다. 1이면 바로 발행합니다 |
| `market.first-trade-fields` | 첫 체결 전 응답을 알아보는 원문 칸(점으로 중첩 경로, 쉼표로 구분). 기본은 누적 거래량과 시가 칸(`application.yml`)이며 모든 칸 값이 0일 때(한쪽만 0이면 아님)만 그 스냅샷을 유형 1의 기준값에서 뺍니다. 바꾸려면 `MARKET_FIRST_TRADE_FIELDS`에 쉼표로 주고, 빈 값이면 거르지 않습니다 |
| `market.republish-interval-minutes` | 한 종목의 `market.move`를 발행한 뒤 유형과 상관없이 같은 종목을 다시 발행하지 않는 시간(분). 0이면 끕니다 |
| `market.republish-exception-percent` | 재발행 간격 안이라도 마지막 발행 가격 대비 이 값(%) 이상 더 움직이면 발행합니다 |

시세처리기는 브리핑의 배경 자료 요청(`market.context.requested`)에 답합니다. 종류마다(환율종합, 시장종합, 세계지수 대륙별, 그 종목의 국내 일봉) 받은 시각이 기준 시각 이전인 것 중 최신 원문 1건을 주고, ETF에 담긴 미국 종목 목록과 종목마다 직전 정규장 등락률을 함께 줍니다. 세계지수는 장이 끝난 지수의 응답 등락률이 0.00으로 오므로 쓰지 않고, 같은 지수의 영업일이 더 이른 직전 저장 행(요청 범위가 아니라 찾는 기간 안에서 고릅니다) 종가와 비교해 계산한 지수별 등락률(`indexRates`)을 함께 줍니다. 아래 값은 안전 상한이며 요청값의 원본은 브리핑 `config.py`입니다. 요청이 더 크면 이 값까지만 줍니다.

| 설정 키 | 설명 |
|---|---|
| `market.members.url-template` | ETF 구성 종목 자동 조회의 요청 주소 틀(환경변수 `MEMBERS_URL_TEMPLATE`). `{fundId}`와 `{date}`(yyyy.MM.dd) 자리가 있습니다. 비어 있으면 자동 조회를 하지 않고 `etf_stock_map`을 직접 고칩니다. ETF별 출처 번호는 환경변수가 아니라 `etf.constituent_source_id`에 있습니다. 아래 "ETF 구성 종목 자동 조회"를 봅니다 |
| `market.members.user-agent` | 요청마다 보내는 식별 문자열(`MEMBERS_USER_AGENT`, ASCII만) |
| `market.members.top-count`, `fetch-day`, `call-interval`, `timeout` | 담을 상위 개수(기본 10), 평소 조회 요일(기본 월요일), 출처를 연달아 부르는 사이의 쉼(기본 2초), 요청 제한 시간(기본 10초) |
| `market.context.max-age` | 기준 시각보다 이 기간 넘게 이른 자료는 없음으로 봅니다 |
| `market.context.max-response-bytes` | 원문 1건의 최대 크기(UTF-8 바이트). 넘으면 원문을 빼고 너무 큼으로 표시합니다 |
| `market.context.max-tickers` | 응답에 담을 미국 종목의 최대 개수 |
| `market.context.world-index-continents` | 배경 자료로 줄 세계지수 대륙구분. 수집기의 마감 후 수집 대륙과 맞춥니다 |
| `market.context.world-index-prev-lookback` | 세계지수 직전 영업일 행을 찾는 기간. 기준 시각보다 이 기간까지 이르게 받은 행에서 찾으며 요청 범위와는 따로입니다 |

시세처리기는 데일리 브리핑의 판 자료 요청(`market.daily.requested`)에도 한 번에 답합니다(응답 `market.daily.responded`, `DailyRequestListener`). 응답은 `schemaVersion` 2입니다. ETF 목록은 사용 중인 멤버의 포트폴리오(`personal.active_portfolio`)에 담긴 국내 ETF입니다(구성 종목은 제외, 같은 ETF를 여러 멤버가 담아도 한 번만). 보유·관심 구분은 멤버마다 달라 응답에 싣지 않습니다(스키마의 `relation` 칸도 없어졌고, 브리핑이 멤버마다 `personal`에서 읽습니다). 받은 시각이 요청 범위 안인 일봉이 있으면 일봉 원문의 일자가 가장 최근인 레코드의 하루 등락(원문 값 그대로)을 줍니다. 일봉이 없거나 읽지 못하면 그 칸들을 null로 두고 `missingReason`(`no-daily-price-in-range`, `daily-price-unreadable`)에 이유를 싣되, 담긴 미국 종목과 ETF 정보는 그대로 줍니다. 그 밖에 직전 정규장 등락률, 시장 배경(환율종합, 시장종합, 세계지수와 계산된 지수 등락률)을 줍니다. 받은 시각 하한, 원문 크기, 미국 종목 수, 세계지수 대륙은 위 `market.context.*`를 함께 쓰고, ETF 수의 안전 상한만 따로 둡니다.

요청에 선택 칸 `includeOperations`가 `true`일 때만 응답에 선택 칸 `operations`(시세처리기 운영 상태)를 더합니다. 수집 종류 4개(장 시작 전, 1초 현재가, 장중, 장 마감 후)마다 범위 안에서 처음·마지막으로 받은 시각과 건수(받은 것이 없으면 시각 null, 건수 0), 시세 실패 종류별 건수(0건 종류도 포함하고 밀린 tick 건너뜀은 호출 상한 초과와 더 새 tick에 밀림으로 나눔), 처리하지 못하고 버린 메시지의 토픽별 건수입니다. 조회 자료 테이블에는 어느 수집이 받았는지 칸이 없어 받은 시각의 한국 시간대로 가립니다(09:00 전 장 시작 전, 16:00 전 장중, 이후 장 마감 후, 해외 일봉은 장 시작 전, 국내 일봉은 장 마감 후). 1초 현재가 건수는 시세 시각(`as_of`)이 받은 시각 앞뒤 1시간 안인 것만 셉니다. 운영 상태 조회가 실패하면 경고 로그만 남기고 `operations` 칸 없이 나머지 응답을 보냅니다. 종목코드는 담지 않습니다.

요청에 선택 칸 `openFlowSince`(시각)를 보내면(아침 판만, 판 시각이 장 시작 이후일 때) 응답의 ETF마다 선택 칸 `openFlow`(아침 판용 개장 흐름)를 더합니다. 저장한 시세(`price_snapshot`) 중 `openFlowSince` 이상 기준 시각 이하이고 받은 시각도 기준 시각 이하인 것만 읽어 첫·마지막 스냅샷을 고르고, 전일 종가(후보 칸 다수결)·시가(원문 시가 칸, 없으면 첫 스냅샷 가격)·`openRate`(전일 종가 대비 시가)·`flowRate`(첫 시세 대비 마지막 시세)·`rateVsPrevClose`(전일 종가 대비 마지막 시세)를 코드가 계산해 문자열로 줍니다. 범위 안 시세가 없으면 `status`가 `no-snapshot`이고 나머지는 null입니다. 한 ETF의 시세 조회가 실패하면 그 ETF만 칸이 없고 나머지 응답은 그대로 갑니다. 이 칸은 `market.daily.responded` v2에 더한 선택 칸이라 칸이 없는 이전 응답과 호환됩니다.

| 설정 키 | 설명 |
|---|---|
| `market.daily.max-etfs` | 판 자료 응답에 담을 보유 ETF의 최대 개수. 넘으면 잘림으로 표시합니다 |

이전 키 `market.move-threshold-percent`는 없어졌습니다. 마이그레이션(`services/market/src/main/resources/db/migration/`)은 서비스 기동 때 Flyway가 적용하며, 단위 테스트는 저장소를 대역으로 쓰므로 SQL은 로컬 Postgres에서 따로 확인해야 합니다.

## 브리핑 서비스 (Python)

같은 종목의 `market.move`를 모아(아래 "같은 종목의 변동 묶음") 묶음마다 근거를 모아 리포트를 만들고 `briefing.generated`로 발행합니다. 아래 1~8번은 묶음 하나가 만든 작업 하나의 흐름이며, 변동 시각 T는 묶음에서 가장 늦은 변동의 `asOf`입니다.

1. 그 변동에 쓸 수 있는 뉴스 수집 완료(`collection.completed`)를 기다립니다. 쓸 수 있는 완료는 `jobType`이 `collect-news`이고, `startedAt`이 변동 시각 T + Δ 이후이며, 그 종목과 `*`가 `incompleteSymbols`에 없는 것입니다. 변동보다 먼저 도착한 최근 완료도 메모리에 보관했다가 확인합니다.
   - 기다리는 동안 변동 시각 T + `early_research_delay`(5분)가 되면(이미 지났으면 바로) 배경 자료(`market.context.requested`)를 먼저 요청해 ETF 이름을 알아내고, 이름이 있으면 ETF 이름과 비중 순위가 높은 구성 종목 이름으로 만든 검색어(최대 `max_research_queries`개)로 재검색(`news.research.requested`, 작업 종류 `move`)을 요청합니다. 그 재검색의 완료 알림(`researchJobId`가 이 작업의 것, 그 종목이 `incompleteSymbols`에 없음)도 정기 수집 완료와 같이 '수집 완료'로 보고, 둘 중 먼저 온 것으로 진행합니다. 먼저 받은 배경 자료는 다시 받지 않습니다. 이름을 모르거나 배경 자료가 오지 않으면 재검색 없이 정기 수집을 기다립니다. 재검색으로 모은 기사는 그 종목 태그가 붙어 일반 기사 조회에 함께 나옵니다(실제 뉴스 서비스에서 확인하지 못함).
2. 세 요청을 함께 보냅니다: 시세처리기에 시세 이력(`market.history.requested`, T - 5분 ~ T + Δ를 일정 간격으로 솎아서)과 배경 자료(`market.context.requested`)를, news 서비스에 기사(`news.articles.requested`, T - 24시간 ~ T + Δ, 양끝 포함)를 요청합니다. 기사는 그 종목과 시장 전체(`*`) 태그만 돌아오며, 다른 종목 태그 기사를 빼는 것은 news 조회가 보장합니다(브리핑은 다시 검사하지 않음).
3. 배경 자료 응답에 미국 종목 목록이 있으면 그 종목들의 공시(`news.filings.requested`, 접수 시각 T - 72시간 ~ T)를 요청합니다. 공시는 하루 한 번 받으므로 + Δ를 두지 않습니다. 미국 종목이 없으면 공시를 요청하지 않고 해당 없음으로 둡니다.
4. 응답이 제한 시간 안에 없으면 새 순번(`seq`)으로 다시 요청하는 규칙은 모든 요청에 같습니다. 기사는 일정 간격으로 다시 요청해, 연속 두 번 같은 기사 목록(순서 무시)이 오면 안정된 것으로 봅니다. 모든 응답을 받아야 다음으로 넘어갑니다. 실행부는 제한 시간을 보기 전에 이미 도착해 쌓인 메시지를 먼저 읽고(한 번에 최대 50개, 읽는 데 쓰는 시간이 120초를 넘으면 멈추고 tick과 제한 시각 처리로 넘어감), 같은 조회를 다시 보낸 뒤에 처음 보낸 순번의 응답이 와도 같은 라운드의 응답으로 받습니다. 다른 문장의 LLM 호출로 응답을 늦게 읽어도 `articles_no_response`로 잘못 끝나지 않게 하려는 것입니다.
5. 받은 자료의 시각을 다시 검사합니다. 발행·접수 시각이 범위 밖인 기사·공시와, 받은 시각이 T 뒤이거나 기준 기간보다 오래된 배경 자료·해외 등락률은 버리고 경고 로그를 남깁니다(테스트로 검증). 변동마다 종목·티커 없이 작업 번호와 자료 종류별 건수·제외·잘림을 한 줄 수집 내역 로그(`collection job=...`)로 남깁니다.
6. 기사(`articleId`), 공시(접수번호), 해외 등락률(`U1`…), 배경 자료(`B1`…)에 근거 번호를 붙여 LLM에 줍니다. 배경 자료에는 받은 시각과 변동보다 얼마나 전 값인지를 붙이고, 원인으로 단정하지 말라고 지시합니다. 코드가 배경 원문에서 꺼낸 숫자(원/달러 환율, 코스피·코스닥)와 비교 숫자(변동률과 지수 등락률의 차이, 담긴 미국 종목 집계)에도 근거 번호(`F1`…)를 붙입니다. LLM은 원인마다 제목, 구체 문장, 뜻 문장과 각 문장의 근거 번호(`refs`, `meaningRefs`)를 내고, 코드가 형식과 인용 번호를 검증해 빈 문장이나 목록 밖 번호가 있거나 문장(제목·구체 문장·뜻 문장·부족한 점)에 미국 티커(해외 등락률·공시·ETF 구성 종목에 있던 것)가 나오면 거부하고 다시 부릅니다(데일리와 같은 `llm_output.py`의 공통 검사이며, 3번 실패하면 `llm_error`인 것도 같습니다). 응답에 ETF 정보(`etf`)가 있으면 이름·추종 대상·업종과 구성 종목을 비중 순위 순으로 `config.py`의 `max_etf_members`개까지 LLM 입력(`etf`)에 넣고, 지시문에 ETF 정보는 원인으로 단정하지 말고 맥락으로만 쓰라고 적습니다. ETF 정보가 없는 이전 형식 응답도 그대로 처리합니다. 본문은 머리말(종목, 시각, 방향과 변동률, 몇 분 사이 움직였는지) → 원인별 묶음(인용한 자료의 제목·출처·시각·링크 포함) → 확인하지 못한 것(받지 못하거나 잘린 자료) → "투자 조언이 아님" 문구 순이며, 머리말·확인하지 못한 것·면책 문구는 코드가 넣습니다. `generatedBy`는 `llm`입니다. LLM 키가 없으면 원인을 추정하지 않고 비교 사실과 기사·공시를 나열하는 같은 모양의 규칙 기반 리포트를 만듭니다. `generatedBy`는 `rule-based`입니다.
7. `briefing.generated`의 근거에는 자료 종류(`kind`)와 근거 번호(`ref`)를 담습니다. 출처는 제공자 이름 대신 중립 표현을 쓰고, 공시와 해외 등락률의 근거 제목에는 어느 회사 자료인지 알 수 있게 미국 티커를 넣습니다(로그에는 넣지 않습니다).

   - 최종 본문에 인용된 근거 번호가 하나도 없으면(근거 부족 리포트, 근거가 없는 규칙 기반 리포트 포함) `briefing.generated`를 발행하지 않습니다. `job_run`은 `succeeded`·사유 `no_evidence`·결과 이벤트 없음으로 끝나고, `job_log`에 받은 자료 건수·확인하지 못한 것 목록·최종 본문을 남깁니다.
8. LLM은 원인마다 근거가 약한지(`weak`)와 부족한 점(`shortfall`, 한 문장)도 냅니다. 원인 하나가 문장 하나이며, 약하다고 한 문장마다 따로 재검색합니다(`sentence_research.py`). 문장마다 검색어만 내는 LLM을 따로 불러(입력: 그 문장, 부족한 점, ETF 정보, 이미 찾은 기사 제목, 배경 숫자) 검색어(최대 3개, 짧은 낱말 2~3개)를 받고, 미국 종목은 구성 종목의 한글 이름으로, 이름이 없을 때만 티커로 검색하게 지시합니다. 재검색 요청(`news.research.requested`, 작업 종류 `move`, 태그는 그 종목)은 문장마다 따로 보내며 재검색 작업 id는 (작업, 문장 번호, 회차)로 정한 uuid라 문장마다 다릅니다. 여러 문장이 약하면 모두 동시에 진행합니다. 같은 재검색 작업 id(`researchJobId`)의 완료 알림을 대기 상한 안에서 기다린 뒤 그 id로 기사를 조회하고(`news.articles.requested`의 `researchJobId`, 순번은 1부터, 다시 요청·안정 판정 규칙은 4번과 같음), 받은 기사는 5번과 같은 시간 범위로 다시 거르고 기존 기사와 다른 문장이 이미 쓴 기사(같은 `articleId`)는 뺍니다. 남은 기사로 그 문장 하나만 다시 쓰며(약하지 않은 문장은 그대로), 근거 번호 검증·목록 밖 번호 거부·미국 티커 거부는 다시 쓰기에도 그대로 적용합니다. 다시 써도 약하면 그 문장만 한 번 더 찾습니다. 상한(`config.py`의 `CoreSettings`): 문장당 찾기 `research_rounds_per_sentence`(2), 작업 하나의 재검색 요청 합계 `max_research_requests`(10, 넘는 문장은 찾지 않고 근거 약함으로 남김), 작업 하나의 재검색 전체 시간 `research_total_limit`(4분, 지나면 그때의 본문으로 냄. 변동부터 메일 이벤트까지 약 10분 안에 끝내려는 값). 끝내 약한 문장에는 본문에 `근거 약함: <부족한 점>` 줄을 붙입니다(데일리와 같은 모양으로 `llm_output.py`의 `weak_note_line`이 만듭니다). 보고서 전체가 근거 부족(`insufficientEvidence` true)이면 문장이 없으므로 보고서 전체를 대상으로 한 번 찾아 다시 쓰고, 다시 써도 부족하면 근거 부족 리포트를 냅니다. 검색어 LLM 실패, 검색어 없음, 수집기의 하루 상한·검색 실패(`incompleteSymbols`), 완료 알림 시간 초과, 기사 조회 무응답, 거른 뒤 남은 기사 없음, 다시 쓰기 실패이면 그 문장은 그대로 둡니다. 새 실패 사유는 없고 로그와 한 줄 재검색 내역 로그(`research job=... researched=... articles=... result=...`), 작업 기록 테이블에만 남깁니다. 검색어와 종목은 화면 로그에 쓰지 않습니다. LLM 키가 없으면 재검색하지 않습니다.

### 같은 종목의 변동 묶음

같은 종목의 변동이 잇따라 와도 리포트가 여러 통 나가지 않도록, `market.move`는 바로 작업으로 만들지 않고 종목마다 모읍니다(`move_group.py`).

- **조용한 시간(기본 0분):** 마지막 변동을 받은 뒤 같은 종목의 변동이 이 시간 동안 더 오지 않으면 모인 변동으로 급변동 작업 1건을 만듭니다. 0분이면 변동마다 바로 작업을 만듭니다(급변동 메일을 변동 뒤 약 10분 안에 보내려는 값이며, 같은 종목 중복은 시세처리기가 막습니다). 길게 두면 변동이 올 때마다 그 시간을 다시 잽니다.
- **최대 대기(기본 30분):** 변동이 계속 와도 첫 변동을 받은 지 30분이 지나면 그때까지 모인 변동으로 작업 1건을 만들고, 그 뒤에 오는 변동은 새 묶음이 됩니다. 리포트가 끝없이 밀리지 않게 하는 상한입니다.
- 두 값은 `config.py`의 `CoreSettings`(`move_quiet_period`, `move_max_wait`)이며 환경변수로 바꿀 수 없습니다. 시각은 변동을 받은 시각이고, 기준은 변동 시각(`asOf`)이 아닙니다.
- 작업은 묶음에서 **가장 늦은 변동**으로 만듭니다. 작업 id(`job_run.job_id`), 변동 시각 T, 가격, 변동률이 그 변동의 것이고 `job_run.input`도 그 이벤트 원문입니다. 모인 변동 전부는 그 작업의 `job_log`에 `moves-grouped` 줄 하나로 남습니다(`data.moves`에 각 변동의 `eventId`, `asOf`, 가격, 변동률, 받은 시각, `attempts`에 모인 개수).
- 종목마다 따로 묶고, 같은 `eventId`는 한 번만 셉니다(이미 내보낸 묶음의 변동이 다시 와도 새 작업을 만들지 않습니다).
- 모으는 중인 변동은 메모리에만 있습니다. 정상 종료(종료 신호)를 받으면 모으는 중이던 묶음마다 작업 한 줄을 만들어 `interrupted_by_restart` 실패로 끝내고 `moves-grouped` 줄에 모인 변동을 남깁니다(리포트·LLM 호출·이벤트 발행 없음, 기록이 실패해도 종료는 막지 않음). 강제 종료(`kill -9`, 전원 꺼짐)만 흔적 없이 사라집니다. 읽은 위치를 처리 전에 확정하는 기존 정책 때문입니다. 작업이 만들어진 뒤에 재시작되면 아래 "재시작으로 끊긴 작업"을 따릅니다.
- **장 시작~아침 판 시작 사이 급변동은 아침 판에 합칩니다(`move_merge.py`):** 변동 시각(`asOf`)이 평일 `DailySettings.market_open`(09:00) 이상 `morning_merge_end`(09:10) 미만이면 급변동 리포트·메일을 따로 만들지 않고 모아 둡니다(작업은 진행 중으로 시작). 아침 판이 이 변동의 시세를 개장 흐름으로 담기 때문입니다. 아침 판이 나가고 그 변동의 ETF 개장 흐름이 판에 실제로 담겼으며(`openFlow`가 `present`이고 범위 안이라 버려지지 않음) 변동 시각이 판이 읽은 개장 흐름 구간(판 시각 30분 전과 09:00 중 늦은 쪽부터) 안이면 모아 둔 변동의 작업을 성공으로 끝내되 `failure_reason`을 `merged_into_morning`으로 남기고(리포트·발행 없음, 마감 후 판의 그날 급변동 줄은 `· 아침 판에 합침`, 서버 상태 브리핑 줄은 성공과 따로 `아침 판에 합침 N`으로 셈), 아침 판이 실패하거나(받을 멤버를 읽지 못함, 멤버 판 전부 실패, 작업 기록 저장 실패) 그 ETF의 개장 흐름이 판에 없으면 모아 둔 변동을 그때 급변동 리포트 흐름으로 넘겨 알림이 사라지지 않게 합니다. 합침 경로도 본 `eventId`를 기억해(`remembered_move_ids`, 묶음기와 같은 1만 개) 합침으로 끝난 변동이 다시 와도 리포트로 나가지 않습니다. 합침 구간이 끝난 뒤 `morning_merge_grace`(기본 30분)가 지나도 아침 판 결과가 없으면(예: 수집기가 tick을 보내지 않음) 같은 식으로 넘기되, 그날 아침 판이 아직 진행 중이면 넘기지 않고 판이 끝나기를 기다립니다(늦게 성공한 판과 급변동 리포트가 둘 다 나가지 않게). **합침 켜기:** 브리핑의 `DailySettings.morning_merge_enabled`는 수집기의 아침 판 tick을 켜는 값과 같은 환경변수 `COLLECTOR_DAILY_BRIEFING_ENABLED`(`true`일 때만 켜짐)로 정해집니다. 꺼져 있으면 아침 판 tick이 오지 않으므로 보류하지 않고 지금처럼 바로 급변동 리포트를 만듭니다. **한계:** 09:00~09:10 사이에 브리핑 서비스가 재시작되면(배포는 그 시간에 하지 않으므로 장애 재시작만 해당) 보류 중이던 그 구간 급변동 알림이 사라질 수 있습니다. 아침 판 결과가 이미 나온 뒤 오는 합침 구간 변동과 09:10 이후 변동은 지금처럼 바로 급변동 리포트가 됩니다. 휴장일에는 시세가 없어 변동도 없으므로 영향이 없습니다. 모아 둔 변동도 메모리에만 있어 정상 종료 때 `interrupted_by_restart` 실패로 끝내고, 강제 종료는 다음 시작 때 같은 표시가 됩니다.

### 데일리 브리핑 (아침 판, 마감 후 판)

수집기가 평일 정해진 시각(아침 판 09:10, 마감 후 판 17:00)에 `schedule.tick`(jobType `daily-briefing-morning`, `daily-briefing-post-close`, 기본 꺼짐)을 보내면 판 하나를 만들고, 그 글을 받을 멤버마다 자기 포트폴리오 기준으로 짠 판을 `briefing.daily.generated`(v2, 멤버 번호만 싣고 메일 주소는 싣지 않음)로 발행합니다(`daily_jobs.py`, `daily_report.py`, `daily_members.py`). 글은 ETF마다 한 번만 쓰고(LLM 호출은 멤버 수와 무관), 메일은 사람마다 다릅니다: 멤버는 사용 중이고 그 판 수신을 켠 멤버 중 포트폴리오에 국내 ETF가 있는 멤버이며 판을 내보내기 직전에 `personal`에서 읽습니다(`member_store.py`). 멤버의 판에는 그 멤버의 ETF와 거기 얽힌 미국 종목·공시·기사·이유 문장만 남고, 보유/관심 묶음과 차트도 그 멤버 기준이며, 근거 번호는 판 전체의 번호를 그대로 씁니다. 받을 멤버를 읽지 못하면 누구에게 보낼지 알 수 없으므로 판을 발행하지 않고 작업을 `members_unreadable`로 실패 처리합니다. 한 멤버의 판을 짜다 실패해도 다른 멤버의 판은 나가며, 작업 기록에는 멤버마다 `member-briefing` 줄(멤버 번호, 결과 이벤트 ID)이 남습니다.

1. 판마다 consumer group을 따로 두어(`briefing-daily-morning`, `briefing-daily-post-close`) tick을 받습니다. 같은 tick(`eventId`)은 한 번만 처리하고, 그룹이 처음 생길 때는 지난 tick을 다시 읽지 않습니다.
2. 범위는 직전 평일의 같은 시각부터 tick의 예정 시각(`scheduledFor`)까지이며 최대 거슬러 가는 기간이 있습니다. 기록을 읽지 않고 예정 시각으로 계산하며 휴장일은 보지 않습니다.
3. 시세처리기에 판 자료(`market.daily.requested`)를 요청합니다. 보유 ETF에 미국 종목이 있으면 그 종목들의 범위 안 공시(`news.filings.requested`)도 요청합니다(두 판 모두). 마감 후 판은 자료를 받은 뒤 ETF마다 기존 기사 요청(`news.articles.requested`, 판 범위, 최신 `DailySettings.etf_max_articles`건)도 보냅니다. 같은 작업의 ETF 요청들은 `seq`를 겹치지 않게 매기고, 응답이 `symbol`과 `seq`로 짝지어집니다. 재요청 규칙은 급변동 근거 조회와 같고(응답 제한 시간 10초, 총 3번), 끝내 답이 없는 ETF는 기사 없음으로 진행하며 `자료 없음` 목록에 `ETF별 기사: <ETF>, 기사 응답을 받지 못함`이 오르고 작업 기록에 `news_no_response`가 남습니다. 받은 자료의 시각을 다시 검사해 범위 밖은 버립니다.
4. 아침 판은 장이 열린 뒤(평일 09:10)에 나가며 미국 지수, 환율, 미국 종목 직전 장 등락, 범위 안 미국 공시, 전날 한국 증시, ETF별 개장 흐름(아래)과 ETF별 기사(Claude 입력에만 담김, 마감 후 판과 같은 요청·응답·걸러내기)를, 마감 후 판은 그날 한국 증시, 미국 지수, 환율, 보유 ETF별 하루 등락을 항목으로 씁니다(마감 후 판의 미국 지수·환율은 Claude 입력과 출처에만 쓰고 본문 절로는 보이지 않습니다). 항목의 숫자·목록과 빠진 항목의 `자료 없음` 표시, 머리말, 면책 문구는 코드가 씁니다. 시세처리기 응답(`market.daily.responded` v2)에서 일봉이 빈 ETF는 목록에서 빼지 않고 `자료 없음: <ETF 이름(없으면 종목코드)>, <이유>` 한 줄로 쓰며, 모든 ETF가 비면 항목 전체가 자료 없음입니다. 빈 ETF가 있어도 그 ETF의 미국 종목·공시 요청·해외 등락률·재검색은 그대로 진행합니다.
   **ETF별 개장 흐름(아침 판만, `open_flow.py`):** 시세처리기가 저장한 시세로 ETF마다 전일 종가 대비 시가 등락(`openRate`), 장 시작 시각(`CoreSettings.open_flow_start` 기본 09:00)부터 판 시각까지 첫 시세 대비 마지막 시세 등락(`flowRate`)과 전일 종가 대비 마지막 시세 등락(`rateVsPrevClose`)을 줍니다(요청 `openFlowSince`, 응답 `openFlow`). 읽는 구간은 판 날짜의 장 시작 시각부터 판 시각까지이고 판이 늦게 시작해도 판 시각에서 `open_flow_max_window`(기본 30분)보다 멀리 거슬러 읽지 않으며, 판 시각이 장 시작 전이면 요청하지 않고 항목을 `자료 없음 (장 시작 전이라 개장 시세가 없음)`으로 둡니다. 브리핑도 받은 값의 시세 시각이 구간 밖이거나 요청하지 않았는데 온 값을 버리고(경고 로그·작업 기록 `excluded.openFlow`) 그 ETF를 `자료 없음`(`범위 안 시세 없음`·`응답에 개장 흐름 칸이 없음`)으로 둡니다. 항목은 ETF마다 근거 번호 `O1`…을 붙여 Claude 입력과 본문에 들어가고(자료 종류 `개장 시세`), 멤버별 판에서도 판 전체의 번호를 그대로 씁니다. 지시문은 개장 흐름의 숫자를 입력 값 그대로 인용하고 새로 계산하지 않게 하며, 방향의 뜻을 마감 후 판 채점 기준(보합 ±0.3%)과 같게 알려 줍니다.
5. LLM은 코드가 붙인 근거 번호를 인용한 요약 문장만 씁니다. 판 입력의 항목에는 값이 빈 ETF와 이유가 `missing`으로 들어가며, 지시문은 그 ETF의 숫자를 지어내거나 인용하지 말라고 합니다. 판 입력에는 응답의 ETF 정보(`etfInfo`, ETF마다 이름·추종 대상·업종과 비중 순위 순 구성 종목, 상한은 `DailySettings.max_etf_members`)도 들어가며, 지시문은 이를 원인으로 단정하지 말고 맥락으로만 쓰라고 합니다. 마감 후 판 입력에는 항목 `ETF별 기사`(기사 번호와 ETF 종목코드), `범위 안 미국 공시`(접수번호), `미국 구성 종목 이슈`(근거 번호 `M1`…)와 최상위 `notableCandidates`(눈에 띄는 점 후보와 관련 근거 번호)가 더해지며, 이 항목들은 Claude 입력에 담기고, 본문에는 8번의 배치로 이유와 `↳` 줄, 출처로 보입니다. 마감 후 판 환율 근거에는 환율종합 원문에 더해 코드가 꺼낸 원/달러 값(`usdKrw`: 값·전일대비·등락률)이 들어가고, 출처 줄에도 그 숫자가 보입니다. 지시문은 미국 자산 ETF(원화로 거래)를 담긴 종목 등락과 달러-원 환율 변화를 함께 보라고 하고, ETF 이유의 근거로 기사·공시·구성 종목 이슈·미국 지수·환율 번호를 쓰게 하며, 관련 없는 기사는 인용하지 말고 문장은 "~습니다"로 끝맺으라고 합니다. 구성 종목 이슈는 ETF마다 비중 상위 `member_rank_limit`개 안의 미국 종목 중 직전 미국 장 등락률 절댓값이 `member_move_percent`% 이상이거나 범위 안 공시가 있는 종목을 비중 순으로 `member_issues_per_etf`개까지입니다. 눈에 띄는 점 후보는 여러 ETF를 묶는 것만이며 ① 업종이 같은데 반대로 움직인 ETF(둘 다 `opposite_move_percent`% 이상) ② 같은 방향으로 `cluster_move_percent`% 이상 움직인 ETF가 `cluster_min_size`개 이상입니다(`daily_inputs.py`). ETF 하나에 관한 구성 종목 이슈는 ETF 줄의 `↳` 줄로, 이유를 찾지 못한 ETF는 ETF 줄 이유로만 보입니다. 후보가 없는 날 Claude 입력의 `notableCandidates`는 빈 목록이고, 이때 Claude가 눈에 띄는 점을 써도 그 부분만 버리고(요약과 ETF 이유는 그대로 씀) 본문에서 `눈에 띄는 점` 절은 글자판·HTML 모두 빠집니다. 마감 후 판은 판 이름 로그 한 줄(`daily post-close collection: ...`)에 기사·제외·잘림·응답 없음·공시·구성 종목 이슈·후보 건수를 남기며 종목코드와 티커는 쓰지 않습니다. 마감 후 판 Claude 출력은 `{"summary":[문장 2개],"etfs":[{"symbol","reason","refs","weak","shortfall"}],"notables":[{"statement","refs","weak","shortfall"}],"fxIssue":{"statement","refs","weak","shortfall"},"rateIssue":{같은 모양},"insufficientEvidence"}`이고일봉이 있는 ETF마다 이유 한 줄을 씁니다. 아침 판 출력은 `{"points":[{"statement","refs","weak","shortfall"}],"etfs":[{"symbol","direction","reason","refs","weak","shortfall"}],"fxIssue":{...},"rateIssue":{...},"insufficientEvidence"}`이며, `etfs`는 입력 `outlookEtfs`(보유·관심 모두, 일봉이 비어도 포함)의 ETF마다 하나씩이고 `direction`은 `상승`·`하락`·`보합`·`예상 어려움` 중 하나입니다. 방향을 정하면 근거 번호가 있어야 하고, 근거가 약해 `예상 어려움`이면 근거 번호 없이 근거 약함으로 씁니다(빠진·겹친·모르는 ETF, 목록 밖 방향은 거부). 아침 판 지시문은 가격 전망 금지를 ETF 방향 예상에 한해 풀지만 매수·매도·보유 권유와 목표 가격·등락률 숫자 예측은 금지하며, 본문은 요약 → 환율 이슈 → 금리 이슈 → `ETF별 예상`(ETF 줄에 `상승 예상` 등 방향, 아래 이유) → 면책(예상은 실제와 다를 수 있다는 줄 포함) 순입니다. 최종 본문의 ETF별 예상은 작업 기록(`briefing.job_log`)에 기록 종류 `etf-outlook` 한 줄(`data.asOf`, `data.outlooks[{symbol, direction, reason, refs, weak, shortfall}]`)로 남아, 마감 후 판이 같은 날 예상과 실제 등락을 대조할 때 읽습니다(Claude가 쓰지 못한 규칙 기반 판에는 이 줄이 없음). 하루 등락이 0.00%인 ETF도 이유를 쓰며 입력의 환율·금리·지수 숫자를 들어 쓰게 합니다. `fxIssue`·`rateIssue`는 환율 이슈·금리 이슈 칸의 문장이며(ETF 이유는 이 칸을 문장 끝 `(환율 이슈 참고)`·`(금리 이슈 참고)`로 짧게 가리킴. 환율은 환율 항목의 원/달러 값과 기사, 금리는 기사 근거이고 금리 숫자는 기사에 있을 때만), 금리 기사가 없으면 근거 번호 없이 근거 약함으로 씁니다. 두 칸은 빠져 있어도 출력을 거부하지 않고 본문의 대체 줄로 나갑니다. ETF 이름·추종 대상(나라 이름·'지수'를 뗀 것도)·구성 종목 이름이 제목과 요약에 하나도 나오지 않는 기사(미국 종목을 담은 ETF는 시장 단어가 있어도 남김)는 그 ETF의 기사에서 빼고(정보가 빈 ETF는 거르지 않음) 뺀 건수를 작업 기록의 `excluded.irrelevantArticles`에, 뺀 기사의 제목을 `irrelevantArticleTitles`에 남깁니다(제목 비교는 띄어쓰기·대소문자를 무시하고 ETF 이름은 영문 대문자 브랜드 접두어를 뗀 나머지로도 비교)(`daily_relevance.py`). 받은 자료에서 이유를 찾지 못한 ETF는 `refs`를 빈 목록으로 두고 근거 약함으로 표시하며(근거 약함이 아닌데 비어 있으면 거부), 본문에는 출처 번호 없이 보입니다. 일봉이 빈 ETF는 코드가 `자료 없음: <ETF>, <이유>` 줄로 쓰므로 이유를 쓰지 않습니다. 목록 밖 번호나 미국 티커(ETF 구성 종목의 티커 포함), JSON 기호(`}`·`]`·`"`·`{`·`[`)로 끝나거나 `}],` 같은 조각을 담은 문장을 쓴 출력은 거부하고 다시 부릅니다(본문은 메일로 나가므로 종목 식별자를 본문에 쓰지 않게 합니다). 마감 후 판은 여기에 더해 빠진·겹친·모르는 ETF, 요약이 2줄이 아님, ETF 이유가 `↳` 줄과 같은 내용(그 ETF 구성 종목 이슈의 종목 이름이나 직전 장 등락률 숫자)을 되풀이함도 거부하며, 시도 상한(`llm_max_attempts`)은 그대로입니다. 끝내 실패하거나 키가 없으면 요약·이유 없이 숫자·목록(과 `↳` 줄) 본문으로 나가며 `generatedBy`가 `rule-based`입니다. 응답을 끝내 받지 못해도 판은 있는 자료로 반드시 나갑니다.
   LLM은 요약 문장마다 근거가 약한지(`weak`)와 부족한 점(`shortfall`)도 냅니다(부족한 점에도 미국 티커를 쓰면 거부). 약한 문장이 있고 검색어 LLM이 있으며 보유 ETF 자료가 있으면 급변동과 같은 문장 단위 재검색을 부릅니다(`news.research.requested`, 작업 종류 `morning`·`post-close`, 태그 `*`, 재검색 작업 id는 tick의 `eventId`·문장 번호·회차에서 정한 uuid). 상한과 재검색·다시 쓰기 규칙은 급변동 8번과 같습니다. 재검색 기사는 판 범위(기존 시간 범위)로 다시 거르고 범위 밖은 버리며, 기사 조회의 `symbol`은 계약상 필수라 정렬한 첫 ETF 코드를 씁니다(재검색 조회는 종목 조건을 보지 않음). 받은 기사는 항목 `재검색 기사`로 근거 번호(`articleId`)·링크와 함께 본문에 더하고 그 문장만 다시 씁니다. 다시 써도 약한 문장에는 `근거 약함: <부족한 점>`을 붙여 냅니다. 재검색 실패·시간 초과·쓸 기사 없음이면 그 문장은 그대로 냅니다.
   마감 후 판은 장이 열린 뒤에 만든 판이면(장 시작 전 판은 읽지 않음) 작업 기록에서 두 가지를 읽어 본문에 두 칸을 더합니다(`daily_review.py`). ① **아침 예상 대조**: 같은 날(한국 시간) 아침 판의 `etf-outlook` 줄(기록이 늦은 것 하나)을 읽어, ETF마다 코드가 예상과 그날 실제 등락을 대조해 `맞음`·`빗나감`을 정합니다. 상승·하락 예상은 실제 등락의 부호로, 등락 절댓값이 `DailySettings.flat_band_percent`(보합 폭, 기본 0.3)% 이하이면 보합입니다. 보합 예상은 보합일 때만, 상승 예상은 등락이 +폭을 넘을 때만, 하락 예상은 -폭 아래일 때만 맞음이고 폭 안이면(경계 ±폭 포함) 상승·하락 예상은 빗나감입니다. `예상 어려움`은 채점하지 않고 `대조 안 함(예상 어려움)`으로만 쓰며 칸 끝의 `채점: 맞음 N · 빗나감 N · 예상 어려움 N개(채점하지 않음) — 기준: …` 줄에 개수와 기준을 보입니다. 일봉이 없는 ETF는 뺍니다. Claude 입력에는 판정이 `reviewEtfs`(예상 방향·이유, 실제 등락률, 판정)로 들어가고, 출력의 `reviews[{symbol, reason, refs, weak, shortfall}]`가 판정이 `맞음`·`빗나감`인 ETF마다 그 이유 한 문장을 근거 번호와 함께 씁니다(빠지거나 겹치면 거부, 대조 대상이 아닌 항목은 버리고, 이유를 찾지 못하면 `refs`를 비우고 근거 약함으로 씀). 판정은 Claude가 바꾸지 않으며 Claude 없는 판에도 판정 줄은 나가고 이유만 빠집니다. 같은 날 아침 판 기록이 없거나(전날 기록은 쓰지 않음) 읽지 못하면 그 칸에 이유를 한 줄로 쓰고 작업 기록에 `missing-items` 줄(사유 `morning_outlook_missing`)로 남깁니다. ② **그날 급변동**: 판 날짜 0시부터 판 시각까지 시작한 급변동 작업(`job_run`, 작업 종류 `move`)을 `시각 ETF 등락 · 리포트 성공|실패|진행 중` 한 줄씩 보이고(장 시작~아침 판 시작 사이 변동을 아침 판에 합친 건은 `· 아침 판에 합침`)(최대 `DailySettings.max_move_lines`건, 넘으면 `외 N건`), 없으면 `없음`, 읽지 못하면 `기록을 읽지 못했습니다`입니다. 두 칸은 ETF 줄 다음, 면책 위에 글자판·HTML 같은 순서로 놓이고, 대조 이유의 근거 번호는 ETF 줄 다음 번호로 이어 붙어 출처 목록에 나옵니다. 작업 기록을 읽지 못해도 판은 나갑니다.
6. 응답을 끝내 받지 못했거나 LLM이 끝내 실패해도 판은 나가고, 그 일은 아래 작업 기록 테이블에 남습니다(판 이름 `morning`·`post-close`가 작업 종류, tick의 `eventId`가 작업 id). 끝내 실패한 사유는 판 자료 무응답이 `context_no_response`, 공시 무응답이 `filings_no_response`, LLM 실패가 `llm_error`입니다. 재요청, 거부된 LLM 시도와 그 입력·응답, 빠진 항목(일봉이 빈 ETF는 `보유 ETF별 하루 등락: <ETF>, <이유>`와 종목코드·이유 코드 `missingEtfs`), 최종 본문도 같은 테이블에 줄로 남습니다. 화면 로그에는 종목코드를 쓰지 않습니다.
7. 마감 후 판만 본문의 면책 문구 아래, 근거 목록(마감 후 판은 출처) 위에 **서버 상태 절**을 코드가 붙입니다(`status.py`). 시세처리기에 판 자료를 요청할 때 `includeOperations`를 `true`로 보내 받은 운영 상태(`operations`)와, 브리핑이 직접 저장한 수집 완료 알림(`briefing.collection_completed`), 작업 기록(`briefing.job_run`)으로 4줄을 셉니다. 범위는 판 범위(양끝 포함)이고 뉴스 수집 완료는 끝난 시각(`finishedAt`), 작업 기록은 시작 시각(`started_at`), 시세처리기 자료는 받은 시각으로 가립니다. 숫자는 모두 코드가 세고 LLM 입력에는 넣지 않으며, LLM 본문이든 규칙 기반 본문이든 같은 자리에 붙습니다. 아침 판은 바뀌지 않습니다.
   - 수집: 장 시작 전, 1초 현재가, 장중, 장 마감 후 수집이 범위 안에서 받은 처음~마지막 시각(없으면 `없음`).
   - 시세: 받은 건수, 실패 건수와 비율(분모는 받은 건수와 실패 건수의 합), 0이 아닌 실패 종류별 건수.
   - 뉴스: 정기 수집 횟수, 일부 종목이 빠진 수집 횟수, 정기 수집이 발행한 기사 수, 재검색 횟수(따로). 재검색 횟수는 범위 안에 끝난 재검색 수집 완료에 이번 판이 보낸 재검색 요청 수를 더합니다. 이번 판의 재검색은 판 기준 시각 뒤에 끝나 범위 안 수집 완료에 없기 때문입니다.
   - 브리핑: 급변동 리포트 수(성공·실패·진행 중·규칙 기반), 실패는 사유별 건수, `근거 없어 보내지 않은 급변동 N건`(인용한 근거가 0개라 결과 이벤트를 내지 않은 작업. `job_run`은 `succeeded`에 사유 `no_evidence`로 끝나며 성공·실패 수에는 세지 않고 주의에도 올리지 않음), 그날 아침 판 결과(성공·실패·진행 중·기록 없음).
   - 주의: 코드·로직을 점검해야 하는 상황만 한 줄로 모읍니다(시세처리기가 버린 메시지, 결과 코드 실패, 가격 해석 실패, 호출 상한 건너뜀, 토큰 발급 실패, 규칙 기반으로 나간 판·급변동 리포트, 급변동 리포트 실패와 사유, `DailySettings.stuck_after`(기본 30분)보다 오래 진행 중인 작업, 아침 판 실패 또는 기록 없음, 작업 기록·뉴스 수집 기록을 읽지 못함). 없으면 `주의: 없음`입니다. 연결 실패·시간 초과·HTTP 오류·깨진 응답·뉴스 빠짐 같은 환경 문제는 줄의 숫자로만 봅니다.
   - 시세처리기가 운영 상태를 주지 않으면 수집·시세 줄은 `자료 없음`으로 쓰고, 수집 완료나 작업 기록을 읽지 못하면 그 줄을 `자료 없음`으로 씁니다. 판은 그대로 나가며 빠진 부분은 `missingItems`와 작업 기록의 빠진 항목 줄(`서버 상태: ...`)에 남습니다.
8. 본문은 글자판(`text`)과 HTML(`html`)을 같은 순서로 만듭니다(`daily_report.py`의 `daily_text`·`post_close_view`, `daily_html.py`). 아침 판은 제목 → 항목별 숫자(ETF별 개장 흐름 포함) → 요약 → 환율 이슈 → 금리 이슈 → ETF별 예상 → 면책 → 서버 상태 → 근거 목록입니다. 마감 후 판은 제목 → 결론 → 그날 한국 증시 → 요약 2줄 → 눈에 띄는 점 → 환율 이슈 → 금리 이슈 → 하루 등락 차트(HTML만) → 보유/관심별 ETF 줄과 그 아래 이유 한 줄, `↳` 구성 종목 줄 → 면책 → 서버 상태 → 출처(작게) 순입니다. 출처 번호(`[출처 n]`)는 본문에 나온 순서(눈에 띄는 점이 먼저)로 붙고, 출처에는 자료 종류·제목·시각·발행처를 쓰며 기사와 공시는 원문 링크를 답니다(HTML은 http(s) 주소만 링크). 근거가 약한 이유와 눈에 띄는 점에는 `근거 약함: <부족한 점>` 줄이 붙습니다. `↳` 줄은 코드가 쓰며 `↳ 비중 2위 <종목 이름> 직전 장 +3.50% · 범위 안 공시`처럼 비중 순위, 종목 이름, 직전 장 등락률, 공시 여부를 보여 줍니다. 티커는 본문에 쓰지 않으며, 이름이 없거나 이름에 티커가 들어 있으면 `미국 종목`으로 씁니다. 구성 종목 이슈의 출처 줄도 `비중 2위 <종목 이름> 직전 장 등락률 +3.50% — 해외 시세 자료`처럼 같은 표기를 씁니다. 마감 후 판 결론 줄 끝에는 이유를 찾지 못한 ETF 수가 `· 이유를 찾지 못한 ETF 1개`로 붙습니다(Claude 본문이면 Claude가 이유를 `받은 자료에서 이유를 찾지 못했습니다.`로 쓴 ETF 수이고, 규칙 기반 본문이면 하루 등락이 있고 기사 응답을 받았으며 기사·공시·구성 종목 이슈가 모두 없는 ETF를 코드가 셉니다. 0개면 붙이지 않습니다). 판 기준 시각이 장 시작 시각(`market_open`, 09:00) 전이고 일봉이 있는 모든 ETF의 일봉이 판을 만든 날 일자이며 등락이 0.00이면 장 시작 전에 만든 판이라 Claude를 부르지 않고, 결론은 `오늘 장 자료가 아직 없습니다`이며 ETF별 이유를 쓰지 않습니다. 환율 이슈·금리 이슈 칸은 Claude 본문이 없는 판에도 있으며, 그때 환율은 코드가 받은 원/달러 숫자를, 금리는 `자료 없음`을 보입니다. 마감 후 판은 결론 바로 아래에 `그날 한국 증시: 코스피 +0.30% · 코스닥 −0.50%` 줄을 씁니다(시장종합 응답에서 꺼낸 등락률, 못 꺼낸 지수나 범위 밖·응답 없음은 `자료 없음`이며 항목이 빠졌으면 `자료 없음` 목록에도 오릅니다). 하루 등락이 없는 ETF에는 `↳` 줄을 붙이지 않습니다.
   - 재검색(마감 후 판): 근거가 약하다고 한 ETF 이유만 다시 검색합니다. 그 ETF의 하루 등락이 그날 코스피종합 등락과 같은 방향(둘 다 0이 아니고 부호가 같으며 차이가 `market_same_direction_gap_percent`%p 이내)이면 시장 전체의 움직임으로 보고 다시 검색하지 않으며, 코스피 등락을 모르면 같은 방향이 아니라고 봅니다. 눈에 띄는 점은 다시 검색하지 않고 `근거 약함`을 그대로 보여 줍니다. 한 판의 요청 상한(`max_research_requests` 10건)과 문장당 횟수는 그대로입니다.
   - 결론(마감 후 판만): `결론: 보유 n개 중 m개 상승 · 관심 n개 중 m개 상승(자료 없음 k) · 가장 크게 오름 <ETF 이름> +x.x% · 가장 크게 내림 <ETF 이름> −x.x%`. 개수와 최대 등락은 코드가 세며 자료 없는 ETF도 n에 넣습니다. 상승 개수는 ETF 줄에 보이는 원래 등락률의 부호로 세고(+0.04%도 상승), 가장 크게 오름·내림은 소수 한 자리로 반올림한 값이 0이 아닐 때만 씁니다. 멤버의 포트폴리오에서 ETF마다 `HELD` 보유 또는 `WATCH` 관심을 붙이며, 구분을 모르는 판 전체 글(작업 기록에 남는 본문)은 `ETF n개 중 m개 상승` 한 묶음이고, 값 있는 ETF가 없으면 `결론: 등락 자료 없음`입니다. 보유/관심 ETF 줄도 같은 묶음으로 나뉩니다.
   - HTML은 두 판 모두 만들며 스타일은 모두 인라인입니다(글꼴 `Noto Sans KR → Apple SD Gothic Neo → 맑은 고딕 → sans-serif`, 제목·결론 굵게, 오른 등락률 빨강·내린 등락률 파랑). 면책·서버 상태·근거 목록은 작은 회색이고, 서버 상태는 '주의'가 `없음`이 아닐 때만 그 줄이 주황입니다. ETF 이름·LLM 문장·근거 설명·서버 상태 줄 같은 이벤트 값은 모두 이스케이프합니다(발송기는 `html`을 받은 그대로 보냄).
   - 하루 등락 차트(마감 후 판만): 보유, 관심 순 가로 막대(묶음 안은 많이 오른 순, 관심 ETF는 이름 뒤 `(관심)`)를 PNG 한 장(`images`, Content-ID `chart-day`)으로 싣습니다. 자료 없는 ETF는 막대에서 빼고 그림 아래에 작게 적습니다. 그림이 한 장 300KB(base64를 푼 크기)를 넘거나 이벤트가 900KB(JSON 바이트)를 넘으면(`DailySettings.max_chart_image_bytes`·`max_event_bytes`, 그림은 4장까지) 그림만 빼고 글자판과 HTML로 보내며, HTML에 `차트 없음(크기 초과)`를 쓰고 `missingItems`에 `하루 등락 차트: 크기 초과`를 더합니다. 그리다 실패해도 메일은 나가고 `하루 등락 차트: 그리지 못함`이 `missingItems`에 남습니다.
   - 차트의 한글 글꼴(Noto Sans KR 보통·굵게)은 파일이 git에 없으므로 `python3 scripts/install_font.py`로 `services/briefing/fonts/`에 내려받습니다(버전 고정 주소와 SHA-256 검사). 글꼴 파일이 없으면 경고 로그(`chart font files not found`)를 남기고 기본 글꼴로 그려 한글이 네모로 보일 수 있습니다. 다른 폴더를 쓰려면 환경변수 `BRIEFING_FONT_DIR`입니다.

설정(범위 상한, 응답 제한 시간, 재시도 횟수, 상한, 마감 후 판의 ETF별 기사 건수 `etf_max_articles`, 구성 종목 이슈 기준 `member_rank_limit`·`member_move_percent`·`member_issues_per_etf`, 눈에 띄는 점 후보 기준 `opposite_move_percent`·`cluster_move_percent`·`cluster_min_size`, 장 시작 전 판 판정과 급변동 합침 구간 시작에 쓰는 장 시작 시각 `market_open`(기본 09:00, 한국 시간), 장 시작부터 이 시각 전까지의 급변동을 아침 판에 합치는 `morning_merge_end`(기본 09:10, 수집기 아침 판 시작 시각과 같아야 함)와 아침 판 결과가 없을 때 합침 구간이 끝난 뒤 보류분을 급변동 리포트로 넘기는 기다림 `morning_merge_grace`(기본 30분), 마감 후 판 예상 채점의 보합 폭 `flat_band_percent`(기본 0.3), 아침 판의 개장 시세 범위 `CoreSettings.open_flow_start`(기본 09:00)·`open_flow_max_window`(기본 30분), 기사 관계 판정의 비교어를 넓히는 `relevance_country_prefixes`(앞의 나라 이름)·`relevance_suffixes`(끝 접미어, 예: '미국 나스닥100 지수'에서 '나스닥100'도 비교), 미국 종목을 담은 ETF가 시장 단어(뉴욕증시, 월가 등)만 있어도 기사를 남기는 `relevance_us_market_terms`(다우는 '다우존스'·'다우지수'로만 적어 다우기술 같은 한국 회사 이름에 걸리지 않게 함), 재검색에서 뺄 '시장과 같은 방향' 기준 `market_same_direction_gap_percent`(기본 1.0, ETF와 코스피 종합 등락의 부호가 같고 차이가 이 %p 이내일 때만 뺌. 예: 코스피 +0.3%, ETF +6%면 재검색), 미국 지수 대륙, LLM 시도 횟수, 멈춘 작업 기준)은 `config.py`의 `DailySettings` 기본값이며 환경변수로 바꿀 수 없습니다. LLM 키와 모델은 건별 리포트와 같은 `ANTHROPIC_API_KEY`, `BRIEFING_LLM_MODEL`을 씁니다.

### 설정

근거 조회 설정(Δ, 자료별 시간 범위와 기준 기간, 시세 이력 간격, 자료별 최대 건수·크기, 응답 제한 시간, 재시도 횟수, 재확인 간격, 대기 상한 등)과 재검색 설정(검색어 최대 개수, 검색어 LLM 시도 횟수, 재검색 완료 알림 대기 상한, 문장당 찾기 횟수, 작업당 재검색 요청 합계, 작업당 재검색 전체 시간)은 `services/briefing/src/briefing/config.py`의 `CoreSettings` 기본값이며 환경변수로 바꿀 수 없습니다. 값과 뜻은 그 파일에 있습니다.

환경변수(`.env`)는 다음과 같습니다.

환경변수가 없을 때 쓰는 값은 `main.py`와 `llm.py`가 원본입니다.

| 환경변수 | 설명 |
|---|---|
| `KAFKA_BOOTSTRAP` | Kafka 주소 |
| `ANTHROPIC_API_KEY` | LLM 키. 비어 있거나 자리표시 값(`change-me`)이면 시작할 때 경고를 남기고 규칙 기반 리포트를 만듭니다. 아래 Parameter Store의 `briefing` 그룹에서 가져옵니다 |
| `BRIEFING_LLM_MODEL` | 리포트를 쓰는 모델 |
| `POSTGRES_DB` | 작업 기록 DB 이름 |
| `BRIEFING_DB_USER` / `BRIEFING_DB_PASSWORD` | 브리핑 전용 DB 계정. 비밀번호는 `.env`에만 둡니다 |

### 작업 기록 테이블

급변동, 데일리 판(아침·마감 후), 뉴스 재검색이 같은 형식으로 `briefing` 스키마의 테이블 둘에 남깁니다. 테이블은 Flyway 번호 파일(`services/briefing/db/migration/V1__create_briefing_tables.sql`)이 만들며, 브리핑은 테이블을 만들지 않고 Flyway가 먼저 끝나 있기를 요구합니다(아래 "DB 준비와 Flyway"). 옛 단일 `briefing.job_log`와 `briefing.failed_jobs`는 더 쓰지 않으며 이미 있는 로컬 DB에서는 직접 지웁니다.

- **`briefing.job_run`(작업마다 한 줄, 시작 때 만들고 끝날 때 고침):** `job_id`(기본키, 급변동은 `market.move`의 `eventId`, 데일리 판은 tick의 `eventId`), `job_kind`(`move`, `morning`, `post-close`), `input`(작업을 시작시킨 이벤트 원문 JSON), `status`(`running`, `succeeded`, `failed`), `failure_reason`(성공으로 끝난 급변동에 `merged_into_morning`이 있으면 아침 판에 합쳐 리포트를 만들지 않은 것), `generated_by`(`llm`, `rule-based`), `target`(급변동은 종목·변동 시각, 데일리는 판 범위), `started_at`, `finished_at`, `final_text`(발행한 최종 본문), `weak_count`(본문에 근거 약함이 남은 문장 수), `research_count`(보낸 재검색 요청 수), `result_event_id`(발행한 결과 이벤트의 `eventId`, 곧 메일의 "브리핑 ID").
- **`briefing.job_log`(일마다 한 줄, `job_id`가 `job_run`을 가리킴):** `id`, `job_id`, `entry_type`, `stage`, `sentence_no`, `round`(문장 단위 줄의 문장 번호와 그 문장의 찾기 회차), `before`·`after`(문장을 다시 쓴 줄의 고치기 전·후 문장), `reason`, `error_type`, `attempts`, `data`(JSON 원문), `occurred_at`.
- **기록 종류(`entry_type`):** `report-written`(받아들인 보고서·다시 쓰기 LLM 시도), `llm-rejected`(거부되거나 실패한 LLM 시도), `query-written`(받아들인 검색어 LLM 시도), `research-requested`(문장 하나의 재검색 요청), `articles-received`(재검색 기사를 받음: 받은·쓸·버린 기사와 이유, 수집기 완료 알림 원문), `sentence-rewritten`(문장을 다시 씀), `check-round`(기사 확인 라운드), `resent`(무응답 재요청), `missing-items`(판은 나갔지만 빠진 항목), `step-failed`(단계가 끝내 실패하거나 포기됨), `moves-grouped`(같은 종목의 변동 여럿을 모아 작업 하나로 만듦, 모인 변동 전부).
- 작업 종류·상태·작성 방식·기록 종류의 값은 `job_log.py`의 enum(`JobKind`, `RunStatus`, `GeneratedBy`, `EntryType`)이 원본이고 DB 칸은 글자이고 CHECK 제약은 Flyway V1 파일에 같은 값으로 적혀 있습니다. 새 값을 더하면 enum과 새 번호 파일(V2 …)을 함께 고칩니다. 둘이 어긋나면 `test_job_log_schema.py`가 실패합니다.
- `data`는 받은 응답과 LLM 입력·원래 응답, 검색어를 가공하거나 줄이지 않고 담습니다. 수집기가 거른 건수는 이벤트에 없어 수집기 로그에서 보고, 이벤트로는 완료 알림 원문(`collectorCompletion`)을 남깁니다. 키, 수신 주소, 보유 수량, 금액은 저장하지 않으며 화면 로그에는 종목코드·티커·검색어를 쓰지 않습니다.
- 저장은 짧게(3번) 다시 시도하고, 작업 끝 줄(`job_run` 고침)이 끝내 실패하면 ERROR 로그를 남기고 그 작업의 결과 이벤트(`briefing.generated`, `briefing.daily.generated`)를 발행하지 않습니다. 기록이 없는 브리핑은 내보내지 않는다는 결정이며, 데일리 판이 반드시 나간다는 원칙의 유일한 예외입니다. 기록은 지우지 않고 쌓으며 보관 기간은 아래 "배포 전에 정할 것"에 있습니다.

- **`briefing.collection_completed`(수집 완료 알림 한 건이 한 줄):** `event_id`(기본키, 같은 알림을 두 번 받아도 한 줄), `job_type`(정기 수집 `collect-news`, 재검색 `research-news`), `scheduled_for`, `started_at`, `finished_at`, `published_count`, `incomplete_symbols`, `research_job_id`(재검색일 때만), `raw`(받은 이벤트 원문 JSON, 가공하지 않음), `received_at`. 서버 상태 절의 뉴스 줄이 `finished_at`으로 범위를 가려 셉니다. 보관 기간은 작업 기록과 함께 정합니다.

작업 하나의 기록은 메일에 적힌 브리핑 ID(또는 작업 id)로 봅니다. 읽기만 하며 테이블을 고치지 않습니다. 출력에 검색어와 종목이 들어 있으므로 화면 로그나 공개 문서에 붙여 넣지 않습니다.

```bash
JOB_HISTORY_DB_URL=postgresql://localhost:5432/stock JOB_HISTORY_DB_USER=<읽기 계정> \
  JOB_HISTORY_DB_PASSWORD=<비밀번호> python3 scripts/job_history.py <브리핑 ID 또는 job_id> [--full]
```

끝내 실패한 작업은 `job_run.status = 'failed'`로 찾고(사유는 `failure_reason`), 단계가 실패한 줄은 `entry_type = 'step-failed'`입니다. 급변동의 끝내 실패 사유:

| 사유 | 뜻 |
|---|---|
| `collection_not_covered` | 쓸 수 있는 수집 완료를 대기 상한 안에 받지 못함 |
| `market_no_response` | 시세 이력 응답을 총 시도 횟수 안에 받지 못함 |
| `news_no_response` | 기사 응답을 총 시도 횟수 안에 받지 못함 |
| `news_unstable` | 최대 라운드 수 안에 기사 목록이 안정되지 않음 |
| `context_no_response` | 배경 자료 응답을 총 시도 횟수 안에 받지 못함 |
| `filings_no_response` | 공시 응답을 총 시도 횟수 안에 받지 못함 |
| `llm_error` | LLM 출력이 3번 모두 형식·인용 검증을 통과하지 못하거나 호출이 실패함 |
| `interrupted_by_restart` | 브리핑이 재시작되면서 끝나지 않은 채 남은 작업(아래 "재시작으로 끊긴 작업"). 모든 작업 종류에 쓰며, 이 사유는 코드의 단계 실패가 아니라 시작할 때 한 번에 붙입니다 |

기록 자체가 실패하면 짧은 간격으로 3번까지 시도하고, 그래도 안 되면 `job_id`, 종목, 사유만 ERROR 로그에 남기고 넘어갑니다.

### 재시작으로 끊긴 작업

브리핑은 작업 상태를 메모리에만 두므로 재시작되면 진행 중이던 작업은 이어지지 않습니다. 시작할 때(토픽을 선언한 뒤, 메시지를 받기 전에) `job_run`에서 아직 `running`인 이전 실행의 작업을 모두 찾아 `failed`, 실패 사유 `interrupted_by_restart`, 끝난 시각 = 이번 브리핑이 시작한 시각으로 끝내고, 작업마다 `step-failed` 줄(단계 `restart`)을 남깁니다. 리포트는 다시 만들지 않으며 이벤트 발행과 LLM 호출도 없습니다. 이번 브리핑이 시작한 뒤에 만든 작업은 건드리지 않습니다. DB에 닿지 못하면 브리핑은 시작하지 않습니다. 서버 상태 절의 급변동 실패 사유에는 `재시작으로 중단`으로 보입니다.

### DB 준비와 Flyway

브리핑 전용 계정과 `briefing` 스키마는 `infra/compose/postgres/init/03-roles-briefing.sql`이 만들고, 테이블은 Flyway가 `services/briefing/db/migration/`의 번호 파일(`V1__…sql`, 이어서 `V2__…`)로 만듭니다. 브리핑은 `CREATE TABLE`을 하지 않으므로 **브리핑을 띄우기 전에 Flyway를 먼저 실행해야 합니다**(Java 서비스 market·news는 기동할 때 Spring이 같은 방식으로 적용합니다). DB에 연결할 수 없거나 테이블이 없으면 브리핑은 시작하지 않습니다.

- 로컬 실행: `docker compose -f infra/compose/docker-compose.yml --env-file .env up -d`가 Postgres가 준비된 뒤 `briefing-migrate`(공식 `flyway/flyway` 이미지, 브리핑 계정으로 `briefing` 스키마에 적용)를 한 번 실행하고 끝납니다. 번호 파일을 더한 뒤에는 `docker compose -f infra/compose/docker-compose.yml --env-file .env run --rm briefing-migrate`로 다시 적용합니다. 이 서비스가 끝난 뒤에 브리핑을 실행합니다.
- 실제 DB 테스트: `scripts/test-all.sh`가 테스트 전용 데이터베이스를 만든 직후 같은 이미지로 Flyway를 먼저 적용하고 나서 브리핑 테스트와 `--live`의 브리핑 기동을 합니다(`FLYWAY_IMAGE`, 처음 한 번 이미지를 받습니다). 켜는 환경변수로 직접 돌릴 때도 대상 DB에 Flyway를 먼저 적용해 둡니다.
- 초기화 스크립트는 DB 볼륨을 처음 만들 때만 실행됩니다. `.env`에 `BRIEFING_DB_USER`, `BRIEFING_DB_PASSWORD`를 넣고, 이미 볼륨이 있으면 볼륨을 지우고 다시 띄워야 브리핑 계정이 생깁니다. 비어 있으면 초기화 스크립트가 실패합니다.
- 번호 파일은 한 번 적용하면 고치지 않고 새 번호로 더합니다(이미 적용한 파일을 바꾸면 Flyway가 검사에서 멈춥니다).
- 코드가 시작할 때 테이블을 만들던 옛 방식으로 이미 테이블이 만들어진 로컬 DB에서는 V1이 "이미 있음"으로 실패합니다. 배포 전이라 지워도 되는 데이터이므로 `briefing` 스키마의 테이블(`job_log`, `job_run`, `collection_completed`, 옛 `job_log_legacy`, `failed_jobs`)을 지우고 Flyway를 다시 적용하거나, 위 "로컬 DB를 지우고 다시 만든다" 명령으로 DB를 새로 만듭니다.

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
# 데일리 메일 차트의 한글 글꼴을 내려받는다(파일은 git에 넣지 않음, 없으면 경고 후 기본 글꼴로 그림)
python3 scripts/install_font.py

# 테스트 (실제 Kafka·DB·LLM을 호출하지 않음)
services/briefing/.venv/bin/python -m pytest services/briefing

# 실행 (저장소 루트에서). 브리핑은 .env 파일을 읽지 않고 환경변수만 읽으므로 필요한 키만 넘긴다
# (.env 전체를 셸로 읽으면 ';'가 들어간 값이 명령으로 해석될 수 있다)
export $(grep -E '^(KAFKA_BOOTSTRAP|ANTHROPIC_API_KEY|BRIEFING_LLM_MODEL|POSTGRES_DB|BRIEFING_DB_USER|BRIEFING_DB_PASSWORD)=' .env | xargs)
PYTHONPATH=services/briefing/src services/briefing/.venv/bin/python -m briefing.main
```

브리핑은 시작할 때 자기가 쓰는 토픽을 선언합니다(목록과 파티션 수는 `main.py`의 `TOPIC_PARTITIONS`). 데일리 브리핑 결과 토픽 `briefing.daily.generated`(멤버마다 판 종류별로 한 통씩, 형식은 `contracts/events/briefing.daily.generated.v2.schema.json`)도 선언합니다. `Ctrl+C`(또는 종료 신호)를 받으면 처리 중인 루프를 멈추고 끝납니다.

변동 알림(`market.move`)은 읽는 즉시 읽은 위치(오프셋)를 확정합니다. 진행 중인 작업은 메모리에만 있으므로 **브리핑이 정상 종료되면 모으는 중이던 변동은 `interrupted_by_restart` 실패 작업으로 남고, 강제 종료(`kill -9`, 전원 꺼짐)되면 리포트도 실패 기록도 남지 않고 사라집니다.**

흐름을 사람이 직접 확인할 때는 `scripts/manual-briefing-move.sh [종목]`을 씁니다. 로컬 Kafka에 가짜 `market.move`와 그 변동에 쓸 수 있는 `collection.completed`를 한 건씩 넣고, 브리핑이 낸 요청과 `briefing.generated`를 보여 줍니다. 미리 인프라, 브리핑, 시세처리기, news 서비스를 띄워 둡니다.

데일리 브리핑을 끝까지 확인할 때는 `scripts/manual-daily-briefing.sh`를 씁니다. 개발용 Kafka·DB를 쓰지 않고 이 실행만의 자원을 만들었다가 끝나면 그것만 지웁니다: 일회용 Kafka 컨테이너(127.0.0.1의 빈 포트), 개발용 Postgres 컨테이너 안의 새 데이터베이스(`daily_e2e_<시각>_<pid>`), 임시 폴더에서 띄운 시세처리기·news·발송기(드라이런)·브리핑. TEST 접두 가짜 수집 자료(`market.insight`, `filing.collected`)와 두 판의 tick을 넣고, 두 판의 `briefing.daily.generated`(계약 검사, 본문의 미국 티커 여부)와 발송기의 드라이런 로그(주소 마스킹)를 확인한 뒤 판 본문을 저장소 밖 파일에 씁니다. 실제 시세·뉴스 검색 API는 부르지 않습니다.

```bash
JAVA_HOME_21=<JDK 21 경로> scripts/manual-daily-briefing.sh                        # 가짜 LLM(로컬 가짜 서버)
JAVA_HOME_21=<JDK 21 경로> DAILY_E2E_REAL_LLM=1 ANTHROPIC_API_KEY=... scripts/manual-daily-briefing.sh   # 실제 Claude
JAVA_HOME_21=<JDK 21 경로> DAILY_E2E_SMTP_SINK=1 scripts/manual-daily-briefing.sh              # 발송기(셸의 SPRING_MAIL_* 는 지우고 SPRING_APPLICATION_JSON 으로 호스트·포트를 넣음)가 127.0.0.1 가짜 SMTP 수신 서버로만 보내고 받은 .eml 을 결과 폴더에 저장
```

- 기본은 로컬 루프백의 가짜 Claude 서버를 SDK 주소(`ANTHROPIC_BASE_URL`)로 지정해 실제 호출이 없습니다. `DAILY_E2E_REAL_LLM=1`이면 셸 환경변수 `ANTHROPIC_API_KEY`로 실제 Claude를 부릅니다(키는 출력하지 않음).
- 서비스 DB 계정은 `.env`(또는 `DAILY_E2E_ENV_FILE`)의 `MARKET_/NEWS_/BRIEFING_DB_USER·PASSWORD`를 읽고, 판 본문은 `DAILY_E2E_OUT_DIR`(기본 임시 폴더, 저장소 안은 거절)에 씁니다.
- 결과에는 Claude 호출 수(브리핑 로그에서 Claude SDK의 HTTP 요청 줄을 셈, 다시 보낸 요청 포함)와 드라이런 메일마다 제목·HTML 글자 수·그림 수와 크기가 나옵니다. 가짜 모드에서는 가짜 서버가 받은 호출 수도 함께 나옵니다.

## 이메일 발송기 (`notifier`)

`briefing.generated`(급변동 건별)와 `briefing.daily.generated`(데일리 브리핑 판) → 이메일. **기본은 드라이런**(로그만 남기고 아무것도 보내지 않음)이며, 켜려면 `.env`에 아래를 채웁니다. 기본값은 `application.yml`이 원본입니다.

| 설정 | 설명 |
|---|---|
| `NOTIFIER_SEND_ENABLED` | `true`일 때만 실제 발송 |
| `NOTIFIER_SENDER` | 발신 주소. **개인정보이므로 `.env`에만** 둡니다. SES 샌드박스에서는 받는 사람 주소도 SES에서 인증된 주소여야 함 |
| `NOTIFIER_DB_USER` / `NOTIFIER_DB_PASSWORD` | 받는 사람을 고르려고 `personal` 스키마와 `market.etf`·`market.stock`을 읽는 읽기 전용 계정. 받는 주소는 설정값이 아니라 `personal.member_mail`에 있고, 메일을 보내기 직전에 읽습니다 |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USERNAME` / `SMTP_PASSWORD` | SES 콘솔의 "SMTP settings"에서 만든 전용 자격증명(STARTTLS) |
| `NOTIFIER_SUBJECT_PREFIX` | 제목 머리말. 데이터가 가짜인 동안 진짜 투자 정보로 오해하지 않도록 |

발송기는 데일리 브리핑 결과 토픽 `briefing.daily.generated`도 브리핑과 같은 파티션 수로 선언합니다(`Topics`). 이 토픽의 메시지 한 건(멤버 한 명의 판 하나)마다 메일 한 통을 만들어 이벤트의 멤버 번호(`memberId`)가 가리키는 멤버에게만 보냅니다(`DailyBriefingListener`). 그 멤버가 사용 중이고 그 판 수신을 켰을 때만 보내며 아니면 경고만 남깁니다. 멤버 번호가 없는 v1 이벤트는 받는 사람을 알 수 없어 건너뜁니다. 제목은 머리말 뒤에 판 종류와 날짜(이번 발송 시각의 한국 날짜)를 붙인 `[시뮬레이션] 아침 판 데일리 브리핑 2026-10-20` 꼴이고, 본문은 받은 글 뒤에 자료 없음 항목, 다룬 범위, 추적 ID, 생성 방식을 붙입니다. 메시지에 선택 칸 `html`이 있으면 글자판과 HTML, 인라인 그림(`images`)을 묶은 메일로 보내고 같은 꼬리를 HTML 끝에 작은 글씨로 붙이며, 없으면 글자 메일입니다. `images` 중 잘못된 그림(깨진 base64, png·jpeg·gif가 아닌 형식, 공백·꺾쇠·줄바꿈이 든 Content-ID, 빈 데이터)은 그 그림만 빼고 나머지로 묶음 메일을 그대로 보내며, 모두 잘못돼도 HTML 묶음 메일은 나갑니다. 뺀 그림 수와 이유 종류만 경고 로그에 남깁니다. 드라이런 로그에는 HTML 글자 수와 그림 크기만 남기고 내용은 찍지 않습니다.

받는 사람 조회가 DB 오류로 실패하면 예외가 그대로 올라가 Kafka 오류 처리가 몇 초 간격으로 세 번 다시 시도하고, 그래도 실패하면 그 메시지는 건너뜁니다. 즉 DB 오류가 재시도 시간(약 9초)보다 길면 그 메일은 보내지지 않습니다(재시도 시간은 아직 바꾸지 않았습니다).

안전 규칙(모두 테스트로 검증): 설정이 없으면 드라이런, 발송을 켰는데 주소/호스트가 비어 있으면 **시작 단계에서 실패**, 같은 브리핑은 한 번만 발송(건별·데일리 모두 `eventId` 기준), 필수 값이 빠지거나 형식이 잘못된 메시지는 보내지 않고 건너뜀, 로그에는 주소를 **마스킹**해서만 출력.

> **`.env`에는 ASCII 문자만 쓰세요.** 스프링이 이 파일을 Latin-1로 읽어 한글 값이 깨집니다. 한글 기본값은 `application.yml`에 있습니다.

```bash
java -jar services/notifier/build/libs/notifier-0.0.1-SNAPSHOT.jar   # 저장소 루트에서
```

## 수집기의 외부 데이터 설정 (`collector`)

수집기에는 공시, 시세, 뉴스 검색 클라이언트가 있습니다. 시세 기본은 가짜 데이터(시뮬레이터)이며, `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 외부 API를 씁니다. 뉴스는 15분 뉴스 수집(`collect-news`)마다 검색해 기사마다 `news.collected`를 발행하고, 기사 전송이 모두 확정된 뒤 `collection.completed`를 한 건 발행합니다([ADR-010](adr/010-event-format.md)). 브리핑의 재검색 요청을 받으면 요청한 검색어로 같은 방식으로 검색해 기사에 재검색 작업 id를 싣고, 끝나면 `jobType` `research-news`로 완료 알림을 보냅니다. 검색 한 번(정기 수집·재검색 모두)마다 거른 기사 건수를 이유별(기간 밖 `tooOld`·`tooFarAhead`, 검색어 낱말 불일치 `unmatched`, 통과한 뒤 앞서 모은 기사와 합쳐진 중복 `duplicates`)로 한 줄 `news filtered ...` 로그에 남기며 검색어와 종목은 쓰지 않습니다. DART 공시는 아직 조회 클라이언트까지만 구현되어 있고 이벤트로 발행하지 않습니다. 미국 종목 공시(SEC EDGAR)는 아래 장 시작 전 수집에서 `filing.collected`로 발행합니다. 기본값은 `application.yml`의 `collector.*`가 원본입니다.

수집 작업 다섯 개(`collect` 1초 현재가, `collect-news` 뉴스, `collect-pre-open` 장 시작 전, `collect-intraday` 장중, `collect-post-close` 마감 후)는 수집기 안 타이머가 직접 실행합니다([ADR-012](adr/012-collector-timers-and-command-topic.md)). 같은 작업은 겹쳐 돌지 않아 앞 회차가 끝나지 않았으면 이번 회차를 건너뛰고 로그를 남기며, 밀린 회차를 뒤에 몰아서 실행하지 않습니다. 1초 현재가는 건너뛴 회차를 종목마다 `TICK_SKIPPED`(`exceptionType=SupersededByNewerTick`) 실패 이벤트로도 남겨 데일리 메일 서버 상태에 보입니다. 타이머 스레드가 여러 개(`spring.task.scheduling.pool.size`)라 오래 걸리는 작업이 1초 현재가와 뉴스를 막지 않습니다. 수집기가 보내는 `schedule.tick`은 다른 서비스로 가는 신호(데일리 판 시작 2개, 구성 종목 조회 `collect-members`)뿐입니다.

**수동 실행:** 작업 하나를 지금 1회 돌리려면 `scripts/collector-command.sh <명령>`을 씁니다. 명령은 위 다섯 이름이고, 스크립트가 `collector.command` 토픽으로 명령 한 건을 보냅니다. 수집기는 받은 즉시 시세 시간대와 타이머 켜기 설정에 상관없이 그 작업을 실행합니다. 같은 작업이 돌고 있으면 건너뛰고(1초 현재가도 건너뜀 이벤트는 남기지 않음), 모르는 명령은 무시하며, 둘 다 수집기 로그에만 남습니다. 같은 `eventId`는 한 번만 실행하고, 수집기가 꺼져 있던 동안 보낸 명령은 켜진 뒤 실행하지 않습니다. 환경변수 `KAFKA_CONTAINER`(기본 `stock-kafka`)와 `KAFKA_BOOTSTRAP`(컨테이너 안에서 붙을 주소, 기본 `localhost:9092`)은 `scripts/publish-tick.sh`와 같습니다. `publish-tick.sh`는 남는 tick(`daily-briefing-morning`, `daily-briefing-post-close`, `collect-members`)을 보낼 때만 씁니다.

```bash
scripts/collector-command.sh collect        # 1초 현재가 한 회차
scripts/collector-command.sh collect-news   # 뉴스 수집 한 번
```

| 설정 | 설명 |
|---|---|
| `COLLECTOR_MARKET_DATA_SOURCE` | `simulator` 또는 `broker` |
| `BROKER_BASE_URL` / `BROKER_APP_KEY` / `BROKER_APP_SECRET` | 시세 제공자의 주소와 키. `broker`일 때 필요 |
| `COLLECTOR_SCHEDULE_ENABLED` | `true`일 때만 수집기 타이머가 1초 현재가와 15분 뉴스를 수집합니다. 코드 기본은 꺼짐입니다 |
| `COLLECTOR_TICK_INTERVAL_MS` | 1초 현재가 타이머 주기(밀리초). 타이머(`COLLECTOR_SCHEDULE_ENABLED=true`)가 켜져 있을 때만 쓰입니다. 회차의 기준 시각(`scheduledFor`)이 초 단위라 1000보다 작게 두면 같은 초의 회차가 같은 기준 시각과 correlationId를 씁니다 |
| `COLLECTOR_QUOTE_PER_TICK` | 1초 현재가 한 회차에서 부르는 종목 수(기본 8). 기본을 상한(기본 10)과 같게 두지 않는 이유: 같게 두면 회차 사이 타이밍 어긋남과 장중 5분마다 도는 다른 조회(환율·시장종합·세계지수)가 같은 상한을 써서 일부 호출을 놓치므로, 상한이 이 값의 1.2배 이상이 되게 여유를 둡니다(가짜 서버 부하 실측). 종목이 이보다 많으면 회차마다 목록을 돌며 번갈아 부르고, 차례가 아닌 종목은 그 회차에 아무 이벤트도 남기지 않습니다. 종목당 조회 간격은 올림(종목 수 / 이 값) x `COLLECTOR_TICK_INTERVAL_MS`이며 시작 로그에 남습니다. 앞 회차가 끝나지 않아 건너뛴 회차는 차례를 넘기지 않고 그 회차 차례 종목만 `SupersededByNewerTick`으로 기록합니다. 1 미만이거나 `BROKER_MAX_CALLS_PER_SECOND`보다 크면 시작하지 못합니다 |
| `COLLECTOR_QUOTE_START` / `COLLECTOR_QUOTE_END` | 1초 현재가 타이머(`collect`)가 조회하는 시간대(한국 시간 `HH:mm`, 양끝 포함, 끝 시각은 분 단위라 `15:30`이면 15:30:59까지). 평일 이 시간대 밖에서는 조회하지 않습니다. 장 밖에서는 현재가가 전일 종가 그대로라 조회해도 쓸 값이 없고, 그 시간에도 응답 칸이 깨져 오기 때문입니다. 뉴스 수집(`collect-news`)과 수동 실행 명령은 이 시간대와 상관없이 돕니다. 휴장일에는 조회하지 않습니다(장 운영 상태 조회로 판정, 아래 `BROKER_MARKET_STATUS_*`) |
| `BROKER_MARKET_STATUS_PATH` / `BROKER_MARKET_STATUS_TODAY_FIELD` / `BROKER_MARKET_STATUS_BUSINESS_DAY_FIELD` | 휴장일 판정에 쓰는 장 운영 상태 조회의 경로와 응답 칸 이름(현재 일자, 기준 영업일). 값은 제공자 문서에서 확인해 .env·Parameter Store에 넣으며 저장소에는 두지 않습니다. 하나라도 비어 있으면 시작할 때 경고를 한 번 남기고 늘 영업일로 봅니다 |
| `BROKER_MARKET_STATUS_TIMEOUT` | 휴장일 한 번 판정에 쓰는 전체 시간 상한(기본 `5s`). 넘으면 남은 시도를 하지 않고 영업일로 봅니다. 모두 실패한 직후 1분은 다시 묻지 않습니다 |
| `BROKER_MAX_CALLS_PER_SECOND` | 초당 시세 호출 상한. 회차당 호출 수(`COLLECTOR_QUOTE_PER_TICK`)가 이보다 크면 시작하지 못합니다. 회차의 차례였는데 다른 조회와 겹쳐 상한에 걸린 종목은 그 회차에서 호출하지 않고 `TICK_SKIPPED` 실패 이벤트로 기록합니다. 상한 때문임은 `exceptionType=RateLimited`로 구분하며, 나중에 계약에 정식 실패 종류가 생기면 바꿀 수 있습니다. **회차당 호출 수와 같게 두면 오래 돌 때 일부 호출을 놓치고 처리도 느려지므로 회차당 호출 수(`COLLECTOR_QUOTE_PER_TICK`)의 1.2배 이상으로 두세요**(가짜 서버 부하 실측). 값을 올릴 때는 제공자 한도와 실측한 안전 속도를 넘지 않게 합니다(`BrokerProperties` 주석) |
| `BROKER_PRICE_CHECK_ENABLED` | 기본 `true`. 시세 응답마다 현재가를 같은 응답의 전일대비·등락률·고가·저가와 맞춰 보고, 판단할 수 있는 검사 중 과반이 어긋나면 시세 이벤트를 보내지 않고 `FIELD_MISMATCH` 실패 이벤트로 기록합니다. 어느 검사에 걸렸는지는 `exceptionType=FieldMismatch:CHANGE+RATE`처럼(`CHANGE` 전일대비, `RATE` 등락률, `HIGH_LOW` 고가·저가), 응답 원문은 `responseRaw`에 담깁니다(토큰 값은 가림). 비교 칸이 비었거나 숫자가 아닌 검사는 판단하지 않고, 판단할 검사가 하나도 없으면 통과시킵니다. 로그에는 종목 코드와 검사 이름만 남습니다. `false`면 검사하지 않습니다 |
| `BROKER_PRICE_CHECK_TOLERANCE` | 위 검사의 허용 오차(가격 비율, 기본 `0.003` = 0.3%, 0 이상 1 미만이 아니면 시작 단계에서 실패). 호가 사이 가운데 값으로 체결되면 현재가가 호가 단위 사이 값으로 오는데 전일대비·등락률은 호가 단위 가격으로 계산돼 와서, 0이면 멀쩡한 값도 버립니다 |
| `BROKER_PRICE_CHECK_BASE_FIELDS` / `BROKER_PRICE_CHECK_CHANGE_FIELD` / `BROKER_PRICE_CHECK_CHANGE_SIGN_FIELD` / `BROKER_PRICE_CHECK_SIGN_UP` / `BROKER_PRICE_CHECK_SIGN_FLAT` / `BROKER_PRICE_CHECK_SIGN_DOWN` / `BROKER_PRICE_CHECK_RATE_FIELD` / `BROKER_PRICE_CHECK_HIGH_FIELD` / `BROKER_PRICE_CHECK_LOW_FIELD` | 위 검사가 읽을 응답 칸 이름(전일 종가, 전일대비, 전일대비 부호 구분, 등락률, 고가, 저가)과 부호 구분 칸의 상승·보합·하락 값. 전일 종가 칸과 부호 값은 쉼표로 여러 개를 줄 수 있고, 전일 종가 칸 중 하나라도 맞으면 전일대비·등락률 검사를 통과합니다. 기본값은 `services/collector/src/main/resources/application.yml`에 있고, 응답이 바뀌면 환경변수로 덮어씁니다. 비어 있는 검사는 판단하지 않으며, 모두 빈 값으로 덮어쓰면 시작할 때 경고를 한 번 남기고 검사 없이 통과시킵니다 |
| `NEWS_SEARCH_BASE_URL` / `NEWS_SEARCH_CLIENT_ID` / `NEWS_SEARCH_CLIENT_SECRET` | 뉴스 검색 제공자의 주소와 키 |
| `NEWS_SEARCH_ID_HEADER` / `NEWS_SEARCH_SECRET_HEADER` | 제공자가 정한 인증 헤더 이름 |
| `NEWS_SEARCH_CALL_INTERVAL_MS` | 검색어를 연달아 호출할 때 호출 사이의 대기(밀리초). 0이면 기다리지 않으며, 짧으면 제공자가 429로 거절할 수 있습니다 |
| `NEWS_RESEARCH_MAX_QUERIES` | 브리핑의 뉴스 재검색 요청(`news.research.requested`) 하나에서 실제로 검색할 검색어 수 상한(기본 3). 넘는 검색어는 버립니다. 검색어마다 하루 검색 호출 상한(`NEWS_SEARCH_DAILY_CALL_LIMIT`)을 한 번씩 쓰고, 상한에 걸리거나 검색이 실패하면 완료 알림의 `incompleteSymbols`에 요청 종목을 담습니다. 0 이하이면 시작 단계에서 실패합니다 |
| `DART_API_KEY` | 공시 API 키 |
| `EDGAR_USER_AGENT` | SEC EDGAR 요청의 User-Agent. SEC가 요구하는 `회사명 연락이메일` 형식입니다. 연락 이메일은 개인 정보이므로 `.env`에만 둡니다. 비어 있으면 공시를 조회하지 않습니다 |
| `EDGAR_FORMS` | 수집할 공시 형식. 쉼표로 구분합니다. 비어 있거나 형식이 틀리면 시작 단계에서 실패합니다 |
| `COLLECTOR_INSIGHT_SCHEDULE_ENABLED` | `true`일 때만 수집기 타이머가 장 마감 후 조회(`collect-post-close`)를 실행합니다. 휴장일에는 실행하지 않습니다(장 운영 상태 조회로 판정, 아래 `BROKER_MARKET_STATUS_*`) |
| `COLLECTOR_INSIGHT_CRON` | 마감 후 조회 시각. 한국 시간 기준 cron(초 분 시 일 월 요일) |
| `COLLECTOR_PRE_OPEN_ENABLED` | `true`일 때만 수집기 타이머가 장 시작 전 수집(`collect-pre-open`)을 실행합니다. 휴장일에는 실행하지 않습니다(장 운영 상태 조회로 판정, 아래 `BROKER_MARKET_STATUS_*`). 공시는 이 수집에서만 받으므로 꺼 두면 수동 실행 명령을 보내지 않는 한 공시를 수집하지 않습니다 |
| `COLLECTOR_PRE_OPEN_CRON` | 장 시작 전 수집 시각. 한국 시간 기준 cron |
| `COLLECTOR_MEMBERS_ENABLED` | `true`일 때만 ETF 구성 종목 조회 tick(`collect-members`)을 평일 하루 한 번 발행합니다. 코드 기본은 꺼짐입니다. 그날 실제로 받을지는 tick을 받은 시세처리기가 정합니다 |
| `COLLECTOR_MEMBERS_CRON` | 구성 종목 조회 tick 시각. 한국 시간 기준 cron(기본 평일 08:00). 휴장일에는 보내지 않습니다(장 운영 상태로 판정, 판정하지 못하면 영업일로 봅니다) |
| `COLLECTOR_DAILY_BRIEFING_ENABLED` | `true`일 때만 데일리 브리핑 시작 tick(아침 판 `daily-briefing-morning`, 마감 후 판 `daily-briefing-post-close`)을 발행합니다. 수집기는 신호만 보내고 자료를 새로 모으지 않습니다. 휴장일에는 발행하지 않습니다(장 운영 상태 조회로 판정, 아래 `BROKER_MARKET_STATUS_*`) |
| `COLLECTOR_DAILY_BRIEFING_MORNING_CRON` | 아침 판 시작 시각. 한국 시간 기준 cron이며 기본은 평일 09:10(`0 10 9 * * MON-FRI`)입니다. 장 시작 뒤 개장 흐름을 담으므로 장 시작(09:00)과 장 시작 전 수집 뒤로 둡니다. 바꾸면 브리핑의 급변동 합침 구간 끝(`DailySettings.morning_merge_end`)도 같이 고쳐야 합니다. `-`를 주면 이 판만 발행하지 않습니다(그러면 장 시작~09:10 급변동은 합쳐질 아침 판이 없어 09:40에 급변동 리포트로 나갑니다) |
| `COLLECTOR_DAILY_BRIEFING_POST_CLOSE_CRON` | 마감 후 판 시작 시각. 한국 시간 기준 cron이며 장 마감 후 조회 뒤로 둡니다. `-`를 주면 이 판만 발행하지 않습니다 |
| `COLLECTOR_INTRADAY_ENABLED` | `true`일 때만 수집기 타이머가 장중 수집(`collect-intraday`)을 실행합니다. 코드 기본은 꺼짐이고, 운영에서는 `true`로 켭니다 |
| `COLLECTOR_INTRADAY_CRON` | 장중 수집 주기. 한국 시간 기준 cron |
| `COLLECTOR_INTRADAY_START` / `COLLECTOR_INTRADAY_END` | 장중 수집 시작·끝 시각(`HH:mm`, 양끝 포함). 이 밖의 타이머 회차는 조회하지 않습니다 |
| `COLLECTOR_INTRADAY_STAGGER_MS` | 장중 조회 사이 대기(밀리초). 같은 초에 몰려 시세 조회가 호출 상한에 걸리지 않게 나눠 부릅니다 |

**뉴스 검색어와 기사 태그, 하루 검색 호출 수:**

- 기사 태그는 기사 주제, 곧 검색어에 걸린 종목(국내 코드, 미국 티커, `*`)입니다. ETF가 담은 구성 종목을 태그에 풀어 적지 않고, 뉴스 서비스가 시작할 때 `market.etf_stock_map`을 읽어 두었다가 기사를 꺼낼 때 넓힙니다(바뀐 구성은 재시작해야 반영). ETF로 꺼내면 그 ETF 태그 기사(`*` 포함)를 최신순으로 먼저, 그다음 구성 종목 태그 기사(앞의 기사와 겹치면 제외)를 최신순으로 놓고 요청 상한까지 자릅니다. 구성 종목 기사가 많아도 ETF 자체 기사가 먼저 남습니다.
- 검색어는 수집기가 시작할 때 `market.news_search_target` 뷰에서 한 번 읽습니다(바뀐 값은 재시작해야 반영). 종목 이름으로 검색하려면 그 종목의 `search_by_name`을 켭니다. 값은 SQL로 넣으며 예시는 `docs/examples/holdings.example.sql`입니다.
- 정기 뉴스 수집은 15분마다 검색어마다 한 번씩 호출하므로 하루(24시간) 96회 × 검색어 수만큼 하루 호출 상한(`NEWS_SEARCH_DAILY_CALL_LIMIT`, 기본 19500)을 씁니다. 재검색은 요청 하나에 검색어 최대 `NEWS_RESEARCH_MAX_QUERIES`(기본 3)개이고 브리핑 작업 하나가 요청을 최대 10번 보내 작업당 최대 30회를 씁니다. 검색어 수별 하루 정기 수집 호출 수(계산: 96 × 검색어 수):

| 검색어 수 | 하루 정기 호출 | 상한 대비 | 재검색에 남는 호출 |
|---|---|---|---|
| 20 | 1,920 | 9.8% | 17,580 |
| 50 | 4,800 | 24.6% | 14,700 |
| 100 | 9,600 | 49.2% | 9,900 |
| 150 | 14,400 | 73.8% | 5,100 |

  재검색을 빼고 정기 수집만으로 상한 안에 드는 검색어는 최대 203개이고(19500 ÷ 96), 재검색에 1,000회를 남겨 두면 192개입니다. 상한에 닿으면 남은 검색어를 건너뛰고 그 종목을 `incompleteSymbols`에 담으며 수집기 로그에 `news daily call limit reached`가 남습니다. 미국 티커 검색이 건너뛰어지거나 실패하면 그 티커를 담은 관심 ETF의 코드가 `incompleteSymbols`에 들어갑니다. 카운터는 메모리에만 있어 재시작하면 0으로 돌아갑니다.


장중 수집은 환율종합, 시장종합, 세계지수(아시아, 아메리카)를 시차를 두고 조회해 마감 후 조회와 같은 `market.insight`로 보냅니다. 시세 조회와 같은 호출 상한을 거치며, 휴장일에는 발행하지 않습니다. 기본값은 `application.yml`의 `collector.intraday.*`입니다.

마감 후 조회(일봉, 세계지수, 환율종합, 시장종합)는 `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 동작하며, 응답 원문을 `market.insight` 이벤트로 발행합니다. 일봉의 시장구분과 조회 건수, 세계지수의 대륙 목록은 `application.yml`의 `collector.insight.*`에 기본값이 있습니다.

장 시작 전 수집(`COLLECTOR_PRE_OPEN_CRON`, 평일 아침)은 국내 장중 변동의 근거로 쓸 직전 미국 장 결과와 밤사이 공시를 미리 받습니다. 한 회차(`collect-pre-open`)에서 아래 둘을 함께 받으며, 앞쪽이 실패해도 공시는 받습니다.

| 받는 것 | 조건 | 결과 |
|---|---|---|
| 세계지수 아메리카(`C`) 1건, 포트폴리오의 미국 종목과 포트폴리오 ETF가 담은 미국 구성 종목(DB의 `personal.collect_target`, 시작할 때 읽음) 해외 일봉 | `COLLECTOR_MARKET_DATA_SOURCE=broker` | 마감 후 조회와 같은 `market.insight`(`world-index`, `overseas-daily-price`)와 같은 테이블. 시세 조회와 같은 호출 상한을 거칩니다 |
| SEC EDGAR 공시 | `EDGAR_USER_AGENT`가 있을 때 | `filing.collected`. 접수 시각이 `collector.edgar.lookback-hours` 안인 공시만 발행합니다. 하루 한 번이라 주말과 연휴를 넘겨도 빠지지 않게 넉넉히 둡니다 |

공시 15분 주기 수집(`collect-filings`)은 없어졌습니다. SEC 공시 이력(submissions)은 조건 조회를 지원하지 않아 매번 최근 이력을 통째로 주므로(회사당 약 167KB, 압축 시 약 22KB, 2026-10-29 실측) 하루 한 번만 받고 gzip 압축을 요청합니다. 서버가 압축하지 않고 주면 그대로 읽습니다. 장 마감 후 수집(16:30)은 국내 일봉·세계지수·환율종합·시장종합만 받습니다. 국내 장 마감 무렵에는 미국 장이 열리지 않아 해외 일봉은 장 시작 전에만 받습니다.

해외 일봉의 거래소 코드는 SEC 티커·거래소 매핑 파일(`collector.edgar.ticker-map-url`)로 고릅니다. 이 파일은 하루 한 번 받고, 새로 받기에 실패하면 전에 받은 파일을 계속 씁니다. 처음 받기부터 실패하면 잠시 다시 받지 않고 해외 일봉 종목은 거래소 조회 실패로 기록됩니다.

해외 일봉 응답의 맨 앞 레코드는 정규장이 끝난 뒤 열리는 장외 거래의 진행 중 막대일 수 있어, 등락률은 `OverseasCloseRateQuery`가 레코드의 정규장 마감 시각으로 고릅니다. 세계지수 응답에는 전일 종가가 없어 미국 지수 등락률은 이 응답만으로 구할 수 없습니다.

시세 조회는 종목마다 병렬로 호출하고 실패해도 재시도하지 않으며 다음 회차에 맡깁니다. 성공하면 응답 원문을 `market.snapshot`의 `raw`에, 실패하면 종목마다 `market.snapshot.failed`를 발행합니다(실패 원문 포함, 키·시크릿·접근토큰 값은 가려서 담습니다). 로그와 예외 메시지에는 서버가 보낸 문구 대신 형식을 확인한 처리 코드(영숫자 8자 이하, 아니면 `invalid`)만 남습니다. 테스트는 로컬 가짜 HTTP 서버(`FakeBrokerServer`)를 쓰며 실제 서버를 호출하지 않습니다.

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

병렬·직렬 비교 시험 `BrokerSerialProbeTest`는 같은 종목들을 병렬(지금 수집기 방식, `BrokerMarketData`), 직렬(한 번에 한 종목), 동시 N개 제한(`limited`, 한 번에 최대 N개)으로 번갈아 조회해, 방식별 호출 수와 깨진 응답 건수(현재가 빈 값, 숫자 아님, 0 이하, 응답 칸 불일치)를 셉니다. 동시 N개 제한은 종목이 늘어도 1초 안에 끝내면서 동시 호출을 줄이는 방식이 깨진 응답을 줄이는지 보려는 것입니다. 응답 칸 불일치(`fieldMismatch`)는 모든 방식에서 수집기와 같은 응답 칸 검사(`BROKER_PRICE_CHECK_*`, 위 수집기 설정 표)로 판단하므로 수집기가 `FIELD_MISMATCH`로 버리는 응답과 기준이 같고, 걸린 검사 조합별 건수(`mismatchChecks`)도 함께 적습니다. 평소 건너뛰고 `BROKER_SERIAL_PROBE=1`일 때만 돌며, 모든 방식이 같은 호출 상한 장치를 거치고 토큰 발급과 현재가 조회만 합니다. 실제 서버를 호출하므로 필요할 때만 장중에 실행합니다. 가짜 서버로 같은 흐름을 리허설하는 `SerialProbeRehearsalTest`는 평소 테스트에서 돕니다.

| 환경변수 | 뜻 |
|---|---|
| `BROKER_BASE_URL`, `BROKER_APP_KEY`, `BROKER_APP_SECRET` | 주소와 키. Parameter Store에서 가져온 값을 환경변수로만 넘깁니다 |
| `BROKER_PROBE_SYMBOLS` | 종목(쉼표 구분). 다른 확인 도구와 같은 값을 씁니다 |
| `BROKER_SERIAL_PROBE_OUT` | 결과 요약 파일. 저장소 안의 경로는 호출 전에 거절합니다 |
| `BROKER_SERIAL_PROBE_MODES` | 돌릴 방식(쉼표 구분, 기본 `parallel,serial,limited`) |
| `BROKER_SERIAL_PROBE_CONCURRENCY` | 동시 N개 제한 방식의 N(기본 3) |
| `BROKER_SERIAL_PROBE_ROUNDS` / `BROKER_SERIAL_PROBE_SECONDS` | 라운드 수(고른 방식을 한 번씩 도는 것이 한 라운드, 기본 350)와 시간 상한(초, 기본 1200 = 20분). 먼저 닿는 쪽에서 멈춥니다. 0.2% 수준의 깨짐을 가리려면 방식별 수천 회가 필요해, 종목 십여 개·응답 0.1초면 20분 안에 방식별 3천 회 남짓을 부르는 값으로 정했습니다. 세 방식을 다 돌면 시간 상한에 먼저 닿아 방식별 호출이 조금 줄어듭니다. 방식 사이 쉼 덕분에 평균은 초당 5~8회이고, 어느 1초에도 호출 상한을 넘지 않습니다 |
| `BROKER_SERIAL_PROBE_PAUSE_MS` | 방식 하나를 돈 뒤 쉬는 시간(기본 1000). 호출 상한의 1초 창을 비웁니다 |
| `BROKER_MAX_CALLS_PER_SECOND` | 호출 상한. 없으면 수집기 기본값 |
| `BROKER_PRICE_CHECK_*` | 응답 칸 검사 설정. 수집기와 같이 `application.yml`로 묶어 읽으므로 따로 넣지 않으면 수집기와 같은 값을 씁니다. 검사가 꺼졌거나 칸 이름이 모두 비면 칸 불일치는 세지 않습니다(`fieldCheck=off`) |

```bash
BROKER_SERIAL_PROBE=1 BROKER_SERIAL_PROBE_OUT=<저장소 밖 경로>/serial-probe.txt \
  ./gradlew :services:collector:test --tests '*BrokerSerialProbeTest' --no-build-cache
```

- 결과 파일과 표준 출력에는 방식별 건수만 쓰고 종목 코드, 시세 값, 응답 원문, 키·토큰은 남기지 않습니다. 시험 동안에는 종목 코드가 들어가는 수집기 실패 로그를 끕니다.
- 센 대상이 아닌 실패(토큰 발급 실패, HTTP 오류, 한도 초과 등)가 한 번이라도 나오면 다음 호출을 하지 않고 멈추며 `stop=`에 실패 종류를 적습니다.
- 방식마다 한 라운드의 종목 조회가 모두 끝나는 데 걸린 시간을 재어 `roundMs(avg/p50/max)`(ms)로 적고, 라운드 줄에도 `ms=`로 남깁니다. 종목이 9개이면 병렬이 동시 9, `limited`(`BROKER_SERIAL_PROBE_CONCURRENCY=3`)가 동시 3, 직렬이 동시 1이라 동시 1·3·9 세 가지를 한 번에 비교합니다. 호출 상한에 걸려 기다리거나 건너뛴 회차와 이상 응답으로 멈춘 회차는 회차 시간에서 빼고, 뺀 회차 수를 요약 줄에 `roundMsExcluded=N`으로 적습니다. 짝수 개수의 `p50`은 가운데 두 값의 평균입니다. 병렬은 수집기 안에서 상한 자리를 잠깐(최대 0.3초) 기다려도 뺄 회차로 잡히지 않으므로, 방식 사이 쉼(`BROKER_SERIAL_PROBE_PAUSE_MS`)을 1초 이상 두어 앞 방식의 호출이 상한 창에서 빠진 뒤 시작하게 합니다.
- 첫 호출이 토큰을 발급받으므로 라운드 전에 현재가를 한 번 불러 토큰을 받아 둡니다. 이 호출은 깨진 응답이 와도 방식별 건수에 넣지 않고 머리줄에 `warmUpCalls=`로 적으며, 여기서 이상 응답이 나오면 라운드를 시작하지 않습니다.
- 라운드마다 시작 방식을 하나씩 밀어 앞 방식의 영향을 고르게 나눕니다. 병렬에서 상한 때문에 보내지 못한 종목은 호출 수가 아니라 `rateLimited`로 셉니다.

## 설정 값 가져오기 (Parameter Store, 선택)

메일 설정(SMTP 호스트·포트·계정, 발신·수신 주소) 6개를 AWS Systems Manager Parameter Store에 두고 `.env`로 가져옵니다. 수집기의 키(`/stock-reading/dev/collector/` 아래, 이름 대응은 `scripts/load_config.py`의 `COLLECTOR_ENV_NAMES`)와 브리핑의 LLM 키(`/stock-reading/dev/briefing/anthropic-api-key` → `ANTHROPIC_API_KEY`)도 같은 방식으로 가져옵니다. 두 그룹은 선택이라 없어도 메일 설정 반영은 막지 않으며, LLM 키가 없으면 브리핑은 규칙 기반 리포트를 만듭니다. 서비스는 계속 환경변수만 읽습니다.

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
- 자격증명은 `AWS_PROFILE` 환경변수나 `--profile` 옵션으로 지정합니다. 지역은 `--region`, `AWS_REGION`, 프로필에 정한 지역 순으로 정합니다.
- 읽기 전용 IAM 자격증명이 필요합니다: [`infra/iam/README.md`](../infra/iam/README.md)

### 업무 데이터와 DB 계정

종목, 구성 종목, 뉴스 검색어, 멤버, 메일 수신 설정, 포트폴리오 같은 업무 데이터는 환경변수나 파일이 아니라 DB가 원본입니다. 비밀값과 실행 설정(Parameter Store → 로더 → `.env`)과 나뉘며, 값은 사람이 SQL로 넣고 서비스는 재시작할 때(발송기는 메일 보내기 직전에) 읽습니다. 이 방식으로 보유 종목 파일 설정, 펀드 번호 설정, 수신자 설정은 없어졌습니다. 수집기(수집 대상, 뉴스 검색어)와 뉴스(ETF 구성 종목)도 시작할 때 DB를 읽기 전용으로 읽고, 종목·검색어를 환경변수나 이벤트로 받던 방식은 없어졌습니다.

| 스키마 | 표 | 내용 |
|---|---|---|
| `market`(주인: 시세처리기) | `etf`, `stock`, `etf_stock_map` | ETF, 주식, ETF가 담은 종목(비중·순위는 연결 표). 한 주식이 여러 ETF에 담길 수 있습니다. `etf.constituent_source_id`가 있는 ETF만 구성 종목을 자동 조회하고, `search_by_name`을 켠 종목은 이름을 뉴스 검색어로 씁니다 |
| `market` | `news_keyword`, `macro_keyword`, `news_keyword_map`, `macro_keyword_map`, 뷰 `news_search_target` | 뉴스 검색어와 거시 주제(환율·금리 등), 검색어가 속한 대상(ETF·주식·거시 주제 중 정확히 하나). 뷰는 검색어와 기사에 달 종목 태그를 계산해 줍니다(거시 주제는 연결된 종목, 연결이 없으면 시장 전체 `*`) |
| `personal`(주인: `PERSONAL_OWNER_DB_USER`) | `member`, `member_mail`, `member_portfolio`, 뷰 `active_portfolio`, `collect_target` | 멤버, 멤버의 메일 주소·수신 설정(급변동·아침 판·마감 후 판), 멤버가 담은 ETF·주식(`HELD` 보유 / `WATCH` 관심). 뷰 `active_portfolio`는 사용 중인 멤버의 포트폴리오만, `collect_target`은 사용 중인 멤버 포트폴리오를 합쳐 국내 1초 시세 대상(`KR_QUOTE`)과 미국 일봉 대상(`US_DAILY`, 포트폴리오 ETF의 미국 구성 종목 포함)을 줍니다 |

DB 계정 키(값은 `.env`에만 두고 문서에는 적지 않습니다. 형식은 `.env.example`):

| 키 | 설명 |
|---|---|
| `PERSONAL_OWNER_DB_USER` / `PERSONAL_OWNER_DB_PASSWORD` | `personal` 스키마의 주인 계정. 마이그레이션(Flyway)이 이 계정으로 표를 만듭니다 |
| `PERSONAL_WRITER_DB_USER` / `PERSONAL_WRITER_DB_PASSWORD` | 값을 넣고 고치는 계정(사람이 SQL을 실행할 때 씁니다). `personal` 전체를 읽고 쓰며 `market.etf`·`market.stock`을 읽습니다 |
| `COLLECTOR_DB_USER` / `COLLECTOR_DB_PASSWORD` | 수집기 계정. `personal.collect_target`과 `market.news_search_target` 두 뷰만 읽습니다 |
| `NOTIFIER_DB_USER` / `NOTIFIER_DB_PASSWORD` | 발송기 계정(읽기 전용). 받는 사람을 고르려고 `member`, `member_mail`, `member_portfolio`와 `market.etf`·`market.stock`을 읽습니다 |
| `BRIEFING_DB_USER` / `BRIEFING_DB_PASSWORD` | 브리핑 계정. 데일리 판을 멤버마다 짜려고 열 단위로만 읽습니다: `member`(`id`, `enabled`), `member_mail`(`member_id`, `enabled`, `receive_morning`, `receive_post_close`), `member_portfolio`(`member_id`, `etf_id`, `relation`), `market.etf`(`id`, `market`, `symbol`). 메일 주소(`member_mail.email`)와 멤버 이름은 읽지 못하고 `personal`에 쓰지도 못합니다 |

SQL로 값을 넣는 예시(모두 가짜 값입니다. 전체는 [`docs/examples/holdings.example.sql`](examples/holdings.example.sql)):

```sql
-- market 계정: ETF와 주식
INSERT INTO market.etf (market, symbol, name) VALUES ('KR', 'TEST01', 'TEST ETF ONE');
INSERT INTO market.stock (market, symbol, name) VALUES ('US', 'FAKEA', 'FAKE CORP A');
-- 쓰기 계정: 멤버, 메일 수신 설정, 포트폴리오
INSERT INTO personal.member (name) VALUES ('TEST MEMBER');
INSERT INTO personal.member_mail (member_id, email)
SELECT id, 'member@example.com' FROM personal.member WHERE name = 'TEST MEMBER';
INSERT INTO personal.member_portfolio (member_id, etf_id, relation)
SELECT m.id, e.id, 'HELD' FROM personal.member m, market.etf e WHERE m.name = 'TEST MEMBER' AND e.symbol = 'TEST01';
```

- **계정과 권한:** `member_mail`(개인정보)의 메일 주소 열은 발송기(`NOTIFIER_DB_USER`)와 쓰기 계정(`PERSONAL_WRITER_DB_USER`)만 읽습니다(브리핑 계정은 번호·수신 설정 열만 읽습니다). 시세처리기는 사용 중인 멤버의 포트폴리오만 보여 주는 뷰 `personal.active_portfolio`만 읽어 포트폴리오에 담긴 ETF를 찾고(`member`·`member_mail`·`member_portfolio`는 읽지 못합니다. 보유·관심 구분은 읽지 않습니다), 수집기(`COLLECTOR_DB_USER`)는 `personal.collect_target`과 `market.news_search_target` 두 뷰와 구성 종목 표(`market.etf`, `market.stock`, `market.etf_stock_map`)를 읽고, 뉴스(`NEWS_DB_USER`)는 구성 종목 표만 읽습니다. 수집기·뉴스는 읽기만 하며 쓰기와 `member_mail` 읽기는 거부됩니다. 계정과 권한은 `infra/db/personal/init.sql`이 만들고 표는 `infra/db/personal/migration/`의 Flyway 파일이 만듭니다. 모든 키는 `.env.example`에 있습니다.
- **만드는 순서:** `personal` 표가 `market.etf`·`market.stock`을 가리키므로 market 표 → 계정·권한 → personal 표 순서입니다. 로컬 Compose는 `docker compose -f infra/compose/docker-compose.yml --env-file .env up -d`가 `market-migrate` → `personal-init` → `personal-migrate`를 차례로 한 번씩 실행합니다. 번호 파일을 더한 뒤에는 `run --rm market-migrate`와 `run --rm personal-migrate`로 다시 적용합니다. 브리핑 계정에 `personal` 읽기 권한을 준 뒤(`init.sql`과 `V2__grant_briefing_read.sql`)에는 이미 만든 DB에도 `run --rm personal-init`과 `run --rm personal-migrate`를 차례로 다시 돌려야 브리핑이 멤버를 읽을 수 있습니다(`init.sql`은 여러 번 돌려도 같습니다). `scripts/test-all.sh`와 `scripts/manual-daily-briefing.sh`는 같은 순서를 `scripts/personal-db-lib.sh`로 테스트 DB에 적용합니다.
- **기존 DB 이전:** market의 V10 마이그레이션이 옛 `instrument`·`etf_member`와 이력(`etf_member_history`→`etf_stock_history`)·원문(`etf_member_raw`→`etf_stock_raw`)의 기존 행을 새 표로 옮깁니다. 단 보유·관심 구분과 ETF의 업종은 새 표에 칸이 없어 옮겨지지 않으므로 포트폴리오와 `constituent_source_id`는 아래 SQL로 다시 넣습니다.
- **처음 값 넣기:** 값은 저장소 밖 비공개 SQL로만 넣습니다. 형식은 가짜 값 예시 [`docs/examples/holdings.example.sql`](examples/holdings.example.sql)을 따르고, market 표는 시세처리기 계정으로, `personal` 표는 쓰기 계정으로 `psql -v ON_ERROR_STOP=1 -U <계정> -d <데이터베이스> -f <파일>`로 실행합니다. 나중에 값을 바꾸는 기능(CUD)은 따로 만듭니다.
- **반영 시점:** 수집기와 뉴스는 시작할 때 한 번 읽으므로 바뀐 값은 재시작해야 반영되고, 읽지 못하면 시작을 멈춥니다(`personal` 스키마와 계정이 먼저 있어야 합니다). 시세처리기는 배경 자료·데일리 ETF 목록이 필요할 때 `personal.active_portfolio`와 market 표를 그때그때 읽습니다. 발송기는 메일을 보내기 직전에 받는 사람을 고릅니다: 급변동은 사용 중이고 급변동 수신을 켠 멤버 중 그 종목을 포트폴리오에 가진 멤버, 아침·마감 후 판은 이벤트가 가리킨 멤버 한 명(사용 중이고 그 판 수신을 켰을 때)입니다. 0명이면 보내지 않고 경고만 남기며 로그의 주소는 가립니다.
- **응답:** `market.context.responded`와 `market.daily.responded`의 `etf`(선택)에 ETF 이름, 추종 대상, 업종과 구성 종목(시장, 심볼, 이름, 비중, 순위)이 담깁니다. ETF 표에는 업종 칸이 없어 업종은 비어 있습니다. 변동 종목이 ETF가 아니면 `etf`가 없습니다. `market.daily.responded`(v2)의 `etf`에는 판 요청의 시간 범위 안에 상위 구성 변경이 반영됐을 때만 `memberChanges`(`since`, `asOf`, `removed`, `added`, `moved`)가 더해지며, 바뀐 것이 없으면 칸이 없습니다.
- **데일리 판의 멤버별 맞춤:** 글은 ETF마다 한 번 쓰고 메일은 멤버마다 그 멤버의 ETF·보유/관심·차트로 나갑니다(위 브리핑 절). LLM이 멤버마다 글을 새로 쓰는 것은 배포 뒤 과제입니다.

### ETF 구성 종목 자동 조회

`collect-members` tick(수집기가 평일 하루 한 번 발행)을 시세처리기가 받아, 출처 번호(`etf.constituent_source_id`)가 있는 ETF의 상위 구성 종목(주식만, 비중 상위 10개)을 받아 바뀐 것만 반영합니다. 전용 consumer group(`market-members`)으로 받습니다. `schedule.tick`은 파티션이 1개라 다른 리스너와 그룹이 같으면 tick이 누락되기 때문입니다.

- **주기:** 평소에는 한 주에 한 번(`market.members.fetch-day`), 지수 교체일이 든 달에는 매일 받습니다. 교체 일정은 `index_rebalance_schedule` 테이블(`tracks`, `effective_date`, 선택 `note`)에 사람이 1년에 한 번 직접 넣습니다. `tracks`는 `etf.tracks`와 같은 값이어야 ETF와 이어집니다. 일정이 없는 ETF는 주 1회만 받습니다. 아직 구성을 한 번도 받지 않은 ETF는 요일을 기다리지 않고 다음 조회 때 바로 받습니다.
- **반영:** 받은 상위 구성의 종목·순위가 이전과 다를 때만 `etf_stock_history`에 새 행을 넣고, 같은 때 `etf_stock_map`(현재 구성) 줄도 맞춥니다. 종목이 들어오거나 나가거나, 상위 10개 안의 순위만 바뀌어도 바뀐 것으로 봅니다. 비중과 이름은 비교하지 않습니다. 그대로면 이력 행의 마지막 확인일(`last_seen_on`)만 고치고, `etf_stock_map`의 비중(`weight`)은 새로 받은 값으로 맞춥니다. 이미 이름이 있는 주식의 이름은 출처 이름으로 바꾸지 않고 이름이 빈 행에만 채웁니다. 새 구성 종목은 `stock` 행으로 넣고, 구성에서 빠진 주식도 `stock` 행은 지우지 않습니다(개인 포트폴리오가 가리킬 수 있어서). 바뀐 `etf_stock_map`은 뉴스와 수집기가 다음에 시작할 때 읽어 반영합니다.
- **원문 보관:** 받을 때마다 응답 원문을 그대로 `etf_stock_raw`(ETF, 기준일, 상태 코드, 원문, 받은 시각)에 저장합니다. 상태가 200이 아니거나 모양이 달라 실패한 응답도 해석 전에 저장합니다(너무 큰 응답은 제외). `etf_stock_history`의 구성은 이 원문에서 주식만 골라 상위 10개로 줄인 계산 결과입니다. 행은 받을 때마다 늘며 지우는 절차는 아직 없습니다.
- **실패:** 출처가 응답하지 않거나, 상태가 200이 아니거나, 응답 모양이 다르거나, 주식 행이 하나도 없으면 그 ETF는 이전 구성을 그대로 두고 로그에 오류 종류만 남기며(주소·종목코드는 남기지 않음) 다음 ETF로 넘어갑니다. 비중이 숫자가 아니거나 0~100을 벗어난 응답도 모양이 다른 응답으로 봅니다.
- **수동 ETF:** 출처 번호가 없는 ETF는 자동으로 받지 않고, `etf_stock_map`을 직접 고칩니다.
- **판에 싣기:** 데일리 응답은 판 요청 시간 범위 안에 반영된 변경만 `memberChanges`로 싣습니다. `since`는 바뀌기 전 구성을 마지막으로 확인한 날, `asOf`는 바뀐 구성을 처음 받은 날입니다. 범위 안에 변경이 여러 번이면 첫 변경 직전 구성과 마지막 구성을 비교하고, 되돌아왔으면 싣지 않습니다.
- **조회 기준:** 출처에는 오늘 직전 평일을 기준일로 요청합니다. 공휴일은 따로 보지 않으며, 그날 자료가 없어 비어 오면 실패로 처리됩니다. 주식만 거르는 기준은 "비중이 있고 종목코드가 주식 코드 모양"이며, 시장은 코드가 숫자로 시작하는 6자리면 `KR`, 미국 티커 모양(영문 대문자 1~5자, 종류 구분 `-`·`.` 1자)이면 `US`이며(출처가 해외 종목코드를 `<티커> US Equity`처럼 거래소 꼬리를 붙여 주면 꼬리를 떼고, 종류 구분 `/`는 `-`로 바꿉니다) 그 밖의 코드(숫자 4자리 등 다른 해외 코드)는 후보에서 빼고 개수만 로그에 남깁니다. 같은 종목코드가 응답에 두 행 이상이면 비중을 합쳐 한 번만 넣습니다. 마지막 확인일이 7일 넘게 지난 ETF는 조회 요일과 상관없이 받을 차례로 봅니다(공휴일 다음 조회가 실패해도 다음 주까지 기다리지 않게). 해외 종목 코드 모양과 채권·파생·다른 ETF가 실제 응답에서 어떻게 보이는지는 아직 실제 응답으로 확인하지 못했습니다(미확인).
- **결정: 시세처리기의 첫 외부 호출.** 지금까지 시세처리기는 외부를 호출하지 않고 수집기가 받아 준 자료만 읽었습니다. 구성 종목은 호출이 주 1회 수준으로 적고(ETF마다 한 번, 한 번에 약 수십 KB), 받은 결과를 곧바로 이력·종목 테이블에 반영해야 하며, 종목 테이블을 쓰는 서비스가 시세처리기뿐이라 시세처리기가 직접 호출합니다. 수집기가 받으면 응답을 이벤트로 옮기고 다시 시세처리기가 받아 테이블에 쓰는 단계가 더해집니다. 이 호출은 시세 제공자 API(`CallRateLimit` 대상)와 무관하며, 출처 주소와 ETF별 번호는 설정값으로만 받습니다. 출처 응답 모양이 예고 없이 바뀔 수 있어, 모양이 다르면 일부만 읽지 않고 실패로 둡니다.
- **자동 수집을 허용한 곳만:** 이용약관이나 robots.txt가 자동 수집을 막는 출처는 쓰지 않고, 그런 ETF는 수동 입력으로 둡니다.

## 테스트

테스트 명령과 변경 범위별로 돌릴 테스트는 [`.claude/rules/testing.md`](../.claude/rules/testing.md), 기능마다 어떤 테스트가 있고 어떤 조건에서 켜지는지는 테스트 지도 [`docs/test-map/`](test-map/README.md)가 원본입니다. 테스트 개수는 실행할 때마다 바뀌므로 적지 않고, 아래 전체 실행의 결과 표로 확인합니다.

`--no-build-cache`가 필요한 이유: 빌드 캐시가 켜져 있으면 입력이 같을 때 테스트를 실행하지 않고 이전 결과를 복원합니다.

### 전체 실행 (실제 DB 포함)

```bash
scripts/test-all.sh          # 단위·가상 테스트 + 실제 DB 테스트, 끝에 기능별 결과 표
scripts/test-all.sh --plan   # 아무것도 띄우지 않고 실행 순서만 보기
```

- 실제 DB 테스트는 개발용 Postgres 컨테이너(`stock-postgres`) 안에 이 실행만의 테스트 전용 데이터베이스(`stock_test_run_<시각>_<pid>`)를 만들고, 서비스 스키마를 `.env`의 서비스 계정 소유로 만들고 브리핑 테이블은 Flyway로 만든 뒤 돌리고 지웁니다. 개발용 데이터베이스의 데이터는 건드리지 않습니다.
- 컨테이너가 꺼져 있으면 켜고, 끝나도 끄지 않습니다. 서비스 계정은 만들거나 바꾸지 않습니다.
- `.env`에 `MARKET_DB_USER`, `MARKET_DB_PASSWORD`, `NEWS_DB_USER`, `NEWS_DB_PASSWORD`, `BRIEFING_DB_USER`, `BRIEFING_DB_PASSWORD`와 personal 계정 키(`PERSONAL_OWNER_DB_USER`·`_PASSWORD`, `PERSONAL_WRITER_DB_USER`·`_PASSWORD`, `COLLECTOR_DB_USER`·`_PASSWORD`, `NOTIFIER_DB_USER`·`_PASSWORD`)가 있어야 하고, 그 계정이 이미 만들어져 있어야 합니다(전체 실행은 계정을 만들지 않으며, 없으면 `서비스 계정 <이름> 이 없음`으로 실패합니다). 계정은 관리자 계정으로 `infra/db/personal/init.sql`을 실행하면 만들어집니다(위 "업무 데이터와 DB 계정"). 테스트 DB에는 market 표 → personal 계정·권한 → personal 표 → 브리핑 표 순서로 Flyway를 적용합니다.
- 결과 표는 지도의 기능마다 돌았음, 건너뜀(이유), 실패를 보여 주고, 하나라도 실패하면 종료 코드 1입니다. 이전 실행이 남긴 테스트 데이터베이스나 컨테이너가 보이면 지우지 않고 지우는 명령을 안내합니다.

**오래된 DB 볼륨에는 news·브리핑 계정이 없을 수 있습니다.** 계정은 초기화 스크립트(`infra/compose/postgres/init/`)가 DB 볼륨을 처음 만들 때만 만들므로, news·브리핑 계정 스크립트가 생기기 전에 만든 볼륨에는 그 계정이 없습니다. 이때 전체 실행은 `서비스 계정 <이름> 이 없음`으로 실제 DB 단계를 실패로 표시합니다. 대처는 위 "DB 준비와 다시 만들기"처럼 볼륨을 지우고 다시 띄우는 것입니다(모든 서비스의 개발용 DB 데이터가 사라집니다).

DB 통합 테스트 하나만 따로 돌릴 때는 켜는 환경변수(지도의 `켜는 조건`)에 비어 있는 새 데이터베이스를 넘깁니다. 마이그레이션을 적용할 새 DB가 필요하므로 개발용 DB와 분리된 임시 컨테이너를 쓰고, 끝나면 지웁니다. 예: 시세처리기

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

켜는 환경변수는 대체로 서비스마다 세 개씩입니다. 서비스끼리 같은 컨테이너를 이어 쓰지 말고 따로 띄웁니다.

| 대상 | 켜는 환경변수 | 주소 형식 |
|---|---|---|
| 시세처리기 (`:services:market`) | `MARKET_IT_DB_URL`, `MARKET_IT_DB_USER`, `MARKET_IT_DB_PASSWORD` | `jdbc:postgresql://...` |
| 발송기 받는 사람·personal 권한 (`:services:notifier`) | `NOTIFIER_IT_DB_URL`, `NOTIFIER_IT_DB_USER`, `NOTIFIER_IT_DB_PASSWORD`와 계정별 `WRITER_IT_DB_USER`·`_PASSWORD`, `COLLECTOR_IT_DB_USER`·`_PASSWORD`, `MARKET_IT_DB_USER`·`_PASSWORD` | `jdbc:postgresql://...`. market·personal 표가 만들어진 DB를 가정합니다. market 테스트(보유 구분·검색어 뷰·구성 종목 이력)도 쓰기 계정 `WRITER_IT_DB_USER`·`_PASSWORD`가 더 필요하고, 옛 표 이관 테스트는 비워도 되는 별도 스키마 이름을 `MARKET_IT_DB_MIGRATION_SCHEMA`로 받습니다 |
| 뉴스 저장 (`:services:news`) | `NEWS_IT_DB_URL`, `NEWS_IT_DB_USER`, `NEWS_IT_DB_PASSWORD` | `jdbc:postgresql://...` |
| 브리핑 (`pytest services/briefing`) | `BRIEFING_IT_DB_URL`, `BRIEFING_IT_DB_USER`, `BRIEFING_IT_DB_PASSWORD`. 멤버 읽기·권한 테스트(`test_member_store_db.py`)는 값을 넣는 `WRITER_IT_DB_USER`·`_PASSWORD`와 `MARKET_IT_DB_USER`·`_PASSWORD`도 필요 | `postgresql://...`. `briefing` 스키마에 Flyway(`services/briefing/db/migration`)를, market·personal 표와 브리핑 계정 권한(`infra/db/personal`)을 먼저 적용한 DB를 가정합니다 |

### 실제 외부 호출 (`--live`)

```bash
TEST_ALL_AWS_PROFILE=<AWS 프로필> scripts/test-all.sh --live
```

`--live`는 위 전체 실행에 더해 실제 외부를 부르는 두 흐름을 돌립니다. 시세 토큰 1회, 시세 1회, LLM 1회, 메일 1통을 씁니다. LLM이 리포트 문장에 근거 약함을 표시하면 브리핑이 재검색을 요청하고, 수집기가 실제 뉴스 검색(작업당 최대 30회)으로 답하며 LLM도 몇 회 더 부릅니다. 결과 표의 LLM 수는 리포트 호출만 셉니다.

1. 시세 흐름: 일회용 Kafka와 테스트 전용 데이터베이스로 시세처리기와 수집기(실제 시세, 종목 1개, 타이머 꺼짐)를 띄우고 수동 실행 명령 `collect` 1건으로 저장까지 확인합니다.
2. 브리핑·메일: news, 테스트용 발송기, 브리핑(실제 LLM)을 띄우고 가짜 변동·기사·수집 완료를 넣어 LLM 리포트와 메일 발송 1통을 확인합니다.

주의:

- **필요할 때만 직접 켭니다.** 실제 호출 한도와 비용을 쓰고 실제 메일이 갑니다.
- 메일 발송은 이 스크립트가 띄운 테스트용 발송기에만 켭니다. 제목 앞에 `[테스트]`를 붙이며, `.env`의 `NOTIFIER_SEND_ENABLED`는 바꾸지 않습니다.
- LLM 리포트 본문은 받은 메일과 `build/test-all-live/briefing-<실행 이름>.txt`(권한 600, git 제외)에만 남습니다.
- 키는 실행할 때마다 Parameter Store에서 새로 읽어 임시 폴더에만 두고, 끝나면 지웁니다. `.env`와 실행 환경변수의 같은 키는 쓰지 않습니다. 필수 키가 하나라도 비면 시세를 부르기 전에 실패로 끝납니다.
- `TEST_ALL_AWS_PROFILE`이 필수입니다. 엉뚱한 계정을 읽지 않도록 기본 자격증명 방식으로 넘어가지 않습니다.

필요한 키(목록의 원본은 `scripts/test-all.sh` 머리 주석과 `LIVE_KEYS`, 이름 대응은 `scripts/load_config.py`):

| 환경변수 | Parameter Store |
|---|---|
| `BROKER_BASE_URL` | `/stock-reading/dev/collector/broker-base-url`. `https://<호스트>[:포트]` 형식이어야 합니다(스크립트가 검사) |
| `TEST_ALL_LIVE_SYMBOL` | `/stock-reading/dev/collector/test-live-symbol`. 시세를 부를 종목 하나(6자리 숫자) |
| `BROKER_APP_KEY`, `BROKER_APP_SECRET` | `/stock-reading/dev/collector/` 아래의 시세 앱키·시크릿 |
| `ANTHROPIC_API_KEY` | `/stock-reading/dev/briefing/anthropic-api-key` |
| `SMTP_HOST`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `NOTIFIER_SENDER`, `TEST_ALL_LIVE_RECIPIENT` (선택 `SMTP_PORT`) | `/stock-reading/dev/notifier/` 아래의 메일 설정. `TEST_ALL_LIVE_RECIPIENT`(파라미터 `recipient`)는 앱이 읽지 않고, 테스트 DB의 가짜 멤버에 넣는 받는 주소로만 씁니다 |
| `NEWS_SEARCH_CLIENT_ID`, `NEWS_SEARCH_CLIENT_SECRET` | `/stock-reading/dev/collector/` 아래의 뉴스 검색 키. 테스트용 수집기에만 넘깁니다 |

`BRIEFING_LLM_MODEL`(선택)과 비밀이 아닌 뉴스 검색 설정 `NEWS_SEARCH_BASE_URL`, `NEWS_SEARCH_ID_HEADER`, `NEWS_SEARCH_SECRET_HEADER`(필수)는 실행 환경변수나 `.env`에서 읽습니다. 필수 설정이 비면 시세를 부르기 전에 실패로 끝납니다.

## 실행 기록 (테스트 기록)

고정 입력 시험에는 가짜 검색 서버 `scripts/fake-news-search.py`를 씁니다. 시세 조회의 종목 수별 부하는 `QuoteFixedLoadTest`(`QUOTE_LOAD=1`일 때만 실행)로 잽니다.

시세 응답 칸 검사(`BROKER_PRICE_CHECK_*`)를 바꾸면 저장해 둔 실제 시세 원문으로 `PriceCheckReplayTest`를 돌려, 버리는 행이 분석으로 정한 목록과 같은지 확인합니다. 입력·기대 파일은 저장소 밖에 두고(저장소 안 경로는 거절), 칸 이름은 `application.yml` 기본값을 쓰며 바꿔 볼 때만 수집기와 같은 환경변수로 넘깁니다. 차이는 건수만 출력합니다.

```bash
# 입력: 한 줄에 eventId<TAB>price<TAB>raw JSON (시세 테이블에서 읽기만 해서 만듦)
# 기대: eventId 한 줄씩인 파일, 또는 첫 칸이 snapshot_event_id 인 CSV
# 칸 이름을 바꿔 볼 때만: export BROKER_PRICE_CHECK_BASE_FIELDS=...  (나머지 BROKER_PRICE_CHECK_* 도 같은 방식)
PRICE_CHECK_REPLAY_INPUT=<저장소 밖>/raws.tsv PRICE_CHECK_REPLAY_EXPECTED=<저장소 밖>/expected.csv \
  ./gradlew :services:collector:cleanTest :services:collector:test --no-build-cache --tests '*PriceCheckReplayTest'
```

급변동 감지 설정(`market.*`)을 바꾸면 같은 원문을 실제 감지·재발행 코드에 받은 순서대로 넣는 `MoveReplayTest`로 `market.move`가 유형별로 몇 건 나오는지 셉니다. 제외 목록을 주면 그 행은 수집기가 버린 것으로 보고 이력과 평가에서 뺍니다. 연속 확인, 튄 점 제외, 첫 체결 전 제외, 재발행 간격도 같은 코드로 적용됩니다. 재발행 상태는 빈 상태에서 시작하고, 결과는 건수만 출력합니다. 유형 2는 날짜(서울 기준)별 발행 건수와 거짓 건수도 `move replay prevClose by date:` 줄로 냅니다. 거짓은 그 종목·그날 후보 칸 값의 최빈값을 참 전일 종가로 보고 다시 계산하면 임계값 미만인 발행입니다.

```bash
# 입력: 한 줄에 eventId<TAB>symbol<TAB>asOf(ISO)<TAB>price<TAB>raw JSON, 시세처리기가 받은 순서
# 제외(선택): eventId 한 줄씩인 파일, 또는 첫 칸이 snapshot_event_id 인 CSV
# 유형 2는 기본값(전일 종가 후보 세 칸의 다수결)으로 평가한다. 칸을 바꾸려면 MARKET_PREV_CLOSE_FIELDS=<칸 경로,칸 경로,...> 를 함께 주고, 빈 값이면 유형 2를 끈다
MOVE_REPLAY_INPUT=<저장소 밖>/snapshots.tsv MOVE_REPLAY_EXCLUDE=<저장소 밖>/rule-hit.csv \
  ./gradlew :services:market:cleanTest :services:market:test --no-build-cache --tests '*MoveReplayTest' -i
```

## 아직 정하지 않은 것

- **변동 알림의 읽은 위치를 언제 확정할지:** 지금은 `market.move`를 읽는 즉시 확정해, 브리핑이 정상 종료되면 모으는 중이던 변동은 실패 작업으로 남고(강제 종료만 기록 없이 사라짐) 이미 만든 작업은 `interrupted_by_restart`로 끝납니다(다시 처리하지 않음으로 정했습니다). 처리를 끝낸 뒤 확정하도록 바꿀지(재시작 후 다시 처리하므로 중복 리포트를 막는 방법도 함께 필요) 정합니다.
- **작업 기록 보관 기간:** `briefing.job_run`과 `briefing.job_log`는 줄을 지우지 않고 쌓습니다. LLM 입력·원래 응답을 그대로 담아 커지므로 보관 기간과 정리 방법(오래된 `job_log` 줄만 지울지, `job_run`까지 지울지)을 정합니다.
- **뉴스 수집 완료 테이블:** `briefing.collection_completed`의 보관 기간과 정리 방법을 정합니다.
- **휴장일 판정:** 시세 제공자의 장 운영 상태 조회로 평일 08:00(`collector.market-status.decide-cron`)에 판정해 그날 하루 동안 씁니다. 판정하지 못하면 영업일로 봅니다. 휴장일에는 구성 종목 조회 tick(`collect-members`)과 데일리 판 tick, 시세·장 시작 전·장중·마감 후 수집을 건너뛰고 뉴스 수집은 계속합니다. 수동 실행 명령(`collector.command`)은 휴장 확인 없이 실행합니다. 실제 휴장일 응답으로 규칙을 확인하는 일이 남았습니다.

## 후속

- **수집 대상과 뉴스 검색어:** 수집기가 시작할 때 `personal.collect_target`(1초 시세·미국 일봉 종목)과 `market.news_search_target`(뉴스 검색어)을 읽기 전용 계정으로 읽고, 뉴스가 `market.etf_stock_map`(ETF 구성 종목)을 읽습니다. 바뀐 값은 재시작해야 반영되며, 처음 데이터(seed SQL)를 넣은 뒤에 수집기·뉴스를 시작(또는 재시작)합니다. 두 목록이 비어 있으면 수집기는 시세 종목이 비었다는 경고를 처음 한 번과 그 뒤 10분마다 남기고, 뉴스 수집은 회차마다 경고를 남기며 수집 대상 종목 전부를 '불완전'(`collection.completed`의 `incompleteSymbols`)으로 알려 브리핑이 "자료를 못 받음"으로 읽게 합니다. 종목 목록·검색어 목록을 알리던 이벤트 두 개와 뉴스의 검색어 표는 없어졌습니다. 남은 일: 값을 바꾸는 기능(CUD). 기사 태그 칸은 미국 티커도 담도록 넓혔습니다(`news_article_symbol.symbol` 16자, `news.collected`의 종목 형식).
- **검색어와 태그 개선:** 정했고 구현했습니다. 태그는 기사 주제로 두고 꺼낼 때 구성 관계로 넓히며, 미국 구성 종목 검색어는 이름으로 만듭니다. 남은 일: 이름이 영어라 검색이 잘 맞는지 실제 기사로 확인하고(맞지 않으면 테이블에서 검색어를 고침), 더는 관심 ETF가 담지 않는 종목의 만들어진 검색어를 정리하는 방법(지금은 지우지 않음).
- **휴장일 발송 관리:** 휴장일에는 데일리 브리핑 시작 tick도 발행하지 않아 판이 나가지 않습니다. 판정하지 못한 날은 영업일로 보고 평소대로 나갑니다.
- **LLM 2단계:** 1단계 LLM이 모은 자료의 중복을 없애고 거른 뒤, 2단계 LLM이 분석해 리포트를 씁니다.

- **외부 접속을 받는 기능:** 브리핑의 Kafka 수신·발행은 "시작"과 "멈추기"로 부를 수 있게 나뉘어 있어 다른 프로그램(예: 어드민 화면)이 같은 실행부를 부를 수 있습니다. 어드민 화면이나 웹 서버는 아직 없습니다. 이런 화면처럼 외부 접속을 받는 기능을 만들 때는 [ADR-007](adr/007-outbound-only.md)(외부 접속을 받지 않는다)을 먼저 갱신하고, 접속 경로와 인증 방식을 정해야 합니다.

## 보안

- 비밀값(`.env`, 토큰, 키)과 개인 정보(보유 종목·금액·계좌)는 저장소에 올리지 않습니다.
- 커밋 전 `gitleaks`가 자동으로 검사합니다: `pre-commit install`
