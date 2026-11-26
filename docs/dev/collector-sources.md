# 수집기의 외부 데이터 설정 (collector)

수집기는 시세, 뉴스 검색, 공시를 외부에서 받아 이벤트로 넘깁니다. 시세 기본은 가짜 데이터(시뮬레이터)이며, `COLLECTOR_MARKET_DATA_SOURCE=broker`일 때만 외부 API를 씁니다. 시세 제공자 API는 조회만 호출합니다([CLAUDE.md](../../CLAUDE.md) 설계 규칙). 기본값과 그 이유의 원본은 `services/collector/src/main/resources/application.yml`의 `collector.*`와 그 주석입니다. 이벤트 모양의 원본은 `contracts/events/`입니다.

- 뉴스: 정기 뉴스 수집(`collect-news`)마다 검색해 기사마다 `news.collected`를 보냅니다. 기사를 다 보낸 뒤 `collection.completed`를 한 건 보냅니다([ADR-010](../adr/010-event-format.md)). 브리핑의 재검색 요청(`news.research.requested`)도 같은 방식으로 검색하고, 끝나면 `jobType` `research-news`로 완료 알림을 보냅니다. 검색 한 번마다 거른 건수를 `news filtered ...` 로그 한 줄로 남기며, 검색어와 종목은 쓰지 않습니다(`NewsCollector`).
- 공시: 미국 종목 공시(SEC EDGAR)는 장 시작 전 수집에서 `filing.collected`로 보냅니다. DART 공시는 조회 클라이언트(`DartClient`)까지만 있고 이벤트로 보내지 않습니다.
- 키 값(`*_KEY`, `*_SECRET`, `DART_API_KEY`)은 Parameter Store에서 가져옵니다([config.md](config.md)). 주소와 헤더 이름은 비밀이 아니지만 코드에 두지 않고 `.env`에 직접 적습니다. 자리 표시는 `.env.example`에 있습니다.

## 수집 작업과 수동 실행

수집 작업 여섯 개는 수집기 안 타이머가 직접 실행합니다. 같은 작업은 겹쳐 돌지 않고, 밀린 회차는 몰아서 실행하지 않습니다([ADR-012](../adr/012-collector-timers-and-command-topic.md)). 작업 이름은 `scripts/collector-command.sh` 머리 주석에 있습니다. 1초 현재가에서 건너뛴 회차는 종목마다 `TICK_SKIPPED`(`exceptionType=SupersededByNewerTick`) 실패 이벤트로 남습니다.

### 수동 실행

작업 하나를 지금 1회 돌리려면 `scripts/collector-command.sh <명령>`을 씁니다. 스크립트는 `collector.command` 토픽으로 명령 한 건을 보냅니다(계약은 `contracts/events/collector.command.v1.schema.json`). 수집기는 시세 시간대와 타이머 켜기 설정에 상관없이 받은 즉시 실행합니다.

- 같은 작업이 돌고 있으면 건너뛰고, 모르는 명령은 무시합니다. 둘 다 수집기 로그에만 남습니다.
- 같은 `eventId`는 한 번만 실행합니다. 수집기가 꺼져 있던 동안 보낸 명령은 실행하지 않습니다. 켜져 있는 동안 Kafka 재분배가 일어나도 그 사이에 보낸 명령은 실행합니다.
- `KAFKA_CONTAINER`, `KAFKA_BOOTSTRAP`(컨테이너 안에서 붙을 주소)으로 대상을 바꿉니다. 기본값은 스크립트에 있습니다.
- `scripts/publish-daily-notice.sh <morning|post-close>`는 수집기 알림을 꺼 둔 로컬에서 데일리 판 시작 알림을 손으로 보낼 때만 씁니다. 같은 날 이미 만든 판은 다시 만들어지지 않습니다.

```bash
scripts/collector-command.sh collect        # 1초 현재가 한 회차
scripts/collector-command.sh collect-news   # 뉴스 수집 한 번
```

## 환경변수

이름과 뜻만 적습니다. 기본값, 허용 범위, 이유는 괄호 안 설정 키의 주석(`application.yml`)에 있습니다. "시작 실패"는 값이 틀리면 수집기가 뜨지 않는다는 뜻입니다.

| 환경변수 | 뜻 (원본 설정 키) |
|---|---|
| `COLLECTOR_MARKET_DATA_SOURCE` | 시세 출처 `simulator` 또는 `broker` (`collector.market-data.source`) |
| `BROKER_BASE_URL` / `BROKER_APP_KEY` / `BROKER_APP_SECRET` | 시세 제공자 주소와 키, `broker`일 때 필요 (`collector.broker.*`) |
| `COLLECTOR_SCHEDULE_ENABLED` | 1초 현재가·정기 뉴스 타이머 켜기, 로컬에서는 끔 (`collector.schedule.*`) |
| `COLLECTOR_TICK_INTERVAL_MS` | 1초 현재가 주기 (`collector.tick-interval-ms`) |
| `COLLECTOR_QUOTE_PER_TICK` | 한 회차에 부르는 종목 수. 종목이 더 많으면 번갈아 부름. `BROKER_MAX_CALLS_PER_SECOND`보다 크면 시작 실패 (`collector.quote-per-tick`) |
| `COLLECTOR_QUOTE_START` / `COLLECTOR_QUOTE_END` | 1초 현재가를 조회하는 평일 시간대 (`collector.quote-hours.*`) |
| `BROKER_MARKET_STATUS_PATH` / `_TODAY_FIELD` / `_BUSINESS_DAY_FIELD` | 휴장일 판정용 장 운영 상태 조회의 경로와 응답 칸 이름. 하나라도 비면 늘 영업일 (`collector.market-status.*`) |
| `BROKER_MARKET_STATUS_TIMEOUT` | 휴장일 판정 한 번의 시간 상한, 넘으면 영업일 (`collector.market-status.lookup-timeout`) |
| `BROKER_QUOTE_TIMEOUT_SECONDS` | 현재가 조회의 응답 대기 제한. 넘으면 `TIMEOUT` 실패로 끊음. 그 밖의 조회는 `collector.broker.timeout-seconds` (`collector.broker.quote-timeout-seconds`) |
| `BROKER_MAX_CALLS_PER_SECOND` | 초당 시세 호출 상한, 앱 전체가 하나를 씀(`CallRateLimit`). 정하는 기준은 키 주석과 `BrokerProperties` (`collector.broker.max-calls-per-second`) |
| `BROKER_PRICE_CHECK_ENABLED` / `_TOLERANCE` | 현재가를 같은 응답의 다른 칸과 맞춰 보는 검사 켜기와 허용 오차. 어긋난 응답은 `FIELD_MISMATCH` 실패로 남김. 오차가 범위 밖이면 시작 실패 (`collector.broker.price-check.*`) |
| `BROKER_PRICE_CHECK_BASE_FIELDS` 외 `BROKER_PRICE_CHECK_*` | 위 검사가 읽을 응답 칸 이름과 부호 값. 모두 비우면 검사 없이 통과 (`collector.broker.price-check.*`) |
| `NEWS_SEARCH_BASE_URL` / `_CLIENT_ID` / `_CLIENT_SECRET` / `_ID_HEADER` / `_SECRET_HEADER` | 뉴스 검색 제공자 주소, 키, 인증 헤더 이름 (`collector.news-search.*`) |
| `NEWS_SEARCH_DAILY_CALL_LIMIT` | 하루 검색 호출 상한, 재시작하면 0부터 (`collector.news-search.daily-call-limit`) |
| `NEWS_SEARCH_CALL_INTERVAL_MS` | 검색 호출 사이 대기 (`collector.news-search.call-interval-ms`) |
| `NEWS_RESEARCH_MAX_QUERIES` | 재검색 요청 하나의 검색어 수 상한 (`collector.news-research.max-queries`) |
| `DART_API_KEY` | 국내 공시 API 키 (`collector.dart.api-key`) |
| `EDGAR_USER_AGENT` | SEC EDGAR 요청의 User-Agent(`회사명 연락이메일`). 개인 정보라 `.env`에만 둠. 비면 공시를 조회하지 않음 (`collector.edgar.user-agent`) |
| `EDGAR_FORMS` | 수집할 공시 형식. 비거나 틀리면 시작 실패 (`collector.edgar.forms`) |
| `COLLECTOR_PRE_OPEN_ENABLED` / `_CRON` | 장 시작 전 수집(`collect-pre-open`) 켜기와 시각 (`collector.pre-open.*`) |
| `COLLECTOR_INTRADAY_ENABLED` / `_CRON` / `_START` / `_END` / `_STAGGER_MS` | 장중 수집(`collect-intraday`) 켜기, 주기, 시간대, 조회 사이 대기 (`collector.intraday.*`) |
| `COLLECTOR_INSIGHT_SCHEDULE_ENABLED` / `COLLECTOR_INSIGHT_CRON` | 마감 후 수집(`collect-post-close`) 켜기와 시각 (`collector.insight.schedule.*`) |
| `COLLECTOR_INSIGHT_RETRY_MAX_ATTEMPTS` / `_INTERVAL` | 마감 후 수집에서 실패한 조회만 다시 부르는 횟수와 간격. 브리핑의 마감 후 판 한도 시각 전에 끝나야 함 (`collector.insight.retry.*`) |
| `COLLECTOR_DAILY_BRIEFING_ENABLED` | 데일리 판 시작 알림(`daily.collection.completed`) 켜기. 브리핑도 같은 값을 읽음 (`collector.daily-briefing.enabled`) |
| `COLLECTOR_DAILY_BRIEFING_MORNING_CRON` / `_POST_CLOSE_CRON` | 아침 판 알림 시각, 휴장일의 마감 후 판 휴장 알림 시각. `-`이면 그 판 알림 없음 (`collector.daily-briefing.*`) |
| `COLLECTOR_MEMBERS_ENABLED` / `_CRON` | ETF 구성 종목 조회(`collect-etf-members`) 켜기와 시각 (`collector.members.*`) |
| `MEMBERS_URL_TEMPLATE` / `MEMBERS_USER_AGENT` | 구성 종목 요청 주소 틀과 식별 문자열. 주소 틀이 비면 자동 조회 안 함 (`collector.members.*`) |

휴장일에는 1초 현재가, 장 시작 전·장중·마감 후 수집, 구성 종목 조회를 건너뛰고, 데일리 판 알림은 판 시각에 휴장 알림을 냅니다.

## 뉴스 검색어와 하루 호출 수

- 검색어는 수집기가 시작할 때 `market.news_search_target` 뷰에서 한 번 읽습니다. 바꾸면 재시작해야 반영됩니다. 종목 이름으로 검색하려면 `search_by_name`을 켭니다. 값은 SQL로 넣으며, 예시는 `docs/examples/holdings.example.sql`입니다([setup.md](setup.md)).
- 기사 태그를 ETF 구성 종목으로 넓히는 일은 뉴스처리기가 합니다([services.md](services.md)의 뉴스처리기 절).
- 하루 정기 수집 호출 수는 (하루 정기 수집 횟수, `collector.schedule.news-interval-ms`로 정해짐) × 검색어 수입니다. 이것과 재검색 호출을 더한 값이 `NEWS_SEARCH_DAILY_CALL_LIMIT`을 넘지 않게 검색어 수를 정합니다.
- 상한에 닿으면 남은 검색어를 건너뛰고, 그 종목을 `incompleteSymbols`에 담습니다. 수집기 로그에는 `news daily call limit reached`가 남습니다. 미국 티커 검색이 빠지면 그 티커를 담은 관심 ETF 코드가 들어갑니다.

## 시세·지수·공시 수집이 내는 이벤트

- 1초 현재가: 종목마다 병렬로 부르고 재시도하지 않습니다. 성공하면 `market.snapshot`(`raw`에 원문), 실패하면 종목마다 `market.snapshot.failed`를 냅니다. 키·시크릿·토큰 값은 가립니다(`BrokerApiException`). 로그에는 서버 문구 대신 처리 코드만 남습니다.
- 장중(`collector.intraday.*`)과 마감 후(`collector.insight.*`, `broker`일 때만): 응답 원문을 `market.insight`로 냅니다. 시세와 같은 호출 상한(`CallRateLimit`)을 거칩니다.
- 장 시작 전(`collector.pre-open.*`): 아래 둘을 한 회차에 받으며, 앞쪽이 실패해도 공시는 받습니다.
  - 세계지수 아메리카와, 보유 미국 종목(`personal.collect_target`)의 해외 일봉 → `market.insight`. `broker`일 때만 받습니다.
  - SEC 공시 → `filing.collected`. `EDGAR_USER_AGENT`가 있을 때만 받고, 기간은 `collector.edgar.lookback-hours`입니다.
- 해외 일봉의 거래소는 `collector.edgar.ticker-map-url` 파일로 고릅니다. 이 파일은 하루 한 번 받고, 실패하면 전에 받은 파일을 씁니다. 해외 등락률을 고르는 규칙은 `OverseasCloseRateQuery` 주석에 있습니다.
- 테스트는 가짜 HTTP 서버(`FakeBrokerServer`)를 쓰며 실제 서버를 부르지 않습니다([testing.md](testing.md)).

## 시세 API 실서버 확인

실제 운영 서버를 부르므로 필요할 때만 최소한으로 실행합니다. `.env`에는 `BROKER_BASE_URL`, `BROKER_APP_KEY`·`BROKER_APP_SECRET`(Parameter Store에서), `COLLECTOR_MARKET_DATA_SOURCE=broker`를 둡니다. 종목은 `BROKER_PROBE_SYMBOLS`(쉼표 구분)로만 주고 코드에는 두지 않습니다. 요약은 `BROKER_PROBE_OUT`, 원문은 `BROKER_PROBE_RAW_DIR`에 씁니다. 실제 응답은 저장소에 올리지 않으므로 원문 위치는 `data/`처럼 gitignore 대상 경로로 줍니다. 키와 토큰 값은 남지 않습니다. 각 시험의 기본값과 호출 횟수는 시험 클래스(`services/collector/src/test/java/.../marketdata/`, `.../insight/`) 머리 주석에 있습니다.

| 시험 | 켜는 환경변수 | 확인하는 것 |
|---|---|---|
| `BrokerLiveProbeTest` | `BROKER_PROBE=1` | 토큰 발급과 종목별 현재가 |
| `PostCloseLiveProbeTest` | `BROKER_POSTCLOSE_PROBE=1` (선택: `BROKER_POSTCLOSE_PROBE_APIS`, `_COUNT`, `_MARKET`, `_CONTINENT`, `_EXCHANGES`) | 마감 후 조회 API의 응답 형식. `price`를 고르면 거래소 구분마다 현재가를 부릅니다 |
| `MarketStatusLiveProbeTest` | `BROKER_MARKET_STATUS_PROBE=1`, `BROKER_MARKET_STATUS_*` | 장 운영 상태 응답과 휴장일 판정. 평일 한 번, 주말·공휴일 한 번 돌려 봅니다 |
| `BrokerRateProbeTest`, `BrokerClientLiveSmokeTest` | `scripts/live_probe.py`로만 실행 | 호출 속도 실험과, 토큰 1회로 현재가·마감 후 조회가 되는지 |
| `BrokerSerialProbeTest` | `BROKER_SERIAL_PROBE=1` | 병렬, 직렬, 동시 N개 제한별 깨진 응답 건수 |

### `live_probe.py` (속도 시험, 스모크)

시험 클래스는 이 도구가 켜 주는 `LIVE_PROBE_GUARD`가 없으면 건너뜁니다. `--probe smoke`를 주면 속도 시험 대신 스모크 시험이 돌고, 이때는 `--ok-code`가 필요 없습니다. 옵션 기본값은 `scripts/live_probe.py`에 있습니다.

```bash
python3 scripts/live_probe.py --profile <AWS 프로필> --base-url <시세 제공자 운영 주소> \
  --symbols <종목 코드> --ok-code <정상 응답 코드> --rates 5,9 --phase-seconds 60
```

- 키는 `.env`가 아니라 Parameter Store에서 직접 읽어 시험 프로세스의 환경변수로만 넘깁니다. 명령줄과 출력에는 남지 않습니다.
- 직전 시험에서 토큰 발급이 거절됐으면, 사람이 `data/.live-probe-last.json`을 지우기 전에는 다시 실행하지 않습니다.
- 직전 시험이 이상 응답으로 멈췄으면 쉬는 시간(`--cooldown-minutes`) 동안 거부합니다. `--force`로 넘길 수 있습니다.
- `JAVA_HOME_21`이 필요하고, 결과는 `data/` 아래에 씁니다.

### `BrokerSerialProbeTest` (병렬·직렬 비교)

응답 칸 불일치는 수집기와 같은 `BROKER_PRICE_CHECK_*` 검사로 셉니다. 모든 방식이 같은 호출 상한을 거칩니다. 장중에만 실행합니다. 같은 흐름을 가짜 서버로 리허설하는 `SerialProbeRehearsalTest`는 평소 테스트에서 돕니다.

| 환경변수 | 뜻 |
|---|---|
| `BROKER_SERIAL_PROBE_OUT` | 결과 요약 파일. 저장소 안 경로는 거절합니다 |
| `BROKER_SERIAL_PROBE_MODES` | 돌릴 방식(`parallel`, `serial`, `limited`) |
| `BROKER_SERIAL_PROBE_CONCURRENCY` | `limited`의 N |
| `BROKER_SERIAL_PROBE_ROUNDS` / `BROKER_SERIAL_PROBE_SECONDS` | 라운드 수와 시간 상한. 먼저 닿는 쪽에서 멈춥니다 |
| `BROKER_SERIAL_PROBE_PAUSE_MS` | 방식 사이 쉼. 호출 상한 창을 비우도록 1초 이상 둡니다 |
| `BROKER_MAX_CALLS_PER_SECOND`, `BROKER_PRICE_CHECK_*` | 따로 주지 않으면 수집기와 같은 값 |

```bash
BROKER_SERIAL_PROBE=1 BROKER_SERIAL_PROBE_OUT=<저장소 밖 경로>/serial-probe.txt \
  ./gradlew :services:collector:test --tests '*BrokerSerialProbeTest' --no-build-cache
```

결과에는 방식별 건수만 쓰고, 종목 코드·시세 값·원문·키는 남기지 않습니다. 셀 대상이 아닌 실패가 한 번이라도 나오면 멈추고 `stop=`에 실패 종류를 적습니다. 출력 필드(`roundMs`, `roundMsExcluded`, `warmUpCalls`, `rateLimited`, `mismatchChecks`)의 뜻은 시험 클래스 주석에 있습니다.

## ETF 구성 종목 자동 조회

수집기 타이머(`collect-etf-members`)는 시세처리기 표(`etf`, `etf_stock_history`, `index_rebalance_schedule`)를 읽기만 해서 오늘 받을 차례인 ETF를 고릅니다. 출처 번호(`etf.constituent_source_id`)가 있는 ETF만 구성을 받고, 응답마다 `etf.members.fetched`(계약: `contracts/events/etf.members.fetched.v1.schema.json`)를 냅니다. 원문 저장, 비교, 반영은 시세처리기(consumer group `market-members`)가 하고, 같은 `eventId`는 한 번만 반영합니다. 동작 규칙의 원본은 `application.yml`의 `collector.members.*` 주석과 `MembersPublisher`, `FundSiteMemberSource`입니다.

- 주기: 평소에는 `collector.members.fetch-day`에 받고, 지수 교체일이 든 달에는 매일 받습니다. 한 번도 받지 않았거나 마지막 확인이 오래된 ETF(`MembersPublisher`)는 요일과 상관없이 받습니다.
- 교체 일정: 사람이 1년에 한 번 `index_rebalance_schedule`(`tracks`, `effective_date`, 선택 `note`)에 넣습니다. `tracks`는 `etf.tracks`와 같은 값이어야 합니다. 일정이 없는 ETF는 평소 주기로만 받습니다.
- 반영: 상위 구성(주식만, `collector.members.top-count`)의 종목이나 순위가 바뀔 때만 `etf_stock_history`에 새 행을 넣고 `etf_stock_map`을 맞춥니다. 그대로면 `last_seen_on`과 비중만 고칩니다. 바뀐 `etf_stock_map`은 뉴스와 수집기가 다음에 시작할 때 반영됩니다.
- 원문: 받을 때마다 `etf_stock_raw`에 쌓입니다. 지우는 절차는 아직 없습니다.
- 실패: 응답이 없거나, 200이 아니거나, 모양이 다르거나, 주식 행이 없으면 이전 구성을 그대로 두고 오류 종류만 로그에 남깁니다.
- 수동 ETF: 출처 번호가 없거나 `MEMBERS_URL_TEMPLATE`이 비어 있으면 `etf_stock_map`을 직접 고칩니다([setup.md](setup.md)).
- 외부 호출은 수집기만 하고, 시세 제공자 호출 상한과는 무관합니다. 출처 주소와 ETF별 번호는 설정값으로만 받습니다. 이용약관이나 robots.txt가 자동 수집을 막는 출처는 쓰지 않습니다.
- 고장 찾기: 수집기 로그에 `member targets unreadable`가 나오면 수집기 계정의 읽기 권한(`infra/db/personal/init.sql`)을 확인합니다.
- 미확인: 해외 종목 코드 모양과 채권·파생·다른 ETF가 실제 응답에서 어떻게 보이는지는 아직 실제 응답으로 확인하지 못했습니다.
