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

시세처리기(market)는 `market.snapshot`을 `price_snapshot`(`as_of` 기준 날짜별 파티션, 응답 원문은 jsonb `raw`)에, `market.snapshot.failed`를 `quote_failure`에 저장합니다. 파티션은 저장 직전에 없으면 만들며(Asia/Seoul 자정 경계), 테이블 구조와 근거는 [ADR-018](adr/018-quote-storage.md)입니다. 원문 보존 기간은 아직 정하지 않았습니다.

시세처리기는 스냅샷마다 급변동을 감지해 `market.move`를 발행합니다([ADR-019](adr/019-move-detection.md)). 유형 1(`window`)은 최근 N분 이력의 가장 오래된 스냅샷 대비, 유형 2(`prevClose`)는 응답 원문의 전일 종가 필드 대비 변동률이 임계값 이상일 때이며, 한 번 발행한 뒤에는 마지막 발행 가격에서 임계값만큼 더 움직일 때만 다시 발행합니다(상태는 `move_state` 테이블에 둡니다). 설정은 `services/market/src/main/resources/application.yml`의 `market.*`이고 환경변수로 덮어쓸 수 있습니다.

| 설정 키 | 기본값 | 설명 |
|---|---|---|
| `market.window-minutes` | `5` | 유형 1이 비교할 최근 이력의 길이(분) |
| `market.window-threshold-percent` | `1.00` | 유형 1 임계값(%) |
| `market.prev-close-field` | `PREV_CLOSE_PLACEHOLDER` | 유형 2가 원문에서 읽을 전일 종가 필드(점으로 중첩 경로). **임시값입니다. 실제 응답을 보고 확정해야 하며, 그 전에는 유형 2가 감지하지 않습니다.** |
| `market.prev-close-threshold-percent` | `3.00` | 유형 2 임계값(%) |

이전 키 `market.move-threshold-percent`는 없어졌습니다. 마이그레이션(`services/market/src/main/resources/db/migration/`)은 서비스 기동 때 Flyway가 적용하며, 단위 테스트는 저장소를 대역으로 쓰므로 SQL은 로컬 Postgres에서 따로 확인해야 합니다.

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
| `COLLECTOR_TICK_INTERVAL_MS` | `schedule.tick`(수집) 발행 간격(밀리초, 기본 1000). 타이머(`COLLECTOR_SCHEDULE_ENABLED=true`)가 켜져 있을 때만 쓰입니다. `scheduledFor`가 초 단위라 1000보다 작게 두면 같은 초의 tick이 중복으로 버려집니다 |
| `COLLECTOR_MAX_TICK_AGE_MS` | 현재보다 이만큼 이상 오래된 tick은 조회하지 않고 종목마다 `TICK_SKIPPED` 실패 이벤트로 기록합니다(기본 3000) |
| `BROKER_MAX_CALLS_PER_SECOND` | 초당 시세 호출 상한(기본 10). 종목 수가 이보다 많으면 시작할 때 경고하고, 넘친 종목은 그 tick에서 호출하지 않고 `TICK_SKIPPED` 실패 이벤트로 기록합니다. 상한 때문임은 `exceptionType=RateLimited`로 구분하며, 나중에 계약에 정식 실패 종류가 생기면 바꿀 수 있습니다 |
| `NEWS_SEARCH_BASE_URL` / `NEWS_SEARCH_CLIENT_ID` / `NEWS_SEARCH_CLIENT_SECRET` | 뉴스 검색 제공자의 주소와 키 |
| `NEWS_SEARCH_ID_HEADER` / `NEWS_SEARCH_SECRET_HEADER` | 제공자가 정한 인증 헤더 이름 |
| `NEWS_SEARCH_QUERIES` | `검색어=종목코드,종목코드;검색어2=*` 형식. 관심 종목이 드러나므로 `.env`에만 둡니다 |
| `NEWS_SEARCH_CALL_INTERVAL_MS` | 검색어를 연달아 호출할 때 호출 사이의 대기(밀리초, 기본 60). 0이면 기다리지 않으며, 짧으면 제공자가 429로 거절할 수 있습니다([측정 기록](test-history/S01-real-api/call-interval.md)) |
| `DART_API_KEY` | 공시 API 키 |

시세 조회는 종목마다 병렬로 호출하고 실패해도 재시도하지 않으며 다음 tick에 맡깁니다. 성공하면 응답 원문을 `market.snapshot`의 `raw`에, 실패하면 종목마다 `market.snapshot.failed`를 발행합니다(실패 원문 포함, 키·시크릿·접근토큰 값은 가려서 담습니다). 로그와 예외 메시지에는 서버가 보낸 문구 대신 형식을 확인한 처리 코드(영숫자 8자 이하, 아니면 `invalid`)만 남습니다. 테스트는 로컬 가짜 HTTP 서버(`FakeBrokerServer`)를 쓰며 실제 서버를 호출하지 않습니다.

키 값(`*_KEY`, `*_SECRET`, `DART_API_KEY`)은 아래 Parameter Store에서 가져옵니다. 주소와 헤더 이름은 비밀이 아니지만 코드에 두지 않고 `.env`에 직접 적습니다.

### 시세 API 연동 확인

시세는 조회만 씁니다(ADR-007). `.env`에는 다음을 둡니다.

- `BROKER_BASE_URL`: 시세 제공자가 안내한 운영 주소
- `BROKER_APP_KEY`, `BROKER_APP_SECRET`: Parameter Store에서 가져옵니다.
- `COLLECTOR_MARKET_DATA_SOURCE=broker`

실제 서버 확인용 수동 테스트: `BrokerLiveProbeTest`는 평소 건너뛰며 `BROKER_PROBE=1`일 때만 토큰 1회와 종목별 현재가를 호출합니다. 요약은 `BROKER_PROBE_OUT`, 응답 원문은 `BROKER_PROBE_RAW_DIR`에 쓰고 키와 토큰 값은 남기지 않습니다. 종목은 `BROKER_PROBE_SYMBOLS`(쉼표 구분)로 지정하며 코드에는 두지 않습니다. 실제 시세 응답은 저장소에 올리지 않으므로 `BROKER_PROBE_RAW_DIR`은 `data/`처럼 gitignore 대상 경로로 지정합니다. 실제 운영 서버를 호출하므로 필요할 때 최소한으로 실행합니다.

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
| Java (수집기, 시세처리기, 발송기, 뉴스 저장) | `./gradlew cleanTest test --no-build-cache` | 341 (실서버 확인·실험용 2개는 건너뜀) |
| Python (브리핑) | `services/briefing/.venv/bin/python -m pytest services/briefing` | 28 |
| Python (설정 로더, 가설 검증 도구) | `services/briefing/.venv/bin/python -m pytest -c scripts/pytest.ini scripts/tests` | 44 |

`--no-build-cache`가 필요한 이유: 빌드 캐시가 켜져 있으면 입력이 같을 때 테스트를 실행하지 않고 이전 결과를 복원합니다.

## 실행 기록 (테스트 기록)

여러 검색어를 반복 수집하는 실행의 사양과 결과는 [`docs/test-history/`](test-history/README.md)에 남깁니다. 고정 입력 시험에는 가짜 검색 서버 `scripts/fake-news-search.py`를 씁니다.

## 보안

- 비밀값(`.env`, 토큰, 키)과 개인 정보(보유 종목·금액·계좌)는 저장소에 올리지 않습니다.
- 커밋 전 `gitleaks`가 자동으로 검사합니다: `pre-commit install`
