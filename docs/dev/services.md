# 시세·뉴스·확인기·발송기

서비스 사이의 약속(내부 조회 API, 저장 끝 알림, 추적 ID)은 [conventions.md](../conventions.md)에, DB 계정과 `.env`는 [setup.md](setup.md)에, 테스트 일반은 [testing.md](testing.md)에 있습니다. 이 문서의 기본값과 필드는 각 서비스의 `application.yml`과 `contracts/events/`가 원본입니다. 시세처리기·뉴스처리기 실행 명령은 [setup.md](setup.md)에 있습니다.

## 시세처리기 (`market`)

### 저장

`market.snapshot`은 `price_snapshot`(`as_of` 기준 날짜별 파티션, 원문은 jsonb `raw`)에, `market.snapshot.failed`는 `quote_failure`에 저장합니다. 저장 직전에 파티션이 없으면 만듭니다(Asia/Seoul 자정 경계). 테이블 구조의 원본은 `services/market/src/main/resources/db/migration/`이며, 서비스가 뜰 때 Flyway가 적용합니다. 단위 테스트는 저장소를 대역으로 쓰므로 SQL은 로컬 Postgres에서 따로 확인합니다.

### 급변동 감지 (`market.move`)

스냅샷마다 두 유형을 평가합니다. 유형 1(`window`)은 최근 이력의 가장 오래된 스냅샷과, 유형 2(`prevClose`)는 원문의 전일 종가(후보 칸 다수결)와 비교하며, 조건이 연속으로 확인될 때만 발행합니다. 한 번 발행한 뒤에는 마지막 발행 가격에서 더 움직여야 다시 발행하고, 종목 단위 재발행 간격이 있습니다. 상태는 그날(한국 날짜) 것만 쓰며 `move_state` 테이블에 둡니다. 오늘 등락률(`todayChangePercent`)은 응답 칸을 옮기지 않고 코드가 계산합니다.

- 규칙과 이유: `services/market/src/main/java/dev/stockreading/market/snapshot/`의 `MoveTracker`(발행 관문, 날짜 경계), `MoveDetector`(기준값, 튄 점, 첫 체결 전 응답, 전일 종가 다수결) 주석
- 설정과 기본값: `services/market/src/main/resources/application.yml`의 `market.*`(`window-minutes`, `window-threshold-percent`, `prev-close-fields`, `prev-close-threshold-percent`, `open-price-fields`, `confirm-count`, `confirm-spread-percent`, `first-trade-fields`, `republish-interval-minutes`, `republish-exception-percent`). 칸 경로 설정은 같은 파일 주석에 적힌 환경변수(`MARKET_PREV_CLOSE_FIELDS` 등)로 덮어쓸 수 있고, 빈 값일 때의 동작도 그 주석에 있습니다.
- 메시지 모양: `contracts/events/market.move.v1.schema.json`

### 내부 조회 API

브리핑이 부르는 API입니다. 요청값의 원본은 브리핑 `config.py`이고, 시세처리기는 요청이 자기 안전 상한보다 크면 상한까지만 답합니다. 시세 이력(`/internal/market/history`)은 `history/HistoryQuery`가 답합니다.

- **배경 자료** `/internal/market/context`: `context/ContextQuery`가 답합니다. 종류별 최신 원문, ETF에 담긴 미국 종목과 직전 정규장 등락률(`insight/OverseasCloseRateQuery`, 고르는 규칙은 그 주석), 세계지수 등락률(`indexRates`, 계산은 `context/WorldIndexRates`)을 줍니다. 안전 상한은 `application.yml`의 `market.context.*`입니다.
- **데일리 판 자료** `/internal/market/daily`: `context/DailyQuery`가 답합니다. 요청·응답 모양과 선택 칸 `includeIntradayFallback`(장중 대체값), `includeOperations`(운영 상태, `context/OperationsRepository`), `openFlowSince`(개장 흐름, `context/OpenFlowCalculator`)의 뜻은 `contracts/events/market.daily.requested.v1.schema.json`, `market.daily.responded.v2.schema.json`이 원본입니다. ETF 목록은 사용 중인 멤버 포트폴리오(`personal.active_portfolio`)의 국내 ETF이며, 보유·관심 구분은 응답에 싣지 않고 브리핑이 멤버마다 `personal`에서 읽습니다. 안전 상한은 `market.context.*`를 함께 쓰고, 따로 두는 것은 `market.daily.*`(`max-etfs`, `open-flow-max-window`)입니다.

## 뉴스처리기 (`news`)

`news.collected`와 함께 공시 이벤트 `filing.collected`를 별도 consumer group(`news-filings`, `FilingListener`)으로 구독해 `filing`(접수번호 유니크)과 `filing_ticker`에 저장합니다. 같은 공시가 다시 오면 새로 연결된 티커만 더하고, 재시도 후에도 실패한 이벤트는 `failed_event`에 남깁니다. 종목·기간 조회는 `FilingQuery`이며 기준 시각보다 늦게 접수된 공시는 돌려주지 않습니다. 기사·공시 조회 API(`/internal/news/articles`, `/internal/news/filings`)와 저장 끝 알림은 [conventions.md](../conventions.md)에 있습니다. 테이블 구조의 원본은 `services/news/src/main/resources/db/migration/`입니다.

## 조치 판정 확인기 (`ops-checker`)

수집기의 판 시작 알림(`daily.collection.completed`)을 받아, 개장일 마감 후 판에서 기준 파일의 지표를 읽기 전용으로 계산합니다. 휴장일에는 휴장 결과만 남기고, 아침 판 알림에는 아무것도 하지 않습니다. 결과는 DB `ops` 스키마(`check_run`, `check_result`)와 S3 결과 파일에 남고, GitHub Actions(`.github/workflows/ops-verdict.yml`)가 그 파일로 이슈를 판정합니다. 판정 절차는 [incidents](../incidents/README.md), 설계는 [ADR-014](../adr/014-ops-checker.md)에 있습니다. 기준 파일 형식의 원본은 `services/ops-checker/src/ops_checker/criteria.py`, 설정과 기본값은 같은 폴더의 `config.py`입니다.

- **DB 계정:** `OPS_CHECKER_DB_USER`·`OPS_CHECKER_DB_PASSWORD`([setup.md](setup.md)). `market`·`briefing` 스키마를 읽기만 하고 `ops`에만 씁니다(`personal`은 읽지 못함). 계정이 없는 기존 개발 DB에서는 `docker compose -f infra/compose/docker-compose.yml --env-file .env run --rm ops-init`과 `run --rm ops-migrate`를 다시 돌립니다.
- **결과 버킷:** `OPS_RESULTS_BUCKET`(S3 버킷 이름)과 AWS 자격증명이 필요합니다. `.env.example`에 없으므로 로컬에서는 환경변수로 직접 줍니다. 운영 값은 [`infra/deploy/README.md`](../../infra/deploy/README.md)에 있습니다.
- **venv:** 확인기 전용 `services/ops-checker/.venv`(`scripts/setup-dev.sh`가 만듦)

```bash
# 테스트 (실제 Kafka·DB·S3를 부르지 않음)
services/ops-checker/.venv/bin/python -m pytest services/ops-checker
# 판정 스크립트 테스트 (가짜 결과 파일, GitHub에 접속하지 않음)
services/briefing/.venv/bin/python -m pytest -c scripts/pytest.ini scripts/tests/test_ops_verdict.py
```

실제 DB 테스트는 켜는 조건이 있을 때만 돌고, `scripts/test-all.sh`의 실제 DB 단계가 임시 DB에서 돌립니다. 켜는 조건의 원본은 [테스트 지도](../test-map/ops-checker.md)입니다.

### 기준 파일 쓰기와 운영 반영

1. 판정 기준을 `ops-checks/<이슈>-<시도>.yml`로 씁니다(형식은 `criteria.py`, 언제 쓰는지는 [incidents](../incidents/README.md)).
2. 지표 SQL을 개발 DB에서 읽기 전용으로 먼저 돌려, 한 행 한 칸(0~1 비율 또는 빈 값)을 내는지 확인합니다.
3. 운영 배포 전에 머지해 이미지에 실려 나가게 합니다. 처음 운영에 반영할 때 할 일은 [`infra/deploy/README.md`](../../infra/deploy/README.md)의 "조치 판정 확인기 운영 반영"에 있습니다.

## 이메일 발송기 (`notifier`)

`briefing.generated`(급변동 건별)와 `briefing.daily.generated`(멤버 한 명의 판 하나)를 받아 이메일로 보냅니다. 메시지 모양은 `contracts/events/`, 토픽 선언은 발송기의 `Topics`가 원본입니다.

**기본은 드라이런입니다**(로그만 남기고 보내지 않음). 실제로 보내려면 사람이 직접 `.env`에서 `NOTIFIER_SEND_ENABLED=true`로 바꾸고 `SMTP_*`를 채웁니다. 키 이름은 `.env.example`, 기본값은 `services/notifier/src/main/resources/application.yml`(`notifier.*`, `spring.mail.*`)이 원본입니다. SMTP 사용자는 발신 주소가 되므로 `.env`에만 둡니다. 채널을 고른 이유는 [ADR-011](../adr/011-notification-channel.md)에 있습니다.

- **받는 사람:** 설정값이 아니라 `personal.member_mail`에 있고, 보내기 직전에 읽기 전용 계정(`NOTIFIER_DB_USER`)으로 읽습니다. 수신 조건과 처리 보장은 `DailyBriefingListener` 주석에 있습니다.
- **DB 오류:** 받는 사람 조회가 실패하면 Kafka 오류 처리가 재시도하고, 그래도 실패하면 그 메시지를 건너뜁니다. DB 오류가 재시도 시간보다 길면 그 메일은 나가지 않습니다(간격과 횟수는 `NotifierApplication`의 `FixedBackOff`).
- **안전 규칙(테스트로 검증):** 설정이 없으면 드라이런이고, 발송을 켰는데 주소나 호스트가 비어 있으면 시작 단계에서 실패합니다. 같은 브리핑은 `eventId` 기준으로 한 번만 보내며, 필수 값이 빠졌거나 형식이 잘못된 메시지는 건너뜁니다. **로그에는 주소를 마스킹해서만 출력하고**, 드라이런 로그에도 본문은 찍지 않습니다.
- **`.env`에는 ASCII만 씁니다**(`.env.example` 참고). 한글 기본값은 `application.yml`에 있습니다.

```bash
java -jar services/notifier/build/libs/notifier-0.0.1-SNAPSHOT.jar   # 저장소 루트에서
```
