# 테스트

원본 문서는 둘입니다. 여기에는 옮겨 적지 않습니다.

- 테스트 명령과 변경 범위별로 돌릴 것: [`.claude/rules/testing.md`](../../.claude/rules/testing.md)
- 기능별 테스트 목록과 켜는 조건(환경변수): 테스트 지도 [`docs/test-map/`](../test-map/README.md)

테스트 개수는 적지 않습니다. 전체 실행의 결과 표로 확인합니다.

## 설치와 환경 준비

JDK 21, `JAVA_HOME_21`, 브리핑·확인기 venv, `.env`를 준비합니다. 방법은 [setup.md](setup.md)의 "준비"에 있습니다. 빠진 것은 `scripts/setup-dev.sh --check`로 확인합니다.

## 실제 DB 테스트 (전체 실행)

```bash
scripts/test-all.sh          # 단위·가상 테스트와 실제 DB 테스트를 돌리고, 끝에 기능별 결과 표 출력
scripts/test-all.sh --plan   # 아무것도 띄우지 않고 실행 단계만 보기
```

단계 목록의 원본은 `--plan` 출력과 `scripts/test-all.sh` 머리 주석입니다.

- 개발용 Postgres 컨테이너 안에 이 실행만 쓰는 테스트 데이터베이스를 만들고, 끝나면 지웁니다. 개발용 데이터베이스의 데이터는 건드리지 않습니다. 컨테이너가 꺼져 있으면 켜고, 끝나도 끄지 않습니다.
- `.env`의 서비스 계정이 DB에 이미 있어야 합니다. 필요한 키는 스크립트의 계정 확인 부분에 나열돼 있으며 `OPS_CHECKER_DB_*`도 들어갑니다.
- 확인기 계정을 뺀 나머지 계정은 만들지 않습니다. 계정이 없으면 `서비스 계정 <이름> 이 없음`으로 실패합니다. 이때는 [setup.md](setup.md)의 "계정이 없을 때 만드는 법"을 따릅니다.
- 확인기 계정은 없으면 스크립트가 `infra/db/ops/init.sql`로 만듭니다.
- 결과 표는 지도의 기능마다 돌았음, 건너뜀(이유), 실패를 보여 줍니다. 하나라도 실패하면 종료 코드 1입니다. 돌아야 할 테스트가 건너뜀이면 원인을 찾습니다.
- 이전 실행이 남긴 테스트 데이터베이스나 컨테이너가 있으면 지우지 않고, 지우는 명령만 안내합니다.

### DB 테스트 하나만 돌리기

켜는 환경변수(지도의 `켜는 조건`)에 비어 있는 새 데이터베이스를 넘깁니다. 개발용 DB와 분리된 임시 컨테이너를 쓰고, 끝나면 지웁니다. 서비스끼리 같은 컨테이너를 이어 쓰지 말고 따로 띄웁니다. 대상 DB에 필요한 마이그레이션과 계정 권한이 무엇인지는 지도의 해당 행과 테스트 코드를 봅니다.

예: 시세처리기

```bash
# 이미지는 infra/compose/docker-compose.yml 의 postgres 와 같은 것을 쓴다. 비밀번호는 일회용 값.
docker run -d --name market-it-pg -e POSTGRES_PASSWORD=change-me -e POSTGRES_DB=demo \
  -p 127.0.0.1::5432 <compose 의 postgres 이미지>
PORT="$(docker port market-it-pg 5432/tcp | head -1 | sed 's/.*://')"   # 준비될 때까지 몇 초 기다린다
MARKET_IT_DB_URL="jdbc:postgresql://127.0.0.1:$PORT/demo" MARKET_IT_DB_USER=postgres \
  MARKET_IT_DB_PASSWORD=change-me \
  ./gradlew :services:market:cleanTest :services:market:test --tests '*IntegrationTest' --no-build-cache
docker rm -f market-it-pg
```

## 실제 외부 호출 (`--live`)

```bash
TEST_ALL_AWS_PROFILE=<AWS 프로필> scripts/test-all.sh --live
```

전체 실행에 더해 시세 흐름 하나와 브리핑·메일 흐름 하나를 실제 외부 호출로 돌립니다. 호출 수, 필요한 키, 단계의 원본은 `scripts/test-all.sh` 머리 주석, `LIVE_KEYS`, `--live --plan` 출력입니다. 키 이름과 Parameter Store 경로의 대응은 `scripts/load_config.py`에 있습니다.

안전 규칙:

- **사람이 직접 결정할 때만 켭니다.** 실제 호출 한도와 비용을 쓰고, 실제 메일이 갑니다.
- `TEST_ALL_AWS_PROFILE`은 필수입니다. 엉뚱한 계정을 읽지 않도록 기본 자격증명으로 넘어가지 않습니다.
- `.env`의 `NOTIFIER_SEND_ENABLED`는 바꾸지 않습니다. 발송은 스크립트가 띄운 테스트용 발송기에서만 켜지고, 제목 앞에 `[테스트]`가 붙습니다.
- 키는 실행할 때마다 Parameter Store에서 새로 읽어 임시 폴더에만 두고, 끝나면 지웁니다. `.env`와 실행 환경변수에 있는 같은 이름의 키는 쓰지 않습니다. 필수 키가 하나라도 비면 시세를 부르기 전에 실패로 끝납니다.
- LLM 리포트 본문은 받은 메일과 `build/test-all-live/`에만 남습니다. 이 폴더는 git에서 제외됩니다.

## 실데이터 재생·부하 테스트

저장해 둔 실제 응답이나 정한 부하로 도는 테스트입니다. 켜는 조건은 지도(`collector.md`, `market.md`)에 있습니다. 입력·기대 파일은 저장소 밖에 둡니다. 저장소 안 경로를 주면 거절합니다. 결과는 건수만 출력합니다.

- **시세 응답 칸 검사**(`BROKER_PRICE_CHECK_*`)를 바꾸면 `PriceCheckReplayTest`를 돌립니다. 버리는 행이 기대 목록과 같은지 봅니다. 칸 이름의 기본값은 수집기의 `application.yml`에 있습니다.
  ```bash
  # 입력: 한 줄에 eventId<TAB>price<TAB>raw JSON / 기대: eventId 한 줄씩, 또는 첫 칸이 snapshot_event_id 인 CSV
  PRICE_CHECK_REPLAY_INPUT=<저장소 밖>/raws.tsv PRICE_CHECK_REPLAY_EXPECTED=<저장소 밖>/expected.csv \
    ./gradlew :services:collector:cleanTest :services:collector:test --no-build-cache --tests '*PriceCheckReplayTest'
  ```
- **급변동 감지 설정**(`market.*`)을 바꾸면 `MoveReplayTest`를 돌립니다. 유형별 `market.move` 건수를 셉니다. 무엇을 평가하는지는 테스트 코드를 봅니다.
  ```bash
  # 입력: 한 줄에 eventId<TAB>symbol<TAB>asOf(ISO)<TAB>price<TAB>raw JSON (받은 순서) / 제외(선택): 위 기대 파일과 같은 형식
  MOVE_REPLAY_INPUT=<저장소 밖>/snapshots.tsv MOVE_REPLAY_EXCLUDE=<저장소 밖>/rule-hit.csv \
    ./gradlew :services:market:cleanTest :services:market:test --no-build-cache --tests '*MoveReplayTest' -i
  ```
- **시세 조회 부하**는 `QuoteFixedLoadTest`로 잽니다. 고정 입력 뉴스 시험에는 가짜 검색 서버 `scripts/fake-news-search.py`를 씁니다.
