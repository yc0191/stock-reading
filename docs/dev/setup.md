# 준비와 DB

## 준비

- JDK 21: 이 프로젝트에서만 지정하면 되고, 기본 Java는 바꾸지 않아도 됩니다.
- Docker Desktop: 실행 중이어야 합니다.
- Gradle은 래퍼(`./gradlew`)를 씁니다.

새로 클론했거나 새 작업 폴더를 만들었으면 `scripts/setup-dev.sh`를 먼저 돌립니다. 이 스크립트는 `.env`와 `JAVA_HOME_21`(JDK 21)을 확인해 알려 주고, 브리핑·확인기 venv(프로젝트 전용 파이썬 환경) 만들기·의존성 설치와 차트 글꼴 내려받기는 직접 합니다. `--check`를 주면 아무것도 만들지 않고 상태만 봅니다. 항목별로 무엇을 하는지는 스크립트 머리 주석에 있습니다.

`.env`는 `.env.example`을 복사해서 만들고 값은 사람이 채웁니다. `.env`는 커밋하지 않습니다. 값에는 ASCII만 씁니다. 스프링이 Latin-1로 읽기 때문에 한글이 깨집니다. 키 목록과 형식의 원본은 `.env.example`입니다.

```bash
# JDK 21 위치 지정 (Homebrew 예시)
export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
export JAVA_HOME_21=$JAVA_HOME

# 빌드와 테스트 (Docker 불필요)
./gradlew test bootJar

# 인프라 기동 (Kafka, PostgreSQL, 마이그레이션)
cp .env.example .env        # 값 채우기
docker compose -f infra/compose/docker-compose.yml --env-file .env up -d

# 서비스 실행 (저장소 루트에서)
java -jar services/collector/build/libs/collector-0.0.1-SNAPSHOT.jar
java -jar services/market/build/libs/market-0.0.1-SNAPSHOT.jar
java -jar services/news/build/libs/news-0.0.1-SNAPSHOT.jar
```

서비스의 시작 순서는 상관없습니다. 토픽은 각 서비스가 선언합니다. 브리핑과 발송기 실행 방법은 [briefing.md](briefing.md)와 [services.md](services.md)의 "이메일 발송기"에 있습니다.

## 업무 데이터의 원본

종목, 구성 종목, 뉴스 검색어, 멤버, 메일 수신 설정, 포트폴리오 같은 업무 데이터는 DB가 원본입니다. 비밀값과 실행 설정은 이와 따로 Parameter Store에서 로더를 거쳐 `.env`로 옵니다([config.md](config.md)). 업무 데이터 값은 사람이 SQL로 넣고, 서비스는 그 값을 읽기만 합니다.

| 스키마 | 표를 만드는 곳 |
|---|---|
| `market` (주인: 시세처리기) | `services/market/src/main/resources/db/migration/` |
| `personal` (주인: `PERSONAL_OWNER_DB_USER`) | `infra/db/personal/migration/` |
| `briefing` | `services/briefing/db/migration/` |
| `ops` (확인기 장부) | `infra/db/ops/migration/` |

표와 뷰가 무엇을 담는지는 각 마이그레이션 파일의 주석을 봅니다.

## DB 계정

계정 값은 `.env`에만 둡니다. 키는 모두 `.env.example`에 있습니다.

| 키 (`_USER` / `_PASSWORD`) | 만드는 곳 | 뜻 |
|---|---|---|
| `MARKET_DB_*` | `infra/compose/postgres/init/01-roles.sql` | 시세처리기 계정이며 `market`의 주인입니다 |
| `NEWS_DB_*` | `infra/compose/postgres/init/02-roles-news.sql` | 뉴스처리기 계정입니다 |
| `BRIEFING_DB_*` | `infra/compose/postgres/init/03-roles-briefing.sql` | 브리핑 계정이며 `briefing`의 주인입니다 |
| `PERSONAL_OWNER_DB_*` | `infra/db/personal/init.sql` | `personal`의 주인이며, Flyway가 이 계정으로 표를 만듭니다 |
| `PERSONAL_WRITER_DB_*` | `infra/db/personal/init.sql` | 사람이 SQL로 값을 넣고 고칠 때 씁니다 |
| `COLLECTOR_DB_*` | `infra/db/personal/init.sql` | 수집기 계정이며 읽기만 합니다 |
| `NOTIFIER_DB_*` | `infra/db/personal/init.sql` | 발송기 계정이며 받는 사람을 고르려고 읽습니다 |
| `OPS_CHECKER_DB_*` | `infra/db/ops/init.sql` | 확인기 계정이며 `ops`에 씁니다 |

계정마다 무엇을 읽고 쓸 수 있는지의 원본은 아래 파일입니다.

- `infra/db/personal/init.sql` 머리 주석
- `infra/db/ops/init.sql` 머리 주석
- 브리핑 계정의 `personal` 열 권한: `infra/db/personal/migration/V2__grant_briefing_read.sql`, `V3__grant_briefing_read_stock.sql`

메일 주소 같은 개인정보 열은 발송기 계정과 쓰기 계정만 읽습니다.

### 계정이 없을 때 만드는 법

- **personal 계정과 확인기 계정:** `init.sql`은 여러 번 돌려도 결과가 같습니다. 이미 있는 계정의 비밀번호는 바꾸지 않습니다.
  ```bash
  docker compose -f infra/compose/docker-compose.yml --env-file .env run --rm personal-init
  docker compose -f infra/compose/docker-compose.yml --env-file .env run --rm ops-init
  ```
  Compose 밖에서는 `.env`의 계정 키를 환경변수로 둔 채 관리자 계정으로 `psql -v ON_ERROR_STOP=1 -d <데이터베이스> -f infra/db/personal/init.sql`을 실행합니다. ops는 `infra/db/ops/init.sql`로 같은 방법을 씁니다.
- **시세처리기·뉴스·브리핑 계정:** `infra/compose/postgres/init/`의 스크립트는 DB 볼륨을 처음 만들 때만 돕니다. `.env`에 키를 넣은 뒤 볼륨을 다시 만듭니다. 키가 비어 있으면 이 스크립트가 실패합니다.

```bash
# 로컬 DB를 지우고 다시 만든다 (모든 서비스의 DB 데이터가 사라진다)
docker compose -f infra/compose/docker-compose.yml --env-file .env down -v
docker compose -f infra/compose/docker-compose.yml --env-file .env up -d
```

## 만드는 순서와 Flyway

`personal` 표는 `market.etf`와 `market.stock`을 가리킵니다. 확인기 권한은 `market`과 `briefing` 스키마가 먼저 있어야 줄 수 있습니다. 그래서 `up -d`는 아래 순서로 한 번씩 돌고 끝납니다. 정확한 의존 관계는 `infra/compose/docker-compose.yml`의 `depends_on`이 원본입니다.

1. 볼륨을 처음 만들 때 계정 초기화(`infra/compose/postgres/init/`)
2. `market-migrate`, `briefing-migrate`
3. `personal-init` → `personal-migrate`
4. `ops-init` → `ops-migrate`

번호 파일을 더한 뒤에는 해당 서비스를 다시 돌립니다.

```bash
docker compose -f infra/compose/docker-compose.yml --env-file .env run --rm <market-migrate|personal-migrate|briefing-migrate|ops-migrate>
```

- 브리핑은 표를 직접 만들지 않습니다. 그래서 브리핑을 띄우기 전에 `briefing-migrate`가 끝나 있어야 합니다. DB에 연결할 수 없거나 표가 없으면 브리핑은 시작하지 않습니다.
- 이미 적용한 번호 파일은 고치지 않고 새 번호로 더합니다. 적용한 파일을 바꾸면 Flyway 검사에서 멈춥니다.
- 실제 DB 테스트는 테스트 전용 데이터베이스에 같은 순서를 적용합니다(`scripts/personal-db-lib.sh`, [testing.md](testing.md)).

## 처음 값 넣기

실제 값은 저장소 밖의 비공개 SQL로만 넣습니다. 형식은 가짜 값으로 쓴 예시 [`docs/examples/holdings.example.sql`](../examples/holdings.example.sql)을 따릅니다. `market` 표는 시세처리기 계정으로, `personal` 표는 쓰기 계정으로 실행합니다.

```bash
psql -v ON_ERROR_STOP=1 -U <계정> -d <데이터베이스> -f <파일>
```

## 바뀐 값이 반영되는 때

- 수집기와 뉴스는 시작할 때 한 번 읽습니다. 그래서 값을 바꾸면 재시작해야 반영됩니다. 읽지 못하면 시작을 멈추므로 `personal` 스키마와 계정이 먼저 있어야 합니다.
- 시세처리기는 필요할 때마다 그때그때 읽습니다.
- 발송기는 메일을 보내기 직전에 받는 사람을 고릅니다. 받는 사람이 0명이면 보내지 않고 경고만 남깁니다.
