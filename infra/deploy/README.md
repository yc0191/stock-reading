# 운영 배포 파일 (infra/deploy)

Lightsail 서버 2대에서 전체를 돌리는 데 쓰는 이미지·Compose·서버 스크립트입니다. 서버 만들기(Terraform)와 이미지를 올리고 버전 값을 쓰는 CI는 이 폴더 밖(`infra/terraform`, `.github/workflows`)에 있습니다.

| 서버 | 돌리는 것 | 파일 |
|---|---|---|
| 데이터 서버 (2GB) | Kafka, Postgres. 서비스 서버의 사설 주소에서만 접속 | `compose/data.yml`, `server/data.env.example`, `server/backup-postgres.sh`, `server/systemd/stock-backup.*` |
| 서비스 서버 (2GB) | collector, market, news, notifier, briefing와 DB 표 만들기 단계(`flyway-migrate`) | `compose/services.yml`, `server/deploy.env.example`, `server/apply-deploy.sh`, `server/systemd/stock-deploy-*` |

## 이미지

- 서비스 5개: `services/<이름>/Dockerfile`. 빌드 컨텍스트는 저장소 루트(`docker build -f services/<이름>/Dockerfile .`)이고 `.dockerignore`가 비밀값(`.env*`)과 큰 폴더를 컨텍스트에서 뺍니다. Java 4개는 빌드 단계에서 `bootJar`를 만들어 jre 이미지에 담고, briefing은 venv(미리 만든 바퀴만 설치)와 한글 글꼴(SHA-256 고정, `scripts/install_font.py`)을 담습니다. 실행 이미지는 `linux/amd64`로 고정합니다(Lightsail은 x86).
- DB 표 만들기 이미지: `infra/deploy/migrate/Dockerfile`. 배포한 버전의 market·personal·briefing SQL과 `personal` 계정·권한 스크립트를 담아, 서버에 있는 저장소 사본이 낡아도 그 버전의 SQL이 적용됩니다. **서비스 이미지와 같은 버전 태그로 올려야 합니다**(CI가 6번째 이미지로 만듭니다).
- 이미지 이름은 `${ECR_REGISTRY}/stock-reading-<이름>:${DEPLOY_VERSION}`(이름: collector, market, news, notifier, briefing, migrate)이고, 버전은 커밋 해시입니다.
- **태그 고정 방침:** 앱 이미지는 커밋 해시 태그로 고정합니다(같은 커밋은 같은 앱 코드). 베이스 이미지(`eclipse-temurin`, `python`)는 일부러 떠다니는 태그(`21-jre`, `3.13-slim`)를 써서 빌드할 때마다 보안 패치를 받습니다. 그래서 같은 커밋도 빌드한 날에 따라 베이스 계층은 다를 수 있습니다. Kafka·Postgres(pgvector)·Flyway는 버전을 고정합니다.
- DB 표 만들기 이미지는 root가 아닌 사용자(`flyway`, uid 10001)로 돕니다(서비스 5개도 uid 10001).
- briefing은 DB 주소를 코드에서 `localhost:5432`로 고정해 읽습니다. 그래서 이미지 안의 `docker-entrypoint.sh`가 `localhost:5432`를 `DB_HOST`로 넘기는 중계(`socat`)를 함께 띄웁니다. 코드가 `DB_HOST`를 읽게 되면 이 중계는 없앱니다.

## 메모리

각 서비스 컨테이너에 메모리 상한(`mem_limit`)과 Java 힙 상한(`-Xmx`)을 둡니다. 값은 로컬 리허설 측정으로 정했고 결과는 [`rehearsal/memory-report.md`](rehearsal/memory-report.md)에 있습니다. 값은 Compose 환경변수(`*_MEM_LIMIT`, `*_JAVA_OPTS`)로 서버에서 바꿀 수 있습니다. DB 표 만들기 단계(`flyway-migrate`)도 상한(`MIGRATE_MEM_LIMIT`, 기본 384m)이 있습니다. 배포 때 서비스가 도는 중에 한 번 더 뜨기 때문입니다.

서버마다 스왑 파일 1GB를 둡니다(처음 한 번 할 일 참고). 메모리가 모자랄 때 프로세스가 강제 종료되는 대신 느려지게 하는 완충입니다.

## 배포 (서비스 서버)

서비스 서버의 systemd 타이머 두 개가 같은 스크립트 `server/apply-deploy.sh`를 부릅니다. Parameter Store 값은 **읽기만** 합니다(쓰는 것은 GitHub Actions입니다).

| 타이머 | 경로 | 하는 일 |
|---|---|---|
| `stock-deploy-scheduled.timer` | `scheduled` | 매일 00:00(Asia/Seoul)에 **대기 버전**을 읽습니다. 지금 버전과 같으면(그날 main이 바뀌지 않았으면) 아무것도 하지 않고, 다르면 ECR에서 받아 서비스만 다시 띄웁니다(`services.yml`만 다룸. Kafka·Postgres는 건드리지 않음) |
| `stock-deploy-urgent.timer` | `urgent` | 5분마다 **긴급 버전**을 읽습니다. 새 값이고 **대기 버전과 같을 때만** 같은 방식으로 적용합니다 |

- **배포 금지 시간:** 매일(주말 포함) 한국 시간 09:00:00~15:30:59에는 어떤 경로로도 적용하지 않습니다. 서버가 꺼져 있다 늦게 돈 정기 실행도 같습니다. 이미지를 받는 사이에 금지 시간이 되면 다시 띄우기 직전에 멈춥니다.
- **금지 시간에 들어온 긴급 요청은 버립니다.** 금지 시간에 처음 본 긴급 값은 "본 요청"으로 기록해 두므로 15:31이 되어도 저절로 적용하지 않습니다. 다시 배포하려면 새 긴급 값을 써야 합니다.
- **긴급 배포는 "대기 버전을 00:00까지 기다리지 않고 지금 적용"만 합니다.** 긴급 값이 대기 버전과 다르면 적용하지 않고 이유(`긴급 버전이 대기 버전과 다름`)를 로그에 남깁니다. 다르면 다음 00:00 정기 배포가 대기 버전으로 되돌려 긴급 수정이 하루 만에 사라지기 때문입니다. 무시한 값은 "본 요청"으로 기록하지 않으므로 대기 버전이 그 값으로 바뀌면 다음 확인(5분 안)에 적용합니다.
  - **GitHub Actions(버튼) 쪽이 따를 규칙:** 긴급 버튼은 대기 버전과 긴급 버전에 **같은 값(대기 버전으로 이미 올라간 커밋)**만 씁니다. 대기 버전보다 새 커밋을 바로 적용하려면 대기 버전도 같은 값으로 함께 써야 합니다(대기 버전을 먼저, 긴급 버전을 나중에 쓰면 중간 상태에서 무시되는 일이 없습니다). 이 규칙은 `infra/terraform`·`.github/workflows` 구간의 몫입니다.
- 정기 경로가 15분 안에 배포 잠금(`lock`)을 얻지 못하면(긴급 배포가 오래 쥔 경우) 종료 코드 1로 끝나 systemd에서 실패로 보입니다. 그날 정기 배포는 다음 00:00으로 미뤄집니다.
- 새 버전을 띄운 뒤 7일 넘게 안 쓴 이미지를 지웁니다(`docker image prune -af --filter until=168h`). 롤백용 최근 이미지는 남습니다.
- 다시 띄우는 데 실패하면 지금 버전 기록(`DEPLOY_STATE_DIR/current`)을 바꾸지 않고 종료 코드 1로 끝납니다. 정기 경로는 다음 00:00에, 긴급 경로는 5분 뒤 확인 때 같은 값을 다시 시도합니다.
- 적용·건너뜀·버림마다 이유를 로그(journald)에 남깁니다. 이미지·설정 값은 남기지 않습니다.
- 서비스를 다시 띄우면 수집기의 뉴스 검색 하루 호출 횟수(메모리에만 있음)가 0으로 돌아갑니다.
- 비밀 설정은 `DEPLOY_CONFIG_PARAM_PATH` 아래 Parameter Store 값에서 받아(이름 하나가 환경변수 하나, 예: `<경로>/MARKET_DB_PASSWORD`) 서버 안 `DEPLOY_STATE_DIR/runtime.env`(권한 600)에만 둡니다. 값은 작은따옴표로 감싸 써서 `$`, `#`, 공백이 들어 있어도 Compose가 그대로 읽습니다. 값에 작은따옴표가 있으면 감쌀 수 없어 배포를 실패로 멈춥니다.
- 받아 온 설정이 비었거나(경로 오류 등) 필수 값(DB 비밀번호 8개)이 비면 서비스를 다시 띄우지 않고 실패로 끝납니다. `services.yml`도 같은 비밀번호가 비면 Compose 단계에서 실패하게 해 두었습니다(`${VAR:?}`).
- 시험·수동 실행을 위한 환경변수: `DEPLOY_NOW`(지금 시각, ISO 8601), `DEPLOY_STATE_DIR`, `DEPLOY_STANDBY_PARAM`, `DEPLOY_URGENT_PARAM`, `DEPLOY_ENV_FILE`.

서버에는 저장소 사본이 `/opt/stock-reading`에 있다고 가정합니다(systemd 유닛의 경로). 사본은 자동으로 갱신하지 않으므로 Compose·스크립트를 바꾼 릴리스는 서버에서 사본을 갱신해야 합니다(DB SQL은 이미지에 들어 있어 해당하지 않습니다).

## 백업

`stock-backup.timer`가 데이터 서버에서 매주 일요일 03:30(Asia/Seoul)에 `backup-postgres.sh`를 돌립니다. `pg_dump`(custom 형식)를 서버 안 임시 파일에 끝까지 받아 비지 않은 것만 `s3://<BACKUP_S3_BUCKET>/<BACKUP_S3_PREFIX>/<DB>-<날짜>-<시각>.dump`로 올립니다. 덤프가 중간에 끊기면 올리지 않고 실패로 끝납니다(5주 지난 파일 삭제는 버킷 수명주기 규칙의 몫).

복원: 새 Postgres를 `data.yml`로 띄운 뒤(초기화 SQL이 기본 계정을 만듦) `pg_restore --clean --if-exists -d <DB> <덤프>`로 넣고, 서비스 서버에서 `flyway-migrate`(`personal` 계정·권한 포함)를 다시 돌립니다.

## 서버 설정 파일

- 서비스 서버 `/etc/stock-reading/deploy.env`: `server/deploy.env.example`을 복사해 채웁니다(비밀값 없음).
- 데이터 서버 `/etc/stock-reading/data.env`: `server/data.env.example`을 복사해 채웁니다(비밀번호 있음, 권한 600).
- systemd 유닛 설치: `server/systemd/*`를 `/etc/systemd/system/`에 두고 서비스 서버에서 `systemctl enable --now stock-deploy-scheduled.timer stock-deploy-urgent.timer`, 데이터 서버에서 `systemctl enable --now stock-backup.timer`.
- **처음 한 번 할 일(두 서버 모두):** 스왑 파일 1GB를 만듭니다. `sudo infra/deploy/server/setup-swap.sh`(이미 스왑이 있으면 아무것도 하지 않음). 재부팅 뒤에도 켜지도록 `/etc/fstab`에 적습니다.
- 서비스 서버가 처음 뜰 때는 먼저 데이터 서버를 `docker compose -f compose/data.yml --env-file /etc/stock-reading/data.env up -d`로 띄운 뒤, `DEPLOY_VERSION=<버전> docker compose -f compose/services.yml --env-file ... up -d`로 서비스를 띄웁니다(이후는 타이머가 합니다).
- 메일 발송(`NOTIFIER_SEND_ENABLED`)은 기본이 꺼짐(드라이런)이며 사람이 서버 설정에서 직접 켭니다.

## 로컬 리허설과 시험

- `rehearsal/run-rehearsal.sh`: 서버 2대(각 메모리 상한 2GB, Docker 안의 Docker)를 흉내 내 `data.yml`·`services.yml` 전체를 띄우고, 가짜 종목·멤버를 넣고, 가짜 tick으로 한 바퀴 돌린 뒤 서비스별 메모리를 잽니다. 외부 API·AWS·메일을 부르지 않습니다. 결과는 `rehearsal/memory-report.md`.
- `rehearsal/test-server-scripts.sh`: 배포 적용·백업 스크립트의 실패 경로(가짜 `aws`·`docker`)와 비밀 설정 값 처리(`$`·`#`·빈 설정·빈 필수 값), 긴급=대기일 때만 적용, 이미지 정리, 잠금 실패. 정상 경로와 금지 시간 경계는 수락 기준의 `deploy-window.sh`가 확인합니다.
- 두 번째 배포 측정: 리허설 마지막에 서비스가 도는 중 새 버전 태그로 `up -d`를 해 DB 표 만들기 단계와 옛·새 서비스가 겹치는 순간의 서버 전체 메모리를 잽니다(`run-rehearsal.sh redeploy`).
- 셸 스크립트는 `shellcheck -S warning`을 통과해야 합니다.
