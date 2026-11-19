# 운영 배포 파일 (infra/deploy)

Lightsail 서버 2대에서 전체를 돌리는 데 쓰는 이미지·Compose·서버 스크립트입니다. 서버 만들기(Terraform, 서버 크기 포함)와 이미지를 올리고 버전 값을 쓰는 CI는 이 폴더 밖(`infra/terraform`, `.github/workflows`)에 있습니다.

| 서버 | 돌리는 것 | 파일 |
|---|---|---|
| 데이터 서버 | Kafka, Postgres | `compose/data.yml`, `server/data.env.example`, `server/backup-postgres.sh`, `server/systemd/stock-backup.*` |
| 서비스 서버 | collector, market, news, notifier, briefing, 조치 판정 확인기(`ops-checker`)와 DB 표 만들기 단계(`flyway-migrate`) | `compose/services.yml`, `server/deploy.env.example`, `server/apply-deploy.sh`, `server/systemd/stock-deploy-*` |

## 이미지

- 서비스: `services/<이름>/Dockerfile`, DB 표 만들기: `infra/deploy/migrate/Dockerfile`. 빌드 컨텍스트는 저장소 루트이고 `.dockerignore`가 비밀값(`.env*`)과 큰 폴더를 뺍니다. 이미지마다 담는 것은 각 Dockerfile에 있습니다. 실행 이미지는 `linux/amd64`로 고정합니다(Lightsail은 x86).
- 만드는 이미지 목록은 `.github/workflows/deploy-images.yml`의 `matrix`이고, 이름은 `${ECR_REGISTRY}/stock-reading-<이름>:${DEPLOY_VERSION}`, 버전은 커밋 해시입니다.
- DB 표 만들기 이미지는 배포한 버전의 SQL과 계정·권한 스크립트를 담습니다. **서비스 이미지와 같은 버전 태그로 올려야 합니다**(CI가 함께 만듭니다).
- **태그 고정 방침:** 앱 이미지는 커밋 해시 태그로 고정합니다(같은 커밋은 같은 앱 코드). 베이스 이미지(Java·파이썬 실행 이미지)는 일부러 떠다니는 태그를 써서 빌드할 때마다 보안 패치를 받습니다. 그래서 같은 커밋도 빌드한 날에 따라 베이스 계층은 다를 수 있습니다. Kafka·Postgres(pgvector)·Flyway는 버전을 고정합니다.
- 모든 이미지는 root가 아닌 사용자로 돕니다(각 Dockerfile의 `USER`).
- briefing은 DB 주소를 코드에서 `localhost:5432`로 고정해 읽습니다. 그래서 이미지 안의 `docker-entrypoint.sh`가 `localhost:5432`를 `DB_HOST`로 넘기는 중계(`socat`)를 함께 띄웁니다. 코드가 `DB_HOST`를 읽게 되면 이 중계는 없앱니다.

## 메모리

각 서비스 컨테이너에 메모리 상한(`mem_limit`)과 Java 힙 상한(`-Xmx`)을 둡니다. 값은 로컬 리허설 측정으로 정했고 결과는 [`rehearsal/memory-report.md`](rehearsal/memory-report.md)에 있습니다. 기본값은 `compose/services.yml`에 있고, Compose 환경변수(`*_MEM_LIMIT`, `*_JAVA_OPTS`)로 서버에서 바꿀 수 있습니다. DB 표 만들기 단계(`flyway-migrate`)도 상한(`MIGRATE_MEM_LIMIT`)이 있습니다. 배포 때 서비스가 도는 중에 한 번 더 뜨기 때문입니다.

서버마다 스왑 파일을 둡니다(크기는 `server/setup-swap.sh`, 처음 한 번 할 일 참고). 메모리가 모자랄 때 프로세스가 강제 종료되는 대신 느려지게 하는 완충입니다.

## 배포 (서비스 서버)

서비스 서버의 systemd 타이머 두 개가 같은 스크립트 `server/apply-deploy.sh`를 부릅니다. 사람이 직접 실행하는 복구 경로(`recover`)도 같은 스크립트입니다(아래 "장중 복구"). Parameter Store 값은 **읽기만** 합니다(쓰는 것은 GitHub Actions입니다).

| 타이머 | 경로 | 하는 일 |
|---|---|---|
| `stock-deploy-scheduled.timer` | `scheduled` | 매일 07:00(Asia/Seoul)에 **대기 버전**을 읽습니다. 지금 버전과 같으면(전날 07:00 뒤로 main이 바뀌지 않았으면) 아무것도 하지 않고, 다르면 ECR에서 받아 서비스만 다시 띄웁니다(`services.yml`만 다룸. Kafka·Postgres는 건드리지 않음) |
| `stock-deploy-urgent.timer` | `urgent` | 5분마다 **긴급 버전**을 읽습니다. 새 값이고 **대기 버전과 같을 때만** 같은 방식으로 적용합니다 |

- **적용 순서:** 저장소 사본 맞추기 → 비밀 설정 받기 → 이미지 받기 → DB 표 만들기(`flyway-migrate`를 서비스와 따로 먼저) → 서비스 다시 띄우기(표 만들기를 뺀 서비스만 `up --no-deps`). 표 만들기가 실패하면 서비스를 바꾸지 않으므로 옛 버전이 계속 돕니다.
- **배포 금지 시간:** 매일(주말 포함) 한국 시간 09:00:00~15:30:59에는 정기·긴급 경로로 적용하지 않습니다. 서버가 꺼져 있다 늦게 돈 정기 실행도 같습니다. 이미지를 받는 사이에 금지 시간이 되면 다시 띄우기 직전에 멈춥니다.
- **금지 시간에 들어온 긴급 요청은 버립니다.** 금지 시간에 처음 본 긴급 값은 "본 요청"으로 기록해 두므로 15:31이 되어도 저절로 적용하지 않습니다. 다시 배포하려면 새 긴급 값을 써야 합니다.
- **긴급 배포는 "대기 버전을 07:00까지 기다리지 않고 지금 적용"만 합니다.** 긴급 값이 대기 버전과 다르면 적용하지 않고 이유(`긴급 버전이 대기 버전과 다름`)를 로그에 남깁니다. 다르면 다음 07:00 정기 배포가 대기 버전으로 되돌려 긴급 수정이 하루 만에 사라지기 때문입니다. 무시한 값도 "본 요청"으로 기록하므로, 나중에 대기 버전이 그 값이 되면 낮에 바로 적용하지 않고 다음 07:00 정기 배포가 적용합니다.
  - 긴급 버튼(`.github/workflows/emergency-deploy.yml`)은 그 시점의 대기 버전 값을 그대로 긴급 버전에 씁니다. 손으로 값을 쓸 때도 대기 버전을 먼저, 긴급 버전을 나중에 같은 값으로 씁니다.
- 정기 경로가 정한 시간(`flock -w`) 안에 배포 잠금(`lock`)을 얻지 못하면(긴급 배포가 오래 쥔 경우) 종료 코드 1로 끝나 systemd에서 실패로 보입니다. 그날 정기 배포는 다음 07:00으로 미뤄집니다.
- 새 버전을 띄운 뒤 이 프로젝트 이미지 중 새 버전과 바로 전 버전(되돌리기용)만 남기고 지웁니다(`prune_old_images`).
- 사본 맞추기·이미지 받기·표 만들기·다시 띄우기 중 하나가 실패하면 지금 버전 기록(`DEPLOY_STATE_DIR/current`)을 바꾸지 않고 종료 코드 1로 끝납니다. 정기 경로는 다음 07:00에, 긴급 경로는 5분 뒤 확인 때 같은 값을 다시 시도합니다.
- 적용·건너뜀·버림마다 이유를 로그(journald)에 남깁니다. 이미지·설정 값은 남기지 않습니다.
- 서비스를 다시 띄우면 수집기의 뉴스 검색 하루 호출 횟수(메모리에만 있음)가 0으로 돌아갑니다.
- 비밀 설정은 `DEPLOY_CONFIG_PARAM_PATH` 아래 Parameter Store 값에서 받아(이름 하나가 환경변수 하나, 예: `<경로>/MARKET_DB_PASSWORD`) 서버 안 `DEPLOY_STATE_DIR/runtime.env`(권한 600)에만 둡니다. 값은 작은따옴표로 감싸 써서 `$`, `#`, 공백이 들어 있어도 Compose가 그대로 읽습니다. 값에 작은따옴표가 있으면 감쌀 수 없어 배포를 실패로 멈춥니다.
- 받아 온 설정이 비었거나(경로 오류 등) 필수 값(`apply-deploy.sh`의 `required_secrets`)이 비면 서비스를 다시 띄우지 않고 실패로 끝납니다. `services.yml`도 같은 비밀번호가 비면 Compose 단계에서 실패하게 해 두었습니다(`${VAR:?}`).
- 시험용 환경변수: `DEPLOY_NOW`(지금 시각, ISO 8601), `DEPLOY_STATE_DIR`, `DEPLOY_STANDBY_PARAM`, `DEPLOY_URGENT_PARAM`, `DEPLOY_ENV_FILE`. 운영 복구에는 `DEPLOY_NOW`가 아니라 `recover`를 씁니다.

### 저장소 사본

서버에는 저장소 사본(`git clone`)이 `/opt/stock-reading`에 있습니다(systemd 유닛의 경로). Compose 파일과 배포 스크립트는 이 사본의 것을 씁니다. 배포 스크립트가 root로 git을 부르므로 사본은 root 소유로 둡니다(소유자가 다르면 git이 사본을 거부합니다).

- 배포 스크립트가 적용 전에 사본을 적용할 버전의 커밋으로 맞추고(`git fetch origin main` 뒤 그 커밋으로 checkout), 맞췄으면 새 스크립트로 처음부터 다시 실행합니다. 그래서 사본은 브랜치가 아닌 커밋을 가리킵니다(detached). 이미지와 Compose 파일·스크립트가 늘 같은 버전입니다.
- 버전 커밋이 main 이력에 없거나, fetch가 실패하거나, 사본에 손으로 고치거나 새로 만든 파일이 있으면(`git status`가 비어 있지 않으면) 사본도 서비스도 건드리지 않고 실패로 끝납니다. 서버 전용 설정은 사본 밖(`/etc/stock-reading/`)에만 둡니다.
- 사본 맞추기가 들어간 스크립트는 사본에 있으므로, 이 기능이 없는 사본은 한 번 손으로 갱신해야 그다음 배포부터 자동으로 맞춥니다.

### 장중 복구

배포가 실패해 서비스가 멈췄는데 금지 시간이라 정기·긴급 경로가 적용하지 않을 때 씁니다. 서비스 서버에서 사람이 직접 실행합니다.

```bash
sudo /opt/stock-reading/infra/deploy/server/apply-deploy.sh recover
```

- 금지 시간과 "지금 버전과 같음"을 보지 않고 **대기 버전**을 적용합니다. 적용 순서(사본 맞추기, 표 만들기 먼저)와 실패 처리는 위와 같습니다.
- 대기 버전 값이 없거나 모양이 틀리면 실패로 끝납니다. 긴급 요청 기록은 건드리지 않습니다.
- 바뀌지 않은 컨테이너는 다시 띄우지 않으므로, 잘 돌고 있는 서비스에 실행해도 재시작되지 않습니다. 대기 버전에 문제가 있어 멈췄다면 고친 커밋이 main에 들어가 대기 버전이 바뀐 뒤에 실행합니다.

## 조치 판정 확인기 운영 반영

확인기(`ops-checker`)는 수집기의 판 시작 알림으로 개장일 마감 후 판에 기준 파일(`ops-checks/`)의 지표를 읽기 전용으로 계산해 DB `ops` 스키마와 S3 결과 파일에 남기고, 평일 18:00(한국 시간) GitHub Actions(`ops-verdict`)가 그 결과로 이슈를 판정합니다([ADR-014](../../docs/adr/014-ops-checker.md)). 처음 운영에 반영할 때 아래를 순서대로 합니다(작업자가 직접).

1. `terraform apply`: 결과 버킷(공개 차단, 암호화, 보관 기간 지나면 삭제), 확인기용 쓰기 전용 사용자(`results/`에 올리기만 허용), Actions용 읽기 전용 신분을 만듭니다([`infra/terraform`](../terraform/README.md)). ECR에 확인기 저장소가 있어야 이미지가 올라갑니다.
2. Parameter Store(`DEPLOY_CONFIG_PARAM_PATH` 아래)에 `OPS_CHECKER_DB_PASSWORD`와, 확인기용 쓰기 전용 사용자(`terraform output ops_results_writer_iam_user`)의 접근 키(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)를 넣습니다. 키는 콘솔에서 만들고, 서버 계정의 키는 넣지 않습니다. 값은 로그·문서에 남기지 않습니다. 비밀번호는 배포 때 `flyway-migrate`가 `ops` 계정을 만들 때도 씁니다.
3. 서비스 서버 `/etc/stock-reading/deploy.env`에 `OPS_RESULTS_BUCKET`(결과 버킷 이름)을 넣습니다(`server/deploy.env.example`). 비어 있으면 확인기가 뜨지 않습니다.
4. GitHub 저장소 변수 `AWS_REGION`·`OPS_RESULTS_BUCKET`과 읽기 신분(`OPS_RESULTS_AWS_ROLE_ARN`, 접근 키를 쓰는 계정은 비밀값 `OPS_RESULTS_AWS_ACCESS_KEY_ID`·`OPS_RESULTS_AWS_SECRET_ACCESS_KEY`)을 넣습니다(`terraform output`).
5. 결과 라벨 `check-passed`, `check-failed`를 만듭니다.
6. 확인기가 든 버전이 서비스 서버에 배포되어 첫 개장일 결과 파일이 올라온 것을 확인한 뒤, 저장소 변수 `OPS_VERDICT_ENABLED`를 `true`로 켭니다. 그 전에 켜면 결과 파일이 없다는 실패 알림이 옵니다. 손 실행(`workflow_dispatch`)은 이 값과 상관없이 돕니다.

- 확인기는 서버 밖에 포트를 열지 않고 Kafka·DB·S3로 나가기만 합니다.
- 결과 파일 올리기에 실패한 날(그날 `ops` 장부의 `check_run`이 오류로 남습니다)의 결과는 다시 만들 수 없습니다. 수동 재계산 명령은 아직 없습니다.

## 백업

`stock-backup.timer`가 데이터 서버에서 매주(시각은 타이머 파일) `backup-postgres.sh`를 돌립니다. `pg_dump`(custom 형식)를 서버 안 임시 파일에 끝까지 받아 비지 않은 것만 `s3://<BACKUP_S3_BUCKET>/<BACKUP_S3_PREFIX>/<DB>-<날짜>-<시각>.dump`로 올립니다. 덤프가 중간에 끊기면 올리지 않고 실패로 끝납니다(오래된 파일 삭제는 버킷 수명주기 규칙의 몫, 기간은 `infra/terraform`의 `backup_retention_days`).

복원: 새 Postgres를 `data.yml`로 띄운 뒤(초기화 SQL이 기본 계정을 만듦) `pg_restore --clean --if-exists -d <DB> <덤프>`로 넣고, 서비스 서버에서 `flyway-migrate`(`personal` 계정·권한 포함)를 다시 돌립니다.

## 서버 설정 파일

- 서비스 서버 `/etc/stock-reading/deploy.env`: `server/deploy.env.example`을 복사해 채웁니다(비밀값 없음).
- 데이터 서버 `/etc/stock-reading/data.env`: `server/data.env.example`을 복사해 채웁니다(비밀번호 있음, 권한 600).
- systemd 유닛 설치: `server/systemd/*`를 `/etc/systemd/system/`에 두고 서비스 서버에서 `systemctl enable --now stock-deploy-scheduled.timer stock-deploy-urgent.timer`, 데이터 서버에서 `systemctl enable --now stock-backup.timer`.
- **처음 한 번 할 일(두 서버 모두):** `sudo infra/deploy/server/setup-swap.sh`로 스왑 파일을 만듭니다(이미 스왑이 있으면 아무것도 하지 않고, 재부팅 뒤에도 켜지도록 `/etc/fstab`에 적습니다).
- 서비스 서버가 처음 뜰 때는 먼저 데이터 서버를 `docker compose -f compose/data.yml --env-file /etc/stock-reading/data.env up -d`로 띄운 뒤, 서비스 서버에서 `sudo .../apply-deploy.sh recover`로 대기 버전을 띄웁니다(이후는 타이머가 합니다).
- 메일 발송(`NOTIFIER_SEND_ENABLED`)은 기본이 꺼짐(드라이런)이며 서버 설정에서 직접 켭니다.

## 로컬 리허설과 시험

- `rehearsal/run-rehearsal.sh`: 서버 2대(메모리 상한은 `REHEARSAL_MEM`, Docker 안의 Docker)를 흉내 내 `data.yml`·`services.yml` 전체를 띄우고, 가짜 종목·멤버를 넣고, 수집기 수동 명령과 데일리 판 시작 알림으로 한 바퀴 돌린 뒤 서비스별 메모리를 잽니다. 외부 API·AWS·메일을 부르지 않습니다. 결과는 `rehearsal/memory-report.md`.
- `rehearsal/test-server-scripts.sh`: 배포 적용·백업 스크립트를 가짜 `aws`·`docker`와 진짜 git(컨테이너 안 가짜 원격 저장소)으로 시험합니다. 확인하는 것은 파일 머리 주석에 있습니다. 정기·긴급 경로의 금지 시간 경계는 이 시험에 없습니다.
- 두 번째 배포 측정: 리허설 마지막에 서비스가 도는 중 새 버전 태그로 `up -d` 한 번을 해, DB 표 만들기 단계와 옛·새 서비스가 겹치는 순간의 서버 전체 메모리를 잽니다(`run-rehearsal.sh redeploy`). 실제 배포는 옛 서비스가 도는 중에 표 만들기를 먼저 따로 돌리므로 순서가 다릅니다(리허설은 아직 `up -d` 한 번).
- 셸 스크립트는 `shellcheck -S warning`을 통과해야 합니다.
