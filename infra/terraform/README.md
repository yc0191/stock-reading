# 운영 서버 만들기 (Terraform)

Lightsail 서버 2대, 방화벽, ECR 저장소 6개, S3 백업 버킷, 배포 버전 값(Parameter Store), IAM을 만듭니다. 서버 안에 올라가는 파일(Compose, 배포 스크립트)은 [`infra/deploy`](../deploy/README.md), 이미지를 올리고 버전 값을 쓰는 CI는 `.github/workflows/`에 있습니다.

## 만드는 것

| 대상 | 내용 |
|---|---|
| Lightsail 2대 | `<접두사>-data`(Kafka·Postgres), `<접두사>-service`(서비스 5개). 묶음은 변수 `bundle_id` |
| 방화벽 | 서버마다 꼭 필요한 접속만 연다(`lightsail.tf`) |
| ECR 6개 | `stock-reading-{collector,market,news,notifier,briefing,migrate}`. 최근 3개만 보관 |
| S3 버킷 | Postgres 백업. 35일(5주) 지나면 삭제, 공개 차단, TLS만 허용 |
| Parameter Store | 대기 버전·긴급 버전 값. 이름만 만들고 값은 CI가 씀(Terraform은 값을 되돌리지 않음) |
| IAM | GitHub Actions 권한(ECR 푸시, 배포 버전 쓰기, 대기 버전 읽기)과 서버용 권한(설정 읽기, 메일 발송, ECR 받기, 백업 쓰기, 배포 버전 읽기). 접근 키는 만들지 않음 |

비밀 설정 값은 이 코드가 만들지 않고 직접 넣습니다.

## 계정 정보는 저장소에 두지 않습니다

계정 번호, 리전, 버킷 이름, 저장소 이름은 변수로 받고 값은 `*.tfvars`(gitignore)에 둡니다. 상태 파일도 저장소 밖에 둡니다.

```sh
STATE_DIR=<저장소 밖의 폴더>   # 상태 파일을 둘 곳
mkdir -p "$STATE_DIR"
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # 값을 채운다
terraform init -backend-config="path=$STATE_DIR/prod.tfstate"
terraform plan
```

- 로컬 상태 파일은 잃어버리면 만든 것을 Terraform이 알지 못합니다. 이 폴더를 백업하세요.
- `.terraform.lock.hcl`은 커밋합니다(플러그인 버전 고정). 플러그인을 올릴 때만 `terraform init -upgrade` 후 파일 변경을 커밋합니다.
- GitHub Actions가 AWS에 들어가는 방식은 `github_auth`로 고릅니다. 기본은 `oidc`(역할)이고, OIDC를 쓸 수 없는 계정은 `access_key`(CI 전용 사용자)를 씁니다. `access_key`는 키를 주기적으로 바꿉니다.
- 계정에 GitHub OIDC 공급자가 이미 있으면 `create_github_oidc_provider = false`로 둡니다(계정에 하나만 만들 수 있음).

## 첫 배포 절차

1. `terraform plan`을 읽고 `terraform apply`를 합니다. 적용 뒤 서버 방화벽 규칙이 `lightsail.tf`와 같은지 확인합니다. 묶음·이미지 ID가 거절되면 `aws lightsail get-bundles`, `get-blueprints`로 확인해 변수를 고칩니다.
2. `terraform output`의 값을 GitHub 저장소 변수·비밀과 서버 설정 파일에 넣습니다(`outputs.tf`, `.github/workflows/deploy-images.yml` 머리 주석).
3. main에 머지해 이미지 6개와 대기 버전 값이 올라가는지 확인합니다. 대기 버전이 커밋 해시가 되기 전에는 서버 타이머를 켜지 않습니다(처음 값은 자리표시라 서버가 받으려다 실패합니다).
4. 서버용 자격 증명을 두 서버에 설정하고, 비밀 설정 값을 넣습니다(필수 이름은 `infra/deploy/server/apply-deploy.sh`의 `required_secrets`).
5. 두 서버에 Docker와 AWS CLI를 설치하고 저장소 사본을 둔 뒤, [`infra/deploy/README.md`](../deploy/README.md)의 "서버 설정 파일", "처음 한 번 할 일"을 따릅니다.
6. 메일 발신 주소 확인을 마친 뒤, 서비스 서버의 `NOTIFIER_SEND_ENABLED`를 직접 `true`로 바꿉니다(기본 꺼짐).

## 이후 배포

- main에 머지하면 CI가 이미지를 올리고 대기 버전을 적습니다. 서비스 서버가 다음 07:00(한국 시간)에 적용합니다.
- 급하면 Actions → `emergency-deploy` → Run workflow. 사유를 쓰고 경고(재시작하면 수집기의 뉴스 검색 하루 호출 횟수가 0으로 돌아감)를 확인해 체크합니다. 한국 시간 09:00~15:30에는 거절됩니다. 서버는 5분 안에 적용합니다.
- 긴급 값을 적는 사이에 새 main이 대기 버전을 바꾸면 서버가 그 요청을 무시합니다. 워크플로가 경고를 남기며, 이때는 대기 버전이 안정된 뒤에 다시 누릅니다.

## 정리

`terraform destroy`는 ECR 이미지가 있는 저장소와 백업이 있는 버킷은 지우지 못합니다(기본 동작). 백업이 필요 없다는 확인 없이 `force_destroy`를 켜지 않습니다.
