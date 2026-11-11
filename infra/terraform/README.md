# 운영 서버 만들기 (Terraform)

Lightsail 서버 2대, 방화벽, ECR 저장소 6개, S3 백업 버킷, 배포 버전 값(Parameter Store), IAM을 만듭니다. 서버 안에 올라가는 파일(Compose, 배포 스크립트)은 [`infra/deploy`](../deploy/README.md), 이미지를 올리고 버전 값을 쓰는 CI는 `.github/workflows/`에 있습니다.

## 만드는 것

| 대상 | 내용 |
|---|---|
| Lightsail 2대 | `<접두사>-data`(Kafka·Postgres), `<접두사>-service`(서비스 5개). 기본 묶음 `small_3_0`(2GB) |
| 방화벽 | 두 서버 모두 22번은 Lightsail 브라우저 SSH(`lightsail-connect`)에만. 데이터 서버의 Kafka(9092)·Postgres(5432)는 서비스 서버 사설 주소(/32)에만. 그 밖은 모두 닫힘 |
| ECR 6개 | `stock-reading-{collector,market,news,notifier,briefing,migrate}`. 최근 3개만 보관 |
| S3 버킷 | Postgres 백업. 35일(5주) 지나면 삭제, 공개 차단, TLS만 허용 |
| Parameter Store | `/stock-reading/prod/deploy/standby-version`, `.../urgent-version`. 이름만 만들고 값은 CI가 씀(Terraform은 값을 되돌리지 않음) |
| IAM | GitHub OIDC 역할(main 브랜치 워크플로만: ECR 푸시, 배포 버전 쓰기·읽기), 서버용 사용자(설정 읽기, SES 발송, ECR 받기, 백업 쓰기, 배포 버전 읽기). **접근 키는 만들지 않음** |

비밀 설정 값(`/stock-reading/prod/env/*`)은 이 코드가 만들지 않습니다. 사람이 직접 넣습니다.

## 계정 정보는 저장소에 두지 않습니다

계정 번호, 버킷 이름, 저장소 이름은 변수로 받고 값은 `*.tfvars`(gitignore)에 둡니다. 상태 파일도 저장소 밖에 둡니다.

```sh
mkdir -p ~/.config/stock-reading/terraform
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # 값을 채운다
terraform init -backend-config="path=$HOME/.config/stock-reading/terraform/prod.tfstate"
terraform plan
```

- 로컬 상태 파일은 잃어버리면 만든 것을 Terraform이 알지 못합니다. 이 폴더를 백업하세요.
- `.terraform.lock.hcl`은 커밋합니다(플러그인 버전 고정: darwin·linux, arm64·amd64 체크섬 포함). 플러그인을 올릴 때만 `terraform init -upgrade` 후 파일 변경을 커밋합니다.
- 계정에 GitHub OIDC 공급자가 이미 있으면 `create_github_oidc_provider = false`로 둡니다(계정에 하나만 만들 수 있음).

## 첫 배포 절차

작업자가 할 일은 **굵게** 표시했습니다.

1. **Terraform 실행용 AWS 프로필을 준비합니다**(`aws_profile`에 이름을 씀). 권한은 Lightsail·ECR·S3·SSM·IAM 생성입니다.
2. `terraform plan`을 읽고 `terraform apply`를 합니다. 확인할 것:
   - 방화벽 규칙이 22번 `lightsail-connect`뿐이어야 합니다. **적용 뒤 `aws lightsail get-instance-port-states --instance-name <접두사>-service`에서 22번이 `0.0.0.0/0`으로 열려 있지 않은지 직접 확인합니다.** 열려 있으면 멈추고 알립니다(별칭만 줄 때의 기본값은 이 코드로 시험하지 못했습니다).
   - 묶음 ID(`small_3_0`)와 이미지 ID(`ubuntu_24_04`)는 적용 때 처음 검증됩니다. 거절되면 `aws lightsail get-bundles`, `get-blueprints`로 확인해 변수를 고칩니다.
3. `terraform output`에서 다음을 얻습니다.
   - `github_actions_role_arn` → **GitHub 저장소 변수 `AWS_ROLE_ARN`에 넣습니다. 같은 곳에 `AWS_REGION`(기본 `ap-southeast-2`)도 넣습니다.** (Settings → Secrets and variables → Actions → Variables. 비밀이 아니라 변수입니다.)
   - `ecr_registry`, `data_server_private_ip`, `backup_bucket` → 서버 설정 파일에 넣습니다.
4. **main에 푸시해(또는 Actions에서 `deploy-images`를 main에 다시 실행) 이미지 6개와 대기 버전 값이 올라가는지 확인합니다.** 대기 버전이 커밋 해시가 되기 전에는 서버 타이머를 켜지 않습니다(처음 값은 자리표시 `placeholder-not-deployed`라 서버가 받으려다 실패합니다).
5. **IAM 사용자 `server_iam_user`의 접근 키를 콘솔에서 발급해 두 서버의 AWS CLI에 설정합니다**(키는 Terraform 상태에 남지 않음). 비밀 설정 값은 `/stock-reading/prod/env/` 아래에 직접 넣습니다(필수 이름은 `infra/deploy/server/apply-deploy.sh`의 `required_secrets`).
6. **두 서버에 Lightsail 콘솔의 브라우저 SSH로 접속해** Docker와 AWS CLI를 설치하고, 저장소 사본을 `/opt/stock-reading`에 둡니다. 스왑 파일, 설정 파일, systemd 유닛, 데이터 서버 → 서비스 서버 순서의 기동은 [`infra/deploy/README.md`](../deploy/README.md)의 "서버 설정 파일", "처음 한 번 할 일"을 따릅니다. 데이터 서버 `data.env`의 `DATA_PRIVATE_IP`는 `data_server_private_ip`입니다.
7. **SES에서 보내는 주소(발신 확인)와 샌드박스 해제 여부를 확인합니다.** 서버 사용자 권한은 이 계정·리전의 모든 SES 발신 아이디(`identity/*`)에 대한 발송입니다.
8. 서비스 서버의 `NOTIFIER_SEND_ENABLED`는 사람이 직접 `true`로 바꿉니다(기본 꺼짐).

## 이후 배포

- GitHub 역할은 main 브랜치 작업만 받을 수 있으므로 main 보호(PR 리뷰)가 마지막 방어선입니다.
- main에 머지하면 CI가 이미지를 올리고 대기 버전을 적습니다. 서비스 서버가 그날 00:00(한국 시간)에 적용합니다.
- 급하면 Actions → `emergency-deploy` → Run workflow. 사유를 쓰고 경고(재시작하면 수집기의 뉴스 검색 하루 호출 횟수가 0으로 돌아감)를 확인해 체크합니다. 한국 시간 09:00~15:30에는 거절됩니다. 서버는 5분 안에 적용합니다.
- 긴급 값을 적는 사이에 새 main이 대기 버전을 바꾸면 서버가 그 요청을 무시하고 "본 요청"으로 기록합니다. 워크플로가 경고를 남기며, 이때는 대기 버전이 안정된 뒤에 다시 누릅니다.

## 정리

`terraform destroy`는 ECR 이미지가 있는 저장소와 백업이 있는 버킷은 지우지 못합니다(기본 동작). 백업이 필요 없다는 확인 없이 `force_destroy`를 켜지 않습니다.
