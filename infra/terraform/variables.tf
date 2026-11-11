variable "aws_region" {
  description = "Lightsail·ECR·S3·Parameter Store를 만들 리전. 서버 설정(AWS_REGION)과 같아야 한다."
  type        = string
  default     = "ap-southeast-2"
}

variable "aws_profile" {
  description = "Terraform 실행용 AWS CLI 프로필 이름. 비우면 기본 자격증명 탐색을 따른다."
  type        = string
  default     = null
}

variable "name_prefix" {
  description = "Lightsail 인스턴스·ECR 저장소·IAM 이름 앞에 붙일 말. 바꾸면 .github/workflows/deploy-images.yml 의 이미지 이름과 infra/deploy/compose/services.yml 의 이미지 이름도 함께 바꿔야 한다."
  type        = string
  default     = "stock-reading"
}

variable "availability_zone" {
  description = "Lightsail 가용 영역. 두 서버가 같은 영역이어야 사설 주소로 닿는다. 비우면 리전의 a 영역."
  type        = string
  default     = null
}

variable "blueprint_id" {
  description = "Lightsail 운영체제 이미지."
  type        = string
  default     = "ubuntu_24_04"
}

variable "bundle_id" {
  description = "Lightsail 묶음. small_3_2 가 2GB 메모리·공개 IPv4(월 $12)이다. 묶음 이름의 끝 번호는 AWS가 바꾸므로 aws lightsail get-bundles 로 확인한다. 서비스 서버가 모자라면 서비스 서버만 4GB 묶음으로 올린다."
  type        = string
  default     = "small_3_2"
}

variable "service_bundle_id" {
  description = "서비스 서버만 따로 정할 묶음. 비우면 bundle_id."
  type        = string
  default     = null
}

variable "github_repository" {
  description = "이미지를 올릴 GitHub 저장소(<소유자>/<이름>). 이 저장소의 main 브랜치 워크플로만 AWS 역할을 받을 수 있다."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository 는 <소유자>/<이름> 꼴이어야 합니다."
  }
}

variable "github_auth" {
  description = "GitHub Actions가 AWS에 들어가는 방법. oidc 는 접근 키 없이 main 작업만 역할을 받는다(권장). access_key 는 OIDC 등록이 막힌 계정(조직 정책)에서 쓰며, CI 전용 IAM 사용자를 만들고 키는 사람이 콘솔에서 발급해 GitHub Secrets 에 넣는다."
  type        = string
  default     = "oidc"

  validation {
    condition     = contains(["oidc", "access_key"], var.github_auth)
    error_message = "github_auth 는 oidc 또는 access_key 여야 합니다."
  }
}

variable "create_github_oidc_provider" {
  description = "github_auth 가 oidc 일 때 GitHub OIDC 공급자를 이 코드가 만들지 여부. 계정에 이미 있으면 false(계정에 하나만 만들 수 있다)."
  type        = bool
  default     = true
}

variable "backup_bucket_name" {
  description = "Postgres 백업을 둘 S3 버킷 이름. 전 세계에서 유일해야 한다."
  type        = string
}

variable "backup_prefix" {
  description = "백업 스크립트의 BACKUP_S3_PREFIX 와 같은 값."
  type        = string
  default     = "postgres"
}

variable "backup_retention_days" {
  description = "백업을 보관하는 날 수. 5주 = 35일."
  type        = number
  default     = 35
}

variable "ecr_keep_images" {
  description = "ECR 저장소마다 남겨 둘 최근 이미지 개수. 문제가 생겨 이전 버전으로 되돌릴 때 쓰려고 남기며, 이 수를 넘으면 오래된 것부터 지운다. 남긴 이미지 용량만큼 요금이 든다."
  type        = number
  default     = 3
}
