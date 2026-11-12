terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # 상태 파일 경로는 변수를 쓸 수 없어 init 때 -backend-config 로 넘긴다(README 참고).
  # 상태 파일에는 비밀이 섞일 수 있어 저장소 밖에 둔다.
  backend "local" {}
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = "stock-reading"
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
