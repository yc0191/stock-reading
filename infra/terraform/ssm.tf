locals {
  # 서버 스크립트(apply-deploy.sh)와 deploy.env.example 의 기본 경로와 같아야 한다.
  parameter_root        = "/stock-reading/prod"
  standby_version_param = "${local.parameter_root}/deploy/standby-version"
  urgent_version_param  = "${local.parameter_root}/deploy/urgent-version"
  config_parameter_path = "${local.parameter_root}/env/"
  parameter_arn_prefix  = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter"
}

# 값은 GitHub Actions 가 쓴다. Terraform 은 이름과 권한 범위만 만들고 값은 바꾸지 않는다.
# 처음 값은 자리표시다. 첫 CI 실행이 끝나 진짜 값이 들어간 뒤에 서버 타이머를 켠다(README의 첫 배포 절차).
resource "aws_ssm_parameter" "standby_version" {
  name        = local.standby_version_param
  type        = "String"
  value       = "placeholder-not-deployed"
  description = "다음 07:00 정기 배포 때 서비스 서버가 적용할 이미지 버전(커밋). main 푸시의 이미지 6개가 모두 올라간 뒤 CI 가 쓴다."

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "urgent_version" {
  name        = local.urgent_version_param
  type        = "String"
  value       = "placeholder-not-deployed"
  description = "긴급 배포 요청. 긴급 배포 버튼이 그 시점의 대기 버전을 그대로 쓴다. 서버는 대기 버전과 다르면 무시한다."

  lifecycle {
    ignore_changes = [value]
  }
}
