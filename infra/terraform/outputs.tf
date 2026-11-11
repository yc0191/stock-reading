output "ecr_registry" {
  description = "서버 deploy.env 의 ECR_REGISTRY 와 같은 값."
  value       = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

output "github_actions_role_arn" {
  description = "GitHub 저장소 변수 AWS_ROLE_ARN 에 넣을 값."
  value       = aws_iam_role.github_actions.arn
}

output "server_iam_user" {
  description = "접근 키를 발급할 사용자 이름(키는 콘솔에서 작업자가 만든다)."
  value       = aws_iam_user.server.name
}

output "data_server_private_ip" {
  description = "서버 설정의 DATA_PRIVATE_IP."
  value       = aws_lightsail_instance.data.private_ip_address
}

output "service_server_private_ip" {
  value = aws_lightsail_instance.service.private_ip_address
}

output "backup_bucket" {
  description = "데이터 서버 data.env 의 BACKUP_S3_BUCKET."
  value       = aws_s3_bucket.backup.bucket
}

output "deploy_version_parameters" {
  value = {
    standby = aws_ssm_parameter.standby_version.name
    urgent  = aws_ssm_parameter.urgent_version.name
  }
}
