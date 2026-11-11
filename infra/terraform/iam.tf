locals {
  ecr_repository_arns = [for r in aws_ecr_repository.image : r.arn]
  account_id          = data.aws_caller_identity.current.account_id
}

# ---------------------------------------------------------------------------
# GitHub Actions: OIDC 역할. 접근 키 없이 main 브랜치 워크플로만 맡는다.
# ---------------------------------------------------------------------------

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 0 : 1

  url = "https://token.actions.githubusercontent.com"
}

locals {
  github_oidc_provider_arn = var.create_github_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : data.aws_iam_openid_connect_provider.github[0].arn
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # main 브랜치에서 돈 워크플로(푸시와 수동 실행)만. 다른 브랜치·PR 은 받을 수 없다.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.name_prefix}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_trust.json
}

data "aws_iam_policy_document" "github_actions" {
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid = "EcrPushImages"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:CompleteLayerUpload",
      "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]
    resources = local.ecr_repository_arns
  }

  statement {
    sid       = "WriteDeployVersions"
    actions   = ["ssm:PutParameter"]
    resources = [aws_ssm_parameter.standby_version.arn, aws_ssm_parameter.urgent_version.arn]
  }

  # 긴급 배포 버튼이 대기 버전을 읽어 긴급 버전에 옮겨 적는다.
  statement {
    sid       = "ReadDeployVersions"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.standby_version.arn]
  }
}

resource "aws_iam_role_policy" "github_actions" {
  name   = "ecr-push-and-deploy-versions"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.github_actions.json
}

# ---------------------------------------------------------------------------
# 서버용 IAM 사용자. 접근 키는 만들지 않는다(작업자가 콘솔에서 발급해 서버에 넣는다).
# ---------------------------------------------------------------------------

resource "aws_iam_user" "server" {
  name = "${var.name_prefix}-server"
}

data "aws_iam_policy_document" "server" {
  # 설정(비밀값)과 배포 버전 값은 읽기만 한다.
  statement {
    sid     = "ReadConfigAndDeployVersions"
    actions = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
    resources = [
      "${local.parameter_arn_prefix}${local.parameter_root}",
      "${local.parameter_arn_prefix}${local.parameter_root}/*",
    ]
  }

  statement {
    sid       = "SendMail"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["arn:aws:ses:${var.aws_region}:${local.account_id}:identity/*"]
  }

  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid = "EcrPullImages"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = local.ecr_repository_arns
  }

  statement {
    sid       = "WriteBackups"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.backup.arn}/${var.backup_prefix}/*"]
  }
}

resource "aws_iam_user_policy" "server" {
  name   = "server-runtime"
  user   = aws_iam_user.server.name
  policy = data.aws_iam_policy_document.server.json
}
