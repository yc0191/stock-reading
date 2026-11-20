# 확인기 결과 파일 버킷. 비율·판정·버전·지문만 담기고, 평일 18:00 판정 작업이 읽는다.
# 백업 버킷(s3.tf)과 따로 두어 읽기 권한이 백업까지 닿지 않게 한다.

resource "aws_s3_bucket" "ops_results" {
  bucket = var.ops_results_bucket_name
}

resource "aws_s3_bucket_public_access_block" "ops_results" {
  bucket                  = aws_s3_bucket.ops_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ops_results" {
  bucket = aws_s3_bucket.ops_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "ops_results" {
  bucket = aws_s3_bucket.ops_results.id

  rule {
    id     = "expire-old-results"
    status = "Enabled"

    filter {}

    expiration {
      days = var.ops_results_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

data "aws_iam_policy_document" "ops_results_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.ops_results.arn, "${aws_s3_bucket.ops_results.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "ops_results" {
  bucket = aws_s3_bucket.ops_results.id
  policy = data.aws_iam_policy_document.ops_results_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.ops_results]
}

# ---------------------------------------------------------------------------
# 판정 작업(GitHub Actions)의 읽기 전용 신분. 배포 신분(iam.tf)과 섞이지 않게 따로 둔다.
# 평일 18:00 schedule 워크플로는 기본 브랜치(develop)의 것으로 돌아 develop 작업만 받는다.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ops_results_reader_trust" {
  count = local.github_oidc ? 1 : 0

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

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:ref:refs/heads/develop"]
    }
  }
}

resource "aws_iam_role" "ops_results_reader" {
  count              = local.github_oidc ? 1 : 0
  name               = "${var.name_prefix}-ops-results-reader"
  assume_role_policy = data.aws_iam_policy_document.ops_results_reader_trust[0].json
}

data "aws_iam_policy_document" "ops_results_reader" {
  statement {
    sid       = "ReadOpsResults"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.ops_results.arn}/results/*"]
  }

  statement {
    sid       = "ListOpsResults"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.ops_results.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["results/*"]
    }
  }
}

resource "aws_iam_role_policy" "ops_results_reader" {
  count  = local.github_oidc ? 1 : 0
  name   = "read-ops-results"
  role   = aws_iam_role.ops_results_reader[0].id
  policy = data.aws_iam_policy_document.ops_results_reader.json
}

# OIDC 를 못 쓰는 계정용. 접근 키는 Terraform 이 만들지 않고 사람이 콘솔에서 발급해 GitHub Secrets 에 넣는다.
resource "aws_iam_user" "ops_results_reader" {
  count = local.github_oidc ? 0 : 1
  name  = "${var.name_prefix}-ops-results-reader"

  # 콘솔에서 키를 발급하면 키 설명이 사용자 태그로 붙는다. apply 가 그 태그를 지우지 않게 한다.
  lifecycle {
    ignore_changes = [tags]
  }
}

resource "aws_iam_user_policy" "ops_results_reader" {
  count  = local.github_oidc ? 0 : 1
  name   = "read-ops-results"
  user   = aws_iam_user.ops_results_reader[0].name
  policy = data.aws_iam_policy_document.ops_results_reader.json
}

# ---------------------------------------------------------------------------
# 확인기가 결과 파일을 올리는 쓰기 전용 신분. 서버 계정 키를 확인기 컨테이너에 넣지 않으려고 따로 둔다.
# 읽기·삭제는 주지 않는다. 접근 키는 Terraform 이 만들지 않고 사람이 콘솔에서 발급해 비밀 설정에 넣는다.
# ---------------------------------------------------------------------------

resource "aws_iam_user" "ops_results_writer" {
  name = "${var.name_prefix}-ops-results-writer"

  # 콘솔에서 키를 발급하면 키 설명이 사용자 태그로 붙는다. apply 가 그 태그를 지우지 않게 한다.
  lifecycle {
    ignore_changes = [tags]
  }
}

data "aws_iam_policy_document" "ops_results_writer" {
  statement {
    sid       = "WriteOpsResults"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.ops_results.arn}/results/*"]
  }
}

resource "aws_iam_user_policy" "ops_results_writer" {
  name   = "write-ops-results"
  user   = aws_iam_user.ops_results_writer.name
  policy = data.aws_iam_policy_document.ops_results_writer.json
}
