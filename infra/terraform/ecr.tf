locals {
  # 서비스 5개와 DB 표 만들기 단계(migrate). CI 가 같은 커밋 태그로 6개를 모두 올린다.
  image_names = ["collector", "market", "news", "notifier", "briefing", "migrate"]
}

resource "aws_ecr_repository" "image" {
  for_each = toset(local.image_names)

  name = "${var.name_prefix}-${each.key}"
  # 같은 커밋을 다시 빌드해 덮어쓸 수 있어야 CI 재실행이 실패하지 않는다.
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "image" {
  for_each   = aws_ecr_repository.image
  repository = each.value.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "최근 ${var.ecr_keep_images}개만 남긴다"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = var.ecr_keep_images
      }
      action = { type = "expire" }
    }]
  })
}
