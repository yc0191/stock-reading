locals {
  lightsail_region  = coalesce(var.lightsail_region, var.aws_region)
  availability_zone = coalesce(var.availability_zone, "${local.lightsail_region}a")
}

resource "aws_lightsail_instance" "data" {
  provider          = aws.lightsail
  name              = "${var.name_prefix}-data"
  availability_zone = local.availability_zone
  blueprint_id      = var.blueprint_id
  bundle_id         = var.bundle_id
  # 데이터 서버는 사설 주소로만 쓰므로 IPv6 로 열릴 길을 만들지 않는다.
  ip_address_type = "ipv4"

  # 리전·가용 영역·묶음·이미지를 바꾸면 Terraform 은 서버를 지우고 새로 만든다. 이 서버의 디스크에 운영 DB 가 있어
  # 그 계획이 나오면 apply 를 거절하게 한다. 일부러 바꾸거나 지울 때만 이 줄을 빼고, 끝나면 되돌린다.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_lightsail_instance" "service" {
  provider          = aws.lightsail
  name              = "${var.name_prefix}-service"
  availability_zone = local.availability_zone
  blueprint_id      = var.blueprint_id
  bundle_id         = coalesce(var.service_bundle_id, var.bundle_id)
  ip_address_type   = "ipv4"

  # 데이터는 없지만 지워지면 서버 준비(자격 증명, 설정 파일, 타이머)를 처음부터 다시 해야 하므로 같은 보호를 둔다.
  lifecycle {
    prevent_destroy = true
  }
}

# 이 리소스가 방화벽 전체를 정한다. 여기에 없는 포트는 닫힌다(기본으로 열려 있던 22·80 포함).
# lightsail-connect 는 Lightsail 콘솔의 브라우저 SSH 가 오는 주소 목록의 별칭이다.
# 서버끼리는 사설 주소로만 접속한다. 서비스 서버는 밖에서 들어오는 접속이 22번 하나뿐이다.
resource "aws_lightsail_instance_public_ports" "service" {
  provider      = aws.lightsail
  instance_name = aws_lightsail_instance.service.name

  port_info {
    protocol          = "tcp"
    from_port         = 22
    to_port           = 22
    cidr_list_aliases = ["lightsail-connect"]
  }
}

resource "aws_lightsail_instance_public_ports" "data" {
  provider      = aws.lightsail
  instance_name = aws_lightsail_instance.data.name

  port_info {
    protocol          = "tcp"
    from_port         = 22
    to_port           = 22
    cidr_list_aliases = ["lightsail-connect"]
  }

  # Kafka
  port_info {
    protocol  = "tcp"
    from_port = 9092
    to_port   = 9092
    cidrs     = ["${aws_lightsail_instance.service.private_ip_address}/32"]
  }

  # Postgres
  port_info {
    protocol  = "tcp"
    from_port = 5432
    to_port   = 5432
    cidrs     = ["${aws_lightsail_instance.service.private_ip_address}/32"]
  }
}
