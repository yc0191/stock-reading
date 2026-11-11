locals {
  availability_zone = coalesce(var.availability_zone, "${var.aws_region}a")
}

resource "aws_lightsail_instance" "data" {
  name              = "${var.name_prefix}-data"
  availability_zone = local.availability_zone
  blueprint_id      = var.blueprint_id
  bundle_id         = var.bundle_id
  # 데이터 서버는 사설 주소로만 쓰므로 IPv6 로 열릴 길을 만들지 않는다.
  ip_address_type = "ipv4"
}

resource "aws_lightsail_instance" "service" {
  name              = "${var.name_prefix}-service"
  availability_zone = local.availability_zone
  blueprint_id      = var.blueprint_id
  bundle_id         = coalesce(var.service_bundle_id, var.bundle_id)
  ip_address_type   = "ipv4"
}

# 이 리소스가 방화벽 전체를 정한다. 여기에 없는 포트는 닫힌다(기본으로 열려 있던 22·80 포함).
# lightsail-connect 는 Lightsail 콘솔의 브라우저 SSH 가 오는 주소 목록의 별칭이다.
# 서버끼리는 사설 주소로만 접속한다. 서비스 서버는 밖에서 들어오는 접속이 22번 하나뿐이다.
resource "aws_lightsail_instance_public_ports" "service" {
  instance_name = aws_lightsail_instance.service.name

  port_info {
    protocol          = "tcp"
    from_port         = 22
    to_port           = 22
    cidr_list_aliases = ["lightsail-connect"]
  }
}

resource "aws_lightsail_instance_public_ports" "data" {
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
