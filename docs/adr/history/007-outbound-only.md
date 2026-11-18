# ADR-007 경위

[ADR-007](../007-outbound-only.md)의 결정이 바뀐 경위입니다(날짜순). ADR 본문에는 현재 결정만 있습니다.

- 2026-11-13 변경: 관리 접속은 Lightsail 콘솔의 브라우저 SSH에서만 받고, 두 서버끼리는 사설 주소로만 통신한다([ADR-013](../013-lightsail-compose-deploy.md)).
- 2026-11-13 변경: 시세처리기·뉴스처리기가 서비스 간 내부 조회(HTTP)를 같은 컨테이너 네트워크 안에서만 받는다. 서버 밖 포트는 열지 않는다(원본: `infra/deploy/compose/services.yml`).
