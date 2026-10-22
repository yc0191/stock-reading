# ADR-015: 이메일 관련 설정·비밀을 AWS Systems Manager Parameter Store에 보관한다

- 상태: **확정** — 지역은 시드니(`ap-southeast-2`)로 쓴다.
- 기록일: 2026-10-20

## 수신자 (2026-10-20)
- 지금은 수신자가 한 명이라 `recipient`를 파라미터 하나(문자열)로 둔다. 시간당 한도(`max-per-hour`)는 비밀이 아니어서 파라미터로 두지 않고 `application.yml` 기본값을 쓴다.
- 수신자가 여러 명이 되면 DB에 암호화해 저장한다(별도 설계·ADR). 그때 `recipient` 파라미터는 없어진다.

## 맥락
이메일 발송 설정(SMTP 자격증명, 발신·수신 주소, 한도 등)을 지금은 로컬 `.env`(git 제외)에 둔다. 운영(EC2)에서도 같은 값을 써야 하는데, 서버에 `.env` 파일을 복사해 두면 비밀이 디스크에 평문으로 남고 값이 여러 곳에 흩어진다.

## 제안
Parameter Store를 **설정·비밀의 단일 출처**로 삼고, 로컬과 운영이 같은 방식으로 읽는다.

- 경로 규칙: `/stock-reading/<환경>/notifier/<항목>` (예: `/stock-reading/prod/notifier/smtp-password`)
- 유형: 비밀(SMTP 사용자 이름·비밀번호, 수신·발신 주소)은 `SecureString`, 나머지(SMTP 호스트·포트, 한도)는 `String`
- 읽는 방법: **애플리케이션 코드는 바꾸지 않는다.** 시작 전에 로더 스크립트가 `aws ssm get-parameters-by-path --with-decryption`으로 값을 읽어 환경변수(로컬은 git 제외 `.env`)로 만든다. 앱의 인터페이스는 계속 환경변수다(ADR-014).
- 운영(EC2)에서는 **인스턴스 IAM 역할**로 읽는다(저장된 키 없음).

## 대안
- AWS Secrets Manager: 자동 교체 기능이 있으나 비밀당 월 요금이 있다(금액 미확인). SMTP 자격증명 교체가 필요해질 때 재검토.
- 현재 방식 유지(`.env`): 로컬에서는 충분하나 운영에서 비밀이 디스크에 남는 문제가 있다.
