# 설계 결정 기록 (ADR)

ADR = Architecture Decision Record. "무엇을, 왜, 어떤 대안을 놓고 정했는지"를 결정 하나당 문서 하나로 남깁니다.
언제 ADR을 쓰는지와 결정이 바뀔 때의 처리는 `.claude/rules/adr.md`를 따릅니다. 프로젝트 배경은 [background.md](../background.md)에 있습니다.

| # | 주제 | 상태 |
|---|---|---|
| [001](001-language-and-framework.md) | 언어/프레임워크 (Java 21 + Spring Boot, 브리핑은 Python) | 확정 |
| [002](002-build-and-module-structure.md) | 빌드 도구와 모듈 구조 | 확정 |
| [003](003-kafka.md) | 메시징: Apache Kafka | 확정 |
| [004](004-postgresql.md) | DB: PostgreSQL + pgvector | 확정 |
| [006](006-microservices.md) | MSA 채택과 서비스 분리 기준 | 확정 |
| [007](007-outbound-only.md) | 외부 접속 없는 아웃바운드 전용으로 시작 | 확정 |
| [008](008-redis.md) | Redis | 확정 |
| [010](010-event-format.md) | 이벤트 포맷 (JSON + 스키마 파일) | 초안 |
| [011](011-notification-channel.md) | 알림 채널: 이메일(AWS SES) | 초안 |

## 상태 표기
- **확정**: 합의했고, 구현이 있으면 테스트로 검증됨
- **초안**: 방향은 정했으나 세부를 구현하며 더 검토할 예정

## 작성 원칙
- 개인정보(이메일 주소, 보유 종목·수량·금액, 계좌), 계정 식별자, 키·토큰은 **쓰지 않습니다**.
- 외부 사실은 근거를 남기고, 확인하지 못한 것은 "미확인"으로 표시합니다.
