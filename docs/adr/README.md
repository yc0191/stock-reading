# 설계 결정 기록 (ADR)

ADR = Architecture Decision Record. "무엇을, 왜, 어떤 대안을 놓고 정했는지"를 결정 하나당 문서 하나로 남깁니다.
결정이 바뀌면 지우지 않고 상태를 `대체됨(Superseded by ADR-0xx)`으로 바꿉니다.

| # | 주제 | 상태 |
|---|---|---|
| [000](000-project-background.md) | 프로젝트 배경과 시스템 구조 도출 | 초안 |
| [001](001-language-and-framework.md) | 언어/프레임워크 (Java 21 + Spring Boot, 브리핑은 Python) | 확정 |
| [002](002-build-and-module-structure.md) | 빌드 도구와 모듈 구조 | 확정 |
| [003](003-kafka.md) | 메시징: Apache Kafka | 확정 |
| [004](004-postgresql.md) | DB: PostgreSQL + pgvector | 확정 |
| [005](005-load-validation.md) | 부하 검증 방침 | 확정 |
| [006](006-microservices.md) | MSA 채택과 서비스 분리 기준 | 확정 |
| [007](007-outbound-only.md) | 외부 접속 없는 아웃바운드 전용으로 시작 | 확정 |
| [008](008-redis.md) | Redis | 확정 |
| [010](010-event-format.md) | 이벤트 포맷 (JSON + 스키마 파일) | 초안 |
| [011](011-notification-channel.md) | 알림 채널: 이메일(AWS SES) | 초안 |
| [012](012-scheduling.md) | 스케줄링: Kafka 이벤트 | 초안 |
| [013](013-topic-declaration.md) | 토픽은 서비스가 선언, 브로커 자동 생성 끔 | 확정 (구현·검증됨) |
| [014](014-notifier-safety-rules.md) | 발송기 안전 규칙 | 확정 (구현·검증됨) |
| [015](015-parameter-store.md) | 이메일 설정·비밀을 Parameter Store에 보관 | 확정 (시드니) |
| [017](017-news-collection-and-storage.md) | 뉴스 수집·발행·저장 (news.collected, 수집 스케줄, news 서비스) | 초안 |

## 상태 표기
- **확정**: 합의했고, 구현이 있으면 테스트로 검증됨
- **초안**: 방향은 정했으나 세부를 구현하며 더 검토할 예정

## 작성 원칙
- 개인정보(이메일 주소, 보유 종목·수량·금액, 계좌), 계정 식별자, 키·토큰은 **쓰지 않습니다**.
- 외부 사실은 근거를 남기고, 확인하지 못한 것은 "미확인"으로 표시합니다.
