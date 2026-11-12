# CLAUDE.md

관심 ETF의 가격 변동 이유를 근거와 함께 정리해 이메일로 알려 주는 Kafka 기반 이벤트 구동 MSA입니다. 설계 배경은 `README.md`와 `docs/adr/`에 있습니다.

## 저장소 지도

| 경로 | 내용 |
|---|---|
| `services/collector`, `market`, `news`, `notifier` | Java 21 + Spring Boot 서비스 (Gradle 모듈) |
| `services/briefing` | Python 서비스 (Gradle 모듈이 아니며 자체 venv 사용) |
| `contracts/events/` | 서비스 간 이벤트 JSON Schema와 v1 샘플 |
| `build-logic/` | 모든 서비스가 공유하는 Gradle 규약 플러그인 (루트 `build.gradle.kts`는 일부러 비어 있음) |
| `scripts/` | 설정 로더 (Python) |
| `docs/adr/` | 설계 결정 기록 (인덱스: `docs/adr/README.md`) |
| `docs/incidents/` | 운영 문제의 조사·원인·고친 과정 기록 (인덱스: `docs/incidents/README.md`) |
| `infra/` | 로컬 인프라 (Docker Compose), IAM 템플릿 |

## 검증

코드나 설정을 바꾼 뒤에는 변경한 범위의 테스트를 실행하고 통과를 확인합니다. 명령, 변경 범위별로 돌릴 테스트, 테스트 지도(`docs/test-map/`) 관리는 `.claude/rules/testing.md`를 따릅니다.

## 어기면 조용히 깨지는 설계 규칙

- 브리핑 근거 자료는 자료마다 정한 시간 범위 안의 것만 씁니다. 사건형(뉴스, 공시)은 발행·접수 시각이, 상태형(지수, 환율, 시장종합, 일봉, 해외 등락률)은 받은 시각이 기준이며, 상태형은 변동 시각보다 늦게 받은 것을 쓰지 않습니다. 범위와 상한의 원본은 `services/briefing/src/briefing/config.py`의 `CoreSettings`이고, 범위 밖 자료는 버립니다.
- LLM은 코드가 붙여 준 근거 번호만 인용합니다. 목록 밖 번호를 쓴 출력은 거부합니다.
- 발송기는 드라이런이 기본입니다. `NOTIFIER_SEND_ENABLED`는 사람이 직접 바꾸며, 로그의 주소는 마스킹합니다. 예외는 사람이 직접 실행을 결정한 `scripts/test-all.sh --live`뿐이며, 이때도 그 스크립트가 띄운 테스트용 발송기에만 발송을 켜고 `.env`는 바꾸지 않습니다.
- 토픽은 사용하는 서비스가 시작할 때 선언하고, 같은 토픽은 선언하는 모든 곳에서 파티션 수가 같아야 합니다. 어긋나면 일부 종목이 오류 없이 누락됩니다.
- `schedule.tick`은 다른 서비스로 가는 신호(데일리 판 시작 `daily-briefing-morning`·`daily-briefing-post-close`, 구성 종목 조회 `collect-members`)에만 씁니다. 이를 구독하는 리스너는 `jobType`마다 consumer group을 따로 둡니다. 이 토픽은 파티션이 1개라 같은 그룹이면 한쪽만 받아 tick이 오류 없이 누락됩니다. 수집기 자신의 수집 작업은 tick 없이 수집기 안 타이머와 수동 실행 명령(`collector.command`)으로 돕니다(ADR-012).
- 시세 제공자 조회는 토큰 발급을 빼고 모두 같은 호출 상한 장치(`CallRateLimit` 빈 하나)를 거칩니다. 따로 세면 조회가 겹칠 때 앱 전체 한도를 넘습니다.
- 이벤트는 `eventId`로 멱등하게 처리합니다.
- 서비스 간 계약은 `contracts/events/`의 스키마입니다. 서비스마다 `Envelope` 등을 복제해 두었으므로 스키마를 바꾸면 모든 곳을 함께 고칩니다.
- 시세 제공자 API는 조회(토큰 발급, 시세)만 호출합니다. 주문·계좌 호출은 코드에 만들지 않습니다(ADR-007).
- 앱키, 시크릿, 접근토큰은 로그·파일·이벤트·문서·커밋에 남기지 않습니다. 토큰은 메모리에만 두고 한 번 발급한 것을 만료 전까지 재사용합니다.
- 외부 API 조회 응답을 저장할 때는 trim 외에 가공하지 않고 원문 그대로 둡니다(토큰 값은 예외).

## 규칙 문서 (해당 작업 직전에만 읽습니다)

- 코드 작성·수정: `.claude/rules/code-style.md`
- 테스트 실행·검증: `.claude/rules/testing.md`
- 이벤트 스키마·토픽·`Envelope` 변경: `.claude/rules/event-contract.md`
- 새 수집 자료를 브리핑 근거로 연결: `.claude/rules/evidence-source.md`
- ADR·README 등 설계 문서 변경: `.claude/rules/adr.md`
- 커밋 메시지 작성: `.claude/rules/commit.md`
- PR 생성·머지: `.claude/rules/pr.md`
- 이슈·마일스톤·프로젝트 보드 생성·수정 (모두 공개됨): `.claude/rules/issue.md`
