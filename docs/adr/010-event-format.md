# ADR-010: 이벤트는 JSON으로 주고받고, 스키마는 언어 중립 파일로 관리한다

- 상태: **초안** (구현은 되어 있으나 세부를 더 검토)
- 기록일: 2026-10-20

## 맥락
서비스 간 통신은 Kafka 이벤트로 한다(ADR-003). Java 서비스와 Python 서비스가 섞여 있고, 과거 이벤트를 리플레이로 다시 읽어야 한다.

## 고려한 대안
Avro + 스키마 레지스트리, Protobuf

## 결정
- 직렬화 포맷은 **JSON**.
- 이벤트 형식은 레포의 `contracts/events/*.schema.json`에 **언어와 무관한 스키마 파일**로 정의하고, 과거 이벤트 예시는 `contracts/events/samples/v1/`에 보관한다.
- 서비스 코드는 DTO를 직접 작성하고, **계약 테스트**로 스키마와의 일치를 검증한다.

## 이유
1. 이벤트 양이 적어 바이너리 포맷의 이점이 작고 **사람이 읽을 수 있는 편의성**이 더 중요하다.
2. 스키마 레지스트리 컨테이너를 추가하지 않아 서버 자원 제약에 맞는다.
3. Python 서비스가 Java 라이브러리를 쓸 수 없으므로 계약은 언어 중립 파일이어야 한다.
4. 이후 필요하면 Avro로 전환할 수 있다.

## 호환 규칙 (리플레이 보장)
- 필드는 추가만 허용하고 삭제·타입 변경은 새 버전으로 한다.
- 새 필드는 선택 항목으로 한다.
- 소비자는 모르는 필드를 무시한다.
- 과거 이벤트 샘플이 현재 스키마와 소비자 코드로 읽히는지 테스트한다(구현됨).

## v1에 추가한 선택 필드와 토픽
호환 규칙에 따라 v1 스키마에 선택 필드만 더했다. 소비자는 필드가 없는 이전 형식도 처리한다.
- `market.snapshot`: `payload.raw` — 시세 조회 응답 원문(문자열). trim 외에 가공하지 않으며 토큰 발급 응답은 담지 않는다.
- `market.snapshot.failed` (새 이벤트): 종목 하나의 시세 조회 실패·건너뜀 기록. 실패 종류, tick(`scheduledFor`, `correlationId`), HTTP 상태, 응답 처리 코드, 소요 시간, 예외 종류, 토큰 상태(`tokenState`: 경과·남은 시간), 동시 호출 수, tick의 성공·실패 건수, 실패 응답 원문을 담는다. 키, 시크릿, 접근토큰 값과 인증 헤더는 스키마에도 이벤트에도 두지 않는다. 처리 코드는 영숫자 8자 이하만 허용하고 아니면 `invalid`로 쓴다.
- `market.move`: `payload.detectionType`(`window` = 유형 1, `prevClose` = 유형 2)과 `payload.moveStartedAt`. 이에 맞춰 `asOf`(변동 시각)의 의미가 유형 1은 움직임 시작 시각, 유형 2는 임계값을 처음 넘은 시각으로 바뀐다. 필드가 없는 이전 이벤트의 `asOf`는 감지한 스냅샷의 시각이다.
- `market.snapshot.failed` 토픽: 파티션 3개, 메시지 키는 종목. `market.snapshot`과 같은 키·파티션 수로 두면 같은 종목의 성공과 실패가 같은 순서로 흐르고, 수집기(생산)와 시세처리기(소비)가 같은 값으로 선언한다(ADR-013). 3은 부하를 재서 정한 값이 아니라 `market.snapshot`과 맞춘 시작값이다.
- `briefing.generated`: `payload.evidence[]`의 `articleId`(`news.articles.responded`의 기사 식별자)와 `url`(기사 주소). 리포트가 인용한 기사를 가리킨다. `payload.generatedBy`의 값으로 기존 `rule-based`에 더해 `llm`을 쓴다.

## 근거 조회에 쓰는 이벤트 (v1에 새로 추가)
브리핑은 `market.move` 한 건마다 근거를 직접 모으지 않고, 시세처리기와 news 서비스에 요청해 응답을 받는다. 요청은 읽기 전용이라 다시 보내도 부작용이 없다. 토픽은 사용하는 서비스가 모두 같은 파티션 수로 선언한다(ADR-013).

| 이벤트 | 보내는 쪽 → 받는 쪽 | 키 | 파티션 | 내용 |
|---|---|---|---|---|
| `collection.completed` | 수집기 → 브리핑 | `jobType` | 1 | 한 번의 수집이 기사 전송을 모두 확정한 뒤(성공·실패 포함) 한 건. `jobType`, `scheduledFor`, `startedAt`(첫 검색 호출 시작), `finishedAt`(전송 확정), `publishedCount`, `incompleteSymbols`(제대로 모으지 못했을 수 있는 종목, 시장 전체는 `*`) |
| `market.history.requested` | 브리핑 → 시세처리기 | 종목 | 3 | `jobId`, `seq`, `symbol`, `since`, `until`, `maxPoints` |
| `market.history.responded` | 시세처리기 → 브리핑 | 종목 | 3 | `jobId`, `seq`, `symbol`, `points`(시각·가격, 시각 오름차순), `truncated` |
| `news.articles.requested` | 브리핑 → news | 종목 | 3 | `jobId`, `seq`, `symbol`, `since`, `until`, `maxArticles`. 해당 종목과 시장 전체(`*`) 기사를 함께 요청한다 |
| `news.articles.responded` | news → 브리핑 | 종목 | 3 | `jobId`, `seq`, `symbol`, `articles`(`articleId`, 제목, 요약, 발행 시각, 출처, 주소, 최신순), `truncated` |

- `jobId`는 작업을 시작시킨 `market.move`의 `eventId`다. 같은 `eventId`를 두 번 받아도 작업은 하나다.
- `seq`는 요청 순번이다. 재전송과 기사 재확인은 새 `seq`를 쓰고 응답은 받은 `seq`를 그대로 돌려준다. 브리핑은 가장 최근 `seq`의 응답만 반영한다.
- 0건 응답도 정상 응답이다. 기사 본문과 개인 정보는 어느 이벤트에도 담지 않는다.
- 조회 구간과 기다리는 규칙은 브리핑 설정이다([로컬 개발](../local-development.md#브리핑-서비스-python)).

## 실제로 겪은 함정
- **시각 형식:** `OffsetDateTime.toString()`은 초가 0이면 초를 생략해 RFC 3339 `date-time`에 어긋난다. 표준 포매터를 쓰고, 해당 케이스 테스트를 둔다.
- **형식 검증이 기본으로 꺼져 있다:** JSON 스키마 검증 라이브러리는 `date-time` 등 형식을 기본으로 검사하지 않는다(Java는 옵션을 켜야 하고, Python `jsonschema`는 선택 패키지가 없으면 조용히 통과). 형식 검사를 켜고, **잘못된 입력이 실제로 거부되는지** 음성 테스트로 확인한다.

## 구현 단계에서 정할 사항
공통 봉투(Envelope) 필드 구성(현재 잠정: `eventId, eventType, schemaVersion, occurredAt, producedAt, correlationId, payload`), 개인 정보가 담긴 메시지의 전달 경로.

## 감수하는 단점
형식 강제를 서버가 아닌 테스트와 규칙으로 직접 지켜야 한다.
