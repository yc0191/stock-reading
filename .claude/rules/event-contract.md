---
paths:
  - "contracts/**"
  - "services/*/src/**/Topics.java"
  - "services/*/src/**/Envelope.java"
  - "services/briefing/src/**"
---

# 이벤트 계약 규칙

이벤트 스키마, 토픽, `Envelope`·`EventFactory` 같은 이벤트 모델을 바꿀 때 읽습니다. 서비스 사이에 일어난 일(알림)은 Kafka 이벤트로, 이미 저장된 자료의 조회는 내부 조회 API(HTTP)로 주고받으며, 조회의 요청·응답 본문도 같은 스키마 파일을 계약으로 씁니다(ADR-006, ADR-010). 서비스마다 이벤트 모델을 복제해 두었기 때문에 한 곳만 고치면 다른 서비스가 조용히 깨집니다.

## 호환성

- `contracts/events/*.v1.schema.json`과 `samples/v1/`의 기존 계약은 호환을 유지합니다. 필드 추가는 가능하지만 이름 변경, 삭제, 타입 변경은 하지 않습니다.
- 소비자는 모르는 필드를 무시해야 합니다. 이를 확인하는 테스트(`consumerIgnoresFieldsAddedInFutureVersions`)를 약하게 만들지 않습니다.
- 호환이 깨지는 변경이 필요하면 `v2` 스키마와 샘플을 새로 추가하고, 결정을 ADR에 남깁니다(`.claude/rules/adr.md`).
- 샘플은 전부 가짜 데이터(`TEST01` 등)입니다.
- 쓰지 않게 된 이벤트의 스키마·샘플은 지우지 않고 가장 높은 버전 설명에 '더 이상 쓰지 않음'과 대체한 방식을 적습니다(과거 이벤트를 다시 읽기 위해).

## 스키마를 바꿀 때 함께 확인할 곳

1. `contracts/events/<토픽>.v1.schema.json`과 `contracts/events/samples/v1/<토픽>.sample.json`
2. 그 이벤트를 만드는 서비스의 `event/` 패키지 (`Envelope`, `EventFactory`, `*Payload`)
3. 그 이벤트를 읽는 서비스의 이벤트 모델 (`SnapshotEnvelope`, `BriefingEnvelope` 등). 조회 본문이면 조회하는 서비스의 `*Request`·조회 클래스와 브리핑의 `queries.py`
4. 계약 테스트: 각 Java 서비스의 `*ContractTest`, `ContractSchemas`와 브리핑의 `tests/test_contracts.py`, `tests/contracts.py`
5. `docs/adr/010-event-format.md`와 README의 흐름도

## 토픽을 추가·변경할 때

- 토픽을 생산하거나 소비하는 모든 서비스가 시작할 때 그 토픽을 선언합니다(Java는 `Topics.java`, 브리핑은 `main.py`). 브로커 자동 생성은 꺼져 있습니다.
- 같은 토픽의 파티션 수는 선언하는 모든 곳에서 같아야 합니다. 어긋나면 일부 종목이 오류 없이 누락됩니다.
- 선언은 테스트가 검증합니다. Java 서비스는 `TopicsConfigTest`, 브리핑은 `tests/test_topics.py`입니다. 토픽을 바꾸면 이 테스트도 함께 고칩니다.
- 이벤트는 `eventId`로 멱등하게 처리합니다. 새 소비자도 같은 이벤트를 두 번 받아도 결과가 같아야 합니다.

## 끝나면

Java 전체와 브리핑 테스트를 실행합니다(`.claude/rules/testing.md`).
