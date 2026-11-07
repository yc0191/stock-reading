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

## v1 확장
호환 규칙에 따라 v1에는 선택 필드와 새 이벤트만 더한다. 필드와 형식의 원본은 `contracts/events/*.schema.json`이고, 토픽의 키와 파티션 수는 각 서비스의 `Topics`다.

근거 조회는 요청·응답 이벤트 쌍(`market.history.*`, `news.articles.*`)과 수집 완료 알림(`collection.completed`)으로 한다. 여러 서비스가 함께 지키는 규칙은 다음과 같다.
- `jobId`는 작업을 시작시킨 `market.move`의 `eventId`다.
- `seq`는 요청 순번이다. 응답은 받은 `seq`를 그대로 돌려주고, 요청한 쪽은 가장 최근 `seq`의 응답만 반영한다.
- 요청은 읽기 전용이라 다시 보내도 부작용이 없고, 0건 응답도 정상 응답이다.

2026-10-30 변경: 데일리 브리핑(하루 2회 판)의 결과를 `briefing.daily.generated` 새 이벤트로 더했다. 건별 `briefing.generated`는 종목 한 건 단위라 필수 필드가 맞지 않아 넓히지 않고 따로 둔다. 판의 시작 신호는 새 토픽 없이 `schedule.tick`의 `jobType` 값 2개로 보낸다.

2026-11-02 변경: 배경 자료(`market.context.*`)와 공시(`news.filings.*`) 요청·응답 쌍을 더했다. 기존 응답에 얹지 않고 자료마다 한 쌍으로 둔 이유: 얹으면 이미 정한 v1 응답의 뜻이 바뀌고, 공시는 배경 자료 응답의 미국 종목 목록을 받은 뒤에야 요청할 수 있어 따로 받는다. ETF별 미국 종목 대응표는 해외 등락률을 저장한 시세처리기가 읽어 그 목록을 응답에 싣는다. 선택 필드도 더했다: `market.history.requested`의 `intervalSeconds`, `briefing.generated` 근거의 `kind`·`ref`, `market.context.responded` 세계지수 항목의 `indexRates`. 필드의 원본은 `contracts/events/`의 스키마다.

2026-11-04 변경: 근거가 부족할 때의 뉴스 재검색을 위해 `news.research.requested` 새 요청 이벤트(브리핑 → 수집기)를 더했다. 응답 이벤트는 따로 두지 않는다. 수집기는 찾은 기사를 기존 `news.collected`로 news에 저장하게 하고, 끝나면 `collection.completed`를 한 건 보내며, 브리핑은 그 완료를 받은 뒤 기존 `news.articles.*` 쌍으로 다시 조회한다. 검색 결과를 수집기가 바로 돌려주지 않는 이유: 기사 저장과 시간 범위·건수 규칙을 news 한 곳에 두기 위해서다. 요청의 `jobId`는 급변동이면 `market.move`의 `eventId`, 데일리 판이면 그 판을 시작시킨 `schedule.tick`의 `eventId`다. 선택 필드 `researchJobId`를 `news.collected`, `collection.completed`, `news.articles.requested`에 더해 재검색 기사, 끝난 재검색, 그 재검색 기사만 조회하는 요청을 표시한다. 재검색 완료의 `jobType`은 `research-news`다.
2026-11-05 변경: 데일리 브리핑 판 자료 요청·응답 쌍(`market.daily.*`)을 더했다. 판은 종목 하나가 아니라 보유 ETF 전체를 다루므로 종목마다 `market.context.*`를 여러 번 보내지 않고 한 쌍으로 한 번에 받는다. 보유 ETF는 따로 정한 목록이 없어 시세처리기가 범위 안에 국내 일봉이 저장된 종목으로 정하고, 해외 등락률·세계지수 계산과 받은 시각 하한은 `market.context.*`와 같은 규칙을 쓴다. 이 쌍의 `jobId`는 판을 시작시킨 `schedule.tick`의 `eventId`다.

2026-11-06 변경: `market.context.responded`와 `market.daily.responded`에 ETF 정보 선택 칸 `etf`(이름, 추종 대상, 업종, 구성 종목과 비중·순위)를 더했다. 선택 칸이라 기존 소비자는 그대로 읽는다. 필드는 `contracts/events`의 스키마가 원본이다.

2026-11-07 변경: `market.daily.responded`에 `v2`(같은 토픽, `schemaVersion` 2)를 더했다. v1은 범위 안에 일봉이 없는 ETF를 목록에서 빼서, 일봉 수집이 실패한 ETF가 브리핑에서 이유 없이 사라졌다. v2는 ETF 목록을 종목 테이블의 보유·관심 국내 ETF로 만들고, 일봉이 없으면 하루 등락 칸을 `null`로 두고 `missingReason`에 이유를 싣는다. 칸이 비는 것은 v1 소비자가 필수로 읽던 값이 없어지는 변경이라 필드 추가가 아니라 새 버전으로 냈다. v1 스키마와 샘플은 그대로 둔다. 필드의 원본은 `contracts/events/market.daily.responded.v2.schema.json`이다.

## 실제로 겪은 함정
- **시각 형식:** `OffsetDateTime.toString()`은 초가 0이면 초를 생략해 RFC 3339 `date-time`에 어긋난다. 표준 포매터를 쓰고, 해당 케이스 테스트를 둔다.
- **형식 검증이 기본으로 꺼져 있다:** JSON 스키마 검증 라이브러리는 `date-time` 등 형식을 기본으로 검사하지 않는다(Java는 옵션을 켜야 하고, Python `jsonschema`는 선택 패키지가 없으면 조용히 통과). 형식 검사를 켜고, **잘못된 입력이 실제로 거부되는지** 음성 테스트로 확인한다.

## 구현 단계에서 정할 사항
개인 정보가 담긴 메시지의 전달 경로.

## 감수하는 단점
형식 강제를 서버가 아닌 테스트와 규칙으로 직접 지켜야 한다.
