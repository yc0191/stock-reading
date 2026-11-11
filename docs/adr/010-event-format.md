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

2026-11-07 변경: `market.daily.responded`에 `v2`(같은 토픽, `schemaVersion` 2)를 더했다. v1은 범위 안에 일봉이 없는 ETF를 목록에서 빼서, 일봉 수집이 실패한 ETF가 브리핑에서 이유 없이 사라졌다. v2는 ETF 목록을 종목 테이블의 보유·관심 국내 ETF로 만들고, 일봉이 없으면 하루 등락 칸을 `null`로 두고 `missingReason`에 이유를 싣는다. 칸이 비는 것은 v1 소비자가 필수로 읽던 값이 없어지는 변경이라 필드 추가가 아니라 새 버전으로 냈다. v1 스키마와 샘플은 그대로 둔다. 필드의 원본은 `contracts/events/market.daily.responded.v2.schema.json`이다. 브리핑은 v2를 읽어 값이 빈 ETF도 목록에서 빼지 않고 `자료 없음: <ETF 이름>, <이유>`로 싣는다(이름이 없으면 종목코드). 빈 ETF와 이유는 LLM 입력에도 넣고, `briefing.daily.generated`의 `missingItems`(`보유 ETF별 하루 등락: <ETF>, <이유>`)와 작업 기록의 빠진 항목 줄에 남긴다. v1 응답(`missingReason` 없음)도 그대로 읽는다.

2026-11-08 변경: `market.daily.requested`에 선택 칸 `includeOperations`를, `market.daily.responded` v2에 선택 칸 `operations`(수집 종류별 받은 시각·건수, 시세 실패 종류별 건수, 처리하지 못하고 버린 메시지 수)를 더했다. 마감 후 판의 서버 상태 절이 쓰며, 요청이 `true`로 보낼 때만 응답에 붙는다. 같은 토픽·같은 버전의 필드 추가라 기존 소비자는 그대로 읽는다. 필드의 원본은 `contracts/events`의 스키마다.

2026-11-08 변경: `market.daily.responded` v2의 ETF마다 선택 칸 `relation`(`HELD` 보유, `WATCH` 관심)을 더했다. 메일에서 보유와 관심을 나눠 보여 주려는 것이며, 값은 종목 테이블의 관계를 그대로 옮기고 보유 수량 같은 개인 정보는 싣지 않는다. 같은 토픽·같은 버전의 필드 추가라 기존 소비자는 그대로 읽고, 칸이 없는 응답은 구분을 모르는 것으로 본다. 필드의 원본은 `contracts/events`의 스키마다.

2026-11-08 변경: `briefing.daily.generated`의 payload에 선택 칸 `html`(꾸민 본문)과 `images`(html이 `cid:`로 가리키는 인라인 그림: Content-ID·형식·base64)를 더했다. `html`이 있으면 발송기가 글자판과 HTML, 그림을 묶은 메일을 보내고, 없으면 지금처럼 글자 메일만 보낸다. 같은 토픽·같은 버전의 필드 추가라 기존 소비자는 그대로 읽는다. 필드의 원본은 `contracts/events`의 스키마다.

2026-11-10 변경: 보유 종목 목록(`market.holdings`, 시세처리기 → 수집기)과 뉴스 검색어 목록(`news.search-queries`, 뉴스 → 수집기) 새 이벤트를 더했다. 두 목록은 설정 값(`ETF_US_HOLDINGS`, `NEWS_SEARCH_QUERIES`)으로 서비스마다 따로 읽다가 어긋나면 종목이 오류 없이 빠졌고, 값을 고치려면 재시작이 필요했다. 이제 원본은 시세처리기와 뉴스의 테이블 하나씩이고, 테이블이 바뀔 때만(재시작 뒤 첫 확인은 한 번 무조건) **그때의 전체 목록**을 낸다. 변경분이 아니라 전체 목록인 이유: 소비자가 중간 이벤트를 놓쳐도 마지막 하나만 받으면 맞는 상태가 되고, 같은 이벤트를 두 번 받아도 결과가 같다. 두 토픽은 키가 고정이고(`holdings`, `search-queries`) 압축 토픽(`cleanup.policy=compact`)이라 마지막 목록만 남아, 소비자가 파티션을 배정받을 때마다 처음부터 읽어 마지막 목록을 받는다(소비자는 전용 consumer group을 쓰고, 읽은 위치가 이미 커밋된 그룹이 재시작해도 받도록 배정 때마다 `seekToBeginning`을 부른다. `auto.offset.reset=earliest`만으로는 읽은 위치가 없는 첫 실행에만 맞다). 키가 하나라 파티션은 1개이고 생산자·소비자 선언이 같아야 한다. 소비자는 목록을 받기 전에는 설정으로 대신하지 않는다(수집기는 빈 목록, 검색어는 수집하지 않고 "아직 받지 못함" 경고를 남김). `contentHash`는 정렬한 목록의 SHA-256이라 소비자가 바뀐 것을 비교할 수 있다. 필드의 원본은 `contracts/events`의 스키마다.

2026-11-10 변경: `news.collected`의 `payload.symbols` 항목 형식을 국내 6자리 코드·`*`에서 market.holdings의 symbol과 같은 형식(국내 코드, 미국 티커, 종류 구분은 `-`, `*`)으로 넓혔다. 미국 구성 종목 이름으로 검색해 찾은 기사에 그 티커를 태그로 붙이려는 것이며, 기존 값은 모두 새 형식에도 맞아 기존 발행 메시지와 샘플이 그대로 통과하고 필드 이름·타입·필수 여부는 그대로라 v1을 유지한다. 뉴스 서비스는 기사 태그 칸(`news_article_symbol.symbol`)을 16자로 넓힌다. 태그는 기사 주제(검색어에 걸린 종목) 그대로 두고 ETF가 담은 구성 종목 관계는 태그에 풀어 적지 않는다. 대신 뉴스 서비스가 `market.holdings`를 받아 기사를 꺼낼 때(`news.articles.requested`) 넓힌다. ETF로 꺼내면 그 ETF 태그 기사(시장 전체 `*` 포함)를 최신순으로 먼저, 그다음 구성 종목 태그 기사(앞의 기사와 겹치는 기사 제외)를 최신순으로 놓고 요청 상한까지 자른다. 이 순서는 `news.articles.responded`의 `articles` 설명에 적었고 칸과 형식은 그대로다. 완료 알림(`collection.completed`)의 `incompleteSymbols`는 국내 코드와 `*`만 받으므로, 미국 티커 검색이 불완전하면 그 티커를 담은 관심 ETF의 코드로 바꿔 알린다. 필드의 원본은 `contracts/events`의 스키마다.

2026-11-10 변경: `market.daily.responded` v2의 `etfs[].etf`에 선택 칸 `memberChanges`(상위 구성 종목의 들어옴·나감·순위 바뀜과 `since`·`asOf`)를 더했다. 판 요청 범위 안에 반영된 변경이 있을 때만 싣고 없으면 칸을 빼므로 기존 소비자와 샘플은 그대로 통과해 v2를 유지한다. `schedule.tick`에는 알려진 `jobType` 값 `collect-members`를 더했다(구조 변경 없음). 칸의 원본은 `contracts/events`의 스키마다.

2026-11-11 변경: `market.holdings`와 `news.search-queries`를 없앴다. 두 목록의 원본이 DB 표가 되어 수집기와 뉴스가 시작할 때 표를 직접 읽는다(ADR-004 변경). 데일리 판 결과는 `briefing.daily.generated` v2로 멤버마다 한 건씩 내며 멤버 번호가 필수이고 주소는 싣지 않는다(필수 칸이 늘어 새 버전). `market.daily.responded` v2의 보유·관심 선택 칸은 멤버마다 달라 지웠고, 아침 판의 개장 흐름을 위해 `market.daily.requested`에 선택 칸 `openFlowSince`, 응답 ETF마다 선택 칸 `openFlow`를 더했다. 필드의 원본은 `contracts/events/`의 스키마다.

2026-11-11 변경: `briefing.generated`의 payload에 선택 칸 `name`(종목·ETF 이름)을 더했다. 급변동 메일의 제목과 본문 첫 줄을 '이름(코드)'로 쓰려는 것이며, 브리핑이 `market.context.responded`의 `etf.name`을 그대로 옮긴다. 이름을 모르면 칸을 뺀다(빈 문자열은 스키마가 거부한다). 소비자는 칸이 없거나 비어 있으면 코드만 쓴다. 같은 토픽·같은 버전의 필드 추가라 기존 소비자는 그대로 읽는다. 필드의 원본은 `contracts/events`의 스키마다.

## 실제로 겪은 함정
- **시각 형식:** `OffsetDateTime.toString()`은 초가 0이면 초를 생략해 RFC 3339 `date-time`에 어긋난다. 표준 포매터를 쓰고, 해당 케이스 테스트를 둔다.
- **형식 검증이 기본으로 꺼져 있다:** JSON 스키마 검증 라이브러리는 `date-time` 등 형식을 기본으로 검사하지 않는다(Java는 옵션을 켜야 하고, Python `jsonschema`는 선택 패키지가 없으면 조용히 통과). 형식 검사를 켜고, **잘못된 입력이 실제로 거부되는지** 음성 테스트로 확인한다.

## 구현 단계에서 정할 사항
개인 정보가 담긴 메시지의 전달 경로.

## 감수하는 단점
형식 강제를 서버가 아닌 테스트와 규칙으로 직접 지켜야 한다.
