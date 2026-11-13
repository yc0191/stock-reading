# news 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 토픽 선언 | NewsApplication, Topics | TopicsConfigTest | 해당 없음 - 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 발행 이벤트와 조회 응답 본문의 모양 | ArticlesPayload, Envelope, EventFactory, StoredPayload | ArticlesContractTest, StoredContractTest | 해당 없음 - 값만 담으며 흐름은 기사 저장·기사 조회 응답 행에서 확인 | 해당 없음 - 외부 의존을 부르지 않음 |
| 기사 저장(수신, 중복 제거, 재검색 표시와 처음 찾은 작업 id, 기사 이벤트마다 처리 결과 기록, 수집 완료의 기사가 모두 처리되면 저장 끝 알림·대기 한도를 넘으면 처리되지 않은 기사를 담아 알림, 오래된 완료·재시도는 기다리지 않고 바로 알림) | ArticleKey, ArticleResult, ArticleResultRepository, CollectionCompletedEnvelope, NewsArticle, NewsArticleRepository, NewsEnvelope, NewsListener, StoredNotifier, StoredProperties | ArticleKeyTest, NewsContractTest, StoredContractTest | NewsListenerTest, StoredNotifierTest | NewsArticleStorageIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD] |
| 기사 처리 실패 보관과 복구 | ErrorHandlerConfig, FailedEvent, FailedEventRecoverer, FailedEventRepository | 해당 없음 - 실패 처리는 Kafka 소비와 저장소를 함께 써서 가상 칸에서 확인 | ErrorHandlerConfigTest, FailedEventRecovererTest | 없음 - 실패 보관 테이블의 실제 DB 테스트는 이번 정비 범위(기사 저장·조회) 밖이라 아직 없음 [DB] |
| 공시 저장과 조회 | Filing, FilingEnvelope, FilingListener, FilingQuery, FilingRepository, StoredFiling | FilingContractTest | FilingListenerTest, FilingQueryTest | FilingIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD] |
| 기사 조회 응답(내부 조회 API, 종목·기간, 재검색 작업 id, ETF는 자체 기사 먼저 그다음 구성 종목 기사, 요청·응답 본문이 옛 요청·응답 이벤트 payload 계약과 같음, API 응답이 조회 클래스의 답과 같음) | ArticleQueryRepository, ArticlesQuery, ArticlesRequest, NewsQueryController | ArticlesContractTest | ArticlesQueryTest, NewsQueryControllerTest | ArticleQueryIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD]; scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 공시 조회 응답(내부 조회 API, 요청·응답 본문이 옛 요청·응답 이벤트 payload 계약과 같음, API 응답이 조회 클래스의 답과 같음) | FilingQueryRepository, FilingsPayload, FilingsQuery, FilingsRequest | FilingsContractTest | FilingsQueryTest, NewsQueryControllerTest, NewsQueryWebServerTest | FilingsQueryIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD] |
| ETF 구성 종목 읽기(시작할 때 market.etf_stock_map을 읽기 전용으로 읽어 보관, 읽지 못하면 시작 중단, 읽기만 가능하고 쓰기는 거부됨) | EtfStockMembers | 해당 없음 - 표를 읽어 모으기만 하며 기사 조회 응답 행의 가상 테스트가 이 보관값을 가짜로 바꿔 확인함 | 해당 없음 - 기사 조회 응답 행에서 가짜 구성 종목으로 확인 | EtfStockMembersIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |
| 추적 ID(보내는 메시지 헤더 correlationId, 메시지 처리 중 로그 줄의 correlationId, HTTP 헤더 이름) | CorrelationId, CorrelationIdConfig, CorrelationIdProducerInterceptor, CorrelationIdRecordInterceptor | CorrelationIdProducerInterceptorTest, CorrelationIdLoggingTest | 해당 없음 - 헤더·로그 문맥만 다루며 외부 의존을 부르지 않음 | 없음 - 실제 Kafka로 서비스를 이어 로그를 찾는 자동 테스트는 아직 없음 |
| 내부 조회 API 공통(실제 웹 서버에 컨트롤러·필터 자동 등록, 요청 헤더 X-Correlation-Id 를 처리 중 로그 줄에, 빠지거나 틀린 요청 칸은 400, 저장소 실패는 내부 내용 없는 500) | CorrelationIdHttpFilter, InvalidRequestException, InvalidRequestHandler, RequestChecks | CorrelationIdHttpFilterTest | NewsQueryControllerTest, NewsQueryWebServerTest | 없음 - 실제 DB와 이어 API를 부르는 자동 테스트는 아직 없음(조회 계산은 조회 클래스(*Query)가 하며 기사·공시 조회 행의 실제 DB 테스트가 지킴) [DB] |

테스트 도우미: ContractSchemas
