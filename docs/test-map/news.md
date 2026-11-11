# news 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 토픽 선언 | NewsApplication, Topics | TopicsConfigTest | 해당 없음 - 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 발행 이벤트의 모양 | ArticlesPayload, Envelope, EventFactory | ArticlesContractTest | 해당 없음 - 값만 담으며 발행 흐름은 기사 조회 응답 행에서 확인 | 해당 없음 - 외부 의존을 부르지 않음 |
| 기사 저장(수신, 중복 제거, 재검색 표시와 처음 찾은 작업 id) | ArticleKey, NewsArticle, NewsArticleRepository, NewsEnvelope, NewsListener | ArticleKeyTest, NewsContractTest | NewsListenerTest | NewsArticleStorageIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD] |
| 기사 처리 실패 보관과 복구 | ErrorHandlerConfig, FailedEvent, FailedEventRecoverer, FailedEventRepository | 해당 없음 - 실패 처리는 Kafka 소비와 저장소를 함께 써서 가상 칸에서 확인 | ErrorHandlerConfigTest, FailedEventRecovererTest | 없음 - 실패 보관 테이블의 실제 DB 테스트는 이번 정비 범위(기사 저장·조회) 밖이라 아직 없음 [DB] |
| 공시 저장과 조회 | Filing, FilingEnvelope, FilingListener, FilingQuery, FilingRepository, StoredFiling | FilingContractTest | FilingListenerTest, FilingQueryTest | FilingIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD] |
| 기사 조회 응답(종목·기간, 재검색 작업 id, ETF는 자체 기사 먼저 그다음 구성 종목 기사) | ArticleQueryRepository, ArticlesPublisher, ArticlesRequestEnvelope, ArticlesRequestListener | ArticlesContractTest | ArticlesPublisherTest, ArticlesRequestListenerTest | ArticleQueryIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD]; scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 공시 조회 응답 | FilingQueryRepository, FilingsPayload, FilingsPublisher, FilingsRequestEnvelope, FilingsRequestListener | FilingsContractTest | FilingsRequestListenerTest | FilingsQueryIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD] |
| ETF 구성 종목 읽기(시작할 때 market.etf_stock_map을 읽기 전용으로 읽어 보관, 읽지 못하면 시작 중단, 읽기만 가능하고 쓰기는 거부됨) | EtfStockMembers | 해당 없음 - 표를 읽어 모으기만 하며 기사 조회 응답 행의 가상 테스트가 이 보관값을 가짜로 바꿔 확인함 | 해당 없음 - 기사 조회 응답 행에서 가짜 구성 종목으로 확인 | EtfStockMembersIntegrationTest [DB, 켜는 조건: NEWS_IT_DB_URL·NEWS_IT_DB_USER·NEWS_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |

테스트 도우미: ContractSchemas
