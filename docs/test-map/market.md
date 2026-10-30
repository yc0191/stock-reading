# market 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 설정 기본값, 토픽 선언 | MarketApplication, MarketProperties, Topics | MarketPropertiesDefaultsTest, TopicsConfigTest | 해당 없음 - 설정을 읽고 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 발행 이벤트의 모양 | Envelope, EventFactory, HistoryPayload, MovePayload | SnapshotContractTest, HistoryContractTest | 해당 없음 - 값만 담으며 발행 흐름은 변동 감지·이력 조회 행에서 확인 | 해당 없음 - 외부 의존을 부르지 않음 |
| 시세 저장(스냅샷 수신, 원문 보존, 날짜별 파티션) | SnapshotEnvelope, SnapshotListener, PriceSnapshotRepository, PricePartitions, PartitionPort, JdbcPartitionPort, PricePoint, RawJson | SnapshotContractTest | SnapshotListenerTest, PricePartitionsTest | 없음 - real-db 구간에서 채움 [DB]; 없음 - test-all 구간에서 채움 [서비스 흐름] |
| 변동 감지와 변동 상태 | ChangeRate, Detection, DetectionType, Evaluation, MoveDetector, MovePublisher, MoveState, MoveStateRepository, MoveTracker | ChangeRateTest, SnapshotContractTest | MoveDetectorTest, MoveTrackerTest, MovePublisherTest | 없음 - real-db 구간에서 채움 [DB] |
| 시세 조회 실패 기록 | FailureEnvelope, QuoteFailureListener, QuoteFailureRepository | SnapshotContractTest | QuoteFailureListenerTest | 없음 - real-db 구간에서 채움 [DB] |
| 시세 이력 조회 응답 | HistoryPublisher, HistoryRequestEnvelope, HistoryRequestListener, PriceHistoryRepository | HistoryContractTest | HistoryPublisherTest, HistoryRequestListenerTest | 없음 - real-db 구간에서 채움 [DB] |
| 인사이트 저장과 해외 마감 등락률 조회 | ApiType, InsightEnvelope, InsightListener, InsightRepository, OverseasCloseRate, OverseasCloseRateQuery, OverseasDailyRow | InsightContractTest | InsightListenerTest, OverseasCloseRateQueryTest | InsightIntegrationTest [DB, 켜는 조건: MARKET_IT_DB_URL·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |
| 인사이트 처리 실패 보관과 복구 | FailedEvent, FailedEventRecoverer, FailedEventRepository, InsightErrorHandling | InsightErrorHandlingWiringTest | FailedEventRecovererTest, InsightErrorHandlingTest | InsightIntegrationTest [DB, 켜는 조건: MARKET_IT_DB_URL·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |

테스트 도우미: ContractSchemas
