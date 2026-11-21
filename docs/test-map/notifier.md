# notifier 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 설정 기본값, 토픽 선언 | NotifierApplication, NotifierProperties, Topics | ConfigDefaultsTest, TopicsConfigTest | 해당 없음 - 설정을 읽고 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 브리핑 수신과 발송 | BriefingListener, BriefingEnvelope, ImageData | BriefingContractTest | BriefingListenerTest | scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 메일 제목, 보낸 사람 이름, 본문, HTML·그림 | EmailComposer, EmailMessage, MailAddresses, MailHtml | EmailComposerTest, DailyEmailComposerTest, MailAddressesTest, SmtpMailSenderTest | 해당 없음 - 문장과 주소만 만들며 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 메일 발송기(드라이런 기본, 묶음 메일) | LoggingMailSender, MailDeliveryException, MailSenderPort, SmtpMailSender | MailSenderWiringTest | SmtpMailSenderTest | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| 데일리 브리핑 수신과 메일 | DailyBriefingEnvelope, DailyBriefingListener, DailyEmailComposer | DailyBriefingContractTest, DailyEmailComposerTest, DailyBriefingDryRunTest | DailyBriefingListenerTest, SmtpMailSenderTest | 없음 - 데일리 브리핑의 실제 메일 발송은 아직 test-all.sh --live 에 넣지 않음 [외부 API] |
| 받는 사람 고르기와 personal 스키마 권한 | JdbcRecipientRepository, Recipient, RecipientPort | 해당 없음 - 쿼리와 권한은 실제 DB에서만 뜻이 있음(문장 만들기는 메일 행) | BriefingListenerTest, DailyBriefingListenerTest | RecipientIntegrationTest [DB, 켜는 조건: NOTIFIER_IT_DB_URL·NOTIFIER_IT_DB_USER·NOTIFIER_IT_DB_PASSWORD·WRITER_IT_DB_USER·WRITER_IT_DB_PASSWORD·COLLECTOR_IT_DB_USER·COLLECTOR_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD]; PersonalSchemaIntegrationTest [DB, 켜는 조건: NOTIFIER_IT_DB_URL·NOTIFIER_IT_DB_USER·NOTIFIER_IT_DB_PASSWORD·WRITER_IT_DB_USER·WRITER_IT_DB_PASSWORD·COLLECTOR_IT_DB_USER·COLLECTOR_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |
| 추적 ID | CorrelationId, CorrelationIdConfig, CorrelationIdRecordInterceptor | CorrelationIdLoggingTest | 해당 없음 - 로그 문맥만 다루며 외부 의존을 부르지 않음 | 없음 - 실제 Kafka로 서비스를 이어 로그를 찾는 자동 테스트는 아직 없음 |

테스트 도우미: ContractSchemas, DisposableDatabase, PersonalDb, TestClock
