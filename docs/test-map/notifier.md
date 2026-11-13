# notifier 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 설정 기본값(드라이런), 토픽 선언 | NotifierApplication, NotifierProperties, Topics | ConfigDefaultsTest, TopicsConfigTest | 해당 없음 - 설정을 읽고 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 브리핑 수신과 발송 | BriefingListener, BriefingEnvelope | BriefingContractTest | BriefingListenerTest | scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 메일 제목의 '이름(코드)'(이름이 없거나 비면 코드만), 짧은 시간 변동 제목의 오늘 등락 먼저(오늘 등락률이 없으면 이전 제목, 분이 없으면 '짧은 시간'), 보낸 사람 이름(급변동 '변동 감지', 데일리 '데일리 브리핑', 한글을 UTF-8로), 본문과 주소 | EmailComposer, EmailMessage, MailAddresses | EmailComposerTest, DailyEmailComposerTest, MailAddressesTest, SmtpMailSenderTest | 해당 없음 - 문장과 주소만 만들며 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 메일 발송기(드라이런 기본, SMTP, 글자판+HTML+인라인 그림 묶음 메일, html 없으면 글자 메일) | LoggingMailSender, MailDeliveryException, MailSenderPort, SmtpMailSender | MailSenderWiringTest | SmtpMailSenderTest | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| 데일리 브리핑 수신과 메일(멤버 번호가 있는 v2 이벤트만, 번호 없는 v1은 건너뜀, html이 있으면 묶음 메일, 없으면 글자 메일, 잘못된 그림만 빼기) | DailyBriefingEnvelope, DailyBriefingListener, DailyEmailComposer | DailyBriefingContractTest, DailyEmailComposerTest, DailyBriefingDryRunTest | DailyBriefingListenerTest, SmtpMailSenderTest | 없음 - 데일리 브리핑의 실제 메일 발송은 아직 test-all.sh --live 에 넣지 않음 [외부 API] |
| 받는 사람 고르기(보내기 직전에 DB에서 고름: 급변동은 그 종목을 포트폴리오에 가진 멤버, 아침·마감 후 판은 이벤트의 멤버 번호가 가리킨 멤버 한 명(그 멤버가 사용 중이고 그 판 수신을 켰을 때만, 아니면 보내지 않음), 멤버·메일 줄 모두 사용 중, 0명이면 보내지 않고 경고, 주소 마스킹)와 personal 스키마 권한(member_mail은 발송기·쓰기 계정만 읽음(브리핑 계정은 주소 열을 못 읽는 것을 briefing/test_member_store_db.py가 확인), 수집 대상 뷰, 표 규칙) | JdbcRecipientRepository, Recipient, RecipientPort | 해당 없음 - 쿼리와 권한은 실제 DB에서만 뜻이 있음(문장 만들기는 메일 행) | BriefingListenerTest, DailyBriefingListenerTest | RecipientIntegrationTest [DB, 켜는 조건: NOTIFIER_IT_DB_URL·NOTIFIER_IT_DB_USER·NOTIFIER_IT_DB_PASSWORD·WRITER_IT_DB_USER·WRITER_IT_DB_PASSWORD·COLLECTOR_IT_DB_USER·COLLECTOR_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD]; PersonalSchemaIntegrationTest [DB, 켜는 조건: NOTIFIER_IT_DB_URL·NOTIFIER_IT_DB_USER·NOTIFIER_IT_DB_PASSWORD·WRITER_IT_DB_USER·WRITER_IT_DB_PASSWORD·COLLECTOR_IT_DB_USER·COLLECTOR_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |
| 추적 ID(메시지 처리 중 로그 줄의 correlationId, HTTP 헤더 이름) | CorrelationId, CorrelationIdConfig, CorrelationIdRecordInterceptor | CorrelationIdLoggingTest | 해당 없음 - 로그 문맥만 다루며 외부 의존을 부르지 않음 | 없음 - 실제 Kafka로 서비스를 이어 로그를 찾는 자동 테스트는 아직 없음 |

테스트 도우미: ContractSchemas, DisposableDatabase, PersonalDb, TestClock
