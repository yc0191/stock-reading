# notifier 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 설정 기본값(드라이런), 토픽 선언 | NotifierApplication, NotifierProperties, Topics | ConfigDefaultsTest, TopicsConfigTest | 해당 없음 - 설정을 읽고 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 브리핑 수신과 발송 | BriefingListener, BriefingEnvelope | BriefingContractTest | BriefingListenerTest | scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 발송 속도 제한 | SendRateLimiter | SendRateLimiterTest | 해당 없음 - 시각만 받아 계산하며 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 메일 본문과 주소 | EmailComposer, EmailMessage, MailAddresses | EmailComposerTest, MailAddressesTest | 해당 없음 - 문장과 주소만 만들며 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 메일 발송기(드라이런 기본, SMTP, 글자판+HTML+인라인 그림 묶음 메일, html 없으면 글자 메일) | LoggingMailSender, MailDeliveryException, MailSenderPort, SmtpMailSender | MailSenderWiringTest | SmtpMailSenderTest | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| 데일리 브리핑 수신과 메일(html이 있으면 묶음 메일, 없으면 글자 메일, 잘못된 그림만 빼기) | DailyBriefingEnvelope, DailyBriefingListener, DailyEmailComposer | DailyBriefingContractTest, DailyEmailComposerTest, DailyBriefingDryRunTest | DailyBriefingListenerTest, SmtpMailSenderTest | 없음 - 데일리 브리핑의 실제 메일 발송은 아직 test-all.sh --live 에 넣지 않음 [외부 API] |
| 받는 사람 고르기(보내기 직전에 DB에서 고름: 급변동은 그 종목을 포트폴리오에 가진 멤버, 아침·마감 후 판은 수신 설정을 켠 멤버, 멤버·메일 줄 모두 사용 중, 0명이면 보내지 않고 경고, 주소 마스킹)와 personal 스키마 권한(member_mail은 발송기·쓰기 계정만 읽음, 수집 대상 뷰, 표 규칙) | JdbcRecipientRepository, Recipient, RecipientPort | 해당 없음 - 쿼리와 권한은 실제 DB에서만 뜻이 있음(문장 만들기는 메일 행) | BriefingListenerTest, DailyBriefingListenerTest | RecipientIntegrationTest [DB, 켜는 조건: NOTIFIER_IT_DB_URL·NOTIFIER_IT_DB_USER·NOTIFIER_IT_DB_PASSWORD·WRITER_IT_DB_USER·WRITER_IT_DB_PASSWORD·COLLECTOR_IT_DB_USER·COLLECTOR_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD]; PersonalSchemaIntegrationTest [DB, 켜는 조건: NOTIFIER_IT_DB_URL·NOTIFIER_IT_DB_USER·NOTIFIER_IT_DB_PASSWORD·WRITER_IT_DB_USER·WRITER_IT_DB_PASSWORD·COLLECTOR_IT_DB_USER·COLLECTOR_IT_DB_PASSWORD·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD] |

테스트 도우미: ContractSchemas, DisposableDatabase, PersonalDb, TestClock
