# notifier 테스트 지도

칸과 값의 형식은 [README](README.md)를 따릅니다.

| 기능 | 대상 | 단위 | 가상 | 실제 |
|---|---|---|---|---|
| 앱 시작, 설정 기본값(드라이런), 토픽 선언 | NotifierApplication, NotifierProperties, Topics | ConfigDefaultsTest, TopicsConfigTest | 해당 없음 - 설정을 읽고 토픽을 선언할 뿐 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 브리핑 수신과 발송 | BriefingListener, BriefingEnvelope | BriefingContractTest | BriefingListenerTest | scripts/test_briefing_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live] |
| 발송 속도 제한 | SendRateLimiter | SendRateLimiterTest | 해당 없음 - 시각만 받아 계산하며 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 메일 제목의 '이름(코드)'(이름이 없거나 비면 코드만), 본문과 주소 | EmailComposer, EmailMessage, MailAddresses | EmailComposerTest, MailAddressesTest | 해당 없음 - 문장과 주소만 만들며 외부 의존을 부르지 않음 | 해당 없음 - 외부 의존을 부르지 않음 |
| 메일 발송기(드라이런 기본, SMTP, 글자판+HTML+인라인 그림 묶음 메일, html 없으면 글자 메일) | LoggingMailSender, MailDeliveryException, MailSenderPort, SmtpMailSender | MailSenderWiringTest | SmtpMailSenderTest | scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live] |
| 데일리 브리핑 수신과 메일(html이 있으면 묶음 메일, 없으면 글자 메일, 잘못된 그림만 빼기) | DailyBriefingEnvelope, DailyBriefingListener, DailyEmailComposer | DailyBriefingContractTest, DailyEmailComposerTest, DailyBriefingDryRunTest | DailyBriefingListenerTest, SmtpMailSenderTest | 없음 - 데일리 브리핑의 실제 메일 발송은 아직 test-all.sh --live 에 넣지 않음 [외부 API] |

테스트 도우미: ContractSchemas, TestClock
