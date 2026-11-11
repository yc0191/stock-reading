package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.stockreading.notifier.mail.EmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import tools.jackson.databind.json.JsonMapper;

class BriefingListenerTest {

    private static final String SAMPLE_ID = "9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77";

    private MailSenderPort mailSender;
    private String sample;

    @BeforeEach
    void setUp() throws IOException {
        mailSender = mock(MailSenderPort.class);
        sample = ContractSchemas.sample("briefing.generated.sample.json");
    }

    private BriefingListener listener(int maxPerHour) {
        NotifierProperties properties = new NotifierProperties(false, "from@example.com", "to@example.com", "[시뮬레이션]", maxPerHour);
        SendRateLimiter limiter = new SendRateLimiter(properties, new TestClock(Instant.parse("2026-10-20T07:00:00Z")));
        return new BriefingListener(JsonMapper.builder().build(), new EmailComposer(properties), mailSender, limiter);
    }

    private String withEventId(String id) {
        return sample.replace(SAMPLE_ID, id);
    }

    @Test
    void sendsOneEmailPerBriefingWithTheComposedSubject() {
        listener(6).onBriefing(sample);

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender).send(email.capture());
        assertThat(email.getValue().subject()).isEqualTo("[시뮬레이션] [TEST 샘플 ETF(TEST01)] 하락 -1.50% 브리핑");
    }

    @Test
    void theSameBriefingDeliveredTwiceIsEmailedOnce() {
        BriefingListener listener = listener(6);

        listener.onBriefing(sample);
        listener.onBriefing(sample);

        verify(mailSender, times(1)).send(any());
    }

    @Test
    void differentBriefingsAreEachEmailed() {
        BriefingListener listener = listener(6);

        listener.onBriefing(withEventId("11111111-1111-4111-8111-111111111111"));
        listener.onBriefing(withEventId("22222222-2222-4222-8222-222222222222"));

        verify(mailSender, times(2)).send(any());
    }

    @Test
    void anExcessBriefingIsNotEmailedOnceTheHourlyLimitIsReached() {
        BriefingListener listener = listener(1);

        listener.onBriefing(withEventId("11111111-1111-4111-8111-111111111111"));
        listener.onBriefing(withEventId("22222222-2222-4222-8222-222222222222"));

        verify(mailSender, times(1)).send(any());
    }

    @Test
    void aFailedSendIsRetriedLaterAndStillEmailedOnce() {
        BriefingListener listener = listener(6);
        doThrow(new IllegalStateException("smtp down")).doNothing().when(mailSender).send(any());

        assertThatThrownBy(() -> listener.onBriefing(sample)).isInstanceOf(IllegalStateException.class);
        listener.onBriefing(sample); // Kafka 에러 핸들러가 같은 메시지를 다시 전달한다

        verify(mailSender, times(2)).send(any());
    }

    @Test
    void aFailedSendDoesNotUseUpTheHourlyAllowance() {
        BriefingListener listener = listener(1);
        doThrow(new IllegalStateException("smtp down")).doNothing().when(mailSender).send(any());

        assertThatThrownBy(() -> listener.onBriefing(sample)).isInstanceOf(IllegalStateException.class);
        listener.onBriefing(sample);

        verify(mailSender, times(2)).send(any()); // 한도가 1이어도 재시도는 허용되었다
    }

    @Test
    void aMalformedMessageIsSkippedWithoutSendingOrThrowing() {
        BriefingListener listener = listener(6);

        listener.onBriefing("not json at all");

        verify(mailSender, never()).send(any());
    }
}
