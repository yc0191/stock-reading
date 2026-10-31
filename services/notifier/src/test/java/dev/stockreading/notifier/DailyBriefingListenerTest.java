package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import dev.stockreading.notifier.mail.DailyEmailComposer;
import dev.stockreading.notifier.mail.EmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import tools.jackson.databind.json.JsonMapper;

class DailyBriefingListenerTest {

    private static final String SAMPLE_ID = "5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40";
    private static final String BRIEFING_SAMPLE_ID = "9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77";

    private MailSenderPort mailSender;
    private String sample;
    private String briefingSample;
    private NotifierProperties properties;
    private SendRateLimiter limiter;

    @BeforeEach
    void setUp() throws IOException {
        mailSender = mock(MailSenderPort.class);
        sample = ContractSchemas.sample("briefing.daily.generated.sample.json");
        briefingSample = ContractSchemas.sample("briefing.generated.sample.json");
        limit(6);
    }

    private void limit(int maxPerHour) {
        properties = new NotifierProperties(false, "from@example.com", "to@example.com", "[시뮬레이션]", maxPerHour);
        limiter = new SendRateLimiter(properties, new TestClock(Instant.parse("2026-10-20T08:00:00Z")));
    }

    private DailyBriefingListener listener() {
        return new DailyBriefingListener(JsonMapper.builder().build(), new DailyEmailComposer(properties), mailSender, limiter);
    }

    // 건별 메일과 같은 한도 장치를 함께 쓰는지 보려고 같은 limiter로 만든다.
    private BriefingListener moveListener() {
        return new BriefingListener(JsonMapper.builder().build(), new EmailComposer(properties), mailSender, limiter);
    }

    private String withEventId(String id) {
        return sample.replace(SAMPLE_ID, id);
    }

    private String briefingWithEventId(String id) {
        return briefingSample.replace(BRIEFING_SAMPLE_ID, id);
    }

    @Test
    void sendsOneEmailPerEditionWithTheComposedSubject() {
        listener().onDailyBriefing(sample);

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender).send(email.capture());
        assertThat(email.getValue().subject()).isEqualTo("[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20");
        assertThat(email.getValue().body()).contains("자료 없음 항목: etf-daily-change:TEST02");
    }

    @Test
    void theSameDailyBriefingDeliveredTwiceIsEmailedOnce() {
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing(sample);
        listener.onDailyBriefing(sample);

        verify(mailSender, times(1)).send(any());
    }

    @Test
    void differentEditionsAreEachEmailed() {
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing(withEventId("11111111-1111-4111-8111-111111111111"));
        listener.onDailyBriefing(withEventId("22222222-2222-4222-8222-222222222222")
                .replace("\"edition\": \"post-close\"", "\"edition\": \"morning\""));

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender, times(2)).send(email.capture());
        assertThat(email.getAllValues()).extracting(EmailMessage::subject).containsExactly(
                "[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20",
                "[시뮬레이션] 아침 판 데일리 브리핑 2026-10-20");
    }

    @Test
    void aDailyBriefingIsEmailedEvenWhenTheHourlyLimitIsAlreadyReached() {
        limit(1);
        moveListener().onBriefing(briefingWithEventId("11111111-1111-4111-8111-111111111111")); // 한도를 채운다

        listener().onDailyBriefing(sample);

        verify(mailSender, times(2)).send(any());
        assertThat(limiter.hasCapacity()).isFalse();
    }

    @Test
    void aSentDailyBriefingCountsTowardTheLimitSoALaterMoveEmailIsBlocked() {
        limit(1);

        listener().onDailyBriefing(sample);
        moveListener().onBriefing(briefingSample);

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender, times(1)).send(email.capture());
        assertThat(email.getValue().subject()).contains("데일리 브리핑");
    }

    @Test
    void aFailedSendIsRetriedLaterAndDoesNotCountTowardTheLimit() {
        limit(1);
        DailyBriefingListener listener = listener();
        doThrow(new IllegalStateException("smtp down")).doNothing().when(mailSender).send(any());

        assertThatThrownBy(() -> listener.onDailyBriefing(sample)).isInstanceOf(IllegalStateException.class);
        assertThat(limiter.hasCapacity()).isTrue(); // 실패한 발송은 세지 않는다
        listener.onDailyBriefing(sample); // Kafka 에러 핸들러가 같은 메시지를 다시 전달한다
        listener.onDailyBriefing(sample);

        verify(mailSender, times(2)).send(any());
        assertThat(limiter.hasCapacity()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not json at all",
            "{\"eventId\": \"5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40\"}",
            "{\"eventId\": \"5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40\", \"payload\": {\"edition\": \"morning\"}}",
            "{\"eventId\": \"5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40\", \"payload\": {\"edition\": \"morning\","
                    + " \"rangeEnd\": \"2026-10-20T08:45:00+09:00\", \"text\": \"  \"}}",
            "{\"payload\": {\"edition\": \"morning\", \"rangeEnd\": \"2026-10-20T08:45:00+09:00\", \"text\": \"t\"}}",
            "{\"eventId\": \"5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40\", \"payload\": {\"edition\": \"morning\","
                    + " \"rangeEnd\": \"tomorrow\", \"text\": \"t\"}}",
            "{\"eventId\": 1, \"payload\": []}"
    })
    void aMalformedMessageIsSkippedWithoutSendingOrThrowing(String message) {
        listener().onDailyBriefing(message);

        verify(mailSender, never()).send(any());
        assertThat(limiter.hasCapacity()).isTrue();
    }

    @Test
    void aMalformedMessageDoesNotBlockTheNextValidOne() {
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing("{broken");
        listener.onDailyBriefing(sample);

        verify(mailSender, times(1)).send(any());
    }
}
