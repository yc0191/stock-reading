package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import dev.stockreading.notifier.mail.DailyEmailComposer;
import dev.stockreading.notifier.mail.EmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import dev.stockreading.notifier.recipient.Recipient;
import dev.stockreading.notifier.recipient.RecipientPort;
import tools.jackson.databind.json.JsonMapper;

class DailyBriefingListenerTest {

    private static final String SAMPLE_ID = "5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40";

    private MailSenderPort mailSender;
    private RecipientPort recipients;
    private String sample;
    private NotifierProperties properties;

    @BeforeEach
    void setUp() throws IOException {
        mailSender = mock(MailSenderPort.class);
        recipients = mock(RecipientPort.class);
        when(recipients.forMember(eq(1L), any())).thenReturn(List.of(new Recipient(1, "to@example.com")));
        when(recipients.forMove(any())).thenReturn(List.of(new Recipient(1, "to@example.com")));
        sample = ContractSchemas.sample("v2", "briefing.daily.generated.sample.json");
        properties = new NotifierProperties(false, "from@example.com", "[시뮬레이션]");
    }

    private DailyBriefingListener listener() {
        return new DailyBriefingListener(JsonMapper.builder().build(), new DailyEmailComposer(properties), mailSender, recipients);
    }

    private String withEventId(String id) {
        return sample.replace(SAMPLE_ID, id);
    }

    @Test
    void sendsOneEmailPerEditionWithTheComposedSubject() {
        listener().onDailyBriefing(sample);

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender).send(eq("to@example.com"), email.capture());
        assertThat(email.getValue().subject()).isEqualTo("[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20");
        assertThat(email.getValue().body()).contains("자료 없음 항목: etf-daily-change:TEST02");
    }

    @Test
    void theSameDailyBriefingDeliveredTwiceIsEmailedOnce() {
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing(sample);
        listener.onDailyBriefing(sample);

        verify(mailSender, times(1)).send(any(), any());
    }

    @Test
    void differentEditionsAreEachEmailed() {
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing(withEventId("11111111-1111-4111-8111-111111111111"));
        listener.onDailyBriefing(withEventId("22222222-2222-4222-8222-222222222222")
                .replace("\"edition\": \"post-close\"", "\"edition\": \"morning\""));

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender, times(2)).send(eq("to@example.com"), email.capture());
        assertThat(email.getAllValues()).extracting(EmailMessage::subject).containsExactly(
                "[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20",
                "[시뮬레이션] 아침 판 데일리 브리핑 2026-10-20");
    }

    @Test
    void aFailedSendIsRetriedLaterAndSentOnce() {
        DailyBriefingListener listener = listener();
        doThrow(new IllegalStateException("smtp down")).doNothing().when(mailSender).send(any(), any());

        assertThatThrownBy(() -> listener.onDailyBriefing(sample)).isInstanceOf(IllegalStateException.class);
        listener.onDailyBriefing(sample); // Kafka 에러 핸들러가 같은 메시지를 다시 전달한다
        listener.onDailyBriefing(sample);

        verify(mailSender, times(2)).send(any(), any());
    }

    @Test
    void aRecipientLookupFailurePropagatesSoTheKafkaErrorHandlerRetries() {
        when(recipients.forMember(anyLong(), any())).thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> listener().onDailyBriefing(sample)).isInstanceOf(IllegalStateException.class);

        verify(mailSender, never()).send(any(), any());
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

        verify(mailSender, never()).send(any(), any());
    }

    @Test
    void aMalformedMessageDoesNotBlockTheNextValidOne() {
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing("{broken");
        listener.onDailyBriefing(sample);

        verify(mailSender, times(1)).send(any(), any());
    }

    @Test
    void theRecipientIsLookedUpByTheMemberInTheEventAndTheEdition() {
        listener().onDailyBriefing(sample);

        verify(recipients).forMember(1L, "post-close");
        verify(mailSender).send(eq("to@example.com"), any());
    }

    @Test
    void eachMembersEventGoesOnlyToThatMembersAddress() {
        when(recipients.forMember(eq(2L), any())).thenReturn(List.of(new Recipient(2, "second@example.com")));
        DailyBriefingListener listener = listener();

        listener.onDailyBriefing(sample);
        listener.onDailyBriefing(withEventId("33333333-3333-4333-8333-333333333333").replace("\"memberId\": 1", "\"memberId\": 2"));

        verify(mailSender, times(1)).send(eq("to@example.com"), any());
        verify(mailSender, times(1)).send(eq("second@example.com"), any());
        verify(mailSender, times(2)).send(any(), any());
    }

    @Test
    void aMemberWhoIsDisabledOrHasTheEditionOffGetsNothingAndTheMessageIsConsumed() {
        when(recipients.forMember(anyLong(), any())).thenReturn(List.of());

        listener().onDailyBriefing(sample);

        verify(mailSender, never()).send(any(), any());
    }

    @Test
    void anEventWithoutAMemberIdIsSkippedBecauseNoOneIsAddressed() throws IOException {
        listener().onDailyBriefing(ContractSchemas.sample("v1", "briefing.daily.generated.sample.json"));

        verify(recipients, never()).forMember(anyLong(), any());
        verify(mailSender, never()).send(any(), any());
    }
}
