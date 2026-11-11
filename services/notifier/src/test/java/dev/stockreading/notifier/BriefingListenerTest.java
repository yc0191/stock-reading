package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
import org.mockito.ArgumentCaptor;

import dev.stockreading.notifier.mail.EmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import dev.stockreading.notifier.recipient.Recipient;
import dev.stockreading.notifier.recipient.RecipientPort;
import tools.jackson.databind.json.JsonMapper;

class BriefingListenerTest {

    private static final String SAMPLE_ID = "9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77";

    private MailSenderPort mailSender;
    private RecipientPort recipients;
    private String sample;

    @BeforeEach
    void setUp() throws IOException {
        mailSender = mock(MailSenderPort.class);
        recipients = mock(RecipientPort.class);
        when(recipients.forMove("TEST01")).thenReturn(List.of(new Recipient(1, "to@example.com")));
        sample = ContractSchemas.sample("briefing.generated.sample.json");
    }

    private BriefingListener listener() {
        NotifierProperties properties = new NotifierProperties(false, "from@example.com", "[시뮬레이션]");
        return new BriefingListener(JsonMapper.builder().build(), new EmailComposer(properties), mailSender, recipients);
    }

    private String withEventId(String id) {
        return sample.replace(SAMPLE_ID, id);
    }

    @Test
    void sendsOneEmailPerBriefingWithTheComposedSubject() {
        listener().onBriefing(sample);

        ArgumentCaptor<EmailMessage> email = ArgumentCaptor.forClass(EmailMessage.class);
        verify(mailSender).send(eq("to@example.com"), email.capture());
        assertThat(email.getValue().subject()).isEqualTo("[시뮬레이션] [TEST01] 하락 -1.50% 브리핑");
    }

    @Test
    void theSameBriefingDeliveredTwiceIsEmailedOnce() {
        BriefingListener listener = listener();

        listener.onBriefing(sample);
        listener.onBriefing(sample);

        verify(mailSender, times(1)).send(any(), any());
    }

    @Test
    void differentBriefingsAreEachEmailed() {
        BriefingListener listener = listener();

        listener.onBriefing(withEventId("11111111-1111-4111-8111-111111111111"));
        listener.onBriefing(withEventId("22222222-2222-4222-8222-222222222222"));

        verify(mailSender, times(2)).send(any(), any());
    }

    @Test
    void aFailedSendIsRetriedLaterAndStillEmailedOnce() {
        BriefingListener listener = listener();
        doThrow(new IllegalStateException("smtp down")).doNothing().when(mailSender).send(any(), any());

        assertThatThrownBy(() -> listener.onBriefing(sample)).isInstanceOf(IllegalStateException.class);
        listener.onBriefing(sample); // Kafka 에러 핸들러가 같은 메시지를 다시 전달한다

        verify(mailSender, times(2)).send(any(), any());
    }

    @Test
    void aMalformedMessageIsSkippedWithoutSendingOrThrowing() {
        BriefingListener listener = listener();

        listener.onBriefing("not json at all");

        verify(mailSender, never()).send(any(), any());
    }

    @Test
    void everyRecipientChosenForTheSymbolGetsTheirOwnEmail() {
        when(recipients.forMove("TEST01")).thenReturn(
                List.of(new Recipient(1, "to@example.com"), new Recipient(2, "other@example.com")));

        listener().onBriefing(sample);

        verify(mailSender).send(eq("to@example.com"), any());
        verify(mailSender).send(eq("other@example.com"), any());
    }

    @Test
    void withNoRecipientsNothingIsSentAndTheMessageIsConsumed() {
        when(recipients.forMove("TEST01")).thenReturn(List.of());

        listener().onBriefing(sample);

        verify(mailSender, never()).send(any(), any());
    }

    @Test
    void aRetryAfterAPartialFailureOnlySendsToThoseNotYetSent() {
        when(recipients.forMove("TEST01")).thenReturn(
                List.of(new Recipient(1, "to@example.com"), new Recipient(2, "other@example.com")));
        BriefingListener listener = listener();
        doNothing().doThrow(new IllegalStateException("smtp down")).doNothing().when(mailSender).send(any(), any());

        assertThatThrownBy(() -> listener.onBriefing(sample)).isInstanceOf(IllegalStateException.class);
        listener.onBriefing(sample);

        verify(mailSender, times(1)).send(eq("to@example.com"), any());
        verify(mailSender, times(2)).send(eq("other@example.com"), any());
    }

    @Test
    void aRecipientLookupFailurePropagatesSoTheKafkaErrorHandlerRetries() {
        when(recipients.forMove("TEST01")).thenThrow(new IllegalStateException("db down"));
        BriefingListener listener = listener();

        assertThatThrownBy(() -> listener.onBriefing(sample)).isInstanceOf(IllegalStateException.class);

        verify(mailSender, never()).send(any(), any());
    }
}
