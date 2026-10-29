package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.edgar.CollectedFiling;
import dev.stockreading.collector.edgar.EdgarApiException;
import dev.stockreading.collector.edgar.EdgarFailure;
import dev.stockreading.collector.edgar.Filing;
import dev.stockreading.collector.edgar.FilingCollector;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class FilingTickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T20:45:00Z"), ZoneId.of("Asia/Seoul"));

    private FilingCollector collector;
    private KafkaTemplate<String, String> kafka;
    private FilingTickListener listener;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        collector = mock(FilingCollector.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        ObjectMapper mapper = JsonMapper.builder().build();
        listener = new FilingTickListener(new FilingPublisher(collector, kafka, mapper, CLOCK), mapper);
    }

    @Test
    void preOpenTickPublishesOneEventPerFilingKeyedByAccessionAndPassesTheContract() throws IOException {
        when(collector.collect()).thenReturn(List.of(
                collected("0000000001-26-000001", "8-K", List.of("2.02", "9.01"), "TESTA"),
                collected("0000000001-26-000002", "6-K", List.of(), "TESTA")));

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.FILING_COLLECTED), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly("0000000001-26-000001", "0000000001-26-000002");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("filing.collected.v1.schema.json", json)).isEmpty();
            assertThat(json)
                    .contains("\"correlationId\":\"collect-pre-open-2026-10-21T08:30:00+09:00\"")
                    .contains("\"occurredAt\":\"2026-10-20T20:30:28Z\"")
                    .contains("\"acceptedAt\":\"2026-10-20T20:30:28Z\"");
        }
        assertThat(value.getAllValues().get(0)).contains("\"items\":[\"2.02\",\"9.01\"]");
        assertThat(value.getAllValues().get(1)).contains("\"items\":[]");
    }

    @Test
    void secondTickDoesNotRepublishTheSameFiling() {
        when(collector.collect()).thenReturn(List.of(collected("0000000001-26-000001", "8-K", List.of("8.01"), "TESTA")));

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));
        listener.onTick(preOpenTick("2026-10-22T08:30:00+09:00"));

        verify(kafka, times(1)).send(eq(Topics.FILING_COLLECTED), anyString(), anyString());
    }

    @Test
    void sameFilingWithANewHoldingTickerIsPublishedAgain() {
        when(collector.collect())
                .thenReturn(List.of(collected("0000000001-26-000001", "8-K", List.of("8.01"), "TESTA")))
                .thenReturn(List.of(collected("0000000001-26-000001", "8-K", List.of("8.01"), "TESTA", "TESTA.B")));

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));
        listener.onTick(preOpenTick("2026-10-22T08:30:00+09:00"));

        verify(kafka, times(2)).send(eq(Topics.FILING_COLLECTED), anyString(), anyString());
    }

    @Test
    void failedSendIsNotRememberedSoTheNextTickRetries() {
        when(collector.collect()).thenReturn(List.of(collected("0000000001-26-000001", "8-K", List.of("8.01"), "TESTA")));
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")))
                .thenReturn(CompletableFuture.completedFuture(null));

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));
        listener.onTick(preOpenTick("2026-10-22T08:30:00+09:00"));
        listener.onTick(preOpenTick("2026-10-23T08:30:00+09:00"));

        verify(kafka, times(2)).send(eq(Topics.FILING_COLLECTED), anyString(), anyString());
    }

    @Test
    void filingThatBreaksTheContractIsNotPublished() {
        when(collector.collect()).thenReturn(List.of(
                collected("0000000001-26-000001", "8-K", List.of(), "testa"),
                collected("bad-accession", "8-K", List.of(), "TESTA")));

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void sameTickDeliveredTwiceCollectsOnlyOnce() {
        when(collector.collect()).thenReturn(List.of());

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));
        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        verify(collector, times(1)).collect();
    }

    @Test
    void otherJobTypesAreIgnored() {
        listener.onTick(tick("collect-news", "2026-10-21T08:30:00+09:00"));
        listener.onTick(tick("collect-post-close", "2026-10-21T16:30:00+09:00"));

        verify(collector, never()).collect();
    }

    @Test
    void oldFilingsTickNoLongerCollects() {
        // 15분 공시 수집이 없어졌으므로 예전 jobType 이 남아 있어도 수집하지 않는다.
        listener.onTick(tick("collect-filings", "2026-10-21T05:45:00+09:00"));

        verify(collector, never()).collect();
    }

    @Test
    void collectFailureDoesNotKillTheListener() {
        when(collector.collect()).thenThrow(new EdgarApiException(EdgarFailure.NOT_CONFIGURED, null));

        assertThatCode(() -> listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"))).doesNotThrowAnyException();
        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void listensWithItsOwnConsumerGroup() throws NoSuchMethodException {
        KafkaListener annotation = FilingTickListener.class.getDeclaredMethod("onTick", String.class)
                .getAnnotation(KafkaListener.class);

        assertThat(annotation.groupId()).isEqualTo("collector-filings");
        assertThat(annotation.topics()).containsExactly(Topics.SCHEDULE_TICK);
    }

    private static CollectedFiling collected(String accession, String form, List<String> items, String... tickers) {
        return new CollectedFiling("0000000001", List.of(tickers), new Filing(accession, form,
                OffsetDateTime.parse("2026-10-20T20:30:28Z"), LocalDate.of(2026, 10, 20), items,
                "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/test-8k.htm"));
    }

    private static String preOpenTick(String scheduledFor) {
        return tick("collect-pre-open", scheduledFor);
    }

    private static String tick(String jobType, String scheduledFor) {
        return """
                {"eventId":"00000000-0000-4000-8000-000000000001","eventType":"schedule.tick","schemaVersion":1,
                 "occurredAt":"%2$s","producedAt":"%2$s","correlationId":"%1$s-%2$s",
                 "payload":{"jobType":"%1$s","scheduledFor":"%2$s"}}""".formatted(jobType, scheduledFor);
    }
}
