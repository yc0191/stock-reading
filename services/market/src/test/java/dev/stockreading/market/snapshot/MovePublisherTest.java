package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import dev.stockreading.market.event.MovePayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class MovePublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-20T16:00:00+09:00");

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private KafkaTemplate<String, String> kafka;
    private MovePublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafka = mock(KafkaTemplate.class);
        publisher = new MovePublisher(kafka, mapper, CLOCK);
    }

    private static Detection detection(DetectionType type, long previous, long price, String change, OffsetDateTime startedAt) {
        return new Detection(type, previous, price, new BigDecimal(change), new BigDecimal("1.00"), startedAt);
    }

    @Test
    void publishedEventMatchesTheContractAndKeepsTheCorrelationId() throws IOException {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-20T15:58:00+09:00");
        publisher.publish("TEST02", detection(DetectionType.WINDOW, 10_000, 9_850, "-1.50", startedAt),
                startedAt, AS_OF, "collect-2026-10-20T16:00:00+09:00");

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("market.move"), eq("TEST02"), json.capture());

        assertThat(ContractSchemas.validate("market.move.v1.schema.json", json.getValue())).isEmpty();
        MoveEvent event = mapper.readValue(json.getValue(), MoveEvent.class);
        assertThat(event.eventType()).isEqualTo("market.move");
        assertThat(event.correlationId()).isEqualTo("collect-2026-10-20T16:00:00+09:00");
        assertThat(event.occurredAt()).isEqualTo("2026-10-20T16:00:00+09:00");
        assertThat(event.payload().changePercent()).isEqualByComparingTo("-1.50");
        assertThat(event.payload().previousPrice()).isEqualTo(10_000L);
        assertThat(event.payload().price()).isEqualTo(9_850L);
    }

    @Test
    void windowEventCarriesTheDetectionTypeAndTheMoveStartAsAsOf() throws IOException {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-20T15:58:00+09:00");
        publisher.publish("TEST02", detection(DetectionType.WINDOW, 10_000, 10_150, "1.50", startedAt), startedAt, AS_OF, "corr-1");

        MovePayload payload = publishedPayload("TEST02");

        assertThat(payload.detectionType()).isEqualTo("window");
        assertThat(payload.moveStartedAt()).isEqualTo("2026-10-20T15:58:00+09:00");
        assertThat(payload.asOf()).isEqualTo("2026-10-20T15:58:00+09:00");
        assertThat(payload.thresholdPercent()).isEqualByComparingTo("1.00");
    }

    @Test
    void prevCloseEventCarriesTheDetectionTypeAndTheFirstCrossingTime() throws IOException {
        OffsetDateTime crossedAt = OffsetDateTime.parse("2026-10-20T09:30:00+09:00");
        publisher.publish("TEST03", detection(DetectionType.PREV_CLOSE, 10_000, 10_400, "4.00", crossedAt), crossedAt, AS_OF, "corr-1");

        MovePayload payload = publishedPayload("TEST03");

        assertThat(payload.detectionType()).isEqualTo("prevClose");
        assertThat(payload.moveStartedAt()).isEqualTo("2026-10-20T09:30:00+09:00");
        assertThat(payload.asOf()).isEqualTo("2026-10-20T09:30:00+09:00");
    }

    @Test
    void eventCarriesTodayChangeWhenTheDetectionHasIt() throws IOException {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-20T15:59:00+09:00");
        Detection detection = new Detection(DetectionType.WINDOW, 12_000, 12_120, new BigDecimal("1.00"),
                new BigDecimal("1.00"), startedAt, new BigDecimal("-2.00"));
        publisher.publish("TEST04", detection, startedAt, AS_OF, "corr-1");

        assertThat(publishedPayload("TEST04").todayChangePercent()).isEqualByComparingTo("-2.00");
    }

    @Test
    void eventLeavesTodayChangeOutWhenTheDetectionHasNone() throws IOException {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-20T15:59:00+09:00");
        publisher.publish("TEST05", detection(DetectionType.WINDOW, 10_000, 10_150, "1.50", startedAt), startedAt, AS_OF, "corr-1");

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("market.move"), eq("TEST05"), json.capture());
        assertThat(ContractSchemas.validate("market.move.v1.schema.json", json.getValue())).isEmpty();
        assertThat(json.getValue()).doesNotContain("todayChangePercent");
    }

    private MovePayload publishedPayload(String symbol) throws IOException {
        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("market.move"), eq(symbol), json.capture());
        assertThat(ContractSchemas.validate("market.move.v1.schema.json", json.getValue())).isEmpty();
        return mapper.readValue(json.getValue(), MoveEvent.class).payload();
    }

    @Test
    void committedSampleMatchesTheContract() throws IOException {
        assertThat(ContractSchemas.validate("market.move.v1.schema.json", ContractSchemas.sample("market.move.sample.json"))).isEmpty();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MoveEvent(String eventType, String correlationId, String occurredAt, MovePayload payload) {
    }
}
