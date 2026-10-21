package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import dev.stockreading.market.MarketProperties;
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
        publisher = new MovePublisher(kafka, mapper, new MarketProperties(new BigDecimal("1.00")), CLOCK);
    }

    @ParameterizedTest(name = "change {0}% with threshold 1.00% -> published={1}")
    @CsvSource({
            "0.00,  false",
            "0.99,  false",
            "-0.99, false",
            "1.00,  true",
            "-1.00, true",
            "1.50,  true",
            "-2.37, true"
    })
    void publishesOnlyWhenTheAbsoluteChangeReachesTheThreshold(String change, boolean expected) {
        boolean published = publisher.publishIfSignificant(
                "TEST01", 10_000, 10_150, new BigDecimal(change), AS_OF, "corr-1");

        assertThat(published).isEqualTo(expected);
        if (expected) {
            verify(kafka, times(1)).send(eq("market.move"), eq("TEST01"), anyString());
        } else {
            verify(kafka, never()).send(anyString(), anyString(), anyString());
        }
    }

    @Test
    void publishedEventMatchesTheContractAndKeepsTheCorrelationId() throws IOException {
        publisher.publishIfSignificant("TEST02", 10_000, 9_850, new BigDecimal("-1.50"), AS_OF, "collect-2026-10-20T16:00:00+09:00");

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
    void committedSampleMatchesTheContract() throws IOException {
        assertThat(ContractSchemas.validate("market.move.v1.schema.json", ContractSchemas.sample("market.move.sample.json"))).isEmpty();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MoveEvent(String eventType, String correlationId, String occurredAt, MovePayload payload) {
    }
}
