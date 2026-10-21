package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.marketdata.SimulatorMarketData;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = List.of("TEST01", "TEST02", "TEST03");

    private KafkaTemplate<String, String> kafka;
    private TickListener listener;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafka = mock(KafkaTemplate.class);
        ObjectMapper mapper = JsonMapper.builder().build();
        listener = new TickListener(
                new SimulatorMarketData(CLOCK), new CollectorProperties(SYMBOLS, 1_000), kafka, mapper, CLOCK);
    }

    @Test
    void publishesOneSnapshotPerSymbolKeyedBySymbolAndMatchingTheContract() throws IOException {
        listener.onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), key.capture(), value.capture());

        assertThat(key.getAllValues()).containsExactlyElementsOf(SYMBOLS);
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", json)).isEmpty();
        }
    }

    @Test
    void ignoresDuplicateTickForTheSameScheduledTime() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json");

        listener.onTick(tick);
        listener.onTick(tick);

        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
    }

    @Test
    void ignoresUnknownJobTypes() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json").replace("\"collect\"", "\"something-else\"");

        listener.onTick(tick);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }
}
