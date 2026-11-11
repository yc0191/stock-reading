package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.DailyPayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 발행기가 v2 계약으로 보내는지, 일봉 없는 ETF가 스키마를 통과하는 모양으로 나가는지 확인한다. 값은 모두 가짜다. */
class DailyPublisherTest {

    private static final String JOB_ID = "8d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    @SuppressWarnings("unchecked")
    void publishesSchemaVersion2WithTheJobIdAsKeyAndEtfsWithoutADailyPrice() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        DailyPublisher publisher = new DailyPublisher(kafka, mapper,
                Clock.fixed(Instant.parse("2026-10-20T08:00:01Z"), ZoneId.of("Asia/Seoul")));
        DailyPayload payload = new DailyPayload(JOB_ID, 1, List.of(DailyPayload.Etf.withoutDaily("TEST01",
                DailyPayload.NO_DAILY_PRICE_IN_RANGE, List.of("TESTA"), false, null)), List.of(),
                List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false);

        publisher.publish(payload, "2f6a1c8e-3b7d-4e59-9a0c-6d4e8b2f1a75",
                "daily-briefing-post-close-2026-10-20T17:00:00+09:00");

        ArgumentCaptor<String> message = ArgumentCaptor.captor();
        verify(kafka).send(eq(Topics.MARKET_DAILY_RESPONDED), eq(JOB_ID), message.capture());
        assertThat(mapper.readTree(message.getValue()).get("schemaVersion").asInt()).isEqualTo(2);
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", message.getValue())).isEmpty();
        assertThat(message.getValue()).doesNotContain("\"relation\"");
    }
}
