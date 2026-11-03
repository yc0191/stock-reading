package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

class InsightListenerTest {

    private static final UUID EVENT_ID = UUID.fromString("5c0e7a41-92d3-4b6f-8e1a-3f4d5c6b7a89");
    private static final OffsetDateTime FETCHED_AT = OffsetDateTime.parse("2026-10-20T16:30:01.800+09:00");

    private InsightRepository repository;
    private InsightListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(InsightRepository.class);
        listener = new InsightListener(repository, JsonMapper.builder().build());
    }

    @Test
    void storesTheResponseTextExactlyAsReceived() throws IOException {
        String sample = ContractSchemas.sample("market.insight.sample.json");
        String response = JsonMapper.builder().build().readTree(sample).at("/payload/response").asString();

        listener.onInsight(sample);

        verify(repository).insertIfAbsent(ApiType.DAILY_PRICE, EVENT_ID, "TEST01", response, FETCHED_AT);
    }

    @Test
    void redeliveredEventIsHandledWithoutError() throws IOException {
        when(repository.insertIfAbsent(any(), any(), any(), anyString(), any())).thenReturn(false);

        listener.onInsight(ContractSchemas.sample("market.insight.sample.json"));

        verify(repository).insertIfAbsent(any(), any(), any(), anyString(), any());
    }

    @Test
    void summariesWithoutTargetAreStoredWithNullTarget() throws IOException {
        String message = ContractSchemas.sample("market.insight.sample.json")
                .replace("\"daily-price\"", "\"market-summary\"")
                .replace("\"target\": \"TEST01\"", "\"target\": null");

        listener.onInsight(message);

        verify(repository).insertIfAbsent(eq(ApiType.MARKET_SUMMARY), any(), isNull(), anyString(), any());
    }

    @Test
    void overseasDailyPriceIsStoredInItsOwnTableWithTheTicker() throws IOException {
        String sample = ContractSchemas.sample("market.insight.sample.json");
        String message = sample.replace("\"daily-price\"", "\"overseas-daily-price\"")
                .replace("\"target\": \"TEST01\"", "\"target\": \"TESTB.X\"");
        String response = JsonMapper.builder().build().readTree(sample).at("/payload/response").asString();

        listener.onInsight(message);

        verify(repository).insertIfAbsent(ApiType.OVERSEAS_DAILY_PRICE, EVENT_ID, "TESTB.X", response, FETCHED_AT);
        assertThat(ApiType.OVERSEAS_DAILY_PRICE.table()).isEqualTo("overseas_daily_price");
        assertThat(ApiType.OVERSEAS_DAILY_PRICE.hasTarget()).isTrue();
    }

    @Test
    void unknownApiTypeFailsInsteadOfBeingDropped() throws IOException {
        String message = ContractSchemas.sample("market.insight.sample.json").replace("\"daily-price\"", "\"bond-yield\"");

        assertThatThrownBy(() -> listener.onInsight(message)).isInstanceOf(IllegalArgumentException.class);
        verify(repository, never()).insertIfAbsent(any(), any(), any(), any(), any());
    }

    @Test
    void ignoresFieldsOutsideTheContract() throws IOException {
        String message = ContractSchemas.sample("market.insight.sample.json")
                .replace("\"payload\": {", "\"extra\": 1,\n  \"payload\": {\n    \"venue\": \"future\",");

        listener.onInsight(message);

        verify(repository).insertIfAbsent(any(), any(), any(), anyString(), any());
    }

    @Test
    void apiTypeNamesMapToTablesAndTargetColumns() {
        assertThat(ApiType.fromWire("exchange-rate-summary").table()).isEqualTo("exchange_rate_summary");
        assertThat(ApiType.fromWire("world-index").hasTarget()).isTrue();
        assertThat(ApiType.fromWire("market-summary").hasTarget()).isFalse();
    }
}
