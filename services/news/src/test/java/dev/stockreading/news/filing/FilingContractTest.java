package dev.stockreading.news.filing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class FilingContractTest {

    private static final String SCHEMA = "filing.collected.v1.schema.json";
    private static final String SAMPLE = "filing.collected.sample.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private FilingRepository repository;
    private FilingListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(FilingRepository.class);
        listener = new FilingListener(repository, mapper);
    }

    @Test
    void v1SampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, ContractSchemas.sample(SAMPLE))).isEmpty();
    }

    @Test
    void v1SampleIsConvertedToAFiling() throws IOException {
        listener.onFiling(ContractSchemas.sample(SAMPLE));

        ArgumentCaptor<Filing> captor = ArgumentCaptor.forClass(Filing.class);
        verify(repository).save(captor.capture());
        Filing filing = captor.getValue();
        assertThat(filing.eventId()).isEqualTo(UUID.fromString("3f6b2c1e-8d4a-4b7f-9e2c-1a5d7f9b3c20"));
        assertThat(filing.accessionNumber()).isEqualTo("0000000001-26-000001");
        assertThat(filing.acceptedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T20:30:28Z"));
        assertThat(filing.items()).containsExactly("2.02", "9.01");
        assertThat(filing.tickers()).containsExactly("TESTA");
    }

    @Test
    void schemaRequiresEveryFieldTheConsumerReads() throws IOException {
        Map<?, ?> schema = mapper.readValue(ContractSchemas.schema(SCHEMA), Map.class);
        Map<?, ?> payload = (Map<?, ?>) ((Map<?, ?>) schema.get("properties")).get("payload");

        // 소비자가 값을 꺼내 쓰는 필드가 스키마에서 선택이면, 없는 이벤트가 검증을 통과해 소비자만 깨진다.
        assertThat(requiredOf(schema)).contains("eventId", "correlationId", "payload");
        assertThat(requiredOf(payload)).contains(
                "tickers", "cik", "accessionNumber", "form", "items", "acceptedAt", "filingDate", "url");
    }

    @SuppressWarnings("unchecked")
    private static List<String> requiredOf(Map<?, ?> object) {
        return (List<String>) object.get("required");
    }
}
