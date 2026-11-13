package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.json.JsonMapper;

/** 수집기가 보낸 etf.members.fetched 를 계약대로 읽어 반영에 넘기는지, 계약 샘플로 본다(ADR-003/010). */
class MembersFetchedListenerTest {

    private static final String SCHEMA = "etf.members.fetched.v1.schema.json";
    private static final UUID SAMPLE_EVENT = UUID.fromString("7a2b3c4d-5e6f-4a1b-9c2d-3e4f5a6b7c80");

    private final MemberApplyService service = mock(MemberApplyService.class);
    private final MembersFetchedListener listener = new MembersFetchedListener(service, JsonMapper.builder().build());

    private static String sample() throws IOException {
        return ContractSchemas.sample("etf.members.fetched.sample.json");
    }

    @Test
    void theCommittedSampleMatchesTheSchemaAndIsHandedOverWithItsRawUntouched() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();
        String raw = JsonMapper.builder().build().readTree(sample()).at("/payload/response").asString();

        listener.onFetched(sample());

        ArgumentCaptor<MembersFetchedEnvelope.Payload> payload = ArgumentCaptor.forClass(MembersFetchedEnvelope.Payload.class);
        verify(service).apply(eq(SAMPLE_EVENT), payload.capture());
        assertThat(payload.getValue().etfSymbol()).isEqualTo("TEST01");
        assertThat(payload.getValue().response()).isEqualTo(raw).startsWith(" ").endsWith("\n");
        assertThat(payload.getValue().members()).containsExactly(
                new Member("US", "TESTA", "TEST A", "9.50", 1), new Member("KR", "0TEST2", "TEST TWO", "4.25", 2));
        assertThat(payload.getValue().failure()).isNull();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String json = sample()
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"etfSymbol\"", "\"futurePayloadField\": {\"a\": 1},\n    \"etfSymbol\"")
                .replace("\"rank\": 1", "\"rank\": 1, \"futureMemberField\": true");

        assertThat(json).contains("futureEnvelopeField", "futurePayloadField", "futureMemberField");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        listener.onFetched(json);
        verify(service).apply(eq(SAMPLE_EVENT), any());
    }

    @Test
    void unreadableMessagesAreDroppedWithoutThrowing() throws IOException {
        for (String bad : List.of("{not json", "null", sample().replace(SAMPLE_EVENT.toString(), "not-a-uuid"),
                sample().replace("\"eventId\": \"" + SAMPLE_EVENT + "\",", ""))) {
            assertThatCode(() -> listener.onFetched(bad)).as(bad).doesNotThrowAnyException();
        }
        verifyNoInteractions(service);
    }
}
