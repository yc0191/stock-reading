package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.json.JsonMapper;

class NewsResearchListenerTest {

    private static final String SAMPLE = "news.research.requested.sample.json";
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final String REQUESTED_AT = "2026-10-20T16:16:10+09:00";

    private NewsPublisher publisher;
    private ListAppender<ILoggingEvent> logs;

    @BeforeEach
    void setUp() {
        publisher = mock(NewsPublisher.class);
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(NewsResearchListener.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(NewsResearchListener.class)).detachAppender(logs);
    }

    private NewsResearchListener listener(int maxQueries) {
        return new NewsResearchListener(publisher, new NewsResearchProperties(maxQueries), JsonMapper.builder().build());
    }

    @Test
    void runsTheResearchWithTheRequestedQueriesSymbolJobIdAndRequestTime() throws IOException {
        listener(3).onRequest(ContractSchemas.sample(SAMPLE));

        verify(publisher).publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01",
                List.of("테스트 재검색어 하나", "테스트 재검색어 둘"));
    }

    @Test
    void sameRequestEventDeliveredTwiceSearchesOnlyOnce() throws IOException {
        NewsResearchListener listener = listener(3);
        String message = ContractSchemas.sample(SAMPLE);

        listener.onRequest(message);
        listener.onRequest(message);

        verify(publisher, times(1)).publishResearch(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void resentRequestWithANewEventIdIsSearchedAgain() throws IOException {
        NewsResearchListener listener = listener(3);
        String first = ContractSchemas.sample(SAMPLE);
        String resent = first.replace("3e7b1c90-5a24-4d68-b0f3-8c1d2e4a6b57", "4f8c2da1-6b35-4e79-81a4-9d2e3f5b7c68")
                .replace("\"seq\": 1", "\"seq\": 2");

        listener.onRequest(first);
        listener.onRequest(resent);

        verify(publisher, times(2)).publishResearch(anyString(), anyString(), eq(JOB_ID), anyString(), any());
    }

    @Test
    void onlyQueriesUpToTheCollectorCapAreSearched() throws IOException {
        listener(2).onRequest(withQueries("[\"테스트 가\", \"테스트 나\", \"테스트 다\"]"));

        verify(publisher).publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", List.of("테스트 가", "테스트 나"));
    }

    @Test
    void blankAndRepeatedQueriesAreDroppedBeforeTheCap() throws IOException {
        listener(2).onRequest(withQueries("[\"  \", \" 테스트 가 \", \"테스트 가\", \"테스트 나\"]"));

        verify(publisher).publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", List.of("테스트 가", "테스트 나"));
    }

    @Test
    void marketWideTagIsPassedThroughForDailyEditions() throws IOException {
        listener(3).onRequest(ContractSchemas.sample(SAMPLE)
                .replace("\"symbol\": \"TEST01\"", "\"symbol\": \"*\"")
                .replace("\"jobKind\": \"move\"", "\"jobKind\": \"morning\""));

        verify(publisher).publishResearch(eq(CORRELATION_ID), eq(REQUESTED_AT), eq(JOB_ID), eq("*"), any());
    }

    @Test
    void requestWhoseCompletionCannotBeFilledIsIgnored() throws IOException {
        NewsResearchListener listener = listener(3);
        String sample = ContractSchemas.sample(SAMPLE);

        listener.onRequest(sample.replace("\"symbol\": \"TEST01\"", "\"symbol\": \"TEST011\""));
        listener.onRequest(sample.replace("\"jobId\": \"" + JOB_ID + "\"", "\"jobId\": \"not-a-uuid\""));
        listener.onRequest(sample.replace("\"occurredAt\": \"" + REQUESTED_AT + "\"", "\"occurredAt\": \"yesterday\""));

        verify(publisher, never()).publishResearch(anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        listener(3).onRequest(ContractSchemas.sample(SAMPLE)
                .replace("\"payload\":", "\"futureEnvelopeField\": 1, \"payload\":")
                .replace("\"jobId\":", "\"futurePayloadField\": \"x\", \"jobId\":"));

        verify(publisher).publishResearch(anyString(), anyString(), eq(JOB_ID), anyString(), any());
    }

    @Test
    void logsCarryNeitherTheQueriesNorTheSymbol() throws IOException {
        NewsResearchListener listener = listener(1);
        String message = ContractSchemas.sample(SAMPLE);

        listener.onRequest(message);
        listener.onRequest(message);

        assertThat(logs.list).isNotEmpty();
        for (ILoggingEvent event : logs.list) {
            assertThat(event.getFormattedMessage()).doesNotContain("재검색어", "TEST01");
        }
    }

    private static String withQueries(String queriesJson) throws IOException {
        return ContractSchemas.sample(SAMPLE)
                .replace("[\"테스트 재검색어 하나\", \"테스트 재검색어 둘\"]", queriesJson);
    }
}
