package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import tools.jackson.databind.json.JsonMapper;

class EdgarClientTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T00:00:00Z"), ZoneId.of("Asia/Seoul"));

    private FakeEdgarServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    private EdgarClient client(String userAgent) throws Exception {
        server = new FakeEdgarServer();
        EdgarProperties properties = server.properties(userAgent);
        return new EdgarClient(properties, JsonMapper.builder().build(), CLOCK,
                EdgarCallSpacing.perSecond(properties.maxCallsPerSecond()));
    }

    @Test
    void tickerIsMappedToTenDigitCikAndClassSeparatorIsNormalized() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        assertThat(client.cikOf("TESTA")).isEqualTo("0000000001");
        assertThat(client.cikOf("TESTB.C")).isEqualTo("0000000002");
        assertThat(client.cikOf("testb-c")).isEqualTo("0000000002");
    }

    @Test
    void tickerMapIsDownloadedOnceAndReusedWithinADay() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        client.cikOf("TESTA");
        client.cikOf("TESTB");

        assertThat(server.tickerMapCalls.get()).isEqualTo(1);
    }

    @Test
    void unknownTickerIsClassified() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        assertThatThrownBy(() -> client.cikOf("TESTZ"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.UNKNOWN_TICKER));
    }

    @Test
    void submissionsAreParsedIntoFilingMetadata() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        CompanySubmissions submissions = client.submissions("0000000001");

        assertThat(server.paths).containsExactly("/submissions/CIK0000000001.json");
        assertThat(submissions.cik()).isEqualTo("0000000001");
        assertThat(submissions.exchanges()).containsExactly("Nasdaq");
        // 접수 시각이 깨진 행은 빠진다.
        assertThat(submissions.filings()).extracting(Filing::accessionNumber).containsExactly(
                "0000000001-26-000003", "0000000001-26-000002", "0000000001-26-000001", "0000000001-26-000004");

        Filing eightK = submissions.filings().get(0);
        assertThat(eightK.form()).isEqualTo("8-K");
        assertThat(eightK.acceptedAt()).isEqualTo("2026-10-20T20:30:28Z");
        assertThat(eightK.acceptedAt().getOffset()).isEqualTo(ZoneOffset.UTC);
        assertThat(eightK.acceptedAt().getNano()).isZero();
        assertThat(eightK.filingDate()).isEqualTo(LocalDate.of(2026, 10, 20));
        assertThat(eightK.items()).containsExactly("2.02", "9.01");
        assertThat(eightK.url())
                .isEqualTo("https://www.sec.gov/Archives/edgar/data/1/000000000126000003/test-8k.htm");
    }

    @Test
    void gzipIsRequestedAndCompressedResponsesAreDecompressed() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.gzip = true;

        assertThat(client.cikOf("TESTB.C")).isEqualTo("0000000002");
        CompanySubmissions submissions = client.submissions("0000000001");

        assertThat(server.acceptEncodings).containsExactly("gzip", "gzip");
        assertThat(submissions.exchanges()).containsExactly("Nasdaq");
        assertThat(submissions.filings()).extracting(Filing::accessionNumber).containsExactly(
                "0000000001-26-000003", "0000000001-26-000002", "0000000001-26-000001", "0000000001-26-000004");
    }

    @Test
    void uncompressedResponseIsStillReadWhenTheServerIgnoresGzip() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        CompanySubmissions submissions = client.submissions("0000000001");

        assertThat(server.acceptEncodings).containsExactly("gzip");
        assertThat(submissions.filings()).hasSize(4);
    }

    @Test
    void brokenGzipBodyIsClassifiedAsInvalidResponse() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.rawGzipBody = "not gzip".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> client.submissions("0000000001"))
                .isInstanceOfSatisfying(EdgarApiException.class, e -> {
                    assertThat(e.failure()).isEqualTo(EdgarFailure.INVALID_RESPONSE);
                    assertThat(e.httpStatus()).isEqualTo(200);
                });
    }

    @Test
    void sixKHasNoItemsAndMissingPrimaryDocumentFallsBackToTheIndexPage() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        List<Filing> filings = client.submissions("0000000001").filings();

        assertThat(filings.get(1).form()).isEqualTo("6-K");
        assertThat(filings.get(1).items()).isEmpty();
        assertThat(filings.get(3).url()).isEqualTo(
                "https://www.sec.gov/Archives/edgar/data/1/000000000126000004/0000000001-26-000004-index.htm");
    }

    @Test
    void arraysOfDifferentLengthAreRejectedInsteadOfMixingFilings() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.submissionsBody = FakeEdgarServer.SUBMISSIONS.replace("\"form\":[\"8-K\",", "\"form\":[");

        assertThatThrownBy(() -> client.submissions("0000000001"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.INVALID_RESPONSE));
    }

    @Test
    void nonJsonBodyIsClassifiedAsInvalidResponse() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.submissionsBody = "<html>not json</html>";

        assertThatThrownBy(() -> client.submissions("0000000001"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.INVALID_RESPONSE));
    }

    @ParameterizedTest
    @CsvSource({"403,FORBIDDEN", "404,NOT_FOUND", "429,RATE_LIMITED", "400,CLIENT_ERROR", "500,SERVER_ERROR",
            "503,SERVER_ERROR"})
    void httpErrorsAreClassified(int status, EdgarFailure expected) throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.submissionsStatus = status;

        assertThatThrownBy(() -> client.submissions("0000000001"))
                .isInstanceOfSatisfying(EdgarApiException.class, e -> {
                    assertThat(e.failure()).isEqualTo(expected);
                    assertThat(e.httpStatus()).isEqualTo(status);
                });
    }

    @Test
    void connectionFailureIsClassifiedAsNetwork() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.close();

        assertThatThrownBy(() -> client.submissions("0000000001"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.NETWORK));
        server = null;
    }

    @Test
    void missingUserAgentMakesNoCall() throws Exception {
        EdgarClient client = client(" ");

        assertThatThrownBy(() -> client.cikOf("TESTA"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.NOT_CONFIGURED));
        assertThat(server.tickerMapCalls.get()).isZero();
    }

    @Test
    void userAgentIsSentButNeverAppearsInLogsExceptionsOrToString() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        root.addAppender(appender);
        try {
            EdgarClient client = client(FakeEdgarServer.USER_AGENT);
            client.cikOf("TESTA");
            server.submissionsStatus = 403;
            server.submissionsBody = "{\"echo\":\"" + FakeEdgarServer.USER_AGENT + "\"}";

            String message = "";
            try {
                client.submissions("0000000001");
            } catch (EdgarApiException e) {
                message = e.getMessage();
            }
            server.submissionsStatus = 200;
            server.submissionsBody = FakeEdgarServer.SUBMISSIONS.replace("\"yesterday\"", "\"bad\"");
            client.submissions("0000000001");

            assertThat(server.userAgents).containsOnly(FakeEdgarServer.USER_AGENT);
            assertThat(message).contains("FORBIDDEN").doesNotContain("test-contact", "TestCompany");
            assertThat(server.properties(FakeEdgarServer.USER_AGENT).toString())
                    .doesNotContain("test-contact", "TestCompany");
            assertThat(appender.list).isNotEmpty();
            for (ILoggingEvent event : appender.list) {
                assertThat(event.getFormattedMessage()).doesNotContain("test-contact", "TestCompany");
            }
        } finally {
            root.detachAppender(appender);
        }
    }

    @Test
    void exchangeComesFromTheTickerMapAndMissingExchangeIsEmpty() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);

        assertThat(client.exchangesOf("TESTA")).containsExactly("Nasdaq");
        assertThat(client.exchangesOf("TESTB.C")).containsExactly("NYSE");
        assertThat(client.exchangesOf("TESTD")).isEmpty();
        // 거래소를 알려고 회사 이력(submissions)을 따로 받지 않는다.
        assertThat(server.submissionsCalls.get()).isZero();
        assertThat(server.tickerMapCalls.get()).isEqualTo(1);
    }

    @Test
    void failedTickerMapIsNotRetriedForEveryTickerRightAway() throws Exception {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-21T00:00:00Z"));
        EdgarClient client = client(FakeEdgarServer.USER_AGENT, now);
        server.tickerMapStatus = 503;

        for (String ticker : List.of("TESTA", "TESTB", "TESTD")) {
            assertThatThrownBy(() -> client.exchangesOf(ticker))
                    .isInstanceOfSatisfying(EdgarApiException.class,
                            e -> assertThat(e.failure()).isEqualTo(EdgarFailure.SERVER_ERROR));
        }
        assertThat(server.tickerMapCalls.get()).isEqualTo(1);

        // 기다린 뒤에는 다시 받는다.
        server.tickerMapStatus = 200;
        now.set(now.get().plus(EdgarClient.TICKER_MAP_RETRY_AFTER));
        assertThat(client.cikOf("TESTA")).isEqualTo("0000000001");
        assertThat(server.tickerMapCalls.get()).isEqualTo(2);
    }

    @Test
    void previousTickerMapIsKeptWhenADailyRefreshFails() throws Exception {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-21T00:00:00Z"));
        EdgarClient client = client(FakeEdgarServer.USER_AGENT, now);
        assertThat(client.exchangesOf("TESTA")).containsExactly("Nasdaq");

        server.tickerMapStatus = 403;
        now.set(now.get().plus(EdgarClient.TICKER_MAP_TTL).plusSeconds(1));

        assertThat(client.exchangesOf("TESTA")).containsExactly("Nasdaq");
        assertThat(client.cikOf("TESTB")).isEqualTo("0000000002");
        assertThat(server.tickerMapCalls.get()).isEqualTo(2);
    }

    @Test
    void tickerMapWithoutTheExpectedColumnsIsInvalid() throws Exception {
        EdgarClient client = client(FakeEdgarServer.USER_AGENT);
        server.tickerMapBody = "{\"0\":{\"cik_str\":1,\"ticker\":\"TESTA\",\"title\":\"Test A Corp\"}}";

        assertThatThrownBy(() -> client.cikOf("TESTA"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.INVALID_RESPONSE));
    }

    private EdgarClient client(String userAgent, AtomicReference<Instant> now) throws Exception {
        server = new FakeEdgarServer();
        EdgarProperties properties = server.properties(userAgent);
        Clock clock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return now.get();
            }
        };
        return new EdgarClient(properties, JsonMapper.builder().build(), clock,
                EdgarCallSpacing.perSecond(properties.maxCallsPerSecond()));
    }
}
