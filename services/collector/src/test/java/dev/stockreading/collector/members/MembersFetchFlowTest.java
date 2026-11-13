package dev.stockreading.collector.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import com.sun.net.httpserver.HttpServer;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 가상: 가짜 운용사 서버 → 실제 어댑터(FundSiteMemberSource) → 발행기 → 가짜 Kafka 까지 끝까지 돌려 발행한 메시지가
 * 계약(etf.members.fetched)을 통과하는지 본다. 실제 운용사 사이트는 부르지 않는다.
 */
class MembersFetchFlowTest {

    private static final String SCHEMA = "etf.members.fetched.v1.schema.json";
    // 2026-11-09(월) 08:00 한국 시간
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-08T23:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final String OK_BODY = """
            {"pdf": {"list": [
              {"secNm": "TEST A", "itmNo": "TESTA US Equity", "ratio": " 9.50 "},
              {"secNm": "TEST KR", "itmNo": "0TEST2", "ratio": "4.25"},
              {"secNm": "현금", "itmNo": "CASH", "ratio": null}]}}
            """;

    private HttpServer server;
    private final Map<String, Integer> statusByFund = new ConcurrentHashMap<>();
    private final Map<String, String> bodyByFund = new ConcurrentHashMap<>();
    private final List<String> requested = new CopyOnWriteArrayList<>();
    private final ObjectMapper mapper = JsonMapper.builder().build();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final MemberTargetsPort targets = mock(MemberTargetsPort.class);

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/pdf/", exchange -> {
            String fund = exchange.getRequestURI().getPath().substring("/pdf/".length());
            requested.add(fund);
            byte[] bytes = bodyByFund.getOrDefault(fund, "").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusByFund.getOrDefault(fund, 200), bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                exchange.getResponseBody().write(bytes);
            }
            exchange.close();
        });
        server.start();
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private MembersPublisher publisher() {
        String template = "http://127.0.0.1:" + server.getAddress().getPort() + "/pdf/{fundId}?d={date}";
        MemberSourceProperties properties = new MemberSourceProperties(template, "test-agent/1.0", 10, Duration.ZERO,
                Duration.ofSeconds(5), DayOfWeek.MONDAY);
        return new MembersPublisher(properties, targets, new FundSiteMemberSource(properties, mapper), kafka, mapper,
                CLOCK);
    }

    private List<JsonNode> published() {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, org.mockito.Mockito.atLeast(0)).send(eq(Topics.ETF_MEMBERS_FETCHED), anyString(), value.capture());
        return value.getAllValues().stream().map(mapper::readTree).toList();
    }

    @Test
    void successFailureAndOversizedResponsesFlowAsTheContractSays() throws IOException {
        when(targets.targets(LocalDate.parse("2026-11-09"))).thenReturn(List.of(
                new MemberTargetsPort.Target("TEST01", "F1", null, false),
                new MemberTargetsPort.Target("TEST02", "F2", null, false),
                new MemberTargetsPort.Target("TEST03", "F3", null, false),
                new MemberTargetsPort.Target("TEST04", "F4", null, false)));
        bodyByFund.put("F1", OK_BODY);
        statusByFund.put("F2", 503);
        bodyByFund.put("F2", "unavailable");
        bodyByFund.put("F3", "{\"pdf\": {\"list\": {}}}");
        // Kafka 메시지 상한 때문에 원문 상한을 넘는 응답은 싣지 않는다(이전 구성 유지).
        bodyByFund.put("F4", "x".repeat(FundSiteMemberSource.MAX_BODY_BYTES + 1));

        publisher().publishFetched("collect-etf-members-2026-11-09T08:00:00+09:00");

        // ETF마다 요청은 한 번이다.
        assertThat(requested).containsExactly("F1", "F2", "F3", "F4");
        List<JsonNode> events = published();
        assertThat(events).hasSize(3);
        for (JsonNode event : events) {
            assertThat(ContractSchemas.validate(SCHEMA, event.toString())).as(event.toString()).isEmpty();
        }
        JsonNode ok = events.get(0).path("payload");
        assertThat(ok.path("etfSymbol").asString()).isEqualTo("TEST01");
        assertThat(ok.path("response").asString()).isEqualTo(OK_BODY);
        assertThat(ok.path("members").get(0).path("symbol").asString()).isEqualTo("TESTA");
        assertThat(ok.path("members").get(0).path("weight").asString()).isEqualTo("9.50");
        assertThat(ok.path("members").get(1).path("market").asString()).isEqualTo("KR");
        JsonNode down = events.get(1).path("payload");
        assertThat(down.path("httpStatus").asInt()).isEqualTo(503);
        assertThat(down.path("response").asString()).isEqualTo("unavailable");
        assertThat(down.path("members").isNull()).isTrue();
        JsonNode shape = events.get(2).path("payload");
        assertThat(shape.path("failure").asString()).isEqualTo("unexpected shape");
        assertThat(events).noneMatch(e -> e.path("payload").path("etfSymbol").asString().equals("TEST04"));
    }

    @Test
    void aRawUnderItsLimitWhoseMessageWouldPassTheKafkaLimitIsNotSent() {
        // 따옴표가 많은 원문은 상한(900KB) 안이어도 이스케이프로 메시지가 1MB를 넘을 수 있다. 그런 메시지는 보내지 않는다.
        String row = "{\"itmNo\": \"AAAA\", \"secNm\": \"N\", \"ratio\": \"1\"},";
        String body = "{\"pdf\": {\"list\": [" + row.repeat((FundSiteMemberSource.MAX_BODY_BYTES - 40) / row.length())
                + "{\"itmNo\": \"BBBB\", \"ratio\": \"2\"}]}}";
        assertThat(body.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(FundSiteMemberSource.MAX_BODY_BYTES);
        when(targets.targets(LocalDate.parse("2026-11-09"))).thenReturn(List.of(
                new MemberTargetsPort.Target("TEST01", "F1", null, false),
                new MemberTargetsPort.Target("TEST02", "F2", null, false)));
        bodyByFund.put("F1", body);
        bodyByFund.put("F2", OK_BODY);

        publisher().publishFetched("collect-etf-members-2026-11-09T08:00:00+09:00");

        List<JsonNode> events = published();
        assertThat(events).singleElement()
                .satisfies(e -> assertThat(e.path("payload").path("etfSymbol").asString()).isEqualTo("TEST02"));
    }
}
