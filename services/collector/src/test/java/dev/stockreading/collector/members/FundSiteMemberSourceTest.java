package dev.stockreading.collector.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

/** 출처를 흉내 내는 로컬 가짜 서버로 어댑터를 확인한다. 실제 서버는 호출하지 않는다. */
class FundSiteMemberSourceTest {

    private static final String BODY = """
            {"pdf": {"list": [
              {"secNm": "예금", "itmNo": "KRD010010001", "ratio": null},
              {"secNm": " FAKE CORP B ", "itmNo": "FAKEB", "ratio": "8.25"},
              {"secNm": "FAKE CORP A", "itmNo": "FAKEA", "ratio": "12.5"},
              {"secNm": "TEST KR STOCK", "itmNo": "9999T0", "ratio": "10.0"},
              {"secNm": "TEST BOND", "itmNo": "KR1234567890", "ratio": "20.0"},
              {"secNm": "TEST ZERO", "itmNo": "ZERO1", "ratio": "0.00"},
              {"secNm": "FAKE CORP C", "itmNo": "FAKE.C", "ratio": "1.0"}
            ]}}
            """;

    private HttpServer server;
    private final List<String> userAgents = new CopyOnWriteArrayList<>();
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private volatile int status = 200;
    private volatile String body = BODY;
    private final List<String> rawBodies = new CopyOnWriteArrayList<>();
    private final List<Integer> rawStatuses = new CopyOnWriteArrayList<>();
    private final MemberSourcePort.RawRecorder raw = (httpStatus, text) -> {
        rawStatuses.add(httpStatus);
        rawBodies.add(text);
    };

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            userAgents.add(exchange.getRequestHeaders().getFirst("User-Agent"));
            requests.add(exchange.getRequestURI().toString());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private FundSiteMemberSource source(int topCount) {
        String template = "http://127.0.0.1:" + server.getAddress().getPort() + "/pdf/{fundId}.do?base={date}";
        return new FundSiteMemberSource(new MemberSourceProperties(template, "test-agent/1.0", topCount,
                Duration.ZERO, Duration.ofSeconds(5), DayOfWeek.MONDAY), JsonMapper.builder().build());
    }

    @Test
    void picksStockRowsByWeightDescendingWithRanksAndSkipsCashBondsAndZeroWeights() {
        List<Member> members = source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw);

        assertThat(members).extracting(Member::symbol, Member::market, Member::rank).containsExactly(
                org.assertj.core.groups.Tuple.tuple("FAKEA", "US", 1),
                org.assertj.core.groups.Tuple.tuple("9999T0", "KR", 2),
                org.assertj.core.groups.Tuple.tuple("FAKEB", "US", 3),
                org.assertj.core.groups.Tuple.tuple("FAKE.C", "US", 4));
        // 이름은 앞뒤 공백만 걷고, 비중은 원문 문자열을 그대로 둔다.
        assertThat(members.get(2).name()).isEqualTo("FAKE CORP B");
        assertThat(members.get(0).weight()).isEqualTo("12.5");
    }

    @Test
    void keepsOnlyTheTopCountAndBreaksTiesBySymbol() {
        body = """
                {"pdf": {"list": [
                  {"itmNo": "BBB", "ratio": "5"}, {"itmNo": "AAA", "ratio": "5"}, {"itmNo": "CCC", "ratio": "6"}]}}
                """;

        List<Member> members = source(2).fetchTop("F1", LocalDate.parse("2026-10-16"), raw);

        assertThat(members).extracting(Member::symbol).containsExactly("CCC", "AAA");
        assertThat(members.get(0).name()).isNull();
    }

    // 실제 응답처럼 해외 종목코드에 거래소 꼬리가 붙어 와도 미국 종목은 티커로 받고, 다른 거래소 종목은 건너뛴다.
    @Test
    void usEquitySuffixedCodesBecomeTickersAndOtherExchangesAreSkipped() {
        body = """
                {"pdf": {"list": [
                  {"secNm": "FAKE CORP A", "itmNo": "FAKEA US Equity", "ratio": "9"},
                  {"secNm": "FAKE CORP B", "itmNo": "FAKE/B US Equity", "ratio": "8"},
                  {"secNm": "FAKE JAPAN", "itmNo": "9999 JP Equity", "ratio": "7"},
                  {"secNm": "현금", "itmNo": "CASH", "ratio": null}]}}
                """;

        List<Member> members = source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw);

        assertThat(members).extracting(Member::market, Member::symbol, Member::rank).containsExactly(
                org.assertj.core.groups.Tuple.tuple("US", "FAKEA", 1), org.assertj.core.groups.Tuple.tuple("US", "FAKE-B", 2));
    }

    @Test
    void aVeryLongNameIsCutToTheInstrumentNameColumnLength() {
        body = "{\"pdf\": {\"list\": [{\"secNm\": \"" + "N".repeat(150) + "\", \"itmNo\": \"AAA\", \"ratio\": \"5\"}]}}";

        assertThat(source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw).get(0).name()).hasSize(100);
    }

    @Test
    void sendsTheConfiguredUserAgentAndTheDateInTheTemplateFormat() {
        source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw);

        assertThat(userAgents).containsExactly("test-agent/1.0");
        assertThat(requests).containsExactly("/pdf/F1.do?base=2026.10.16");
    }

    @Test
    void aNon200ResponseFailsWithoutTheUrlInTheMessage() {
        status = 500;

        assertThatThrownBy(() -> source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw))
                .isInstanceOf(MemberSourcePort.MemberSourceException.class)
                .hasMessage("unexpected status 500");
    }

    @Test
    void aChangedResponseShapeFailsInsteadOfReadingPartOfIt() {
        for (String bad : new String[] {"not json", "{}", "{\"pdf\": {\"list\": {}}}", "{\"pdf\": {\"list\": []}}",
                "{\"pdf\": {\"list\": [{\"itmNo\": \"KRD010010001\", \"ratio\": null}]}}",
                "{\"pdf\": {\"list\": [{\"itmNo\": \"AAA\", \"ratio\": \"abc\"}]}}",
                "{\"pdf\": {\"list\": [{\"itmNo\": \"AAA\", \"ratio\": \"150\"}]}}"}) {
            body = bad;
            assertThatThrownBy(() -> source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw))
                    .as(bad).isInstanceOf(MemberSourcePort.MemberSourceException.class);
        }
    }

    @Test
    void aServerThatIsDownFailsAsASourceException() {
        server.stop(0);

        assertThatThrownBy(() -> source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw))
                .isInstanceOf(MemberSourcePort.MemberSourceException.class);
    }

    @Test
    void theResponseIsHandedOverVerbatimBeforeParsingEvenWhenItFails() {
        body = "  not json\n";
        assertThatThrownBy(() -> source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw))
                .isInstanceOf(MemberSourcePort.MemberSourceException.class);
        status = 503;
        body = "unavailable";
        assertThatThrownBy(() -> source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw))
                .isInstanceOf(MemberSourcePort.MemberSourceException.class);

        assertThat(rawStatuses).containsExactly(200, 503);
        assertThat(rawBodies).containsExactly("  not json\n", "unavailable");
    }

    @Test
    void rowsWithTheSameCodeAreMergedByAddingTheirWeightsAndListedOnce() {
        body = """
                {"pdf": {"list": [
                  {"secNm": "DUP CORP", "itmNo": "DUPA", "ratio": "3.5"}, {"itmNo": "OTHER", "ratio": "5"},
                  {"secNm": "DUP CORP", "itmNo": "DUPA", "ratio": "2.5"}]}}
                """;

        List<Member> members = source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw);

        assertThat(members).extracting(Member::symbol, Member::weight, Member::rank).containsExactly(
                org.assertj.core.groups.Tuple.tuple("DUPA", "6.0", 1),
                org.assertj.core.groups.Tuple.tuple("OTHER", "5", 2));
    }

    @Test
    void onlyKoreanCodesAndUsTickerShapesAreKeptAndOtherOverseasCodesAreSkipped() {
        body = """
                {"pdf": {"list": [
                  {"itmNo": "7203", "ratio": "9"}, {"itmNo": "0700", "ratio": "8"}, {"itmNo": "ABCDEF", "ratio": "7"},
                  {"itmNo": "BRK-B", "ratio": "6"}, {"itmNo": "BF.B", "ratio": "5"}, {"itmNo": "9999T1", "ratio": "4"},
                  {"itmNo": "FAKD", "ratio": "3"}]}}
                """;

        List<Member> members = source(10).fetchTop("F1", LocalDate.parse("2026-10-16"), raw);

        assertThat(members).extracting(Member::market, Member::symbol).containsExactly(
                org.assertj.core.groups.Tuple.tuple("US", "BRK-B"), org.assertj.core.groups.Tuple.tuple("US", "BF.B"),
                org.assertj.core.groups.Tuple.tuple("KR", "9999T1"), org.assertj.core.groups.Tuple.tuple("US", "FAKD"));
    }
}
