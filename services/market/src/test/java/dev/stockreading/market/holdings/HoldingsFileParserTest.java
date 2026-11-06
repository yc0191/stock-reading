package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class HoldingsFileParserTest {

    private static final String FILE = """
            [
              {"kind": "ETF", "market": "KR", "symbol": "TEST01", "name": "TEST ETF ONE", "tracks": "TEST INDEX",
               "sector": "TEST SECTOR", "relation": "HELD",
               "members": [
                 {"market": "US", "symbol": "FAKEA", "name": "FAKE CORP A", "weight": 12.5, "rank": 1},
                 {"market": "US", "symbol": "FAKE.B", "weight": 8.25},
                 {"market": "KR", "symbol": "TEST09"}
               ]},
              {"kind": "ETF", "market": "KR", "symbol": "TEST02", "relation": "WATCH",
               "members": [{"market": "US", "symbol": "FAKEA", "weight": 30}]},
              {"kind": "STOCK", "market": "KR", "symbol": "TEST09", "name": "TEST CORP KR", "relation": "HELD"}
            ]
            """;

    private static HoldingsPlan parse(String json) {
        return HoldingsFileParser.parse(json, JsonMapper.builder().build());
    }

    @Test
    void blankFileMeansNoHoldings() {
        assertThat(parse("").instruments()).isEmpty();
        assertThat(parse(null).memberships()).isEmpty();
        assertThat(parse("  \n").instruments()).isEmpty();
    }

    @Test
    void membersOnlyInsideAnEtfBecomeNoneRelationStocksAndSharedStocksAreOneRow() {
        HoldingsPlan plan = parse(FILE);

        assertThat(plan.instruments()).extracting(HoldingsPlan.Instrument::symbol)
                .containsExactly("TEST01", "FAKEA", "FAKE-B", "TEST09", "TEST02");
        HoldingsPlan.Instrument fakeA = plan.instruments().get(1);
        assertThat(fakeA.kind()).isEqualTo("STOCK");
        assertThat(fakeA.relation()).isEqualTo("NONE");
        // 구성 종목에 적힌 이름이 이름 없는 종목에 채워진다.
        assertThat(fakeA.name()).isEqualTo("FAKE CORP A");
        // 최상위에 따로 적힌 종목은 그 값이 우선이고, 같은 종목이 두 줄이 되지 않는다.
        assertThat(plan.instruments().get(3).relation()).isEqualTo("HELD");
        assertThat(plan.instruments().get(3).name()).isEqualTo("TEST CORP KR");
        assertThat(plan.instruments().get(0).tracks()).isEqualTo("TEST INDEX");
    }

    @Test
    void sharedStockGetsOneMembershipPerEtfAndMissingRankFollowsTheWrittenOrder() {
        HoldingsPlan plan = parse(FILE);

        assertThat(plan.memberships()).extracting(m -> m.etfSymbol() + ">" + m.memberSymbol() + "#" + m.rank())
                .containsExactly("TEST01>FAKEA#1", "TEST01>FAKE-B#2", "TEST01>TEST09#3", "TEST02>FAKEA#1");
        assertThat(plan.memberships().get(0).weight()).isEqualByComparingTo(new BigDecimal("12.5"));
        assertThat(plan.memberships().get(2).weight()).isNull();
    }

    @Test
    void errorsNameThePositionAndKindButNeverTheValue() {
        assertError("{}", "맨 위는 종목 배열");
        assertError("[1]", "1번째 항목");
        assertError("not json", "JSON 형식이 아닙니다");
        assertError("[{\"kind\":\"BOND\",\"market\":\"KR\",\"symbol\":\"TEST01\"}]", "kind");
        assertError("[{\"kind\":\"ETF\",\"market\":\"kr\",\"symbol\":\"TEST01\"}]", "market 형식");
        assertError("[{\"kind\":\"ETF\",\"market\":\"KR\",\"symbol\":\"test01\"}]", "symbol 형식");
        assertError("[{\"kind\":\"ETF\",\"market\":\"KR\",\"symbol\":\"TEST1\"}]", "국내 ETF 종목코드");
        assertError("[{\"kind\":\"STOCK\",\"market\":\"US\",\"symbol\":\"FAKEA\",\"tracks\":\"X\"}]", "ETF에만");
        assertError("[{\"kind\":\"STOCK\",\"market\":\"US\",\"symbol\":\"FAKEA\",\"members\":[]}]", "ETF에만");
        assertError("[{\"kind\":\"STOCK\",\"market\":\"US\",\"symbol\":\"FAKEA\",\"relation\":\"OWN\"}]", "relation");
        assertError("[{\"kind\":\"STOCK\",\"market\":\"US\",\"symbol\":\"FAKEA\"},"
                + "{\"kind\":\"STOCK\",\"market\":\"US\",\"symbol\":\"FAKEA\"}]", "2번째 항목에 같은 종목");
    }

    @Test
    void memberErrorsNameTheEtfPositionAndTheMemberPosition() {
        assertMemberError("{\"market\":\"US\",\"symbol\":\"FAKEA\",\"weight\":101}", "1번째 구성 종목에 weight");
        assertMemberError("{\"market\":\"US\",\"symbol\":\"FAKEA\",\"weight\":\"5\"}", "weight 는 숫자");
        assertMemberError("{\"market\":\"US\",\"symbol\":\"FAKEA\",\"weight\":1.23456}", "소수 4자리");
        assertMemberError("{\"market\":\"US\",\"symbol\":\"FAKEA\",\"rank\":0}", "rank");
        assertMemberError("{\"market\":\"US\",\"symbol\":\"TEST01\"}".replace("US", "KR"), "자기 자신");
        assertMemberError("{\"market\":\"US\",\"symbol\":\"FAKEA\"},{\"market\":\"US\",\"symbol\":\"FAKEA\"}",
                "2번째 구성 종목에 같은 구성 종목");
        assertMemberError("{\"market\":\"US\",\"symbol\":\"FAKEA\",\"rank\":1},{\"market\":\"US\",\"symbol\":\"FAKEB\","
                + "\"rank\":1}", "비중 순위");
    }

    private static void assertError(String json, String contains) {
        assertThatThrownBy(() -> parse(json)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(contains)
                .hasMessageNotContaining("FAKEA").hasMessageNotContaining("TEST01").hasMessageNotContaining("BOND");
    }

    private static void assertMemberError(String members, String contains) {
        String json = "[{\"kind\":\"ETF\",\"market\":\"KR\",\"symbol\":\"TEST01\",\"members\":[" + members + "]}]";
        assertThatThrownBy(() -> parse(json)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1번째 항목").hasMessageContaining(contains)
                .hasMessageNotContaining("FAKEA").hasMessageNotContaining("TEST01");
    }
}
