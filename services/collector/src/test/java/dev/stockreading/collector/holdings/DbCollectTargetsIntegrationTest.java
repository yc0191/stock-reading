package dev.stockreading.collector.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.stockreading.collector.CollectorDb;

/**
 * 수집 대상(personal.collect_target)과 ETF 구성(market.etf_stock_map)을 수집기 계정으로 읽는 것을 실제 Postgres로 확인한다.
 * 가짜 멤버·종목(COLL 접두어)을 쓰기 계정으로 넣고, 수집기 계정이 읽은 결과를 본다. 수집기 계정이 쓰지도 못하고
 * 개인정보를 읽지도 못하는지는 notifier의 PersonalSchemaIntegrationTest가 본다.
 */
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class DbCollectTargetsIntegrationTest {

    private final JdbcClient writer = CollectorDb.writer();
    private final JdbcClient market = CollectorDb.market();

    @BeforeEach
    void seed() {
        CollectorDb.reset();
        market.sql("INSERT INTO etf (market, symbol, name) VALUES ('KR', 'COLL01', 'COLL ETF ONE'), "
                + "('KR', 'COLL02', 'COLL ETF TWO'), ('KR', 'COLL04', 'COLL ETF FOUR')").update();
        market.sql("INSERT INTO stock (market, symbol) VALUES ('KR', 'COLL03'), ('US', 'COLLA'), ('US', 'COLLB'), "
                + "('US', 'COLLC'), ('US', 'COLLD')").update();
        map("COLL01", "COLLA", 1);
        map("COLL01", "COLLB", 2);
        map("COLL01", "COLL03", 3);
        map("COLL02", "COLLC", 1);
        // 같은 주식을 두 ETF가 담아도 한 번만 나오고, 담은 ETF는 둘 다 알려야 한다.
        map("COLL04", "COLLA", 1);
        long active = member("COLL ACTIVE", true);
        long inactive = member("COLL INACTIVE", false);
        portfolioEtf(active, "COLL01", "HELD");
        portfolioEtf(active, "COLL04", "WATCH");
        portfolioStock(active, "COLL03", "WATCH");
        portfolioStock(active, "COLLD", "HELD");
        // 사용 중이 아닌 멤버의 포트폴리오는 수집 대상이 아니다.
        portfolioEtf(inactive, "COLL02", "HELD");
    }

    private void map(String etf, String stock, int rank) {
        market.sql("INSERT INTO etf_stock_map (etf_id, stock_id, rank) VALUES ((SELECT id FROM etf WHERE symbol = :e), "
                + "(SELECT id FROM stock WHERE symbol = :s), :r)").param("e", etf).param("s", stock).param("r", rank).update();
    }

    private long member(String name, boolean enabled) {
        return writer.sql("INSERT INTO personal.member (name, enabled) VALUES (:n, :e) RETURNING id")
                .param("n", name).param("e", enabled).query(Long.class).single();
    }

    private void portfolioEtf(long member, String etf, String relation) {
        writer.sql("INSERT INTO personal.member_portfolio (member_id, etf_id, relation) VALUES (:m, "
                + "(SELECT id FROM market.etf WHERE symbol = :s), :r)").param("m", member).param("s", etf).param("r", relation).update();
    }

    private void portfolioStock(long member, String stock, String relation) {
        writer.sql("INSERT INTO personal.member_portfolio (member_id, stock_id, relation) VALUES (:m, "
                + "(SELECT id FROM market.stock WHERE symbol = :s), :r)").param("m", member).param("s", stock).param("r", relation).update();
    }

    private static List<String> mine(List<String> symbols) {
        return symbols.stream().filter(s -> s.startsWith("COLL")).toList();
    }

    @Test
    void quoteSymbolsAreTheDomesticEtfsAndStocksInActiveMembersPortfolios() {
        DbCollectTargets targets = new DbCollectTargets(CollectorDb.collector());

        assertThat(mine(targets.symbols())).containsExactly("COLL01", "COLL03", "COLL04");
    }

    @Test
    void usTickersAreUsStocksInPortfoliosPlusUsMembersOfPortfolioEtfsWithoutDuplicates() {
        DbCollectTargets targets = new DbCollectTargets(CollectorDb.collector());

        // COLLC는 사용 중이 아닌 멤버의 ETF에만 있어 빠진다. COLLA는 두 ETF가 담아도 한 번이다.
        assertThat(mine(targets.allTickers())).containsExactly("COLLA", "COLLB", "COLLD");
    }

    @Test
    void etfsHoldingATickerAreOnlyThePortfolioEtfsThatHoldItInCodeOrder() {
        DbCollectTargets targets = new DbCollectTargets(CollectorDb.collector());

        assertThat(targets.etfsHolding("COLLA")).containsExactly("COLL01", "COLL04");
        assertThat(targets.etfsHolding("COLLB")).containsExactly("COLL01");
        assertThat(targets.etfsHolding("COLLC")).isEmpty();
        assertThat(targets.etfsHolding("COLLD")).isEmpty();
    }

    @Test
    void theLoadedListsStayTheSameUntilARestart() {
        DbCollectTargets targets = new DbCollectTargets(CollectorDb.collector());
        long member = member("COLL LATER", true);
        portfolioStock(member, "COLLC", "HELD");

        assertThat(mine(targets.allTickers())).doesNotContain("COLLC");
        assertThat(mine(new DbCollectTargets(CollectorDb.collector()).allTickers())).contains("COLLC");
    }

    @Test
    void startingFailsWhenTheTargetsCannotBeRead() {
        assertThatThrownBy(() -> new DbCollectTargets(CollectorDb.market())).isInstanceOf(RuntimeException.class);
    }
}
