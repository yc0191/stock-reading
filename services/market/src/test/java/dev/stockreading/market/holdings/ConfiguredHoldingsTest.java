package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import tools.jackson.databind.json.JsonMapper;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class ConfiguredHoldingsTest {

    private static final String FILE = """
            [
              {"kind": "ETF", "market": "KR", "symbol": "TEST01", "name": "한글 이름",
               "members": [{"market": "US", "symbol": "FAKEA"}, {"market": "US", "symbol": "FAKE.C"}]},
              {"kind": "ETF", "market": "KR", "symbol": "TEST02",
               "members": [{"market": "US", "symbol": "FAKEB"}, {"market": "KR", "symbol": "TEST09"}]}
            ]
            """;

    @TempDir
    Path dir;

    private ConfiguredHoldings configured(String json) throws IOException {
        Path file = dir.resolve("holdings.json");
        Files.writeString(file, json, StandardCharsets.UTF_8);
        return new ConfiguredHoldings(new HoldingsProperties(file.toString()), JsonMapper.builder().build());
    }

    @Test
    void readsTheUtf8FileIntoAPlan() throws IOException {
        HoldingsPlan plan = configured(FILE).plan();

        assertThat(plan.instruments()).hasSize(6);
        assertThat(plan.instruments().get(0).name()).isEqualTo("한글 이름");
    }

    @Test
    void blankPathMeansNoHoldingsAndUnreadableFileFailsWithoutThePath() {
        assertThat(new ConfiguredHoldings(new HoldingsProperties(""), JsonMapper.builder().build())
                .plan().instruments()).isEmpty();
        assertThat(new ConfiguredHoldings(new HoldingsProperties(null), JsonMapper.builder().build())
                .plan().instruments()).isEmpty();

        String missing = dir.resolve("no-such-file.json").toString();
        ConfiguredHoldings unreadable = new ConfiguredHoldings(new HoldingsProperties(missing),
                JsonMapper.builder().build());
        assertThatThrownBy(unreadable::plan)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageNotContaining("no-such-file");
    }

    @Test
    void theFileIsNotReadUntilAPlanIsAsked() {
        // 테이블에 행이 있으면 파일을 읽지 않으므로 없는 파일이어도 만들 때는 실패하지 않아야 한다.
        String missing = dir.resolve("no-such-file.json").toString();

        assertThat(new ConfiguredHoldings(new HoldingsProperties(missing), JsonMapper.builder().build()))
                .isNotNull();
    }

    @Test
    void malformedFileFailsWithThePositionOnly() {
        assertThatThrownBy(() -> configured("[{\"kind\":\"ETF\"}]").plan())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1번째 항목");
    }

    @Test
    void startupLogHasCountsButNotCodesTickersOrNames() throws IOException {
        Logger logger = (Logger) LoggerFactory.getLogger(ConfiguredHoldings.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            configured(FILE).plan();
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(appender.list).extracting(ILoggingEvent::getFormattedMessage)
                .singleElement().asString()
                .contains("6개")
                .doesNotContain("TEST0").doesNotContain("FAKE").doesNotContain("한글");
    }

    @Test
    void propertiesToStringHidesTheValues() {
        assertThat(new HoldingsProperties("/some/where/holdings.json").toString())
                .doesNotContain("where");
    }
}
