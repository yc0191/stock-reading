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

    private ConfiguredHoldings configured(String json, String usByEtf) throws IOException {
        Path file = dir.resolve("holdings.json");
        Files.writeString(file, json, StandardCharsets.UTF_8);
        return new ConfiguredHoldings(new HoldingsProperties(usByEtf, file.toString()), JsonMapper.builder().build());
    }

    @Test
    void readsTheUtf8FileIntoAPlan() throws IOException {
        HoldingsPlan plan = configured(FILE, "TEST01=FAKEA,FAKE-C;TEST02=FAKEB").plan();

        assertThat(plan.instruments()).hasSize(6);
        assertThat(plan.instruments().get(0).name()).isEqualTo("한글 이름");
    }

    @Test
    void blankPathMeansNoHoldingsAndUnreadableFileFailsWithoutThePath() {
        assertThat(new ConfiguredHoldings(new HoldingsProperties("", ""), JsonMapper.builder().build())
                .plan().instruments()).isEmpty();
        assertThat(new ConfiguredHoldings(new HoldingsProperties(null, null), JsonMapper.builder().build())
                .plan().instruments()).isEmpty();

        String missing = dir.resolve("no-such-file.json").toString();
        assertThatThrownBy(() -> new ConfiguredHoldings(new HoldingsProperties("", missing),
                JsonMapper.builder().build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageNotContaining("no-such-file");
    }

    @Test
    void usMembersMustMatchTheCollectorSettingAsSetsIgnoringOrder() throws IOException {
        configured(FILE, "TEST02=FAKEB;TEST01=FAKE.C,FAKEA");

        // 파일에는 있는데 설정에 없음, 설정에는 있는데 파일에 없음, 티커가 다름 모두 시작 실패다.
        for (String usByEtf : new String[] {"", "TEST01=FAKEA,FAKE-C", "TEST01=FAKEA,FAKE-C;TEST02=FAKEB;TEST03=FAKED",
                "TEST01=FAKEA,FAKE-C;TEST02=FAKEX"}) {
            assertThatThrownBy(() -> configured(FILE, usByEtf))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("어긋납니다")
                    .hasMessageNotContaining("TEST0").hasMessageNotContaining("FAKE");
        }
    }

    @Test
    void emptyFileWithACollectorSettingIsAMismatch() {
        assertThatThrownBy(() -> new ConfiguredHoldings(new HoldingsProperties("TEST01=FAKEA", ""),
                JsonMapper.builder().build())).hasMessageContaining("어긋난 ETF 1개");
    }

    @Test
    void malformedCollectorSettingAndMalformedFileBothFailStartup() {
        assertThatThrownBy(() -> configured(FILE, "TEST01=FAKEA;TEST02=fakeb"))
                .hasMessageContaining("2번째").hasMessageNotContaining("fakeb");
        assertThatThrownBy(() -> configured("[{\"kind\":\"ETF\"}]", ""))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1번째 항목");
    }

    @Test
    void startupLogHasCountsButNotCodesTickersOrNames() throws IOException {
        Logger logger = (Logger) LoggerFactory.getLogger(ConfiguredHoldings.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            configured(FILE, "TEST01=FAKEA,FAKE-C;TEST02=FAKEB");
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
        assertThat(new HoldingsProperties("TEST01=FAKEA", "/some/where/holdings.json").toString())
                .doesNotContain("TEST01").doesNotContain("FAKEA").doesNotContain("where");
    }
}
