package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class ConfiguredHoldingsTest {

    private static HoldingsPort holdings(String spec) {
        return new ConfiguredHoldings(new HoldingsProperties(spec));
    }

    @Test
    void returnsTickersOfOneEtfInConfiguredOrder() {
        HoldingsPort port = holdings("TEST01=FAKEA,FAKEB;TEST02=FAKEB,FAKE.C");

        assertThat(port.topTickers("TEST01")).containsExactly("FAKEA", "FAKEB");
        assertThat(port.topTickers("TEST02")).containsExactly("FAKEB", "FAKE-C");
    }

    @Test
    void etfMissingFromTheMapHasNoUsTickers() {
        assertThat(holdings("TEST01=FAKEA").topTickers("TEST99")).isEmpty();
        assertThat(holdings("TEST01=FAKEA").topTickers(null)).isEmpty();
    }

    @Test
    void blankSettingMeansNoUsTickersForAnyEtf() {
        assertThat(holdings("").topTickers("TEST01")).isEmpty();
        assertThat(holdings(null).topTickers("TEST01")).isEmpty();
    }

    @Test
    void malformedSettingFailsConstructionWithoutEchoingTheValue() {
        assertThatThrownBy(() -> holdings("TEST01=FAKEA;TEST02=fakeb"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageNotContaining("TEST02")
                .hasMessageNotContaining("fakeb");
    }

    @Test
    void startupLogHasTheCountButNotTheCodesOrTickers() {
        Logger logger = (Logger) LoggerFactory.getLogger(ConfiguredHoldings.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            holdings("TEST01=FAKEA,FAKEB;TEST02=FAKEC");
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(appender.list).extracting(ILoggingEvent::getFormattedMessage)
                .singleElement().asString()
                .contains("2개")
                .doesNotContain("TEST01").doesNotContain("FAKEA").doesNotContain("FAKEC");
    }

    @Test
    void propertiesToStringHidesTheValue() {
        assertThat(new HoldingsProperties("TEST01=FAKEA").toString())
                .doesNotContain("TEST01")
                .doesNotContain("FAKEA");
    }
}
