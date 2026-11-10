package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.DayOfWeek;
import java.time.Duration;

import org.junit.jupiter.api.Test;

class MemberSourcePropertiesTest {

    private static MemberSourceProperties of(String template, String fundIds) {
        return new MemberSourceProperties(template, fundIds, null, 0, null, null, null);
    }

    @Test
    void parsesEtfToFundIdPairsAndSkipsBrokenOnes() {
        assertThat(of("http://x/{fundId}", " TEST01 : F1 ,TEST02:F2,broken,:x,y:").fundIdByEtf())
                .containsOnly(org.assertj.core.api.Assertions.entry("TEST01", "F1"),
                        org.assertj.core.api.Assertions.entry("TEST02", "F2"));
        assertThat(of("http://x/{fundId}", null).fundIdByEtf()).isEmpty();
    }

    @Test
    void isDisabledWithoutATemplateAndFillsDefaults() {
        assertThat(of("", "TEST01:F1").enabled()).isFalse();
        assertThat(of(null, "TEST01:F1").enabled()).isFalse();
        MemberSourceProperties properties = of("http://x/{fundId}", "TEST01:F1");
        assertThat(properties.enabled()).isTrue();
        assertThat(properties.topCount()).isEqualTo(10);
        assertThat(properties.fetchDay()).isEqualTo(DayOfWeek.MONDAY);
        assertThat(properties.timeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(properties.userAgent()).isNotBlank();
    }

    @Test
    void toStringHidesTheValues() {
        assertThat(of("http://secret.example/{fundId}", "TEST01:F1").toString())
                .doesNotContain("secret").doesNotContain("TEST01");
    }
}
