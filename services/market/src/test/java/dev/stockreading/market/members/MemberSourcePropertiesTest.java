package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.DayOfWeek;
import java.time.Duration;

import org.junit.jupiter.api.Test;

class MemberSourcePropertiesTest {

    private static MemberSourceProperties of(String template) {
        return new MemberSourceProperties(template, null, 0, null, null, null);
    }

    @Test
    void isDisabledWithoutATemplateAndFillsDefaults() {
        assertThat(of("").enabled()).isFalse();
        assertThat(of(null).enabled()).isFalse();
        MemberSourceProperties properties = of("http://x/{fundId}");
        assertThat(properties.enabled()).isTrue();
        assertThat(properties.topCount()).isEqualTo(10);
        assertThat(properties.fetchDay()).isEqualTo(DayOfWeek.MONDAY);
        assertThat(properties.timeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(properties.userAgent()).isNotBlank();
    }

    @Test
    void toStringHidesTheValues() {
        assertThat(of("http://secret.example/{fundId}").toString())
                .doesNotContain("secret").doesNotContain("TEST01");
    }
}
