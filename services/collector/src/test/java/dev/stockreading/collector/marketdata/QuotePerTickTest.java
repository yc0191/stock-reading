package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class QuotePerTickTest {

    @Test
    void valuesFromOneUpToTheCallLimitPass() {
        assertThat(QuotePerTick.checked(1, 10).value()).isEqualTo(1);
        assertThat(QuotePerTick.checked(8, 10).value()).isEqualTo(8);
        assertThat(QuotePerTick.checked(10, 10).value()).isEqualTo(10);
    }

    @Test
    void valuesBelowOneOrAboveTheCallLimitFailAtStartup() {
        for (int bad : new int[] {0, -1, 11}) {
            assertThatThrownBy(() -> QuotePerTick.checked(bad, 10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("quote-per-tick=" + bad);
        }
    }
}
