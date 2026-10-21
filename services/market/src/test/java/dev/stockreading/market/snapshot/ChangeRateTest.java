package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ChangeRateTest {

    @ParameterizedTest(name = "{0} -> {1} = {2}%")
    @CsvSource({
            "100,   110,   10.00",
            "100,    90,  -10.00",
            "10000, 10000,  0.00",
            "3,       4,   33.33",
            "3,       2,  -33.33",
            "7,       8,   14.29",
            "100000, 100001, 0.00",
            "100000, 100005, 0.01",
            "200,     1,  -99.50"
    })
    void computesPercentRoundedHalfUpToTwoDecimals(long previous, long current, String expected) {
        assertThat(ChangeRate.percent(previous, current)).isEqualByComparingTo(new BigDecimal(expected));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, -10_000})
    void rejectsNonPositivePreviousPrice(long previous) {
        assertThatThrownBy(() -> ChangeRate.percent(previous, 100))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
