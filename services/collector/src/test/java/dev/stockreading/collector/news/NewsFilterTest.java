package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

class NewsFilterTest {

    private static final Instant NOW = Instant.parse("2026-10-22T07:00:00Z");

    private static NewsItem item(String title, String summary, Instant publishedAt) {
        return new NewsItem(title, summary, "https://a.example.com/" + title.hashCode(), "",
                OffsetDateTime.ofInstant(publishedAt, ZoneOffset.ofHours(9)), "example.com",
                new LinkedHashSet<>(), new LinkedHashSet<>());
    }

    private static NewsFilter.Result apply(String query, NewsItem... items) {
        return NewsFilter.apply(query, List.of(items), NOW);
    }

    @Test
    void passesItemWhenEveryQueryTokenIsInTitleOrSummary() {
        NewsFilter.Result result = apply("테스트 환율", item("테스트 지수 하락", "원달러 환율 급등", NOW));

        assertThat(result.passed()).hasSize(1);
    }

    @Test
    void dropsItemMissingAnyQueryToken() {
        NewsFilter.Result result = apply("테스트 환율", item("테스트 지수 하락", "금리 소식", NOW));

        assertThat(result.passed()).isEmpty();
        assertThat(result.unmatched()).isEqualTo(1);
    }

    @Test
    void tokenMatchIgnoresCaseAndExtraWhitespace() {
        NewsFilter.Result result = apply("  test   FOMC ", item("Test news", "fomc 회의", NOW));

        assertThat(result.passed()).hasSize(1);
    }

    @Test
    void itemExactlyTwentyFourHoursOldPassesAndOneSecondOlderIsDropped() {
        NewsFilter.Result result = apply("테스트",
                item("테스트 경계", "", NOW.minusSeconds(24 * 3600)),
                item("테스트 초과", "", NOW.minusSeconds(24 * 3600 + 1)));

        assertThat(result.passed()).extracting(NewsItem::title).containsExactly("테스트 경계");
        assertThat(result.tooOld()).isEqualTo(1);
    }

    @Test
    void itemExactlyOneHourAheadPassesAndOneSecondFurtherIsDropped() {
        NewsFilter.Result result = apply("테스트",
                item("테스트 경계", "", NOW.plusSeconds(3600)),
                item("테스트 초과", "", NOW.plusSeconds(3601)));

        assertThat(result.passed()).extracting(NewsItem::title).containsExactly("테스트 경계");
        assertThat(result.tooFarAhead()).isEqualTo(1);
    }

    @Test
    void eachDroppedItemIsCountedOnceUnderTheFirstFailingReason() {
        NewsFilter.Result result = apply("테스트",
                item("무관한 오래된 기사", "", NOW.minusSeconds(48 * 3600)),
                item("무관한 먼 미래 기사", "", NOW.plusSeconds(5 * 3600)),
                item("무관한 기사", "", NOW),
                item("테스트 기사", "", NOW));

        assertThat(result.tooOld()).isEqualTo(1);
        assertThat(result.tooFarAhead()).isEqualTo(1);
        assertThat(result.unmatched()).isEqualTo(1);
        assertThat(result.passed()).hasSize(1);
    }
}
