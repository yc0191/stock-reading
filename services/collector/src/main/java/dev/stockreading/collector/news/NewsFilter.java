package dev.stockreading.collector.news;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

// 검색 결과 한 검색어분을 규칙으로 거른다. 검색 API 는 검색어와 느슨하게만 맞는 기사도 돌려주고,
// 발행 시각이 엉뚱한 기사도 섞여 있어서 저장 쪽으로 넘기기 전에 여기서 막는다.
final class NewsFilter {

    static final Duration MAX_AGE = Duration.ofHours(24);
    // 제공자와 이쪽 시계의 오차는 허용하되, 그보다 먼 미래 시각은 잘못된 데이터로 본다.
    static final Duration MAX_FUTURE = Duration.ofHours(1);

    record Result(List<NewsItem> passed, int tooOld, int tooFarAhead, int unmatched) {
    }

    private NewsFilter() {
    }

    /** 발행 시각이 [now - 24시간, now + 1시간] 이고, 검색어를 공백으로 나눈 토큰이 모두 제목+요약에 있는 기사만 통과시킨다. */
    static Result apply(String query, List<NewsItem> items, Instant now) {
        List<String> tokens = tokensOf(query);
        Instant oldest = now.minus(MAX_AGE);
        Instant latest = now.plus(MAX_FUTURE);
        List<NewsItem> passed = new ArrayList<>();
        int tooOld = 0;
        int tooFarAhead = 0;
        int unmatched = 0;
        for (NewsItem item : items) {
            Instant publishedAt = item.publishedAt().toInstant();
            if (publishedAt.isBefore(oldest)) {
                tooOld++;
            } else if (publishedAt.isAfter(latest)) {
                tooFarAhead++;
            } else if (!containsAll(item, tokens)) {
                unmatched++;
            } else {
                passed.add(item);
            }
        }
        return new Result(passed, tooOld, tooFarAhead, unmatched);
    }

    private static List<String> tokensOf(String query) {
        List<String> tokens = new ArrayList<>();
        for (String token : query.trim().split("\\s+")) {
            if (!token.isEmpty()) {
                tokens.add(token.toLowerCase(Locale.ROOT));
            }
        }
        return tokens;
    }

    private static boolean containsAll(NewsItem item, List<String> tokens) {
        String text = (item.title() + " " + item.summary()).toLowerCase(Locale.ROOT);
        return tokens.stream().allMatch(text::contains);
    }
}
