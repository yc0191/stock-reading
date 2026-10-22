package dev.stockreading.collector.news;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record NewsQuery(String query, Set<String> symbols) {

    // 시장 전체 이슈는 종목 자리에 이 값을 쓴다.
    public static final String MARKET_WIDE = "*";

    /** "검색어=종목코드,종목코드;검색어2=*" 형식을 읽는다. 비어 있거나 잘못된 항목은 건너뛴다. */
    public static List<NewsQuery> parse(String spec) {
        List<NewsQuery> result = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return result;
        }
        for (String entry : spec.split(";")) {
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String query = entry.substring(0, separator).trim();
            Set<String> symbols = new LinkedHashSet<>();
            for (String symbol : entry.substring(separator + 1).split(",")) {
                if (!symbol.isBlank()) {
                    symbols.add(symbol.trim());
                }
            }
            if (!query.isEmpty() && !symbols.isEmpty()) {
                result.add(new NewsQuery(query, symbols));
            }
        }
        return result;
    }
}
