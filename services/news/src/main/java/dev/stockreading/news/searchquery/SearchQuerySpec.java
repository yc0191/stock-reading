package dev.stockreading.news.searchquery;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import dev.stockreading.news.event.SearchQueriesPayload;

/**
 * 빈 검색어 테이블을 처음 채울 때 읽는 글("검색어=종목코드,종목코드;검색어2=*")을 검증해 목록으로 바꾼다.
 * 항목은 ";"나 줄바꿈으로 나누고, 종목은 국내 종목코드, 미국 티커(종류 구분은 "." 또는 "-" 뒤 1~2자), "*"(시장 전체)다.
 * 종류 구분은 보유 종목 목록과 같이 "-"로 맞춘다. 보유 종목이 드러나지 않도록 오류 메시지에는 값을 넣지 않고
 * 몇 번째 항목인지와 오류 종류만 적는다.
 */
final class SearchQuerySpec {

    static final int QUERY_MAX = 200;

    private static final Pattern SYMBOL = Pattern.compile("\\*|[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?");

    private SearchQuerySpec() {
    }

    static List<SearchQueriesPayload.Query> parse(String spec) {
        List<SearchQueriesPayload.Query> result = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return result;
        }
        Set<String> seen = new LinkedHashSet<>();
        int position = 0;
        for (String entry : spec.split("[;\\r\\n]")) {
            position++;
            if (entry.isBlank()) {
                continue;
            }
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                throw error(position, "구조 오류가 있습니다(검색어=종목코드[,종목코드...] 형태여야 합니다)");
            }
            String query = entry.substring(0, separator).trim();
            if (query.isEmpty()) {
                throw error(position, "구조 오류가 있습니다(검색어=종목코드[,종목코드...] 형태여야 합니다)");
            }
            if (query.length() > QUERY_MAX) {
                throw error(position, "검색어 길이 오류가 있습니다(" + QUERY_MAX + "자 이하여야 합니다)");
            }
            if (!seen.add(query)) {
                throw error(position, "검색어 중복 오류가 있습니다(같은 검색어는 한 번만 적습니다)");
            }
            Set<String> symbols = new LinkedHashSet<>();
            for (String symbol : entry.substring(separator + 1).split(",")) {
                if (symbol.isBlank()) {
                    continue;
                }
                String trimmed = symbol.trim();
                if (!SYMBOL.matcher(trimmed).matches()) {
                    throw error(position, "종목코드 형식 오류가 있습니다(국내 종목코드, 미국 티커 또는 *)");
                }
                symbols.add(trimmed.replace('.', '-'));
            }
            if (symbols.isEmpty()) {
                throw error(position, "구조 오류가 있습니다(검색어=종목코드[,종목코드...] 형태여야 합니다)");
            }
            result.add(new SearchQueriesPayload.Query(query, List.copyOf(symbols)));
        }
        return result;
    }

    private static IllegalArgumentException error(int position, String kind) {
        return new IllegalArgumentException("검색어 파일의 " + position + "번째 항목에 " + kind);
    }
}
