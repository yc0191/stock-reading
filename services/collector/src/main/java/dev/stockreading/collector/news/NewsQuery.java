package dev.stockreading.collector.news;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public record NewsQuery(String query, Set<String> symbols) {

    // 시장 전체 이슈는 종목 자리에 이 값을 쓴다.
    public static final String MARKET_WIDE = "*";

    // 이벤트 계약의 종목코드 형식과 같다. 이 형식이 아닌 기사는 발행 단계에서 조용히 버려지므로 설정 단계에서 막는다.
    private static final Pattern SYMBOL = Pattern.compile("[0-9A-Z]{6}|\\*");

    /** "검색어=종목코드,종목코드;검색어2=*" 형식을 읽는다. 비어 있거나 잘못된 항목은 건너뛴다. */
    public static List<NewsQuery> parse(String spec) {
        return parse(spec, false);
    }

    /**
     * parse 와 같지만 잘못된 항목을 건너뛰지 않고 IllegalArgumentException 으로 실패시킨다.
     * 공백뿐인 항목(끝의 ";" 포함)은 무시하고, 나머지는 "검색어=종목코드[,종목코드...]" 형태여야 하며
     * 종목코드는 6자리 숫자·대문자나 "*" 여야 한다.
     * 메시지에는 검색어와 코드 원문을 넣지 않고, 세미콜론으로 나눈 설정의 몇 번째 항목인지와 오류 종류만 적는다.
     */
    public static List<NewsQuery> parseValidated(String spec) {
        return parse(spec, true);
    }

    private static List<NewsQuery> parse(String spec, boolean strict) {
        List<NewsQuery> result = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return result;
        }
        int position = 0;
        for (String entry : spec.split(";")) {
            position++;
            if (strict && entry.isBlank()) {
                continue;
            }
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                if (strict) {
                    throw structureError(position);
                }
                continue;
            }
            String query = entry.substring(0, separator).trim();
            Set<String> symbols = new LinkedHashSet<>();
            for (String symbol : entry.substring(separator + 1).split(",")) {
                if (!symbol.isBlank()) {
                    if (strict && !SYMBOL.matcher(symbol.trim()).matches()) {
                        throw new IllegalArgumentException("검색어 설정의 " + position
                                + "번째 항목에 종목코드 형식 오류가 있습니다(6자리 숫자·대문자 또는 *)");
                    }
                    symbols.add(symbol.trim());
                }
            }
            if (query.isEmpty() || symbols.isEmpty()) {
                if (strict) {
                    throw structureError(position);
                }
                continue;
            }
            result.add(new NewsQuery(query, symbols));
        }
        return result;
    }

    private static IllegalArgumentException structureError(int position) {
        return new IllegalArgumentException("검색어 설정의 " + position
                + "번째 항목에 구조 오류가 있습니다(검색어=종목코드[,종목코드...] 형태여야 합니다)");
    }
}
