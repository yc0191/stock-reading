package dev.stockreading.collector.news;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// 뉴스 검색 응답의 텍스트 정제와 중복 판정용 키 만들기.
final class NewsText {

    private static final Pattern TAG = Pattern.compile("<[^>]*>");
    private static final Pattern ENTITY = Pattern.compile("&(#[xX][0-9a-fA-F]+|#[0-9]+|[a-zA-Z]+);");
    private static final Pattern WHITESPACE = Pattern.compile("[\\s\\u00A0]+");
    private static final Pattern LEADING_BRACKETS = Pattern.compile("^(\\s*\\[[^\\]]*\\])+");
    private static final Pattern NOT_LETTER_OR_DIGIT = Pattern.compile("[^\\p{L}\\p{N}]+");
    private static final Map<String, String> NAMED = Map.of(
            "quot", "\"", "amp", "&", "lt", "<", "gt", ">", "apos", "'", "nbsp", " ");

    private NewsText() {
    }

    /** 태그를 먼저 지운 뒤 엔티티를 되돌린다. 순서가 반대면 &lt;b&gt; 가 가짜 태그로 살아난다. */
    static String clean(String raw) {
        if (raw == null) {
            return "";
        }
        String withoutTags = TAG.matcher(raw).replaceAll("");
        return WHITESPACE.matcher(unescape(withoutTags)).replaceAll(" ").trim();
    }

    private static String unescape(String text) {
        Matcher matcher = ENTITY.matcher(text);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(out, Matcher.quoteReplacement(decode(matcher.group(0), matcher.group(1))));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    private static String decode(String whole, String body) {
        if (body.startsWith("#")) {
            try {
                int codePoint = body.charAt(1) == 'x' || body.charAt(1) == 'X'
                        ? Integer.parseInt(body.substring(2), 16)
                        : Integer.parseInt(body.substring(1));
                return Character.isValidCodePoint(codePoint) ? new String(Character.toChars(codePoint)) : whole;
            } catch (NumberFormatException e) {
                return whole;
            }
        }
        return NAMED.getOrDefault(body, whole);
    }

    /** 맨 앞 말머리([특징주] 등)와 문장부호·공백을 뺀 제목. 같은 기사를 받아쓴 경우를 묶는 데 쓴다. */
    static String titleKey(String title) {
        String withoutPrefix = LEADING_BRACKETS.matcher(title).replaceFirst("");
        return NOT_LETTER_OR_DIGIT.matcher(withoutPrefix).replaceAll("").toLowerCase(Locale.ROOT);
    }

    /** 호스트+경로+쿼리로 비교한다. 스킴, 추적 파라미터(utm_*), 조각(#), 끝의 /는 무시한다. */
    static String urlKey(String url) {
        try {
            URI uri = new URI(url.trim());
            String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
            String path = uri.getRawPath() == null ? "" : uri.getRawPath();
            if (path.length() > 1 && path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            String query = uri.getRawQuery() == null ? "" : Arrays.stream(uri.getRawQuery().split("&"))
                    .filter(part -> !part.toLowerCase(Locale.ROOT).startsWith("utm_"))
                    .sorted()
                    .collect(Collectors.joining("&"));
            return host + path + (query.isEmpty() ? "" : "?" + query);
        } catch (URISyntaxException e) {
            return url.trim().toLowerCase(Locale.ROOT);
        }
    }
}
