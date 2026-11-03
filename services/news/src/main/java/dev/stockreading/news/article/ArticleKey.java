package dev.stockreading.news.article;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * 같은 기사가 추적 파라미터나 표기만 달리해 다시 들어와도 한 행으로 모이도록 URL에서 키를 계산한다.
 * 수집기와 키를 공유하지 않고 서비스마다 독립적으로 계산하는 것이 설계다.
 */
final class ArticleKey {

    private static final String TRACKING_PARAM_PREFIX = "utm_";

    private ArticleKey() {
    }

    static String of(String url) {
        return sha256Hex(normalize(url));
    }

    static String normalize(String url) {
        String trimmed = url.strip();
        URI uri = parse(trimmed);
        if (uri == null || uri.getRawAuthority() == null) {
            // 절대 URL이 아니면 정규화할 근거가 없으므로 원문 그대로 키로 쓴다.
            return trimmed;
        }

        String authority = uri.getRawAuthority().toLowerCase(Locale.ROOT);
        if (authority.startsWith("www.")) {
            authority = authority.substring("www.".length());
        }

        String path = uri.getRawPath() == null ? "" : uri.getRawPath();
        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        String query = normalizeQuery(uri.getRawQuery());
        return query.isEmpty() ? authority + path : authority + path + "?" + query;
    }

    private static URI parse(String url) {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private static String normalizeQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }
        return Arrays.stream(rawQuery.split("&"))
                .filter(param -> !param.isEmpty())
                .filter(param -> !param.toLowerCase(Locale.ROOT).startsWith(TRACKING_PARAM_PREFIX))
                .sorted(ArticleKey::compareByNameThenValue)
                .collect(Collectors.joining("&"));
    }

    private static int compareByNameThenValue(String a, String b) {
        String[] left = a.split("=", 2);
        String[] right = b.split("=", 2);
        int byName = left[0].compareTo(right[0]);
        if (byName != 0) {
            return byName;
        }
        return (left.length > 1 ? left[1] : "").compareTo(right.length > 1 ? right[1] : "");
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
