package dev.stockreading.collector.news;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
class NewsSearchClient implements NewsSearchPort {

    private static final Logger log = LoggerFactory.getLogger(NewsSearchClient.class);
    static final int MAX_DISPLAY = 100;

    private final NewsSearchProperties properties;
    private final ObjectMapper mapper;
    private final HttpClient http;

    NewsSearchClient(NewsSearchProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(properties.timeoutSeconds())).build();
    }

    @Override
    public List<NewsItem> search(String query, int display) {
        if (display < 1 || display > MAX_DISPLAY) {
            throw new IllegalArgumentException("display 는 1~" + MAX_DISPLAY + " 이어야 합니다");
        }
        if (isBlank(properties.baseUrl()) || isBlank(properties.clientId()) || isBlank(properties.clientSecret())
                || isBlank(properties.idHeader()) || isBlank(properties.secretHeader())) {
            throw new NewsSearchApiException("NEWS_SEARCH_* 설정(주소, 아이디, 시크릿, 헤더 이름)이 비어 있습니다");
        }
        String url = properties.baseUrl() + "/v1/search/news.json?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)
                + "&display=" + display + "&start=1&sort=date";
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .header(properties.idHeader(), properties.clientId())
                .header(properties.secretHeader(), properties.clientSecret())
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                throw new NewsSearchApiException("뉴스 검색 오류 http=" + response.statusCode() + " code=" + errorCode(response.body()));
            }
            return toItems(mapper.readTree(response.body()), query);
        } catch (IOException e) {
            // 원인 예외 메시지에 요청 정보가 들어갈 수 있어 연결하지 않는다.
            throw new NewsSearchApiException("뉴스 검색 호출 실패: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NewsSearchApiException("뉴스 검색 호출 중단", e);
        }
    }

    private List<NewsItem> toItems(JsonNode body, String query) {
        List<NewsItem> items = new ArrayList<>();
        int skipped = 0;
        for (JsonNode node : body.path("items")) {
            String originalLink = node.path("originallink").asString("");
            String providerLink = node.path("link").asString("");
            String url = originalLink.isBlank() ? providerLink : originalLink;
            String title = NewsText.clean(node.path("title").asString(""));
            OffsetDateTime publishedAt = parseTime(node.path("pubDate").asString(""));
            if (url.isBlank() || title.isBlank() || publishedAt == null) {
                skipped++;
                continue;
            }
            Set<String> queries = new LinkedHashSet<>();
            queries.add(query);
            items.add(new NewsItem(title, NewsText.clean(node.path("description").asString("")), url, providerLink,
                    publishedAt, hostOf(url), new LinkedHashSet<>(), queries));
        }
        if (skipped > 0) {
            log.warn("news items skipped (missing url/title/time) count={}", skipped);
        }
        return items;
    }

    private static OffsetDateTime parseTime(String raw) {
        try {
            return OffsetDateTime.parse(raw, DateTimeFormatter.RFC_1123_DATE_TIME);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String hostOf(String url) {
        try {
            String host = URI.create(url).getHost();
            return host == null ? "" : host.replaceFirst("^www\\.", "");
        } catch (IllegalArgumentException e) {
            return "";
        }
    }

    private String errorCode(String body) {
        try {
            return mapper.readTree(body).path("errorCode").asString("");
        } catch (RuntimeException e) {
            return "";
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
