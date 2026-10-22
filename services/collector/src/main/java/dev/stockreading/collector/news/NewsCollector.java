package dev.stockreading.collector.news;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class NewsCollector {

    private static final Logger log = LoggerFactory.getLogger(NewsCollector.class);

    private final NewsSearchPort search;
    private final NewsSearchProperties properties;

    NewsCollector(NewsSearchPort search, NewsSearchProperties properties) {
        this.search = search;
        this.properties = properties;
    }

    /** 설정(collector.news-search.queries)의 검색어로 수집한다. */
    public List<NewsItem> collect() {
        return collect(NewsQuery.parse(properties.queries()));
    }

    // 같은 URL이거나 정제한 제목이 같으면 한 건으로 합친다. 제목이 비슷할 뿐인 기사는 합치지 않는다.
    // 잘못 합치면 서로 다른 기사를 잃기 때문이고, 유사도 판단은 뉴스 선별 단계의 몫이다.
    // 한 검색어가 실패해도 나머지는 계속하고, 결과는 최신순이다.
    public List<NewsItem> collect(List<NewsQuery> queries) {
        Map<String, NewsItem> byUrl = new LinkedHashMap<>();
        Map<String, String> urlKeyByTitleKey = new LinkedHashMap<>();
        for (NewsQuery query : queries) {
            List<NewsItem> found;
            try {
                found = search.search(query.query(), properties.display());
            } catch (NewsSearchApiException e) {
                log.warn("news query failed query={} reason={}", query.query(), e.getMessage());
                continue;
            }
            for (NewsItem item : found) {
                NewsItem tagged = withSymbols(item, query);
                String urlKey = NewsText.urlKey(tagged.url());
                String titleKey = NewsText.titleKey(tagged.title());
                String existingKey = null;
                if (byUrl.containsKey(urlKey)) {
                    existingKey = urlKey;
                } else if (!titleKey.isEmpty()) {
                    existingKey = urlKeyByTitleKey.get(titleKey);
                }
                if (existingKey != null) {
                    byUrl.put(existingKey, byUrl.get(existingKey).mergedWith(tagged));
                } else {
                    byUrl.put(urlKey, tagged);
                    if (!titleKey.isEmpty()) {
                        urlKeyByTitleKey.put(titleKey, urlKey);
                    }
                }
            }
        }
        List<NewsItem> result = new ArrayList<>(byUrl.values());
        result.sort(Comparator.comparing(NewsItem::publishedAt).reversed());
        return result;
    }

    private static NewsItem withSymbols(NewsItem item, NewsQuery query) {
        return new NewsItem(item.title(), item.summary(), item.url(), item.providerUrl(), item.publishedAt(),
                item.source(), new LinkedHashSet<>(query.symbols()), item.matchedQueries());
    }
}
