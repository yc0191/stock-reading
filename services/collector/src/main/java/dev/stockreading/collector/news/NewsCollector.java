package dev.stockreading.collector.news;

import java.time.Clock;
import java.time.Instant;
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
    private final Clock clock;
    private final DailyCallLimit dailyCallLimit;
    private final List<NewsQuery> configuredQueries;

    NewsCollector(NewsSearchPort search, NewsSearchProperties properties, Clock clock) {
        this.search = search;
        this.properties = properties;
        this.clock = clock;
        this.dailyCallLimit = new DailyCallLimit(properties.dailyCallLimit(), clock);
        // 설정 오타로 모든 기사가 조용히 버려지지 않도록 시작할 때 한 번 검증해서 보관한다.
        this.configuredQueries = NewsQuery.parseValidated(properties.queries());
    }

    /** 설정(collector.news-search.queries)의 검색어로 수집한다. */
    public List<NewsItem> collect() {
        return collect(configuredQueries);
    }

    // 같은 URL이거나 정제한 제목이 같으면 한 건으로 합친다. 제목이 비슷할 뿐인 기사는 합치지 않는다.
    // 잘못 합치면 서로 다른 기사를 잃기 때문이고, 유사도 판단은 뉴스 선별 단계의 몫이다.
    // 한 검색어가 실패해도 나머지는 계속하고, 결과는 최신순이다.
    // 거르기는 검색어별 결과에 병합 전에 한다. 병합 뒤에는 어느 검색어에 대한 판정인지 알 수 없다.
    // 검색어는 관심 종목이 드러날 수 있어 로그에 남기지 않고 순번만 남긴다.
    public List<NewsItem> collect(List<NewsQuery> queries) {
        Map<String, NewsItem> byUrl = new LinkedHashMap<>();
        Map<String, String> urlKeyByTitleKey = new LinkedHashMap<>();
        Instant now = clock.instant();
        int passed = 0;
        int tooOld = 0;
        int tooFarAhead = 0;
        int unmatched = 0;
        int skippedByLimit = 0;
        int index = 0;
        for (NewsQuery query : queries) {
            index++;
            if (!dailyCallLimit.tryAcquire()) {
                skippedByLimit++;
                continue;
            }
            List<NewsItem> found;
            try {
                found = search.search(query.query(), properties.display());
            } catch (NewsSearchApiException e) {
                log.warn("news query failed index={} reason={}", index, e.getMessage());
                continue;
            }
            NewsFilter.Result filtered = NewsFilter.apply(query.query(), found, now);
            passed += filtered.passed().size();
            tooOld += filtered.tooOld();
            tooFarAhead += filtered.tooFarAhead();
            unmatched += filtered.unmatched();
            for (NewsItem item : filtered.passed()) {
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
        if (skippedByLimit > 0) {
            log.warn("news daily call limit reached, skipped queries count={} limit={}", skippedByLimit, properties.dailyCallLimit());
        }
        log.info("news filtered passed={} tooOld={} tooFarAhead={} unmatched={}", passed, tooOld, tooFarAhead, unmatched);
        List<NewsItem> result = new ArrayList<>(byUrl.values());
        result.sort(Comparator.comparing(NewsItem::publishedAt).reversed());
        return result;
    }

    private static NewsItem withSymbols(NewsItem item, NewsQuery query) {
        return new NewsItem(item.title(), item.summary(), item.url(), item.providerUrl(), item.publishedAt(),
                item.source(), new LinkedHashSet<>(query.symbols()), item.matchedQueries());
    }
}
