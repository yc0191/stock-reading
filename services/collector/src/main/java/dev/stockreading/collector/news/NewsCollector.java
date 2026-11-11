package dev.stockreading.collector.news;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final NewsCallPause pause;
    private final SearchQueriesPort searchQueries;

    NewsCollector(NewsSearchPort search, NewsSearchProperties properties, Clock clock, NewsCallPause pause,
            SearchQueriesPort searchQueries) {
        this.search = search;
        this.properties = properties;
        this.clock = clock;
        this.pause = pause;
        this.dailyCallLimit = new DailyCallLimit(properties.dailyCallLimit(), clock);
        this.searchQueries = searchQueries;
    }

    /** DB에서 시작할 때 읽은 검색어 목록으로 수집한다. */
    public List<NewsItem> collect() {
        return collect(searchQueries.queries());
    }

    /** 읽은 검색어가 하나도 없는지. 비면 수집은 아무것도 찾지 못한다. */
    public boolean hasNoQueries() {
        return searchQueries.queries().isEmpty();
    }

    /** collect() 와 같지만 제대로 모으지 못했을 수 있는 종목과 시작 시각도 함께 돌려준다. */
    public NewsCollection collectWithStatus() {
        return collectWithStatus(searchQueries.queries());
    }

    public List<NewsItem> collect(List<NewsQuery> queries) {
        return collectWithStatus(queries).items();
    }

    // 같은 URL이거나 정제한 제목이 같으면 한 건으로 합친다. 제목이 비슷할 뿐인 기사는 합치지 않는다.
    // 잘못 합치면 서로 다른 기사를 잃기 때문이고, 유사도 판단은 뉴스 선별 단계의 몫이다.
    // 한 검색어가 실패해도 나머지는 계속하고, 결과는 최신순이다.
    // 거르기는 검색어별 결과에 병합 전에 한다. 병합 뒤에는 어느 검색어에 대한 판정인지 알 수 없다.
    // 검색어는 관심 종목이 드러날 수 있어 로그에 남기지 않고 순번만 남긴다.
    public NewsCollection collectWithStatus(List<NewsQuery> queries) {
        Map<String, NewsItem> byUrl = new LinkedHashMap<>();
        Map<String, String> urlKeyByTitleKey = new LinkedHashMap<>();
        Instant now = clock.instant();
        // 첫 검색 호출을 시작한 시각이다. 호출이 하나도 없으면 수집을 시작한 시각을 쓴다.
        OffsetDateTime startedAt = null;
        Set<String> incompleteSymbols = new LinkedHashSet<>();
        int passed = 0;
        int tooOld = 0;
        int tooFarAhead = 0;
        int unmatched = 0;
        int duplicates = 0;
        int skippedByLimit = 0;
        int index = 0;
        boolean called = false;
        for (NewsQuery query : queries) {
            index++;
            if (!dailyCallLimit.tryAcquire()) {
                skippedByLimit++;
                incompleteSymbols.addAll(query.symbols());
                continue;
            }
            // 짧은 시간에 호출을 몰면 제공자가 429로 거절해 그 검색어의 기사가 해당 주기에 빠진다.
            if (called && properties.callIntervalMs() > 0) {
                pause.pause(properties.callIntervalMs());
            }
            called = true;
            if (startedAt == null) {
                startedAt = OffsetDateTime.now(clock);
            }
            List<NewsItem> found;
            try {
                found = search.search(query.query(), properties.display());
            } catch (NewsSearchApiException e) {
                log.warn("news query failed index={} reason={}", index, e.getMessage());
                incompleteSymbols.addAll(query.symbols());
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
                    duplicates++;
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
        // 거른 이유별 건수만 남긴다. passed 는 거르기를 통과한 건수이고 duplicates 는 그중 앞서 모은 기사와 합쳐진 건수다.
        log.info("news filtered passed={} tooOld={} tooFarAhead={} unmatched={} duplicates={}",
                passed, tooOld, tooFarAhead, unmatched, duplicates);
        List<NewsItem> result = new ArrayList<>(byUrl.values());
        result.sort(Comparator.comparing(NewsItem::publishedAt).reversed());
        return new NewsCollection(result, incompleteSymbols, startedAt != null ? startedAt : OffsetDateTime.ofInstant(now, clock.getZone()));
    }

    private static NewsItem withSymbols(NewsItem item, NewsQuery query) {
        return new NewsItem(item.title(), item.summary(), item.url(), item.providerUrl(), item.publishedAt(),
                item.source(), new LinkedHashSet<>(query.symbols()), item.matchedQueries());
    }
}
