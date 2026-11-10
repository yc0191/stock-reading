package dev.stockreading.news.searchquery;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.news.event.HoldingsEnvelope.HoldingsPayload;
import dev.stockreading.news.event.HoldingsEnvelope.Instrument;
import dev.stockreading.news.event.HoldingsEnvelope.Membership;

/**
 * 보유 종목 목록으로 관심 ETF가 담은 미국 구성 종목의 검색어를 종목 이름으로 만들어 검색어 테이블에 넣는다.
 * 만들기만 하고 고치거나 지우지 않는다. 그 종목에 이미 검색어가 하나라도 있거나 같은 글자의 검색어가 있으면
 * 건드리지 않으므로, 작업자가 테이블에서 고친 검색어는 덮어쓰이지 않는다. 같은 목록을 다시 받아도 결과가 같다.
 * 이름을 모르는 종목은 검색어를 만들지 않는다. 한 종목의 검색어는 하루 검색 호출을 정기 수집 횟수만큼 쓴다(docs/local-development.md).
 */
@Component
public class GeneratedSearchQueries {

    private static final Logger log = LoggerFactory.getLogger(GeneratedSearchQueries.class);

    // 관심 ETF는 관계가 HELD 또는 WATCH인 국내 ETF다. 구성 종목으로만 아는 NONE 종목은 대상이 아니다.
    private static final Set<String> TRACKED = Set.of("HELD", "WATCH");

    private final SearchQueryRepository repository;

    GeneratedSearchQueries(SearchQueryRepository repository) {
        this.repository = repository;
    }

    public void generate(HoldingsPayload payload) {
        Set<String> trackedEtfs = payload.instruments().stream()
                .filter(i -> "ETF".equals(i.kind()) && "KR".equals(i.market()) && TRACKED.contains(i.relation()))
                .map(Instrument::symbol)
                .collect(Collectors.toSet());
        Set<String> usMembers = new HashSet<>();
        for (Membership m : payload.memberships()) {
            if ("KR".equals(m.etfMarket()) && trackedEtfs.contains(m.etfSymbol()) && "US".equals(m.memberMarket())) {
                usMembers.add(m.memberSymbol());
            }
        }
        // 같은 회사가 여러 티커(주식 종류)로 있으면 이름이 같으므로 검색어 하나에 티커를 모아 한 번만 검색한다.
        Map<String, Set<String>> symbolsByName = new TreeMap<>();
        for (Instrument i : payload.instruments()) {
            if ("US".equals(i.market()) && usMembers.contains(i.symbol()) && i.name() != null
                    && !i.name().isBlank() && i.name().strip().length() <= SearchQuerySpec.QUERY_MAX) {
                symbolsByName.computeIfAbsent(i.name().strip(), k -> new LinkedHashSet<>()).add(i.symbol());
            }
        }
        int created = repository.insertGenerated(new LinkedHashMap<>(symbolsByName));
        // 이름과 종목은 보유 종목이 드러나므로 개수만 남긴다.
        log.info("이름으로 검색어를 만들었습니다: 새 검색어 {}개, 대상 미국 구성 종목 {}개", created, usMembers.size());
    }
}
