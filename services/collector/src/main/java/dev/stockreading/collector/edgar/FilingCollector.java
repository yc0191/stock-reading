package dev.stockreading.collector.edgar;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.holdings.HoldingsPort;

/**
 * 보유 미국 종목의 최근 공시를 모은다. 같은 회사(CIK)는 한 번만 조회한다.
 * 티커와 CIK 는 보유 종목이 드러나므로 로그에 남기지 않고 순번과 개수만 남긴다.
 */
@Component
public class FilingCollector {

    private static final Logger log = LoggerFactory.getLogger(FilingCollector.class);

    private final EdgarPort edgar;
    private final HoldingsPort holdings;
    private final EdgarProperties properties;
    private final Clock clock;
    private final Set<String> forms;

    FilingCollector(EdgarPort edgar, HoldingsPort holdings, EdgarProperties properties, Clock clock) {
        this.edgar = edgar;
        this.holdings = holdings;
        this.properties = properties;
        this.clock = clock;
        this.forms = properties.formSet();
    }

    /**
     * 접수 시각 오름차순으로 돌려준다. 한 종목이 실패해도 나머지는 계속한다.
     * User-Agent 가 없거나 매핑 파일을 받지 못하면 모든 종목이 같은 이유로 실패하므로 바로 예외를 던진다.
     */
    public List<CollectedFiling> collect() {
        List<String> tickers = holdings.allTickers();
        if (tickers.isEmpty()) {
            return List.of();
        }
        if (!properties.hasUserAgent()) {
            throw new EdgarApiException(EdgarFailure.NOT_CONFIGURED, null);
        }
        Map<EdgarFailure, Integer> failures = new EnumMap<>(EdgarFailure.class);
        Map<String, List<String>> tickersByCik = new LinkedHashMap<>();
        int index = 0;
        for (String ticker : tickers) {
            index++;
            try {
                tickersByCik.computeIfAbsent(edgar.cikOf(ticker), cik -> new ArrayList<>()).add(ticker);
            } catch (EdgarApiException e) {
                if (e.failure() != EdgarFailure.UNKNOWN_TICKER) {
                    throw e;
                }
                failures.merge(e.failure(), 1, Integer::sum);
                log.warn("edgar ticker not found index={}", index);
            }
        }

        Instant now = clock.instant();
        Duration lookback = Duration.ofHours(properties.lookbackHours());
        List<CollectedFiling> result = new ArrayList<>();
        int otherForm = 0;
        int tooOld = 0;
        int companyIndex = 0;
        for (Map.Entry<String, List<String>> company : tickersByCik.entrySet()) {
            companyIndex++;
            CompanySubmissions submissions;
            try {
                submissions = edgar.submissions(company.getKey());
            } catch (EdgarApiException e) {
                failures.merge(e.failure(), 1, Integer::sum);
                log.warn("edgar submissions failed companyIndex={} failure={} http={}",
                        companyIndex, e.failure(), e.httpStatus());
                continue;
            }
            FilingFilter.Result filtered = FilingFilter.apply(submissions.filings(), forms, now, lookback);
            otherForm += filtered.otherForm();
            tooOld += filtered.tooOld();
            for (Filing filing : filtered.passed()) {
                result.add(new CollectedFiling(company.getKey(), List.copyOf(company.getValue()), filing));
            }
        }
        result.sort(Comparator.comparing((CollectedFiling c) -> c.filing().acceptedAt())
                .thenComparing(c -> c.filing().accessionNumber()));
        log.info("edgar collected filings={} companies={} otherForm={} tooOld={} failures={}",
                result.size(), tickersByCik.size(), otherForm, tooOld, failures);
        return result;
    }
}
