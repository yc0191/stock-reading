package dev.stockreading.news.filing;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.springframework.stereotype.Component;

/**
 * 종목과 기간으로 공시를 읽는다. 변동 시각 이후에 나온 자료는 원인 근거가 될 수 없으므로,
 * 접수 시각이 기준 시각(asOf)보다 늦은 공시는 기간 안이어도 돌려주지 않는다(같으면 포함).
 * 24시간 같은 오래된 자료 한도는 여기서 고정하지 않고 호출자가 from으로 정한다.
 */
@Component
public class FilingQuery {

    private final FilingRepository repository;

    FilingQuery(FilingRepository repository) {
        this.repository = repository;
    }

    /**
     * @param ticker 보유 종목 설정에 적은 표기 그대로의 티커
     * @param from   접수 시각 하한(포함)
     * @param to     접수 시각 상한(포함)
     * @param asOf   기준 시각. 이보다 늦게 접수된 공시는 돌려주지 않는다(같으면 포함)
     * @return 접수 시각 순으로 정렬한 공시
     */
    public List<StoredFiling> find(String ticker, OffsetDateTime from, OffsetDateTime to, OffsetDateTime asOf) {
        Objects.requireNonNull(ticker, "ticker");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(asOf, "asOf");
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("from must not be after to");
        }

        // 수집기는 종류 구분을 "-" 로 맞춰 저장하므로 "." 로 찾아도 같은 종목을 찾게 한다.
        String normalized = ticker.trim().toUpperCase(Locale.ROOT).replace('.', '-');
        OffsetDateTime upper = to.isAfter(asOf) ? asOf : to;
        if (from.isAfter(upper)) {
            return List.of();
        }
        // 저장소가 상한을 지키더라도 기준 시각 규칙은 여기서 한 번 더 확인한다. 어기면 조용히 잘못된 근거가 나간다.
        return repository.findByTicker(normalized, from, upper).stream()
                .filter(filing -> !filing.acceptedAt().isAfter(asOf))
                .toList();
    }
}
