package dev.stockreading.market.insight;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 저장된 해외 일봉 원문에서 기준 시각 직전 미국 정규장 마감의 등락률을 읽는다.
 *
 * <p>응답 맨 앞 레코드와 최상위 등락율은 장외 거래의 진행 중 막대일 수 있어 위치로 고르지 않는다.
 * 레코드 일자(dt)마다 그날 정규장 마감(뉴욕 16:00)을 계산해, 기준 시각 이하로 마감된 것 중 가장 최근 것을 고른다.
 * 행은 받은 시각으로 거르지 않지만(기준 시각 뒤에 받은 행도 읽음), 받을 때 아직 마감 전이던 레코드는 미완성이라 뺀다.
 * 같은 일자가 여러 행에 있으면 가장 늦게 받은 행의 값을 쓴다. 조기 폐장일도 16:00으로 계산한다.
 */
@Component
public class OverseasCloseRateQuery {

    /** 종목마다 읽는 최근 행 수. 하루 한 번 받으므로 열흘치 안에서 고른다. */
    static final int ROWS_TO_READ = 10;

    static final String RATE_FIELD = "up_dwn_r_p2";

    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final LocalTime REGULAR_CLOSE = LocalTime.of(16, 0);

    private static final Logger log = LoggerFactory.getLogger(OverseasCloseRateQuery.class);

    private final InsightRepository repository;
    private final ObjectMapper mapper;

    OverseasCloseRateQuery(InsightRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * @param ticker 저장할 때의 target(티커)
     * @param asOf   기준 시각. 이보다 늦게 마감한 장은 고르지 않는다(같으면 포함)
     * @return 고른 레코드. 후보가 없으면 빈 값
     */
    public Optional<OverseasCloseRate> find(String ticker, OffsetDateTime asOf) {
        Objects.requireNonNull(ticker, "ticker");
        Objects.requireNonNull(asOf, "asOf");

        // 수집기는 종류 구분을 "-" 로 맞춰 저장하므로 "." 로 찾아도 같은 종목을 찾게 한다.
        String normalized = ticker.trim().toUpperCase(Locale.ROOT).replace('.', '-');
        List<OverseasDailyRow> rows = repository.recentOverseasDailyPrices(normalized, ROWS_TO_READ);
        OverseasCloseRate best = null;
        for (int i = 0; i < rows.size(); i++) {
            OverseasDailyRow row = rows.get(i);
            JsonNode records = readRecords(row.response(), i);
            for (JsonNode record : records) {
                OverseasCloseRate candidate = candidate(normalized, record, row.fetchedAt());
                if (candidate == null || candidate.closeAt().isAfter(asOf)
                        || candidate.closeAt().isAfter(row.fetchedAt())) {
                    continue;
                }
                if (best == null || isBetter(candidate, best)) {
                    best = candidate;
                }
            }
        }
        return Optional.ofNullable(best);
    }

    // 더 최근 장이 우선이고, 같은 장이면 더 늦게 받은 값이 우선이다.
    private static boolean isBetter(OverseasCloseRate candidate, OverseasCloseRate best) {
        int byClose = candidate.closeAt().compareTo(best.closeAt());
        if (byClose != 0) {
            return byClose > 0;
        }
        return candidate.fetchedAt().isAfter(best.fetchedAt());
    }

    private JsonNode readRecords(String response, int rowIndex) {
        try {
            return mapper.readTree(response).path("dataBody").path("out2");
        } catch (JacksonException e) {
            // 티커는 보유 종목이 드러나므로 로그에는 행 순번만 남긴다.
            log.warn("overseas daily row skipped: response is not JSON row={}", rowIndex);
            return mapper.createArrayNode();
        }
    }

    private static OverseasCloseRate candidate(String ticker, JsonNode record, OffsetDateTime fetchedAt) {
        JsonNode dt = record.path("dt");
        JsonNode rate = record.path(RATE_FIELD);
        if (!dt.isString() || rate.isMissingNode() || rate.isNull()) {
            return null;
        }
        LocalDate date;
        try {
            date = LocalDate.parse(dt.asString().strip(), DateTimeFormatter.BASIC_ISO_DATE);
        } catch (DateTimeParseException e) {
            return null;
        }
        OffsetDateTime closeAt = date.atTime(REGULAR_CLOSE).atZone(NEW_YORK).toOffsetDateTime();
        return new OverseasCloseRate(ticker, dt.asString(), closeAt, rate.asString(), fetchedAt);
    }
}
