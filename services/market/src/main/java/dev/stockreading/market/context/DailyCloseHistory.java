package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;

/**
 * 그래프용 기간 종가. 저장한 국내 일봉 원문은 한 건에 약 30거래일이 들어 있으므로, 받은 시각이 범위 안인 원문들의 레코드를
 * 일자로 모아 같은 일자는 가장 늦게 받은 원문의 값을 쓴다(장중에 받은 원문의 그날 값은 뒤에 받은 원문이 고친다).
 * 저장한 원문은 바꾸지 않고 값은 공백만 걷어 옮긴다.
 */
class DailyCloseHistory {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter DATE = DateTimeFormatter.BASIC_ISO_DATE;

    private final BackgroundRepository repository;
    private final DailyPriceReader reader;

    DailyCloseHistory(BackgroundRepository repository, DailyPriceReader reader) {
        this.repository = repository;
        this.reader = reader;
    }

    DailyPayload.DailyCloses collect(String symbol, OffsetDateTime since, OffsetDateTime asOf, int max) {
        // 기준 시각의 한국 날짜보다 뒤 일자는 쓰지 않는다. 일자는 yyyyMMdd라 문자열 비교가 날짜 비교와 같다.
        String lastDate = asOf.atZoneSameInstant(KST).format(DATE);
        Map<String, DailyPriceReader.Close> byDate = new HashMap<>();
        OffsetDateTime newest = null;
        for (BackgroundRepository.StoredResponse stored : repository.dailyPriceHistory(symbol, since, asOf)) {
            // 저장소가 기간을 지키더라도 여기서 한 번 더 확인한다. 어기면 판 시각 뒤에 받은 일봉이 조용히 그래프에 들어간다.
            if (stored.fetchedAt().isBefore(since) || stored.fetchedAt().isAfter(asOf)) {
                continue;
            }
            List<DailyPriceReader.Close> closes = reader.closes(stored.response());
            boolean used = false;
            for (DailyPriceReader.Close close : closes) {
                if (close.date().compareTo(lastDate) > 0) {
                    continue;
                }
                used = true;
                // 늦게 받은 원문부터 읽으므로 먼저 넣은 값이 이긴다.
                byDate.putIfAbsent(close.date(), close);
            }
            if (used && (newest == null || stored.fetchedAt().isAfter(newest))) {
                newest = stored.fetchedAt();
            }
        }
        if (byDate.isEmpty()) {
            return DailyPayload.DailyCloses.none();
        }
        List<DailyPriceReader.Close> recent = new ArrayList<>(byDate.values());
        recent.sort(Comparator.comparing(DailyPriceReader.Close::date).reversed());
        boolean truncated = recent.size() > max;
        List<DailyPayload.DailyClose> points = recent.subList(0, Math.min(max, recent.size())).reversed().stream()
                .map(c -> new DailyPayload.DailyClose(c.date(), c.close(), c.rate()))
                .toList();
        return new DailyPayload.DailyCloses(DailyPayload.DAILY_CLOSES_PRESENT, EventFactory.formatKst(newest),
                truncated, points);
    }
}
