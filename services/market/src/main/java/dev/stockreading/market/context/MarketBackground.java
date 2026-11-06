package dev.stockreading.market.context;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.holdings.EtfInfo;
import dev.stockreading.market.insight.OverseasCloseRate;

/**
 * 배경 자료 한 건과 해외 등락률 한 건을 응답 모양으로 만든다. 변동 한 건(market.context)과 데일리 판(market.daily)이
 * 같은 규칙으로 고르도록 두 응답이 함께 쓴다.
 */
class MarketBackground {

    private final BackgroundRepository repository;
    private final WorldIndexRates worldIndexRates;

    MarketBackground(BackgroundRepository repository, WorldIndexRates worldIndexRates) {
        this.repository = repository;
        this.worldIndexRates = worldIndexRates;
    }

    /** 시장 전체 배경: 환율종합, 시장종합, 세계지수(대륙 순서대로). */
    List<ContextPayload.Background> market(List<String> continents, OffsetDateTime since, OffsetDateTime asOf,
            int maxBytes) {
        List<ContextPayload.Background> background = new ArrayList<>();
        background.add(single(BackgroundKind.EXCHANGE_RATE_SUMMARY, null, since, asOf, maxBytes));
        background.add(single(BackgroundKind.MARKET_SUMMARY, null, since, asOf, maxBytes));
        for (String continent : continents) {
            background.add(worldIndex(continent, since, asOf, maxBytes));
        }
        return background;
    }

    ContextPayload.Background single(
            BackgroundKind kind, String target, OffsetDateTime since, OffsetDateTime asOf, int maxBytes) {
        Optional<BackgroundRepository.StoredResponse> latest = repository.latest(kind, target, since, asOf);
        return latest.map(row -> background(kind, target, row, maxBytes))
                .orElseGet(() -> ContextPayload.Background.missing(kind.wireValue(), target));
    }

    private ContextPayload.Background worldIndex(String continent, OffsetDateTime since, OffsetDateTime asOf,
            int maxBytes) {
        List<BackgroundRepository.StoredResponse> rows = repository.worldIndexRows(continent, since, asOf);
        if (rows.isEmpty()) {
            return ContextPayload.Background.missing(BackgroundKind.WORLD_INDEX.wireValue(), continent);
        }
        BackgroundRepository.StoredResponse latest = rows.get(0);
        return background(BackgroundKind.WORLD_INDEX, continent, latest, maxBytes)
                .withIndexRates(worldIndexRates.compute(latest.response(), rows, since, asOf));
    }

    // 응답의 티커는 요청한 표기(대응표 표기)로 되돌려 브리핑이 tickers와 짝을 지을 수 있게 한다.
    static ContextPayload.CloseRate closeRate(String ticker, OverseasCloseRate rate) {
        return new ContextPayload.CloseRate(ticker, rate.date(), EventFactory.formatKst(rate.closeAt()),
                rate.rate().strip(), EventFactory.formatKst(rate.fetchedAt()));
    }

    /** ETF 정보를 응답 모양으로 만든다. 구성 종목은 비중 순위 순으로 maxMembers개까지 담는다. */
    static ContextPayload.EtfInfo etfInfo(EtfInfo info, int maxMembers) {
        List<ContextPayload.EtfMember> members = info.members().stream().limit(maxMembers)
                .map(m -> new ContextPayload.EtfMember(m.market(), m.symbol(), m.name(),
                        m.weight() == null ? null : m.weight().stripTrailingZeros().toPlainString(), m.rank()))
                .toList();
        return new ContextPayload.EtfInfo(info.name(), info.tracks(), info.sector(), members,
                info.members().size() > members.size());
    }

    private static ContextPayload.Background background(
            BackgroundKind kind, String target, BackgroundRepository.StoredResponse row, int maxBytes) {
        String fetchedAt = EventFactory.formatKst(row.fetchedAt());
        int bytes = row.response().getBytes(StandardCharsets.UTF_8).length;
        if (bytes > maxBytes) {
            return ContextPayload.Background.tooLarge(kind.wireValue(), target, fetchedAt, bytes);
        }
        return ContextPayload.Background.ok(kind.wireValue(), target, row.response(), fetchedAt);
    }
}
