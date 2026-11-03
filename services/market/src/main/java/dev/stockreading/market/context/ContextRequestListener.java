package dev.stockreading.market.context;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.holdings.HoldingsPort;
import dev.stockreading.market.insight.OverseasCloseRate;
import dev.stockreading.market.insight.OverseasCloseRateQuery;
import tools.jackson.databind.ObjectMapper;

/**
 * 변동 종목 한 개의 배경 자료, 담긴 미국 종목 티커, 티커별 직전 정규장 등락률을 답한다.
 *
 * <p>상태형 자료라 기준 시각(asOf) 뒤에 받은 자료는 쓰지 않는다. 받은 시각 하한은 요청 since와 기준 시각에서
 * 안전 상한 기간을 뺀 시각 중 늦은 쪽이다. 저장 상태를 바꾸지 않는 조회라 같은 요청(eventId)을 다시 받아도
 * 같은 내용으로 다시 답할 뿐 부작용이 없고, 브리핑은 jobId·seq로 짝을 지어 중복 응답을 버린다.
 */
@Component
class ContextRequestListener {

    private final BackgroundRepository repository;
    private final WorldIndexRates worldIndexRates;
    private final OverseasCloseRateQuery closeRateQuery;
    private final HoldingsPort holdings;
    private final ContextProperties properties;
    private final ContextPublisher publisher;
    private final ObjectMapper mapper;

    ContextRequestListener(BackgroundRepository repository, WorldIndexRates worldIndexRates,
            OverseasCloseRateQuery closeRateQuery, HoldingsPort holdings, ContextProperties properties,
            ContextPublisher publisher, ObjectMapper mapper) {
        this.repository = repository;
        this.worldIndexRates = worldIndexRates;
        this.closeRateQuery = closeRateQuery;
        this.holdings = holdings;
        this.properties = properties;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.MARKET_CONTEXT_REQUESTED, groupId = "market")
    void onRequest(String message) {
        ContextRequestEnvelope event = mapper.readValue(message, ContextRequestEnvelope.class);
        publisher.publish(answer(event.payload()), event.eventId(), event.correlationId());
    }

    ContextPayload answer(ContextRequestEnvelope.RequestPayload request) {
        OffsetDateTime asOf = OffsetDateTime.parse(request.asOf());
        OffsetDateTime requestedSince = OffsetDateTime.parse(request.since());
        if (request.maxResponseBytes() < 1 || request.maxTickers() < 1 || requestedSince.isAfter(asOf)) {
            // 잘못된 요청은 응답하지 않는다. 브리핑이 제한 시간 뒤 다시 보내거나 실패로 기록한다.
            throw new IllegalArgumentException("invalid context request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }
        OffsetDateTime floor = asOf.minus(properties.maxAge());
        OffsetDateTime since = requestedSince.isAfter(floor) ? requestedSince : floor;
        int maxBytes = Math.min(request.maxResponseBytes(), properties.maxResponseBytes());
        int maxTickers = Math.min(request.maxTickers(), properties.maxTickers());

        List<String> allTickers = holdings.topTickers(request.symbol());
        List<String> tickers = allTickers.stream().limit(maxTickers).toList();
        List<ContextPayload.CloseRate> closeRates = new ArrayList<>();
        for (String ticker : tickers) {
            closeRateQuery.find(ticker, asOf, since).ifPresent(rate -> closeRates.add(closeRate(ticker, rate)));
        }

        List<ContextPayload.Background> background = new ArrayList<>();
        background.add(single(BackgroundKind.EXCHANGE_RATE_SUMMARY, null, since, asOf, maxBytes));
        background.add(single(BackgroundKind.MARKET_SUMMARY, null, since, asOf, maxBytes));
        for (String continent : properties.worldIndexContinents()) {
            background.add(worldIndex(continent, since, asOf, maxBytes));
        }
        background.add(single(BackgroundKind.DAILY_PRICE, request.symbol(), since, asOf, maxBytes));

        return new ContextPayload(request.jobId(), request.seq(), request.symbol(), tickers, closeRates, background,
                allTickers.size() > tickers.size());
    }

    // 응답의 티커는 요청한 표기(대응표 표기)로 되돌려 브리핑이 tickers와 짝을 지을 수 있게 한다.
    private static ContextPayload.CloseRate closeRate(String ticker, OverseasCloseRate rate) {
        return new ContextPayload.CloseRate(ticker, rate.date(), EventFactory.formatKst(rate.closeAt()),
                rate.rate().strip(), EventFactory.formatKst(rate.fetchedAt()));
    }

    private ContextPayload.Background single(
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
