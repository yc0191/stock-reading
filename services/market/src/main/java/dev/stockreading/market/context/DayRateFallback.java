package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.snapshot.ChangeRate;
import dev.stockreading.market.snapshot.RawQuote;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 범위 안 일봉이 없는 ETF의 하루 등락 대체값. 받은 시각이 [since, asOf] 안인 마지막 장중 시세의 가격과 그 원문의
 * 전일 종가(후보 칸 과반, 전일 대비 급변동 감지와 같은 규칙)로 코드가 계산한다(ADR-007). 응답의 등락률 칸은 옮기지 않는다.
 */
class DayRateFallback {

    private final IntradayQuotePort quotes;
    private final ObjectMapper mapper;
    private final List<String> prevCloseFields;

    DayRateFallback(IntradayQuotePort quotes, ObjectMapper mapper, List<String> prevCloseFields) {
        this.quotes = quotes;
        this.mapper = mapper;
        this.prevCloseFields = prevCloseFields == null ? List.of() : prevCloseFields;
    }

    /** 계산할 수 없으면(시세 없음, 전일 종가를 읽지 못함·0 이하) 일봉 없는 ETF를 그대로 돌려준다. */
    DailyPayload.Etf apply(DailyPayload.Etf without, OffsetDateTime since, OffsetDateTime asOf) {
        Optional<IntradayQuotePort.Quote> found = quotes.lastReceived(without.symbol(), since, asOf);
        if (found.isEmpty() || prevCloseFields.isEmpty()) {
            return without;
        }
        IntradayQuotePort.Quote quote = found.get();
        // 저장소가 기간을 지키더라도 여기서 한 번 더 확인한다. 어기면 판 시각 뒤에 받은 시세가 조용히 나간다.
        if (quote.receivedAt().isBefore(since) || quote.receivedAt().isAfter(asOf) || quote.price() <= 0) {
            return without;
        }
        JsonNode raw = parse(quote.raw());
        Long prevClose = raw == null ? null : RawQuote.majority(raw, prevCloseFields);
        // 전일 종가 0은 등락률을 계산할 수 없다. 예외로 넘기면 로그에서 원인을 구분할 수 없다.
        if (prevClose == null || prevClose <= 0) {
            return without;
        }
        return without.withIntradayFallback(Long.toString(prevClose),
                ChangeRate.percent(prevClose, quote.price()).toPlainString(),
                new DailyPayload.IntradayFallback(Long.toString(quote.price()), EventFactory.formatKst(quote.receivedAt())));
    }

    private JsonNode parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return mapper.readTree(raw);
        } catch (JacksonException e) {
            return null;
        }
    }
}
