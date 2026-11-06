package dev.stockreading.market.context;

import java.util.Optional;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 국내 일봉 원문에서 일자가 가장 최근인 레코드의 일자·종가·전일 종가·등락률을 읽는다.
 *
 * <p>값은 계산하지 않고 원문을 공백만 걷어 옮긴다. 국내 일봉은 원문 등락률이 종가·전일 종가로 계산한 값과 맞고
 * (세계지수처럼 0으로 오지 않는다), 외부 응답은 가공하지 않는다는 규칙을 따른다. 레코드 순서는 응답이 보장하지 않으므로
 * 위치가 아니라 일자로 고른다. 일자는 yyyyMMdd라 문자열 비교가 날짜 비교와 같다.
 */
class DailyPriceReader {

    static final String DATE_FIELD = "dt";
    static final String CLOSE_FIELD = "cls_prc_p2";
    static final String PREV_CLOSE_FIELD = "bdy_cls_prc_p2";
    static final String RATE_FIELD = "up_dwn_r_p2";

    private final ObjectMapper mapper;

    DailyPriceReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** @return 네 값을 모두 읽은 레코드 중 일자가 가장 최근인 것. 없거나 원문이 JSON이 아니면 빈 값 */
    Optional<Record> latest(String response) {
        JsonNode records;
        try {
            records = mapper.readTree(response).path("dataBody").path("out2");
        } catch (JacksonException e) {
            return Optional.empty();
        }
        Record best = null;
        for (JsonNode node : records) {
            String date = text(node, DATE_FIELD);
            String close = text(node, CLOSE_FIELD);
            String prevClose = text(node, PREV_CLOSE_FIELD);
            String rate = text(node, RATE_FIELD);
            if (date == null || close == null || prevClose == null || rate == null) {
                continue;
            }
            if (best == null || date.compareTo(best.date()) > 0) {
                best = new Record(date, close, prevClose, rate);
            }
        }
        return Optional.ofNullable(best);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isString() || value.asString().isBlank()) {
            return null;
        }
        return value.asString().strip();
    }

    record Record(String date, String close, String prevClose, String rate) {
    }
}
