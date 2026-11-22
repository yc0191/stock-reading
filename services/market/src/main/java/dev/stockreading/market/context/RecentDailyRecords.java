package dev.stockreading.market.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * 배경 자료로 주는 국내 일봉 원문에서 최근 일자 레코드만 남긴다. 저장한 원문은 기간 그래프용으로 길게 받으므로 그대로 주면
 * 배경 원문 크기 상한을 넘어 '너무 큼'이 된다. 저장한 원문은 바꾸지 않고 응답에 담는 사본만 줄인다.
 *
 * <p>레코드 순서는 응답이 보장하지 않으므로 일자(dt, yyyyMMdd)로 최근 것을 고르고, 남긴 레코드는 원문의 순서를 지킨다.
 * 원문이 JSON이 아니거나 레코드 목록이 없거나 이미 그 수 이하면 원문을 그대로 돌려준다.
 */
final class RecentDailyRecords {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private RecentDailyRecords() {
    }

    static String keep(String response, int records) {
        JsonNode root;
        try {
            root = MAPPER.readTree(response);
        } catch (JacksonException e) {
            return response;
        }
        if (!(root.path("dataBody") instanceof ObjectNode body) || !(body.path(DailyPriceReader.RECORDS) instanceof ArrayNode rows)
                || rows.size() <= records) {
            return response;
        }
        List<JsonNode> all = new ArrayList<>();
        rows.forEach(all::add);
        List<JsonNode> newest = all.stream()
                .sorted(Comparator.comparing((JsonNode row) -> row.path(DailyPriceReader.DATE_FIELD).asString("").strip())
                        .reversed())
                .limit(records)
                .toList();
        ArrayNode kept = MAPPER.createArrayNode();
        all.stream().filter(newest::contains).forEach(kept::add);
        body.set(DailyPriceReader.RECORDS, kept);
        return MAPPER.writeValueAsString(root);
    }
}
