package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.databind.JsonNode;

/** 시세 응답 원문(raw)에서 가격 칸을 읽는 규칙. 변동 감지(전일 종가)와 개장 흐름(시가)이 같은 규칙을 쓴다. */
public final class RawQuote {

    private RawQuote() {
    }

    /** 점(.) 경로로 칸을 찾아 양의 정수 가격으로 읽는다. 없거나 숫자가 아니거나 0 이하이거나 소수이면 null. */
    public static Long readField(JsonNode root, String field) {
        JsonNode node = root;
        for (String name : field.split("\\.")) {
            node = node.path(name);
        }
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        BigDecimal value;
        try {
            value = new BigDecimal(node.asString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
        if (value.signum() <= 0 || value.stripTrailingZeros().scale() > 0) {
            return null;
        }
        try {
            return value.longValueExact();
        } catch (ArithmeticException e) {
            return null;
        }
    }

    /**
     * 읽을 수 있는 값 중 설정한 칸의 절반을 넘는 칸이 같은 값일 때만 그 값을 돌려준다. 제공자가 전일 종가 칸을
     * 하루 안에서도 깨뜨려 보내서(2026-11-04 확인) 한 칸만 믿을 수 없다. 다수가 없으면 null.
     */
    public static Long majority(JsonNode root, List<String> fields) {
        Map<Long, Integer> votes = new HashMap<>();
        for (String field : fields) {
            Long value = readField(root, field);
            if (value != null) {
                votes.merge(value, 1, Integer::sum);
            }
        }
        List<Long> majority = new ArrayList<>();
        votes.forEach((value, count) -> {
            if (count * 2 > fields.size()) {
                majority.add(value);
            }
        });
        return majority.size() == 1 ? majority.get(0) : null;
    }
}
