package dev.stockreading.collector.marketdata;

import java.math.BigDecimal;
import java.util.List;

import tools.jackson.databind.json.JsonMapper;

/** 테스트용 칸 이름(가짜)으로 만든 검사. 실제 제공자 칸 이름은 공개 파일에 쓰지 않는다. */
final class PriceChecks {

    static final String BASE = "TEST_BASE";
    static final String BASE_ALT = "TEST_BASE_ALT";
    static final String CHANGE = "TEST_CHANGE";
    static final String SIGN = "TEST_SIGN";
    static final String RATE = "TEST_RATE";
    static final String HIGH = "TEST_HIGH";
    static final String LOW = "TEST_LOW";

    private PriceChecks() {
    }

    static PriceCheckProperties properties(boolean enabled, String tolerance) {
        return new PriceCheckProperties(enabled, new BigDecimal(tolerance), List.of(BASE, BASE_ALT), CHANGE, SIGN,
                List.of("U"), List.of("F"), List.of("D"), RATE, HIGH, LOW);
    }

    /** 기본 허용 오차(0.3%)로 켠 검사. */
    static PriceFieldCheck fake() {
        return new PriceFieldCheck(properties(true, "0.003"), JsonMapper.builder().build());
    }

    /** 가짜 응답 본문. 값은 문자열 그대로 넣으며 null 이면 그 칸을 뺀다. */
    static String body(String price, String base, String change, String sign, String rate, String high, String low) {
        StringBuilder data = new StringBuilder("\"now_prc\":\"" + price + "\"");
        append(data, BASE, base);
        append(data, CHANGE, change);
        append(data, SIGN, sign);
        append(data, RATE, rate);
        append(data, HIGH, high);
        append(data, LOW, low);
        return "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{" + data + "}}";
    }

    private static void append(StringBuilder data, String field, String value) {
        if (value != null) {
            data.append(",\"").append(field).append("\":\"").append(value).append('"');
        }
    }
}
