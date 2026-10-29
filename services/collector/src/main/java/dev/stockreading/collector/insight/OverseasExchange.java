package dev.stockreading.collector.insight;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SEC 공시 이력의 거래소 이름을 해외 시세 조회의 거래소 코드로 바꾼다.
 * 상장 거래소가 여럿이면 처음으로 바꿀 수 있는 이름을 쓰고, 하나도 없으면 null 이다.
 */
final class OverseasExchange {

    static final String NASDAQ = "NAS";
    static final String NYSE = "NYS";
    static final String AMEX = "AMX";

    // NYSE American 은 예전 이름(NYSE MKT, AMEX)으로 남아 있는 회사가 있어 함께 받는다.
    private static final Map<String, String> CODES = Map.of(
            "NASDAQ", NASDAQ,
            "NYSE", NYSE,
            "NYSE AMERICAN", AMEX,
            "NYSE MKT", AMEX,
            "AMEX", AMEX);

    private OverseasExchange() {
    }

    static String codeOf(List<String> secExchanges) {
        if (secExchanges == null) {
            return null;
        }
        for (String name : secExchanges) {
            if (name == null) {
                continue;
            }
            String code = CODES.get(name.trim().toUpperCase(Locale.ROOT));
            if (code != null) {
                return code;
            }
        }
        return null;
    }
}
