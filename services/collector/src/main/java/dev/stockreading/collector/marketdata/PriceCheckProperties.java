package dev.stockreading.collector.marketdata;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 현재가를 같은 응답의 다른 칸과 맞춰 보는 검사의 설정. 칸 이름과 부호 구분 값은 application.yml 에 확인한 값을
 * 기본값으로 두고 환경변수로 덮어쓸 수 있다. 이름이 비어 있는 검사는 판단하지 않는다.
 *
 * @param enabled 끄면 검사하지 않고 모든 응답을 시세로 보낸다
 * @param tolerance 가격 비율로 준 허용 오차(0.003 = 0.3%). 0 이상 1 미만
 * @param baseFields 전일 종가로 쓸 수 있는 칸 이름들. 하나라도 맞으면 전일대비·등락률 검사를 통과한다
 * @param changeField 전일대비(절댓값) 칸 이름
 * @param changeSignField 전일대비 부호 구분 칸 이름
 * @param signUp 부호 구분 칸에서 상승을 뜻하는 값들
 * @param signFlat 부호 구분 칸에서 보합을 뜻하는 값들
 * @param signDown 부호 구분 칸에서 하락을 뜻하는 값들
 * @param rateField 등락률(%) 칸 이름
 * @param highField 당일 고가 칸 이름
 * @param lowField 당일 저가 칸 이름
 */
@ConfigurationProperties("collector.broker.price-check")
public record PriceCheckProperties(
        boolean enabled,
        BigDecimal tolerance,
        List<String> baseFields,
        String changeField,
        String changeSignField,
        List<String> signUp,
        List<String> signFlat,
        List<String> signDown,
        String rateField,
        String highField,
        String lowField) {

    // 2026-11-04 장중 실데이터에서 중간가 체결(반 호가) 차이는 넘기고 깨진 값은 걸렀던 값이다.
    static final BigDecimal DEFAULT_TOLERANCE = new BigDecimal("0.003");

    public PriceCheckProperties {
        if (tolerance == null) {
            tolerance = DEFAULT_TOLERANCE;
        }
        if (tolerance.signum() < 0 || tolerance.compareTo(BigDecimal.ONE) >= 0) {
            throw new IllegalArgumentException("collector.broker.price-check.tolerance 는 0 이상 1 미만이어야 합니다");
        }
        baseFields = names(baseFields);
        signUp = names(signUp);
        signFlat = names(signFlat);
        signDown = names(signDown);
    }

    /** 켜져 있지만 칸 이름이 하나도 없어 아무 검사도 할 수 없는지. */
    boolean noFieldsConfigured() {
        return baseFields.isEmpty() && isBlank(highField) && isBlank(lowField);
    }

    // 빈 환경변수는 [""] 로 묶이므로 빈 이름을 걸러 낸다.
    private static List<String> names(List<String> values) {
        return values == null ? List.of() : values.stream().map(String::trim).filter(v -> !v.isEmpty()).toList();
    }

    static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
