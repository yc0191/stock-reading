package dev.stockreading.collector.edgar;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;

// userAgent 는 SEC 가 요구하는 "회사명 연락이메일" 값이며 개인 정보라서 환경변수로만 받는다.
// forms 는 "8-K,8-K/A,6-K" 처럼 쉼표로 이은 공시 형식 목록이다.
// maxCallsPerSecond 는 SEC 공정 접근 한도(초당 10회)를 넘지 않게 1~10 만 허용한다.
// lookbackHours 는 접수 시각이 이 시간 안인 공시만 발행하는 범위다. 재시작 뒤 재발행 범위도 이 값으로 정해진다.
@ConfigurationProperties("collector.edgar")
public record EdgarProperties(
        String baseUrl, String tickerMapUrl, String archivesBaseUrl, String userAgent,
        int timeoutSeconds, int maxCallsPerSecond, String forms, int lookbackHours) {

    static final int SEC_MAX_CALLS_PER_SECOND = 10;
    private static final Pattern FORM = Pattern.compile("[0-9A-Z][0-9A-Z/-]{0,19}");

    public EdgarProperties {
        if (maxCallsPerSecond < 1 || maxCallsPerSecond > SEC_MAX_CALLS_PER_SECOND) {
            throw new IllegalArgumentException("collector.edgar.max-calls-per-second 는 1~10 이어야 합니다");
        }
        if (timeoutSeconds < 1) {
            throw new IllegalArgumentException("collector.edgar.timeout-seconds 는 1 이상이어야 합니다");
        }
        if (lookbackHours < 1) {
            throw new IllegalArgumentException("collector.edgar.lookback-hours 는 1 이상이어야 합니다");
        }
        // 형식 목록이 비거나 틀리면 모든 공시가 조용히 걸러지므로 시작 단계에서 실패시킨다.
        parseForms(forms);
    }

    /** 수집할 공시 형식(대문자, 설정 순서). */
    public Set<String> formSet() {
        return parseForms(forms);
    }

    boolean hasUserAgent() {
        return userAgent != null && !userAgent.isBlank();
    }

    static Set<String> parseForms(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("EDGAR_FORMS 가 비어 있습니다. 예: 8-K,8-K/A,6-K");
        }
        Set<String> result = new LinkedHashSet<>();
        String[] parts = raw.split(",", -1);
        for (int i = 0; i < parts.length; i++) {
            String form = parts[i].trim().toUpperCase(Locale.ROOT);
            if (!FORM.matcher(form).matches()) {
                throw new IllegalArgumentException("EDGAR_FORMS 의 " + (i + 1) + "번째 항목 형식이 잘못되었습니다");
            }
            result.add(form);
        }
        return result;
    }

    // 연락 이메일이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "EdgarProperties[baseUrl=" + baseUrl + ", userAgent=***, maxCallsPerSecond=" + maxCallsPerSecond
                + ", forms=" + Arrays.toString(formSet().toArray()) + ", lookbackHours=" + lookbackHours + "]";
    }
}
