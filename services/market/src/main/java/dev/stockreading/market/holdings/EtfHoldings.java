package dev.stockreading.market.holdings;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * ETF 한 종목과 그 ETF가 담은 미국 상위 종목 티커 목록. 티커 순서는 설정에 적힌 순서다.
 * 수집기와 같은 설정 값을 같은 형식으로 읽는다(서비스끼리 코드를 공유하지 않아 복제해 둔다, ADR-006).
 */
record EtfHoldings(String etfCode, List<String> tickers) {

    // 이벤트 계약의 종목코드 형식과 같다. 다른 형식의 ETF 코드는 뒤 단계에서 조용히 버려지므로 설정 단계에서 막는다.
    private static final Pattern ETF_CODE = Pattern.compile("[0-9A-Z]{6}");

    // 미국 티커는 영문 대문자·숫자 1~6자이고, 종류(클래스) 구분이 있으면 "."이나 "-" 뒤에 1~2자가 붙는다.
    // 수집기와 같은 설정 값을 읽으므로 수집기와 같은 규칙으로 두 구분자를 모두 허용한다.
    private static final Pattern TICKER = Pattern.compile("[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?");

    public EtfHoldings {
        tickers = List.copyOf(tickers);
    }

    /**
     * "ETF코드=티커,티커;ETF코드2=티커" 형식을 읽는다. 비어 있으면 빈 목록이고, 잘못된 항목이 하나라도 있으면
     * IllegalArgumentException 으로 실패한다. 공백뿐인 항목(끝의 ";" 포함)은 무시한다.
     * 같은 ETF 코드가 두 번 나오면 실패하고, 한 ETF 안의 같은 티커는 한 번만 남긴다. 종류 구분은 "-" 로 맞춰 돌려준다.
     * 보유 종목이 드러나지 않도록 메시지에는 코드와 티커 원문을 넣지 않고, 세미콜론으로 나눈 설정의
     * 몇 번째 항목인지와 오류 종류만 적는다.
     */
    static List<EtfHoldings> parseValidated(String spec) {
        List<EtfHoldings> result = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return result;
        }
        Set<String> seenEtfCodes = new LinkedHashSet<>();
        int position = 0;
        for (String entry : spec.split(";")) {
            position++;
            if (entry.isBlank()) {
                continue;
            }
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                throw structureError(position);
            }
            String etfCode = entry.substring(0, separator).trim();
            if (etfCode.isEmpty()) {
                throw structureError(position);
            }
            if (!ETF_CODE.matcher(etfCode).matches()) {
                throw formatError(position, "ETF 코드 형식 오류가 있습니다(6자리 숫자·대문자여야 합니다)");
            }
            if (!seenEtfCodes.add(etfCode)) {
                throw formatError(position, "ETF 코드 중복 오류가 있습니다(같은 ETF 코드는 한 번만 적습니다)");
            }
            Set<String> tickers = new LinkedHashSet<>();
            for (String ticker : entry.substring(separator + 1).split(",")) {
                if (ticker.isBlank()) {
                    continue;
                }
                String trimmed = ticker.trim();
                if (!TICKER.matcher(trimmed).matches()) {
                    throw formatError(position,
                            "티커 형식 오류가 있습니다(영문 대문자·숫자 1~6자, 종류 구분은 . 또는 - 뒤 1~2자)");
                }
                // 종류 구분 표기는 SEC 와 같은 "-" 로 맞춘다. 수집기가 이 표기로 저장하므로 같은 표기로 찾아야 한다.
                tickers.add(trimmed.replace('.', '-'));
            }
            if (tickers.isEmpty()) {
                throw structureError(position);
            }
            result.add(new EtfHoldings(etfCode, new ArrayList<>(tickers)));
        }
        return result;
    }

    private static IllegalArgumentException structureError(int position) {
        return formatError(position, "구조 오류가 있습니다(ETF코드=티커[,티커...] 형태여야 합니다)");
    }

    private static IllegalArgumentException formatError(int position, String kind) {
        return new IllegalArgumentException("보유 종목 설정의 " + position + "번째 항목에 " + kind);
    }
}
