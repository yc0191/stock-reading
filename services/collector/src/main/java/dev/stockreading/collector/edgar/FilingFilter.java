package dev.stockreading.collector.edgar;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** 공시 형식과 접수 시각으로 거른다. 변동 시각 기준의 시간 규칙은 조회하는 쪽이 적용하고, 여기서는 수집 범위만 정한다. */
final class FilingFilter {

    record Result(List<Filing> passed, int otherForm, int tooOld) {
    }

    private FilingFilter() {
    }

    static Result apply(List<Filing> filings, Set<String> forms, Instant now, Duration lookback) {
        Instant oldest = now.minus(lookback);
        List<Filing> passed = new ArrayList<>();
        int otherForm = 0;
        int tooOld = 0;
        for (Filing filing : filings) {
            if (!forms.contains(filing.form())) {
                otherForm++;
            } else if (filing.acceptedAt().toInstant().isBefore(oldest)) {
                tooOld++;
            } else {
                passed.add(filing);
            }
        }
        return new Result(passed, otherForm, tooOld);
    }
}
