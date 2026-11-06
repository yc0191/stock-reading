package dev.stockreading.collector.schedule;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 1초 시세 tick 을 발행하는 시간대. start·end 는 한국 시간 "HH:mm" 이고 양끝을 포함한다.
 * 장 밖에서는 현재가가 전일 종가 그대로라 조회해도 쓸 값이 없고, 그 시간에도 응답 칸이 깨져 오므로 발행하지 않는다.
 * 뉴스 tick 은 이 시간대와 상관없이 발행한다.
 */
@ConfigurationProperties("collector.quote-hours")
public record QuoteHoursProperties(String start, String end) {

    static final String DEFAULT_START = "09:00";
    static final String DEFAULT_END = "15:30";

    public QuoteHoursProperties {
        if (start == null || start.isBlank()) {
            start = DEFAULT_START;
        }
        if (end == null || end.isBlank()) {
            end = DEFAULT_END;
        }
        if (parse(start, "start").isAfter(parse(end, "end"))) {
            throw new IllegalArgumentException("collector.quote-hours.start 가 end 보다 늦다");
        }
    }

    LocalTime startTime() {
        return LocalTime.parse(start);
    }

    LocalTime endTime() {
        return LocalTime.parse(end);
    }

    private static LocalTime parse(String value, String name) {
        try {
            return LocalTime.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("collector.quote-hours." + name + " 는 HH:mm 형식이어야 한다", e);
        }
    }
}
