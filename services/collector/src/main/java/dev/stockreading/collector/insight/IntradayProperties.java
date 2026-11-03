package dev.stockreading.collector.insight;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 장중 조회 설정. 발행 주기(cron)는 enabled 가 true 일 때만 쓰인다.
 * start·end 는 한국 시간 "HH:mm" 이고 양끝을 포함한다. cron 이 시간 단위로만 범위를 정할 수 있어 끝 시각은 여기서 거른다.
 * staggerMs 는 장중 조회 사이의 대기다.
 */
@ConfigurationProperties("collector.intraday")
public record IntradayProperties(boolean enabled, String cron, String start, String end, long staggerMs) {

    static final String DEFAULT_START = "09:00";
    static final String DEFAULT_END = "15:30";
    // 조회 4건 사이 대기 3번이 Kafka 의 poll 간격 한도(기본 5분)를 넘지 않게 상한을 둔다.
    static final long MAX_STAGGER_MS = 60_000;

    public IntradayProperties {
        if (start == null || start.isBlank()) {
            start = DEFAULT_START;
        }
        if (end == null || end.isBlank()) {
            end = DEFAULT_END;
        }
        if (parse(start, "start").isAfter(parse(end, "end"))) {
            throw new IllegalArgumentException("collector.intraday.start 가 end 보다 늦다");
        }
        if (staggerMs < 0 || staggerMs > MAX_STAGGER_MS) {
            throw new IllegalArgumentException("collector.intraday.stagger-ms 는 0 이상 " + MAX_STAGGER_MS + " 이하여야 한다");
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
            throw new IllegalArgumentException("collector.intraday." + name + " 는 HH:mm 형식이어야 한다", e);
        }
    }
}
