package dev.stockreading.collector;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector")
public record CollectorProperties(List<String> symbols, long tickIntervalMs, long maxTickAgeMs) {

    static final long DEFAULT_MAX_TICK_AGE_MS = 3_000;

    public CollectorProperties {
        if (maxTickAgeMs <= 0) {
            maxTickAgeMs = DEFAULT_MAX_TICK_AGE_MS;
        }
    }
}
