package dev.stockreading.collector;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector")
public record CollectorProperties(long tickIntervalMs) {
}
