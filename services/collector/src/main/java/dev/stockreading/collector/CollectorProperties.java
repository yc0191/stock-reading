package dev.stockreading.collector;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector")
public record CollectorProperties(List<String> symbols, long tickIntervalMs) {
}
