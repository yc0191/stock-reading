package dev.stockreading.market;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** @param moveThresholdPercent 직전 스냅샷 대비 이 값(%) 이상 변하면(절댓값) "변동"으로 본다. */
@ConfigurationProperties("market")
public record MarketProperties(BigDecimal moveThresholdPercent) {
}
