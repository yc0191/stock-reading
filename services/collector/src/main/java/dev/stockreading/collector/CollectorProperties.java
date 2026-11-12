package dev.stockreading.collector;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param quotePerTick 1초 현재가 한 회차에서 부르는 종목 수. 종목이 더 많으면 회차마다 목록을 돌며 번갈아 부른다.
 */
@ConfigurationProperties("collector")
public record CollectorProperties(long tickIntervalMs, @DefaultValue("8") int quotePerTick) {
}
