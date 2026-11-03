package dev.stockreading.market.context;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 배경 자료 응답(market.context)의 안전 상한. 기간과 상한의 원본은 브리핑 설정이고, 요청값이 이보다 크면 이 값까지만 준다.
 *
 * @param maxAge               받은 시각(해외 등락률은 장 마감 시각)이 기준 시각보다 이 기간 넘게 이르면 없음으로 본다
 * @param maxResponseBytes     배경 자료 원문 1건의 최대 크기(UTF-8 바이트)
 * @param maxTickers           응답에 담을 미국 종목 티커의 최대 개수
 * @param worldIndexContinents 배경 자료로 줄 세계지수 대륙구분(설정 순서대로 담는다)
 */
@ConfigurationProperties("market.context")
public record ContextProperties(
        Duration maxAge,
        int maxResponseBytes,
        int maxTickers,
        List<String> worldIndexContinents) {

    public ContextProperties {
        // 상한이 비면 응답이 끝없이 커지거나 모든 자료가 없음이 되어 조용히 틀린 응답이 나가므로 시작 단계에서 막는다.
        if (maxAge == null || maxAge.isNegative() || maxAge.isZero()) {
            throw new IllegalArgumentException("market.context.max-age must be positive");
        }
        if (maxResponseBytes < 1 || maxTickers < 1) {
            throw new IllegalArgumentException("market.context.max-response-bytes and max-tickers must be positive");
        }
        worldIndexContinents = worldIndexContinents == null ? List.of() : List.copyOf(worldIndexContinents);
    }
}
