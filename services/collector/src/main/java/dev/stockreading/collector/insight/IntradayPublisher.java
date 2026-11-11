package dev.stockreading.collector.insight;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 장중 조회: 환율종합, 시장종합, 세계지수 아시아·아메리카를 차례로 조회하고 조회 사이에 staggerMs 만큼 쉰다.
 * 이벤트와 키는 장 마감 후 조회와 같은 market.insight 다. 한 건이 실패해도 나머지는 계속한다.
 */
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
public class IntradayPublisher {

    // 장중에는 막 열린 아시아 장과 직전에 끝난 미국 장의 지수만 본다. 유럽은 국내 장중에 열리지 않는다.
    static final List<String> INTRADAY_CONTINENTS = List.of("S", "C");

    private static final Logger log = LoggerFactory.getLogger(IntradayPublisher.class);

    private final InsightPublisher publisher;
    private final InsightSourcePort source;
    private final IntradayProperties properties;
    private final InsightCallPause pause;

    IntradayPublisher(InsightPublisher publisher, InsightSourcePort source, IntradayProperties properties,
                      InsightCallPause pause) {
        this.publisher = publisher;
        this.source = source;
        this.properties = properties;
        this.pause = pause;
    }

    // 4건을 한꺼번에 보내면 같은 1초에 시세 조회 자리를 여럿 빼앗으므로 시차를 둔다. 상한 장치는 조회마다 따로 거친다.
    public void publishIntraday(String correlationId, String scheduledFor) {
        List<Call> calls = new ArrayList<>();
        calls.add(new Call(InsightPublisher.EXCHANGE_RATE_SUMMARY, null, source::exchangeRateSummary));
        calls.add(new Call(InsightPublisher.MARKET_SUMMARY, null, source::marketSummary));
        for (String continent : INTRADAY_CONTINENTS) {
            calls.add(new Call(InsightPublisher.WORLD_INDEX, continent, () -> source.worldIndex(continent)));
        }
        int published = 0;
        for (int i = 0; i < calls.size(); i++) {
            if (i > 0) {
                pause.pause(properties.staggerMs());
            }
            Call call = calls.get(i);
            if (publisher.publishOne(call.apiType(), call.target(), call.fetch(), correlationId, scheduledFor)) {
                published++;
            }
        }
        log.info("sent {} intraday market.insight events corr={} failed={}",
                published, correlationId, calls.size() - published);
    }

    private record Call(String apiType, String target, Supplier<String> fetch) {
    }
}
