package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.DailyCollectionPayload;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.insight.InsightPublisher;
import dev.stockreading.collector.marketdata.MarketDayPort;
import dev.stockreading.collector.trace.CorrelationId;
import tools.jackson.databind.ObjectMapper;

/**
 * 데일리 판(아침 판, 마감 후 판)의 시작 신호인 daily.collection.completed 를 발행한다. 자료는 모으지 않는다.
 * 아침 판은 개장 흐름 구간이 끝나는 시각(morning-cron)에 그날 장 시작 전 수집 결과를 담아 내고, 마감 후 판은 마감 후 수집이
 * 끝나는 즉시 CollectorJobs 가 부른다. 휴장일(시세 제공자의 장 운영 상태로 판정, 판정하지 못하면 영업일)에는 판 시각에
 * '휴장이라 수집 안 함'을 낸다. collector.daily-briefing.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.daily-briefing.enabled", havingValue = "true")
class DailyCollectionPublisher {

    static final String EVENT_TYPE = "daily.collection.completed";
    static final String EDITION_MORNING = "morning";
    static final String EDITION_POST_CLOSE = "post-close";
    static final String STATUS_COLLECTED = "collected";
    static final String STATUS_MARKET_CLOSED = "market-closed";
    static final String SEC_FILINGS = "sec-filings";
    // 그 수집이 통째로 돌지 않았거나 실패했을 때 못 받은 자료로 싣는 종류.
    static final List<String> PRE_OPEN_SOURCES =
            List.of(InsightPublisher.WORLD_INDEX, InsightPublisher.OVERSEAS_DAILY_PRICE, SEC_FILINGS);
    static final List<String> POST_CLOSE_SOURCES = List.of(InsightPublisher.DAILY_PRICE, InsightPublisher.WORLD_INDEX,
            InsightPublisher.EXCHANGE_RATE_SUMMARY, InsightPublisher.MARKET_SUMMARY);

    private static final Logger log = LoggerFactory.getLogger(DailyCollectionPublisher.class);
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");

    private final MarketDayPort marketDay;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;
    // 그날 장 시작 전 수집이 못 받은 자료. 아침 판 알림 때 읽는다. 날짜가 다르면 그날은 돌지 않은 것이다.
    private final AtomicReference<PreOpenResult> preOpen = new AtomicReference<>();

    DailyCollectionPublisher(MarketDayPort marketDay, KafkaTemplate<String, String> kafka, ObjectMapper mapper,
                             Clock clock) {
        this.marketDay = marketDay;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 장 시작 전 수집이 끝날 때(타이머·수동 실행 모두) 부른다. 같은 날 다시 돌면 나중 결과로 바꾼다. */
    void recordPreOpen(Collection<String> missingSources) {
        preOpen.set(new PreOpenResult(today(), List.copyOf(missingSources)));
    }

    @Scheduled(cron = "${collector.daily-briefing.morning-cron}", zone = "Asia/Seoul")
    void publishMorning() {
        String scheduledFor = now();
        String correlationId = "daily-" + EDITION_MORNING + "-" + scheduledFor;
        // 메시지 처리 밖(타이머 스레드)에서 찍는 줄이라 알림의 correlationId 를 직접 로그 문맥에 둔다.
        CorrelationId.within(correlationId, () -> {
            if (!marketDay.isBusinessDay()) {
                publish(EDITION_MORNING, STATUS_MARKET_CLOSED, scheduledFor, correlationId, List.of());
                return;
            }
            PreOpenResult result = preOpen.get();
            if (result == null || !result.day().equals(today())) {
                log.warn("pre-open collection did not run today; morning edition starts with no pre-open data");
                publish(EDITION_MORNING, STATUS_COLLECTED, scheduledFor, correlationId, PRE_OPEN_SOURCES);
                return;
            }
            publish(EDITION_MORNING, STATUS_COLLECTED, scheduledFor, correlationId, result.missingSources());
        });
    }

    /**
     * 휴장일에만 마감 후 판의 '휴장이라 수집 안 함'을 낸다. 영업일에는 마감 후 수집이 끝날 때 publishPostClose 가 낸다.
     * 마감 후 수집 타이머(PostCloseTimer)가 꺼져 있어도 휴장 알림은 나가도록 따로 둔다.
     */
    @Scheduled(cron = "${collector.daily-briefing.post-close-cron}", zone = "Asia/Seoul")
    void publishPostCloseHoliday() {
        String scheduledFor = now();
        String correlationId = "daily-" + EDITION_POST_CLOSE + "-" + scheduledFor;
        CorrelationId.within(correlationId, () -> {
            if (!marketDay.isBusinessDay()) {
                publish(EDITION_POST_CLOSE, STATUS_MARKET_CLOSED, scheduledFor, correlationId, List.of());
            }
        });
    }

    /** 마감 후 수집(타이머·수동 실행 모두)이 끝나면 부른다. 판의 기준 시각은 수집이 끝난 지금이다. */
    void publishPostClose(String correlationId, Collection<String> missingSources) {
        publish(EDITION_POST_CLOSE, STATUS_COLLECTED, now(), correlationId, List.copyOf(missingSources));
    }

    private void publish(String edition, String status, String scheduledFor, String correlationId,
                         List<String> missingSources) {
        try {
            Envelope<DailyCollectionPayload> event = EventFactory.create(clock, EVENT_TYPE, OffsetDateTime.now(clock),
                    correlationId, new DailyCollectionPayload(edition, status, scheduledFor, missingSources));
            kafka.send(Topics.DAILY_COLLECTION_COMPLETED, edition, mapper.writeValueAsString(event))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.warn("daily collection send failed edition={} errorType={}",
                                    edition, error.getClass().getSimpleName());
                        }
                    });
            log.info("published daily.collection.completed edition={} status={} scheduledFor={} missingSources={}",
                    edition, status, scheduledFor, missingSources);
        } catch (RuntimeException e) {
            log.warn("daily collection publish failed edition={} errorType={}", edition, e.getClass().getSimpleName());
        }
    }

    private String now() {
        return EventFactory.format(OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS));
    }

    private LocalDate today() {
        return OffsetDateTime.now(clock).atZoneSameInstant(SEOUL).toLocalDate();
    }

    private record PreOpenResult(LocalDate day, List<String> missingSources) {
    }
}
