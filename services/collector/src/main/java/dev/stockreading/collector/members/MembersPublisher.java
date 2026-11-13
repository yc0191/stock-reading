package dev.stockreading.collector.members;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import tools.jackson.databind.ObjectMapper;

/**
 * 출처 번호가 있는 ETF 중 오늘 받을 차례인 ETF의 구성 종목을 운용사 사이트에서 받아, 응답마다 etf.members.fetched 한 건을 발행한다.
 * 평소에는 한 주에 한 번(fetchDay), 지수 교체일이 든 달에는 매일 받는다. 원문 저장과 앞 구성과의 비교·반영은 시세처리기가 한다.
 * 한 ETF의 실패가 다른 ETF를 막지 않는다. 받지 못한 ETF는 이벤트가 없거나 실패 이벤트라 시세처리기가 이전 구성을 그대로 둔다.
 */
@Component
public class MembersPublisher {

    static final String EVENT_TYPE = "etf.members.fetched";

    private static final Logger log = LoggerFactory.getLogger(MembersPublisher.class);
    private static final long STALE_DAYS = 7;
    private static final Pattern SYMBOL = Pattern.compile("^[0-9A-Z]{6}$");
    // Kafka 기본 메시지 상한(1MB)보다 조금 작게 둔다. 원문 상한(900KB) 안이어도 따옴표 이스케이프로 메시지가 커질 수 있어
    // 보내기 전에 한 번 더 막는다. 넘으면 보내지 않고 실패로 남긴다(시세처리기는 이전 구성을 유지한다).
    static final int MAX_MESSAGE_BYTES = 1_000_000;

    private final MemberSourceProperties properties;
    private final MemberTargetsPort targets;
    private final MemberSourcePort source;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    MembersPublisher(MemberSourceProperties properties, MemberTargetsPort targets, MemberSourcePort source,
                     KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.properties = properties;
        this.targets = targets;
        this.source = source;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 오늘(한국 시간) 받을 차례인 ETF의 구성을 받아 발행한다. */
    public void publishFetched(String correlationId) {
        if (!properties.enabled()) {
            log.info("member source is not configured; skipping");
            return;
        }
        LocalDate today = LocalDate.now(clock);
        List<MemberTargetsPort.Target> all;
        try {
            all = targets.targets(today);
        } catch (RuntimeException e) {
            log.warn("member targets unreadable errorType={}", e.getClass().getSimpleName());
            return;
        }
        if (all.isEmpty()) {
            // 구성 종목을 받을 펀드 번호(constituent_source_id)를 가진 ETF가 없으면 조용히 멈추므로 한 줄 남긴다.
            log.info("no ETF has a constituent source id; nothing to fetch");
            return;
        }
        int fetched = 0;
        int published = 0;
        int failed = 0;
        for (MemberTargetsPort.Target target : all) {
            if (!isDue(target, today)) {
                continue;
            }
            if (!SYMBOL.matcher(target.symbol()).matches()) {
                // 스키마를 어기는 종목코드는 조회하지 않는다. 종목코드는 보유 종목이 드러나므로 남기지 않는다.
                log.warn("member fetch skipped reason=invalid-symbol");
                continue;
            }
            if (fetched > 0) {
                pause();
            }
            fetched++;
            Outcome outcome = fetchAndPublish(target, today, correlationId);
            if (outcome != Outcome.PUBLISHED) {
                failed++;
            }
            if (outcome != Outcome.NO_RESPONSE) {
                published++;
            }
        }
        log.info("member fetch done date={} fetched={} published={} failed={}", today, fetched, published, failed);
    }

    private enum Outcome { PUBLISHED, PUBLISHED_FAILURE, NO_RESPONSE }

    private Outcome fetchAndPublish(MemberTargetsPort.Target target, LocalDate today, String correlationId) {
        LocalDate baseDate = baseDate(today);
        Raw raw = new Raw();
        List<Member> members = null;
        String failure = null;
        try {
            members = source.fetchTop(target.fundId(), baseDate, raw);
        } catch (RuntimeException e) {
            failure = e instanceof MemberSourcePort.MemberSourceException ? e.getMessage()
                    : "unexpected error " + e.getClass().getSimpleName();
            // 종목코드와 주소가 드러나지 않게 오류 종류와 사유만 남긴다.
            log.warn("member fetch failed errorType={} reason={}", e.getClass().getSimpleName(), failure);
        }
        if (raw.status == null) {
            // 응답 자체가 없으면(연결 실패, 시간 초과, 원문이 너무 큼) 저장할 원문이 없어 발행하지 않는다.
            return Outcome.NO_RESPONSE;
        }
        try {
            OffsetDateTime fetchedAt = raw.at;
            Envelope<MembersFetchedPayload> event = EventFactory.create(clock, EVENT_TYPE, fetchedAt, correlationId,
                    new MembersFetchedPayload(target.symbol(), baseDate.toString(), today.toString(),
                            EventFactory.format(fetchedAt), raw.status, raw.body, members, failure));
            String message = mapper.writeValueAsString(event);
            if (message.getBytes(StandardCharsets.UTF_8).length > MAX_MESSAGE_BYTES) {
                log.warn("member publish skipped reason=message too large");
                return Outcome.NO_RESPONSE;
            }
            kafka.send(Topics.ETF_MEMBERS_FETCHED, target.symbol(), message)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.warn("member send failed errorType={}", error.getClass().getSimpleName());
                        }
                    });
        } catch (RuntimeException e) {
            log.warn("member publish failed errorType={}", e.getClass().getSimpleName());
            return Outcome.NO_RESPONSE;
        }
        return failure == null ? Outcome.PUBLISHED : Outcome.PUBLISHED_FAILURE;
    }

    /**
     * 조회 요일이거나 지수 교체 달이면 받는다. 마지막 확인일이 7일 넘게 지난 ETF는 요일과 상관없이 받는다.
     * 조회 요일이 공휴일 다음이라 출처가 비어 와 실패하면 다음 주까지 낡은 구성이 남기 때문이다.
     */
    boolean isDue(MemberTargetsPort.Target target, LocalDate today) {
        // 처음 받는 ETF는 구성이 하나도 없으니 조회 요일까지 기다리지 않는다.
        if (target.lastSeenOn() == null || today.getDayOfWeek() == properties.fetchDay()) {
            return true;
        }
        if (ChronoUnit.DAYS.between(target.lastSeenOn(), today) > STALE_DAYS) {
            return true;
        }
        return target.rebalanceMonth();
    }

    /** 구성을 받을 기준일: 오늘 직전의 평일. 공휴일은 따로 보지 않고, 그 날 자료가 없으면 출처가 비어 와 실패로 처리된다. */
    static LocalDate baseDate(LocalDate today) {
        LocalDate date = today.minusDays(1);
        while (date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY) {
            date = date.minusDays(1);
        }
        return date;
    }

    private void pause() {
        long millis = properties.callInterval().toMillis();
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 출처가 해석 전에 넘겨준 응답. 받은 시각도 함께 둔다. */
    private final class Raw implements MemberSourcePort.RawRecorder {

        private Integer status;
        private String body;
        private OffsetDateTime at;

        @Override
        public void record(int httpStatus, String text) {
            status = httpStatus;
            body = text;
            at = OffsetDateTime.now(clock);
        }
    }
}
