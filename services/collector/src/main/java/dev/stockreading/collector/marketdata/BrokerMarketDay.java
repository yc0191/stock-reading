package dev.stockreading.collector.marketdata;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 시세 제공자의 장 운영 상태 조회로 휴장일을 판정한다.
 * 응답의 현재 일자 칸과 기준 영업일 칸이 같으면 영업일이다. 주말·공휴일에는 기준 영업일이
 * 직전 영업일에 머문다고 보고 세운 규칙이며, 실제 휴장일 응답으로는 아직 확인하지 못했다.
 * 경로와 두 칸 이름은 설정({@link MarketStatusProperties})으로 받고, 비어 있으면 늘 영업일로 본다.
 */
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class BrokerMarketDay implements MarketDayPort {

    static final int MAX_ATTEMPTS = 3;
    static final Duration DEFAULT_RETRY_PAUSE = Duration.ofMillis(200);
    // 1초 tick 이 매번 부르지 않게, 조회가 모두 실패한 직후에는 이 시간 동안 다시 부르지 않고 영업일로 본다.
    static final Duration FAILURE_COOLDOWN = Duration.ofMinutes(1);
    // 시세 조회와 같은 스케줄러 스레드에서 불리므로 오래 기다리지 않는다.
    private static final Duration LIMITER_WAIT = Duration.ofSeconds(1);
    private static final ZoneId KOREA = ZoneId.of("Asia/Seoul");
    private static final Logger log = LoggerFactory.getLogger(BrokerMarketDay.class);

    private final BrokerClient client;
    private final CallRateLimit limiter;
    private final MarketStatusProperties status;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final Duration retryPause;

    private LocalDate knownDate;
    private boolean knownBusinessDay;
    private Instant failedAt;

    @Autowired
    BrokerMarketDay(BrokerClient client, CallRateLimit limiter, MarketStatusProperties status, ObjectMapper mapper,
            Clock clock) {
        this(client, limiter, status, mapper, clock, DEFAULT_RETRY_PAUSE);
    }

    BrokerMarketDay(BrokerClient client, CallRateLimit limiter, MarketStatusProperties status, ObjectMapper mapper,
            Clock clock, Duration retryPause) {
        this.client = client;
        this.limiter = limiter;
        this.status = status;
        if (!status.configured()) {
            log.warn("market status path or date fields are not set; every day is treated as a business day");
        }
        this.mapper = mapper;
        this.clock = clock;
        this.retryPause = retryPause;
    }

    // 한국 날짜마다 조회 성공 결과를 하나 기억한다. tick 을 발행하는 스레드가 여럿이라 같은 조회가 겹치지 않게 잠근다.
    @Override
    public synchronized boolean isBusinessDay() {
        if (!status.configured()) {
            return true;
        }
        LocalDate today = LocalDate.now(clock.withZone(KOREA));
        if (today.equals(knownDate)) {
            return knownBusinessDay;
        }
        if (failedAt != null && clock.instant().isBefore(failedAt.plus(FAILURE_COOLDOWN))) {
            return true;
        }
        // 스케줄러 스레드를 오래 잡지 않도록 한 번 판정에 쓰는 전체 시간에 상한을 둔다.
        long deadline = System.nanoTime() + status.lookupTimeout().toNanos();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            Duration remaining = Duration.ofNanos(deadline - System.nanoTime());
            if (remaining.isNegative() || remaining.isZero()) {
                failedAt = clock.instant();
                log.warn("market status lookup exceeded {} ms; treating today as a business day",
                        status.lookupTimeout().toMillis());
                return true;
            }
            try {
                boolean businessDay = lookup(remaining);
                knownDate = today;
                knownBusinessDay = businessDay;
                failedAt = null;
                if (!businessDay) {
                    log.info("market status says today is not a business day date={}", today);
                }
                return businessDay;
            } catch (BrokerApiException e) {
                // 서버 문구는 싣지 않고 우리가 만든 문구(원인 구분용 코드)만 남긴다.
                log.warn("market status lookup failed attempt={}/{} type={} reason={}",
                        attempt, MAX_ATTEMPTS, e.failureType(), e.getMessage());
            } catch (RuntimeException e) {
                log.warn("market status lookup failed attempt={}/{} exception={}",
                        attempt, MAX_ATTEMPTS, e.getClass().getSimpleName());
            }
            if (attempt < MAX_ATTEMPTS) {
                pause(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            }
        }
        failedAt = clock.instant();
        log.warn("market status lookup failed {} times; treating today as a business day", MAX_ATTEMPTS);
        return true;
    }

    private boolean lookup(Duration remaining) {
        if (!limiter.tryAcquire(remaining.compareTo(LIMITER_WAIT) < 0 ? remaining : LIMITER_WAIT)) {
            throw new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과",
                    null, null, BrokerMarketData.RATE_LIMITED, null);
        }
        // 남은 시간보다 짧은 제한 시간으로 불러, 느린 응답이 상한을 넘기지 않게 한다.
        JsonNode data = mapper.readTree(client.marketStatusRaw(status.path(), remaining)).path("dataBody");
        String now = text(data, status.todayField());
        String standard = text(data, status.businessDayField());
        if (now.isEmpty() || standard.isEmpty()) {
            throw new BrokerApiException(FailureType.RESULT_CODE_FAILED, "장 운영 상태 응답에 일자 칸이 없습니다");
        }
        return now.equals(standard);
    }

    private static String text(JsonNode data, String field) {
        JsonNode found = data.findValue(field);
        return found == null ? "" : found.asString("").trim();
    }

    private void pause(Duration remaining) {
        try {
            Thread.sleep(retryPause.compareTo(remaining) < 0 ? retryPause : remaining);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
