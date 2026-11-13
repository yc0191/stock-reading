package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.insight.InsightPublisher;
import dev.stockreading.collector.insight.IntradayPublisher;
import dev.stockreading.collector.members.MembersPublisher;
import dev.stockreading.collector.trace.CorrelationId;

/**
 * 수집 작업 여섯 개의 실행 입구. 수집기 타이머와 수동 실행 명령(CollectorCommandListener)이 모두 여기를 부른다.
 * 같은 작업은 겹쳐 돌지 않는다: 앞 회차가 끝나지 않았으면 이번 회차는 실행하지 않고 ALREADY_RUNNING 을 돌려준다.
 * 회차마다 기준 시각(scheduledFor, 초 단위로 자른 지금)과 correlationId(jobType-scheduledFor)를 정한다.
 */
@Component
public class CollectorJobs {

    /** 작업 이름(jobType)은 이벤트의 jobType·correlationId 와 수동 실행 명령 이름으로 쓰인다. */
    public enum Job {
        COLLECT("collect"),
        COLLECT_NEWS("collect-news"),
        COLLECT_PRE_OPEN("collect-pre-open"),
        COLLECT_INTRADAY("collect-intraday"),
        COLLECT_POST_CLOSE("collect-post-close"),
        COLLECT_ETF_MEMBERS("collect-etf-members");

        private final String jobType;

        Job(String jobType) {
            this.jobType = jobType;
        }

        public String jobType() {
            return jobType;
        }

        public static Optional<Job> of(String jobType) {
            for (Job job : values()) {
                if (job.jobType.equals(jobType)) {
                    return Optional.of(job);
                }
            }
            return Optional.empty();
        }
    }

    public enum Outcome { STARTED, ALREADY_RUNNING }

    private static final Logger log = LoggerFactory.getLogger(CollectorJobs.class);

    private final QuotePublisher quotes;
    private final NewsPublisher news;
    private final FilingPublisher filings;
    // 증권사 출처일 때만 있다. 시뮬레이터에는 장 시작 전·장중·마감 후 시세 정보 조회가 없다.
    private final Optional<InsightPublisher> insight;
    private final Optional<IntradayPublisher> intraday;
    private final MembersPublisher members;
    // 데일리 판을 켰을 때만 있다. 장 시작 전·마감 후 수집 결과를 데일리 판 시작 알림으로 넘긴다.
    private final Optional<DailyCollectionPublisher> daily;
    private final Clock clock;
    private final Map<Job, AtomicBoolean> running = new EnumMap<>(Job.class);

    CollectorJobs(QuotePublisher quotes, NewsPublisher news, FilingPublisher filings,
                  Optional<InsightPublisher> insight, Optional<IntradayPublisher> intraday, MembersPublisher members,
                  Optional<DailyCollectionPublisher> daily, Clock clock) {
        this.quotes = quotes;
        this.news = news;
        this.filings = filings;
        this.insight = insight;
        this.intraday = intraday;
        this.members = members;
        this.daily = daily;
        this.clock = clock;
        for (Job job : Job.values()) {
            running.put(job, new AtomicBoolean());
        }
    }

    /** 부른 스레드에서 작업을 끝까지 실행한다. */
    public Outcome run(Job job) {
        Round round = round(job);
        if (!tryEnter(job, round, true)) {
            return Outcome.ALREADY_RUNNING;
        }
        try {
            // 타이머 스레드에서 돌므로 레코드 인터셉터가 걸지 않는다. 회차의 correlationId 를 직접 로그 문맥에 둔다.
            CorrelationId.within(round.correlationId(), () -> execute(job, round));
        } finally {
            running.get(job).set(false);
        }
        return Outcome.STARTED;
    }

    /**
     * 실행 자리는 부른 스레드에서 잡고, 작업은 새 스레드에서 실행한다. 부른 쪽은 작업이 끝나기를 기다리지 않는다.
     * 1초 현재가 타이머가 이 방식을 쓴다. 타이머 스레드에서 직접 실행하면 조회가 1초를 넘길 때 밀린 회차가
     * 끝난 직후 몰아서 실행되므로, 타이머는 제때 돌며 앞 회차가 아직 돌고 있는 회차를 건너뜀으로 남긴다.
     */
    public Outcome start(Job job) {
        return start(job, true);
    }

    /**
     * 수동 실행 명령의 입구. start 와 같이 새 스레드에서 1회 실행하고, 같은 작업이 돌고 있으면 건너뛴다.
     * 건너뛰어도 1초 현재가의 건너뜀 이벤트는 남기지 않는다. 밀린 타이머 회차가 아니라 사람이 더 보낸 명령이라,
     * 데일리 메일 서버 상태의 SupersededByNewerTick 개수에 섞이면 안 된다.
     */
    public Outcome startOnDemand(Job job) {
        return start(job, false);
    }

    private Outcome start(Job job, boolean recordSkip) {
        Round round = round(job);
        if (!tryEnter(job, round, recordSkip)) {
            return Outcome.ALREADY_RUNNING;
        }
        // 작업 스레드는 메시지 처리 밖이라 레코드 인터셉터가 걸지 않으므로, 회차의 correlationId 를 직접 로그 문맥에 둔다.
        Runnable task = () -> CorrelationId.within(round.correlationId(), () -> {
            try {
                execute(job, round);
            } catch (RuntimeException e) {
                log.error("job failed jobType={} corr={} errorType={}",
                        job.jobType(), round.correlationId(), e.getClass().getSimpleName());
            } finally {
                running.get(job).set(false);
            }
        });
        try {
            Thread.ofVirtual().name("collector-" + job.jobType()).start(task);
        } catch (RuntimeException | Error e) {
            running.get(job).set(false);
            throw e;
        }
        return Outcome.STARTED;
    }

    /** 지금 시작하면 회차가 쓸 correlationId. 타이머가 작업을 부르기 전에 찍는 줄(휴장일 건너뜀 등)에 쓴다. */
    public String correlationId(Job job) {
        return round(job).correlationId();
    }

    private boolean tryEnter(Job job, Round round, boolean recordSkip) {
        if (running.get(job).compareAndSet(false, true)) {
            return true;
        }
        log.warn("job skipped, previous run still running jobType={} scheduledFor={}", job.jobType(), round.scheduledFor());
        if (recordSkip && job == Job.COLLECT) {
            quotes.publishSkipped(round.correlationId(), round.scheduledFor());
        }
        return false;
    }

    private Round round(Job job) {
        String scheduledFor = EventFactory.format(OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS));
        return new Round(scheduledFor, job.jobType() + "-" + scheduledFor);
    }

    private void execute(Job job, Round round) {
        String corr = round.correlationId();
        String at = round.scheduledFor();
        switch (job) {
            case COLLECT -> quotes.publishCollected(corr, at);
            case COLLECT_NEWS -> news.publishCollected(corr, at);
            case COLLECT_PRE_OPEN -> {
                // 해외 일봉·세계지수와 SEC 공시를 한 회차에서 함께 받는다. 앞쪽이 실패해도 공시는 받는다.
                // 끝까지 가지 못하면 모두 못 받은 것으로 남는다.
                Set<String> missing = new LinkedHashSet<>(DailyCollectionPublisher.PRE_OPEN_SOURCES);
                try {
                    Set<String> found = ifBroker(job, insight, publisher -> publisher.publishPreOpen(corr, at),
                            List.of(InsightPublisher.WORLD_INDEX, InsightPublisher.OVERSEAS_DAILY_PRICE));
                    if (!filings.publishCollected(corr)) {
                        found.add(DailyCollectionPublisher.SEC_FILINGS);
                    }
                    missing = found;
                } finally {
                    // 수집이 일부 실패하거나 예외로 끝나도 아침 판 알림이 그 사실을 담도록 결과는 반드시 남긴다.
                    Set<String> result = missing;
                    daily.ifPresent(publisher -> publisher.recordPreOpen(result));
                }
            }
            case COLLECT_INTRADAY -> ifBroker(job, intraday, publisher -> {
                publisher.publishIntraday(corr, at);
                return Set.of();
            }, List.of());
            case COLLECT_POST_CLOSE -> {
                Set<String> missing = new LinkedHashSet<>(DailyCollectionPublisher.POST_CLOSE_SOURCES);
                try {
                    missing = ifBroker(job, insight, publisher -> publisher.publishAll(corr, at),
                            DailyCollectionPublisher.POST_CLOSE_SOURCES);
                } finally {
                    // 수집이 일부 실패하거나 예외로 끝나도 마감 후 판 알림은 반드시 낸다.
                    Set<String> result = missing;
                    // 재시도 대기 중 종료로 끊기면 인터럽트 표시가 켜진 채라 전송이 실패할 수 있다.
                    // 알림을 보내는 동안만 표시를 비우고, 보낸 뒤 되돌려 종료가 이어지게 한다.
                    boolean interrupted = Thread.interrupted();
                    try {
                        daily.ifPresent(publisher -> publisher.publishPostClose(corr, result));
                    } finally {
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
            case COLLECT_ETF_MEMBERS -> members.publishFetched(corr);
        }
    }

    /** 증권사 출처가 아니거나 조회가 예외로 끝나면 그 수집의 자료 종류(whenFailed)를 모두 못 받은 것으로 돌려준다. */
    private static <T> Set<String> ifBroker(Job job, Optional<T> publisher, Function<T, Set<String>> action,
                                            List<String> whenFailed) {
        if (publisher.isEmpty()) {
            log.info("market insight skipped, market data source is not the broker jobType={}", job.jobType());
            return new LinkedHashSet<>(whenFailed);
        }
        try {
            return new LinkedHashSet<>(action.apply(publisher.get()));
        } catch (RuntimeException e) {
            log.warn("market insight failed jobType={} errorType={}", job.jobType(), e.getClass().getSimpleName());
            return new LinkedHashSet<>(whenFailed);
        }
    }

    private record Round(String scheduledFor, String correlationId) {
    }
}
