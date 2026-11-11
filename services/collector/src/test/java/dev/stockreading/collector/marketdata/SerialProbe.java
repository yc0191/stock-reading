package dev.stockreading.collector.marketdata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * 같은 종목 목록을 병렬(수집기와 같은 {@link BrokerMarketData#fetch}), 직렬(한 번에 한 종목),
 * 동시 N개 제한(한 번에 최대 N개)으로 번갈아 조회해 깨진 응답을 방식별로 센다.
 * 모든 방식이 같은 {@link CallRateLimit} 을 거치고 현재가 조회만 한다.
 * 응답 칸 불일치는 모든 방식에서 수집기와 같은 {@link PriceFieldCheck} 로 판단해 수집기가 버리는 기준 그대로 센다.
 * 결과에는 건수만 남기고 종목 코드, 시세 값, 응답 원문은 남기지 않는다.
 */
final class SerialProbe {

    enum Mode {
        PARALLEL("parallel"), SERIAL("serial"), LIMITED("limited");

        final String label;

        Mode(String label) {
            this.label = label;
        }
    }

    /**
     * @param rounds 고른 방식을 한 번씩 돈 것을 한 라운드로 센 최대 라운드 수
     * @param maxMillis 이 시간이 지나면 다음 라운드를 시작하지 않는다
     * @param pauseMillis 방식 하나를 돈 뒤 쉬는 시간. 호출 상한의 1초 창을 비워 다음 방식이 상한에 걸리지 않게 한다
     * @param concurrency 동시 N개 제한 방식에서 한 번에 처리 중일 수 있는 호출 수
     * @param modes 돌릴 방식. 라운드마다 시작 방식을 하나씩 밀어 순서를 바꾼다
     */
    record Settings(int rounds, long maxMillis, long pauseMillis, int concurrency, List<Mode> modes) {

        Settings {
            if (concurrency < 1) {
                throw new IllegalArgumentException("동시 호출 수는 1 이상이어야 합니다");
            }
            if (modes.isEmpty()) {
                throw new IllegalArgumentException("방식을 하나 이상 골라야 합니다");
            }
            modes = List.copyOf(modes);
        }

        Settings(int rounds, long maxMillis, long pauseMillis) {
            this(rounds, maxMillis, pauseMillis, 3, List.of(Mode.values()));
        }
    }

    /** 방식 하나의 누적 건수. calls 는 실제로 보낸 현재가 호출 수이며 상한 때문에 보내지 못한 것은 rateLimited 에만 센다. */
    static final class Tally {
        int calls;
        int ok;
        int emptyPrice;
        int notNumber;
        int nonPositive;
        int fieldMismatch;
        int rateLimited;
        int abnormal;
        // 칸 불일치에서 걸린 검사 조합별 건수(예: CHANGE+RATE). 검사 이름만 담는다.
        final Map<String, Integer> mismatchChecks = new TreeMap<>();
        final Map<String, Integer> abnormalKinds = new TreeMap<>();
        // 한 회차에서 이 방식이 종목을 모두 조회하는 데 걸린 시간(ms). 회차마다 하나씩 쌓는다.
        final List<Long> roundMillis = new ArrayList<>();
        // 호출 상한 자리를 기다린 호출 수. 직렬·동시 N개 제한에서 시간에 상한 대기가 섞였는지 가린다.
        int waited;
        // 이상으로 멈췄거나 상한 대기·건너뜀이 있어 회차 시간 표본에서 뺀 회차 수.
        int roundMsExcluded;

        int broken() {
            return emptyPrice + notNumber + nonPositive + fieldMismatch;
        }

        // 회차 시간 요약: 평균/중앙값/최대(ms). 회차를 재지 않았으면 빈 문자열.
        String roundTimes() {
            if (roundMillis.isEmpty()) {
                return "";
            }
            List<Long> sorted = roundMillis.stream().sorted().toList();
            long sum = sorted.stream().mapToLong(Long::longValue).sum();
            return String.format(" roundMs(avg/p50/max)=%d/%d/%d", sum / sorted.size(),
                    median(sorted), sorted.get(sorted.size() - 1));
        }

        // 개수가 짝수이면 가운데 두 값의 평균.
        static long median(List<Long> sorted) {
            int n = sorted.size();
            return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
        }

        String line(Mode mode) {
            double rate = calls == 0 ? 0 : broken() * 100.0 / calls;
            return String.format("%s: calls=%d ok=%d empty=%d notNumber=%d nonPositive=%d fieldMismatch=%d%s"
                            + " rateLimited=%d abnormal=%d%s broken=%d brokenRate=%.2f%%%s",
                    mode.label, calls, ok, emptyPrice, notNumber, nonPositive, fieldMismatch,
                    mismatchChecks.isEmpty() ? "" : " mismatchChecks=" + mismatchChecks, rateLimited,
                    abnormal, abnormalKinds.isEmpty() ? "" : " abnormalKinds=" + abnormalKinds, broken(), rate,
                    roundTimes() + (roundMsExcluded > 0 ? " roundMsExcluded=" + roundMsExcluded : ""));
        }

        private void add(Tally other) {
            calls += other.calls;
            ok += other.ok;
            emptyPrice += other.emptyPrice;
            notNumber += other.notNumber;
            nonPositive += other.nonPositive;
            fieldMismatch += other.fieldMismatch;
            rateLimited += other.rateLimited;
            abnormal += other.abnormal;
            waited += other.waited;
            roundMsExcluded += other.roundMsExcluded;
            other.mismatchChecks.forEach((k, v) -> mismatchChecks.merge(k, v, Integer::sum));
            other.abnormalKinds.forEach((k, v) -> abnormalKinds.merge(k, v, Integer::sum));
            roundMillis.addAll(other.roundMillis);
        }
    }

    /** warmUpCalls 는 회차 전에 토큰을 받아 두려고 보낸 현재가 호출 수이며 방식별 건수에는 넣지 않는다. */
    record Result(Map<Mode, Tally> tallies, int roundsDone, String stopReason, List<String> roundLines, long elapsedMs,
            int warmUpCalls) {
    }

    // 직렬·동시 N개 제한은 보내는 수가 적어 상한에 자리가 날 때까지 넉넉히 기다린다. 병렬은 수집기와 같은 대기 시간을 쓴다.
    private static final Duration SERIAL_WAIT = Duration.ofSeconds(2);

    private final BrokerClient client;
    private final BrokerMarketData marketData;
    private final CallRateLimit limiter;
    private final PriceFieldCheck priceCheck;
    private final Settings settings;

    /** marketData 는 같은 priceCheck 와 limiter 로 만든 것이어야 두 방식이 같은 기준·같은 상한으로 센다. */
    SerialProbe(BrokerClient client, BrokerMarketData marketData, CallRateLimit limiter, PriceFieldCheck priceCheck,
            Settings settings) {
        this.client = client;
        this.marketData = marketData;
        this.limiter = limiter;
        this.priceCheck = priceCheck;
        this.settings = settings;
    }

    // 키를 틀렸거나 한도에 걸린 서버를 계속 두드리지 않도록, 센 대상이 아닌 실패(abnormal)가 한 번이라도 나오면 멈춘다.
    Result run(List<String> symbols) throws InterruptedException {
        // 수집기 코드의 실패 로그에는 종목 코드가 들어가므로 이 시험 동안에는 끈다.
        Logger quoteLog = (Logger) LoggerFactory.getLogger(BrokerMarketData.class);
        Level previous = quoteLog.getLevel();
        quoteLog.setLevel(Level.OFF);
        try {
            return runRounds(symbols);
        } finally {
            quoteLog.setLevel(previous);
        }
    }

    private Result runRounds(List<String> symbols) throws InterruptedException {
        Map<Mode, Tally> totals = new EnumMap<>(Mode.class);
        for (Mode mode : settings.modes()) {
            totals.put(mode, new Tally());
        }
        List<String> roundLines = new ArrayList<>();
        String stopReason = "none";
        long start = System.nanoTime();
        // 첫 호출이 토큰을 발급받으므로 회차 전에 한 번 불러 둔다. 그러지 않으면 1회차 첫 방식의 회차 시간에 발급 시간이 섞인다.
        Tally warmUp = new Tally();
        if (!symbols.isEmpty()) {
            callOne(symbols.get(0), warmUp);
            if (warmUp.abnormal > 0) {
                return new Result(totals, 0, "warmUp abnormal " + warmUp.abnormalKinds.keySet(), roundLines,
                        elapsedMs(start), warmUp.calls);
            }
            if (settings.pauseMillis() > 0) {
                Thread.sleep(settings.pauseMillis());
            }
        }
        int round = 0;
        rounds:
        while (round < settings.rounds() && elapsedMs(start) < settings.maxMillis()) {
            round++;
            // 앞 방식이 서버 상태에 주는 영향을 고르게 나누려고 라운드마다 시작 방식을 하나씩 민다.
            List<Mode> modes = settings.modes();
            StringBuilder line = new StringBuilder("round " + round + ":");
            for (int i = 0; i < modes.size(); i++) {
                Mode mode = modes.get((round - 1 + i) % modes.size());
                long modeStart = System.nanoTime();
                Tally tally = switch (mode) {
                    case PARALLEL -> parallel(symbols);
                    case SERIAL -> serial(symbols);
                    case LIMITED -> limited(symbols);
                };
                long modeMillis = elapsedMs(modeStart);
                // 이상으로 멈춘 회차와 상한 대기·건너뜀이 있는 회차는 모든 종목을 제때 조회한 시간이 아니라 표본에서 뺀다.
                if (tally.abnormal > 0 || tally.rateLimited > 0 || tally.waited > 0) {
                    tally.roundMsExcluded++;
                } else {
                    tally.roundMillis.add(modeMillis);
                }
                totals.get(mode).add(tally);
                line.append(' ').append(mode.label).append("(calls=").append(tally.calls)
                        .append(" broken=").append(tally.broken()).append(" abnormal=").append(tally.abnormal)
                        .append(" ms=").append(modeMillis).append(')');
                if (tally.abnormal > 0) {
                    stopReason = "abnormal " + tally.abnormalKinds.keySet();
                    roundLines.add(line.toString());
                    break rounds;
                }
                if (settings.pauseMillis() > 0) {
                    Thread.sleep(settings.pauseMillis());
                }
            }
            roundLines.add(line.toString());
        }
        return new Result(totals, round, stopReason, roundLines, elapsedMs(start), warmUp.calls);
    }

    // 지금 수집기와 같은 방식: 종목마다 가상 스레드로 동시에 보내고 상한을 넘는 종목은 RateLimited 로 건너뛴다.
    private Tally parallel(List<String> symbols) {
        Tally tally = new Tally();
        FetchResult result = marketData.fetch(symbols);
        for (PriceQuote quote : result.quotes()) {
            tally.calls++;
            tally.ok++;
        }
        for (QuoteFailure failure : result.failures()) {
            if (failure.type() == FailureType.TICK_SKIPPED && BrokerMarketData.RATE_LIMITED.equals(failure.exceptionType())) {
                tally.rateLimited++;
                continue;
            }
            tally.calls++;
            countFailure(failure.type(), failure.exceptionType(), tally);
        }
        return tally;
    }

    private Tally serial(List<String> symbols) throws InterruptedException {
        Tally tally = new Tally();
        for (String symbol : symbols) {
            callOne(symbol, tally);
            if (tally.abnormal > 0) {
                break;
            }
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return tally;
    }

    // 병렬과 직렬 사이: 종목마다 가상 스레드를 쓰되 한 번에 처리 중인 호출을 concurrency 개 이하로 묶는다.
    // 상한 대기는 직렬과 같게 넉넉히 해, 상한 때문에 건너뛴 건수가 아니라 동시 호출 수의 영향만 비교되게 한다.
    private Tally limited(List<String> symbols) throws InterruptedException {
        Tally tally = new Tally();
        Semaphore slots = new Semaphore(settings.concurrency());
        AtomicBoolean stop = new AtomicBoolean();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String symbol : symbols) {
                slots.acquire();
                if (stop.get()) {
                    slots.release();
                    break;
                }
                executor.submit(() -> {
                    try {
                        Tally one = new Tally();
                        callOne(symbol, one);
                        synchronized (tally) {
                            tally.add(one);
                        }
                        if (one.abnormal > 0) {
                            stop.set(true);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        slots.release();
                    }
                });
            }
        }
        return tally;
    }

    private void callOne(String symbol, Tally tally) throws InterruptedException {
        if (!limiter.tryAcquire(Duration.ZERO)) {
            tally.waited++;
            if (!limiter.tryAcquire(SERIAL_WAIT)) {
                tally.rateLimited++;
                return;
            }
        }
        tally.calls++;
        try {
            PriceQuote quote = client.currentPrice(symbol);
            List<PriceFieldCheck.Check> failed = priceCheck.failedChecks(quote.price(), quote.raw());
            if (failed.isEmpty()) {
                tally.ok++;
            } else {
                countFailure(FailureType.FIELD_MISMATCH, PriceFieldCheck.exceptionType(failed), tally);
            }
        } catch (BrokerApiException e) {
            countFailure(e.failureType(), e.exceptionType(), tally);
        } catch (RuntimeException e) {
            countFailure(FailureType.RESULT_CODE_FAILED, e.getClass().getSimpleName(), tally);
        }
    }

    private static void countFailure(FailureType type, String exceptionType, Tally tally) {
        switch (type) {
            case EMPTY_PRICE -> tally.emptyPrice++;
            case FIELD_MISMATCH -> {
                tally.fieldMismatch++;
                String checks = exceptionType != null && exceptionType.startsWith(PriceFieldCheck.EXCEPTION_PREFIX)
                        ? exceptionType.substring(PriceFieldCheck.EXCEPTION_PREFIX.length())
                        : "UNKNOWN";
                tally.mismatchChecks.merge(checks, 1, Integer::sum);
            }
            // BrokerClient 는 숫자로 읽지 못한 현재가와 0 이하 현재가를 같은 종류로 보내므로 예외 종류로 나눈다.
            case PRICE_PARSE_FAILED -> {
                if (exceptionType == null) {
                    tally.nonPositive++;
                } else {
                    tally.notNumber++;
                }
            }
            default -> {
                tally.abnormal++;
                tally.abnormalKinds.merge(type.name(), 1, Integer::sum);
            }
        }
    }

    /** 결과 요약. 건수와 설정만 쓰고 종목 코드나 시세 값은 쓰지 않는다. */
    static List<String> summary(Result result, Settings settings, int symbolCount, int capPerSecond,
            boolean fieldCheck) {
        List<String> lines = new ArrayList<>();
        lines.add(String.format("# serial-probe symbols=%d rounds=%d/%d pauseMs=%d capPerSecond=%d concurrency=%d"
                        + " fieldCheck=%s elapsedMs=%d warmUpCalls=%d",
                symbolCount, result.roundsDone(), settings.rounds(), settings.pauseMillis(), capPerSecond,
                settings.concurrency(), fieldCheck ? "on" : "off", result.elapsedMs(), result.warmUpCalls()));
        for (Mode mode : settings.modes()) {
            lines.add(result.tallies().get(mode).line(mode));
        }
        lines.add("stop=" + result.stopReason());
        lines.addAll(result.roundLines());
        return lines;
    }

    // 실제 응답에서 센 결과도 저장소에 들어가지 않도록, 저장소 안의 경로는 호출 전에 거절한다.
    static Path outsideRepo(Path out, Path workingDir) {
        Path target = out.toAbsolutePath().normalize();
        Path root = repoRoot(workingDir.toAbsolutePath().normalize());
        if (root != null && target.startsWith(root)) {
            throw new IllegalArgumentException("결과 파일은 저장소 밖 경로로 지정해야 합니다");
        }
        return target;
    }

    static void write(Path target, List<String> lines) throws IOException {
        Files.writeString(target, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
    }

    private static Path repoRoot(Path dir) {
        for (Path p = dir; p != null; p = p.getParent()) {
            if (Files.exists(p.resolve(".git"))) {
                return p;
            }
        }
        return null;
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
