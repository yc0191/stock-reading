package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.slf4j.MDC;

import dev.stockreading.collector.insight.InsightPublisher;
import dev.stockreading.collector.insight.IntradayPublisher;
import dev.stockreading.collector.members.MembersPublisher;
import dev.stockreading.collector.schedule.CollectorJobs.Job;
import dev.stockreading.collector.schedule.CollectorJobs.Outcome;
import dev.stockreading.collector.trace.CorrelationId;

class CollectorJobsTest {

    // 화요일 16:00:00.700(한국 시간). 회차 기준 시각은 초 단위로 자른다.
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00.700Z"), ZoneId.of("Asia/Seoul"));
    private static final String AT = "2026-10-20T16:00:00+09:00";

    private final QuotePublisher quotes = mock(QuotePublisher.class);
    private final NewsPublisher news = mock(NewsPublisher.class);
    private final FilingPublisher filings = mock(FilingPublisher.class);
    private final InsightPublisher insight = mock(InsightPublisher.class);
    private final IntradayPublisher intraday = mock(IntradayPublisher.class);
    private final MembersPublisher members = mock(MembersPublisher.class);
    // 막아 둔 작업을 테스트가 끝나면 풀어 준다.
    private final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void releaseBlockedJobs() {
        release.countDown();
    }

    private CollectorJobs broker() {
        return new CollectorJobs(quotes, news, filings, Optional.of(insight), Optional.of(intraday), members,
                Optional.empty(), CLOCK);
    }

    private CollectorJobs simulator() {
        return new CollectorJobs(quotes, news, filings, Optional.empty(), Optional.empty(), members,
                Optional.empty(), CLOCK);
    }

    private final DailyCollectionPublisher daily = mock(DailyCollectionPublisher.class);

    private CollectorJobs withDaily(Optional<InsightPublisher> source) {
        return new CollectorJobs(quotes, news, filings, source, Optional.of(intraday), members, Optional.of(daily), CLOCK);
    }

    @Test
    void preOpenRecordsWhatItMissedForTheMorningNotice() {
        when(insight.publishPreOpen(anyString(), anyString())).thenReturn(Set.of("overseas-daily-price"));
        when(filings.publishCollected(anyString())).thenReturn(false);

        withDaily(Optional.of(insight)).run(Job.COLLECT_PRE_OPEN);

        verify(daily).recordPreOpen(Set.of("overseas-daily-price", "sec-filings"));
    }

    @Test
    void preOpenThatGotEverythingRecordsNothingMissing() {
        when(insight.publishPreOpen(anyString(), anyString())).thenReturn(Set.of());
        when(filings.publishCollected(anyString())).thenReturn(true);

        withDaily(Optional.of(insight)).run(Job.COLLECT_PRE_OPEN);

        verify(daily).recordPreOpen(Set.of());
    }

    @Test
    void preOpenWhoseInsightPartFailsOrIsNotTheBrokerCountsBothOfItsSourcesAsMissing() {
        doThrow(new IllegalStateException("insight down")).when(insight).publishPreOpen(anyString(), anyString());
        when(filings.publishCollected(anyString())).thenReturn(true);

        withDaily(Optional.of(insight)).run(Job.COLLECT_PRE_OPEN);
        withDaily(Optional.empty()).run(Job.COLLECT_PRE_OPEN);

        verify(daily, times(2)).recordPreOpen(Set.of("world-index", "overseas-daily-price"));
    }

    @Test
    void preOpenStillRecordsAResultWhenTheFilingsPartThrows() {
        when(insight.publishPreOpen(anyString(), anyString())).thenReturn(Set.of());
        doThrow(new IllegalStateException("edgar down")).when(filings).publishCollected(anyString());

        assertThatThrownBy(() -> withDaily(Optional.of(insight)).run(Job.COLLECT_PRE_OPEN))
                .isInstanceOf(IllegalStateException.class);

        verify(daily).recordPreOpen(Set.of("world-index", "overseas-daily-price", "sec-filings"));
    }

    @Test
    void postCloseSendsTheNoticeWithWhatItMissedWhenItEnds() {
        when(insight.publishAll(anyString(), anyString())).thenReturn(Set.of("market-summary"));

        withDaily(Optional.of(insight)).run(Job.COLLECT_POST_CLOSE);

        verify(daily).publishPostClose("collect-post-close-" + AT, Set.of("market-summary"));
    }

    @Test
    void postCloseThatFailsOrIsNotTheBrokerStillSendsTheNoticeWithEverySourceMissing() {
        doThrow(new IllegalStateException("insight down")).when(insight).publishAll(anyString(), anyString());
        Set<String> all = Set.of("daily-price", "world-index", "exchange-rate-summary", "market-summary");

        withDaily(Optional.of(insight)).run(Job.COLLECT_POST_CLOSE);
        withDaily(Optional.empty()).run(Job.COLLECT_POST_CLOSE);

        verify(daily, times(2)).publishPostClose("collect-post-close-" + AT, all);
    }

    @Test
    void aManualPostCloseRunAlsoSendsTheNotice() throws Exception {
        when(insight.publishAll(anyString(), anyString())).thenReturn(Set.of());

        withDaily(Optional.of(insight)).startOnDemand(Job.COLLECT_POST_CLOSE);

        verify(daily, timeout(5_000)).publishPostClose("collect-post-close-" + AT, Set.of());
    }

    @Test
    void otherJobsSendNoDailyNotice() {
        CollectorJobs jobs = withDaily(Optional.of(insight));

        for (Job job : new Job[] {Job.COLLECT, Job.COLLECT_NEWS, Job.COLLECT_INTRADAY, Job.COLLECT_ETF_MEMBERS}) {
            jobs.run(job);
        }

        verifyNoInteractions(daily);
    }

    @Test
    void jobNamesAreTheJobTypesAndUnknownNamesAreEmpty() {
        assertThat(Arrays.stream(Job.values()).map(Job::jobType)).containsExactly(
                "collect", "collect-news", "collect-pre-open", "collect-intraday", "collect-post-close",
                "collect-etf-members");
        for (Job job : Job.values()) {
            assertThat(Job.of(job.jobType())).contains(job);
        }
        assertThat(Job.of("collect-members")).isEmpty();
        assertThat(Job.of("daily-briefing-morning")).isEmpty();
        assertThat(Job.of(null)).isEmpty();
    }

    @Test
    void logLinesInsideAJobCarryTheRoundCorrelationId() throws Exception {
        // 작업 스레드에서 찍는 로그 줄도 회차 correlationId 로 찾을 수 있어야 한다.
        CompletableFuture<String> during = new CompletableFuture<>();
        doAnswer(invocation -> {
            during.complete(MDC.get(CorrelationId.KEY));
            return null;
        }).when(news).publishCollected(anyString(), anyString());

        assertThat(simulator().start(Job.COLLECT_NEWS)).isEqualTo(Outcome.STARTED);

        assertThat(during.get(5, TimeUnit.SECONDS)).isEqualTo("collect-news-" + AT);
    }

    @Test
    void logLinesOfAJobRunOnTheCallingThreadCarryTheRoundCorrelationIdAndTheOldValueComesBack() {
        // 장 시작 전·장중·마감 후·뉴스 타이머는 타이머 스레드에서 작업을 끝까지 돌린다.
        String[] during = new String[1];
        doAnswer(invocation -> {
            during[0] = MDC.get(CorrelationId.KEY);
            return null;
        }).when(news).publishCollected(anyString(), anyString());
        MDC.put(CorrelationId.KEY, "timer");

        assertThat(simulator().run(Job.COLLECT_NEWS)).isEqualTo(Outcome.STARTED);

        assertThat(during[0]).isEqualTo("collect-news-" + AT);
        assertThat(MDC.get(CorrelationId.KEY)).isEqualTo("timer");
        MDC.remove(CorrelationId.KEY);
    }

    @Test
    void theCorrelationIdForTimersIsTheOneTheNextRoundUses() {
        assertThat(simulator().correlationId(Job.COLLECT_PRE_OPEN)).isEqualTo("collect-pre-open-" + AT);
    }

    @Test
    void eachJobCallsItsCollectionWithTheRoundTimeAndCorrelationId() {
        CollectorJobs jobs = broker();

        for (Job job : Job.values()) {
            assertThat(jobs.run(job)).isEqualTo(Outcome.STARTED);
        }

        verify(quotes).publishCollected("collect-" + AT, AT);
        verify(news).publishCollected("collect-news-" + AT, AT);
        verify(insight).publishPreOpen("collect-pre-open-" + AT, AT);
        verify(filings).publishCollected("collect-pre-open-" + AT);
        verify(intraday).publishIntraday("collect-intraday-" + AT, AT);
        verify(insight).publishAll("collect-post-close-" + AT, AT);
        verify(members).publishFetched("collect-etf-members-" + AT);
    }

    @Test
    void preOpenCollectsOverseasAndIndexFirstAndFilingsEvenWhenTheFirstPartFails() {
        doThrow(new IllegalStateException("insight down")).when(insight).publishPreOpen(anyString(), anyString());

        broker().run(Job.COLLECT_PRE_OPEN);

        InOrder order = inOrder(insight, filings);
        order.verify(insight).publishPreOpen("collect-pre-open-" + AT, AT);
        order.verify(filings).publishCollected("collect-pre-open-" + AT);
    }

    @Test
    void simulatorSourceSkipsTheMarketInsightPartsButStillCollectsFilings() {
        CollectorJobs jobs = simulator();

        jobs.run(Job.COLLECT_PRE_OPEN);
        jobs.run(Job.COLLECT_INTRADAY);
        jobs.run(Job.COLLECT_POST_CLOSE);

        verify(filings).publishCollected("collect-pre-open-" + AT);
        verifyNoInteractions(insight, intraday);
    }

    @ParameterizedTest
    @EnumSource(Job.class)
    void theSameJobDoesNotRunTwiceAtOnceAndRunsAgainAfterTheFirstEnds(Job job) throws Exception {
        CollectorJobs jobs = broker();
        CountDownLatch entered = new CountDownLatch(1);
        Runnable block = () -> {
            entered.countDown();
            await(release);
        };
        blockOn(job, block);

        CompletableFuture<Outcome> first = CompletableFuture.supplyAsync(() -> jobs.run(job));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(jobs.run(job)).isEqualTo(Outcome.ALREADY_RUNNING);
        assertThat(jobs.start(job)).isEqualTo(Outcome.ALREADY_RUNNING);

        release.countDown();
        assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(Outcome.STARTED);
        assertThat(jobs.run(job)).isEqualTo(Outcome.STARTED);
    }

    @Test
    void aLongJobDoesNotBlockOtherJobs() throws Exception {
        CollectorJobs jobs = broker();
        CountDownLatch entered = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            await(release);
            return null;
        }).when(insight).publishAll(anyString(), anyString());

        CompletableFuture.runAsync(() -> jobs.run(Job.COLLECT_POST_CLOSE));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(jobs.run(Job.COLLECT)).isEqualTo(Outcome.STARTED);
        assertThat(jobs.run(Job.COLLECT_NEWS)).isEqualTo(Outcome.STARTED);
        assertThat(jobs.run(Job.COLLECT_INTRADAY)).isEqualTo(Outcome.STARTED);
    }

    @Test
    void startReturnsAtOnceAndASkippedQuoteRoundIsRecordedOncePerRound() throws Exception {
        CollectorJobs jobs = broker();
        CountDownLatch entered = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            await(release);
            return null;
        }).when(quotes).publishCollected(anyString(), anyString());

        assertThat(jobs.start(Job.COLLECT)).isEqualTo(Outcome.STARTED);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(jobs.start(Job.COLLECT)).isEqualTo(Outcome.ALREADY_RUNNING);
        assertThat(jobs.start(Job.COLLECT)).isEqualTo(Outcome.ALREADY_RUNNING);

        verify(quotes, times(2)).publishSkipped("collect-" + AT, AT);
        release.countDown();
        // 앞 회차가 끝나도 건너뛴 회차를 몰아서 조회하지 않는다.
        verify(quotes, timeout(5_000).times(1)).publishCollected(anyString(), anyString());
        awaitStarted(jobs, Job.COLLECT);
        verify(quotes, timeout(5_000).times(2)).publishCollected(anyString(), anyString());
    }

    @Test
    void onDemandStartRunsOnceAndASkippedQuoteRunIsNotRecordedAsAnEvent() throws Exception {
        CollectorJobs jobs = broker();
        CountDownLatch entered = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            await(release);
            return null;
        }).when(quotes).publishCollected(anyString(), anyString());

        assertThat(jobs.startOnDemand(Job.COLLECT)).isEqualTo(Outcome.STARTED);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        // 수동 실행과 타이머는 같은 자리를 쓴다. 어느 쪽이 먼저 잡았든 다른 쪽은 건너뛴다.
        assertThat(jobs.startOnDemand(Job.COLLECT)).isEqualTo(Outcome.ALREADY_RUNNING);
        assertThat(jobs.run(Job.COLLECT)).isEqualTo(Outcome.ALREADY_RUNNING);

        // 타이머 쪽(run)이 건너뛴 한 번만 건너뜀 이벤트로 남는다.
        verify(quotes, times(1)).publishSkipped("collect-" + AT, AT);
        release.countDown();
        verify(quotes, timeout(5_000).times(1)).publishCollected("collect-" + AT, AT);
    }

    @Test
    void onlyTheQuoteJobRecordsSkippedRoundsAsEvents() throws Exception {
        CollectorJobs jobs = broker();
        CountDownLatch entered = new CountDownLatch(1);
        blockOn(Job.COLLECT_NEWS, () -> {
            entered.countDown();
            await(release);
        });

        CompletableFuture.runAsync(() -> jobs.run(Job.COLLECT_NEWS));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(jobs.run(Job.COLLECT_NEWS)).isEqualTo(Outcome.ALREADY_RUNNING);

        verify(quotes, never()).publishSkipped(anyString(), anyString());
    }

    @Test
    void aFailingJobFreesItsSlot() throws Exception {
        CollectorJobs jobs = broker();
        doThrow(new IllegalStateException("fetch failed")).doNothing()
                .when(quotes).publishCollected(anyString(), anyString());

        assertThat(jobs.start(Job.COLLECT)).isEqualTo(Outcome.STARTED);
        verify(quotes, timeout(5_000).times(1)).publishCollected(anyString(), anyString());
        awaitStarted(jobs, Job.COLLECT);
        verify(quotes, timeout(5_000).times(2)).publishCollected(anyString(), anyString());

        // run 은 실패를 부른 쪽으로 그대로 올리고, 자리는 푼다.
        doThrow(new IllegalStateException("news failed")).doNothing()
                .when(news).publishCollected(anyString(), anyString());
        assertThatThrownBy(() -> jobs.run(Job.COLLECT_NEWS)).isInstanceOf(IllegalStateException.class);
        assertThat(jobs.run(Job.COLLECT_NEWS)).isEqualTo(Outcome.STARTED);
    }

    // 작업 스레드가 자리를 막 풀었을 수 있으므로, 앞 회차가 끝날 때까지 잠깐 다시 시도한다.
    private static void awaitStarted(CollectorJobs jobs, Job job) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (jobs.start(job) != Outcome.STARTED) {
            assertThat(System.nanoTime()).as(job + " 의 자리가 풀리지 않았다").isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    private void blockOn(Job job, Runnable block) {
        Answer<Void> answer = call -> {
            block.run();
            return null;
        };
        switch (job) {
            case COLLECT -> doAnswer(answer).when(quotes).publishCollected(anyString(), anyString());
            case COLLECT_NEWS -> doAnswer(answer).when(news).publishCollected(anyString(), anyString());
            case COLLECT_PRE_OPEN -> doAnswer(answer).when(filings).publishCollected(anyString());
            case COLLECT_INTRADAY -> doAnswer(answer).when(intraday).publishIntraday(anyString(), anyString());
            case COLLECT_POST_CLOSE -> doAnswer(answer).when(insight).publishAll(anyString(), anyString());
            case COLLECT_ETF_MEMBERS -> doAnswer(answer).when(members).publishFetched(anyString());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
