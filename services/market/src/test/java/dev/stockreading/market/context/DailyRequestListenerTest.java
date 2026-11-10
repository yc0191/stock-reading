package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.holdings.EtfInfo;
import dev.stockreading.market.holdings.HoldingsPort;
import dev.stockreading.market.insight.OverseasCloseRate;
import dev.stockreading.market.insight.OverseasCloseRateQuery;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 저장소와 해외 등락률 조회를 대역으로 바꿔 데일리 판 자료 요청 한 건의 응답 조립 전체를 돌린다. 값은 모두 가짜다. */
class DailyRequestListenerTest {

    private static final String JOB_ID = "8d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68";
    private static final String CORRELATION_ID = "daily-briefing-post-close-2026-10-20T17:00:00+09:00";
    private static final String REQUEST_EVENT_ID = "2f6a1c8e-3b7d-4e59-9a0c-6d4e8b2f1a75";
    // 샘플 요청의 기준 시각과 since(4일 전).
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-20T17:00:00+09:00");
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-16T17:00:00+09:00");

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private BackgroundRepository repository;
    private OverseasCloseRateQuery closeRateQuery;
    private HoldingsPort holdings;
    private OperationsReader operationsReader;
    private DailyPublisher publisher;

    @BeforeEach
    void setUp() {
        repository = mock(BackgroundRepository.class);
        closeRateQuery = mock(OverseasCloseRateQuery.class);
        holdings = mock(HoldingsPort.class);
        operationsReader = mock(OperationsReader.class);
        publisher = mock(DailyPublisher.class);
        when(repository.latest(any(), any(), any(), any())).thenReturn(Optional.empty());
        when(repository.worldIndexRows(anyString(), any(), any())).thenReturn(List.of());
        when(repository.latestDailyPrices(any(), any())).thenReturn(List.of());
        when(closeRateQuery.find(anyString(), any(), any())).thenReturn(Optional.empty());
        when(holdings.topTickers(anyString())).thenReturn(List.of());
        when(holdings.trackedEtfs()).thenReturn(List.of());
    }

    private DailyRequestListener listener(int maxTickers, int maxEtfs) {
        return new DailyRequestListener(repository, new WorldIndexRates(mapper), closeRateQuery, holdings,
                new ContextProperties(Duration.ofDays(4), 32_768, maxTickers, List.of("C", "S"), Duration.ofDays(7)),
                new DailyProperties(maxEtfs), operationsReader, publisher, mapper);
    }

    private DailyRequestListener listener() {
        return listener(100, 50);
    }

    private static String request() throws IOException {
        return ContractSchemas.sample("market.daily.requested.sample.json");
    }

    private DailyPayload answer(DailyRequestListener listener, String request) throws IOException {
        listener.onRequest(request);
        ArgumentCaptor<DailyPayload> payload = ArgumentCaptor.captor();
        verify(publisher).publish(payload.capture(), eq(REQUEST_EVENT_ID), eq(CORRELATION_ID));
        return payload.getValue();
    }

    private void tracked(String... symbols) {
        when(holdings.trackedEtfs()).thenReturn(
                Arrays.stream(symbols).map(symbol -> new HoldingsPort.TrackedEtf(symbol, "HELD")).toList());
    }

    private static BackgroundRepository.StoredTargetResponse daily(String symbol, String rate, OffsetDateTime fetched) {
        return new BackgroundRepository.StoredTargetResponse(symbol, new BackgroundRepository.StoredResponse(
                DailyPriceReaderTest.body(
                        DailyPriceReaderTest.record("20261019", "10000", "9900", "1.01"),
                        DailyPriceReaderTest.record("20261020", " 9850", " 10000", "  " + rate)),
                fetched));
    }

    @Test
    void echoesJobIdAndSeqAndAnswersTheMarketBackgroundInOrderWithoutAPerSymbolDailyPrice() throws IOException {
        DailyPayload payload = answer(listener(), request());

        assertThat(payload.jobId()).isEqualTo(JOB_ID);
        assertThat(payload.seq()).isEqualTo(1);
        assertThat(payload.etfs()).isEmpty();
        assertThat(payload.truncated()).isFalse();
        assertThat(payload.background()).extracting(ContextPayload.Background::apiType, ContextPayload.Background::target)
                .containsExactly(tuple("exchange-rate-summary", null), tuple("market-summary", null),
                        tuple("world-index", "C"), tuple("world-index", "S"));
        assertThat(payload.background()).extracting(ContextPayload.Background::status).containsOnly("missing");
    }

    @Test
    void trackedEtfsWithDailyPricesCarryTheRawRecordValues() throws IOException {
        tracked("TEST01", "TEST02");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF.minusMinutes(30)), daily("TEST02", "0.50", AS_OF.minusMinutes(29))));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).containsExactly(
                new DailyPayload.Etf("TEST01", "20261020", "9850", "10000", "-1.50", "2026-10-20T16:30:00+09:00",
                        List.of(), false).withRelation("HELD"),
                new DailyPayload.Etf("TEST02", "20261020", "9850", "10000", "0.50", "2026-10-20T16:31:00+09:00",
                        List.of(), false).withRelation("HELD"));
    }

    @Test
    void eachEtfCarriesItsRelationFromTheHoldingsTableIncludingOnesWithoutADailyPrice() throws IOException {
        when(holdings.trackedEtfs()).thenReturn(List.of(new HoldingsPort.TrackedEtf("TEST01", "HELD"),
                new HoldingsPort.TrackedEtf("TEST02", "WATCH"), new HoldingsPort.TrackedEtf("TEST03", "WATCH")));
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF.minusMinutes(30)), daily("TEST02", "0.50", AS_OF.minusMinutes(29))));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::relation,
                DailyPayload.Etf::missingReason).containsExactly(tuple("TEST01", "HELD", null),
                tuple("TEST02", "WATCH", null), tuple("TEST03", "WATCH", DailyPayload.NO_DAILY_PRICE_IN_RANGE));
    }

    @Test
    void trackedEtfWithoutADailyPriceInRangeStaysWithNullsAndTheReasonButKeepsUsStocksInfoAndCloseRates()
            throws IOException {
        tracked("TEST01", "TEST09");
        when(holdings.topTickers("TEST09")).thenReturn(List.of("FAKEZ"));
        when(holdings.etfInfo("TEST09")).thenReturn(Optional.of(new EtfInfo("TEST ETF NINE", null, null, List.of())));
        when(closeRateQuery.find(eq("FAKEZ"), eq(AS_OF), eq(SINCE))).thenReturn(Optional.of(new OverseasCloseRate(
                "FAKEZ", "20261019", OffsetDateTime.parse("2026-10-19T16:00:00-04:00"), "-0.75",
                OffsetDateTime.parse("2026-10-19T23:30:02Z"))));
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(daily("TEST01", "-1.50", AS_OF)));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::missingReason)
                .containsExactly(tuple("TEST01", null), tuple("TEST09", DailyPayload.NO_DAILY_PRICE_IN_RANGE));
        DailyPayload.Etf without = payload.etfs().get(1);
        assertThat(without).extracting(DailyPayload.Etf::date, DailyPayload.Etf::close, DailyPayload.Etf::prevClose,
                DailyPayload.Etf::rate, DailyPayload.Etf::fetchedAt).containsOnlyNulls();
        assertThat(without.tickers()).containsExactly("FAKEZ");
        assertThat(without.etf().name()).isEqualTo("TEST ETF NINE");
        assertThat(payload.closeRates()).extracting(ContextPayload.CloseRate::ticker).containsExactly("FAKEZ");
    }

    @Test
    void everyTrackedEtfWithoutAnyDailyPriceIsStillListedWithTheReason() throws IOException {
        tracked("TEST01", "TEST02");

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::missingReason)
                .containsExactly(tuple("TEST01", DailyPayload.NO_DAILY_PRICE_IN_RANGE),
                        tuple("TEST02", DailyPayload.NO_DAILY_PRICE_IN_RANGE));
    }

    @Test
    void dailyPriceOfASymbolNotInTheInstrumentTableIsNotListed() throws IOException {
        tracked("TEST01");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST77", "0.50", AS_OF)));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol).containsExactly("TEST01");
    }

    @Test
    void dailyPriceFetchedAtTheReferenceTimeIsUsedAndOneSecondLaterCountsAsMissing() throws IOException {
        tracked("TEST01", "TEST02");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST02", "0.50", AS_OF.plusSeconds(1))));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::missingReason)
                .containsExactly(tuple("TEST01", null), tuple("TEST02", DailyPayload.NO_DAILY_PRICE_IN_RANGE));
    }

    @Test
    void dailyPriceOlderThanTheLowerBoundCountsAsMissingAndTheSafetyLimitRaisesTheLowerBound() throws IOException {
        tracked("TEST01", "TEST02");
        // 요청 since가 8일 전이어도 안전 상한 4일이 하한이 된다.
        String wideRequest = request().replace("2026-10-16T17:00:00+09:00", "2026-10-12T17:00:00+09:00");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", SINCE), daily("TEST02", "0.50", SINCE.minusSeconds(1))));

        DailyPayload payload = answer(listener(), wideRequest);

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::missingReason)
                .containsExactly(tuple("TEST01", null), tuple("TEST02", DailyPayload.NO_DAILY_PRICE_IN_RANGE));
        verify(repository).latestDailyPrices(SINCE, AS_OF);
    }

    @Test
    void etfWithoutUsStocksHasNoTickersAndCloseRatesAreAskedOncePerTicker() throws IOException {
        tracked("TEST01", "TEST02", "TEST03");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST02", "0.50", AS_OF), daily("TEST03", "0.10", AS_OF)));
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKE-B"));
        when(holdings.topTickers("TEST03")).thenReturn(List.of("FAKEA"));
        when(closeRateQuery.find(eq("FAKEA"), eq(AS_OF), eq(SINCE))).thenReturn(Optional.of(new OverseasCloseRate(
                "FAKEA", "20261019", OffsetDateTime.parse("2026-10-19T16:00:00-04:00"), "  -1.25",
                OffsetDateTime.parse("2026-10-19T23:30:02Z"))));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::tickers)
                .containsExactly(tuple("TEST01", List.of("FAKEA", "FAKE-B")), tuple("TEST02", List.of()),
                        tuple("TEST03", List.of("FAKEA")));
        assertThat(payload.closeRates()).containsExactly(new ContextPayload.CloseRate(
                "FAKEA", "20261019", "2026-10-20T05:00:00+09:00", "-1.25", "2026-10-20T08:30:02+09:00"));
        verify(closeRateQuery, times(1)).find(eq("FAKEA"), any(), any());
        verify(closeRateQuery, times(1)).find(eq("FAKE-B"), any(), any());
    }

    @Test
    void eachEtfCarriesItsOwnEtfInfoAndAnUnknownOneHasNone() throws IOException {
        tracked("TEST01", "TEST02");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST02", "0.50", AS_OF)));
        when(holdings.etfInfo("TEST01")).thenReturn(Optional.of(new EtfInfo("TEST ETF", null, "TEST SECTOR", List.of(
                new EtfInfo.Member("US", "FAKEA", null, new BigDecimal("5.25"), 1)))));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs().get(0).etf()).isEqualTo(new ContextPayload.EtfInfo("TEST ETF", null, "TEST SECTOR",
                List.of(new ContextPayload.EtfMember("US", "FAKEA", null, "5.25", 1)), false));
        assertThat(payload.etfs().get(1).etf()).isNull();
    }

    @Test
    void etfsOverTheLimitAreCutInSymbolOrderAndMarkedTruncated() throws IOException {
        tracked("TEST01", "TEST02", "TEST03");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST02", "0.50", AS_OF), daily("TEST03", "0.10", AS_OF)));
        when(holdings.topTickers("TEST03")).thenReturn(List.of("FAKEC"));

        DailyPayload requested = answer(listener(), request().replace("\"maxEtfs\": 50", "\"maxEtfs\": 2"));

        assertThat(requested.etfs()).extracting(DailyPayload.Etf::symbol).containsExactly("TEST01", "TEST02");
        assertThat(requested.truncated()).isTrue();
        verify(closeRateQuery, never()).find(eq("FAKEC"), any(), any());
        verify(holdings, never()).topTickers("TEST03");
    }

    @Test
    void requestedEtfLimitAboveTheSafetyLimitIsCappedAndExactlyTheLimitIsNotTruncated() throws IOException {
        tracked("TEST01", "TEST02");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST02", "0.50", AS_OF)));

        DailyPayload payload = answer(listener(100, 2), request());

        assertThat(payload.etfs()).hasSize(2);
        assertThat(payload.truncated()).isFalse();
    }

    @Test
    void tickersOverTheLimitAreCutPerEtfAndMarkedOnThatEtf() throws IOException {
        tracked("TEST01", "TEST02");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                daily("TEST01", "-1.50", AS_OF), daily("TEST02", "0.50", AS_OF)));
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKEB", "FAKEC"));
        when(holdings.topTickers("TEST02")).thenReturn(List.of("FAKED", "FAKEE"));

        DailyPayload payload = answer(listener(), request().replace("\"maxTickers\": 100", "\"maxTickers\": 2"));

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::tickers, DailyPayload.Etf::tickersTruncated)
                .containsExactly(tuple(List.of("FAKEA", "FAKEB"), true), tuple(List.of("FAKED", "FAKEE"), false));
        assertThat(payload.truncated()).isFalse();
        verify(closeRateQuery, never()).find(eq("FAKEC"), any(), any());
    }

    @Test
    void dailyPriceWithoutAReadableRecordKeepsTheEtfWithTheUnreadableReasonAndTheRestAreAnswered() throws IOException {
        tracked("TEST01", "TEST02");
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA"));
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                new BackgroundRepository.StoredTargetResponse("TEST01",
                        new BackgroundRepository.StoredResponse("{\"dataBody\": {\"out2\": []}}", AS_OF)),
                daily("TEST02", "0.50", AS_OF)));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::symbol, DailyPayload.Etf::missingReason,
                DailyPayload.Etf::tickers)
                .containsExactly(tuple("TEST01", DailyPayload.DAILY_PRICE_UNREADABLE, List.of("FAKEA")),
                        tuple("TEST02", null, List.of()));
    }

    @Test
    void dailyPriceThatIsNotJsonCountsAsUnreadableToo() throws IOException {
        tracked("TEST01");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(
                new BackgroundRepository.StoredTargetResponse("TEST01",
                        new BackgroundRepository.StoredResponse("not json", AS_OF))));

        DailyPayload payload = answer(listener(), request());

        assertThat(payload.etfs()).extracting(DailyPayload.Etf::missingReason)
                .containsExactly(DailyPayload.DAILY_PRICE_UNREADABLE);
    }

    @Test
    void sameRequestTwiceGivesTheSameAnswer() throws IOException {
        tracked("TEST01");
        when(repository.latestDailyPrices(SINCE, AS_OF)).thenReturn(List.of(daily("TEST01", "-1.50", AS_OF)));
        DailyRequestListener listener = listener();

        listener.onRequest(request());
        listener.onRequest(request());

        ArgumentCaptor<DailyPayload> payloads = ArgumentCaptor.captor();
        verify(publisher, times(2)).publish(payloads.capture(), eq(REQUEST_EVENT_ID), eq(CORRELATION_ID));
        assertThat(payloads.getAllValues().get(0)).isEqualTo(payloads.getAllValues().get(1));
    }

    @Test
    void invalidRequestIsRejectedWithoutPublishing() throws IOException {
        DailyRequestListener listener = listener();
        String sinceAfterAsOf = request().replace("2026-10-16T17:00:00+09:00", "2026-10-21T17:00:00+09:00");
        String zeroEtfs = request().replace("\"maxEtfs\": 50", "\"maxEtfs\": 0");

        assertThatThrownBy(() -> listener.onRequest(sinceAfterAsOf)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(JOB_ID);
        assertThatThrownBy(() -> listener.onRequest(zeroEtfs)).isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(publisher);
    }

    private static final DailyPayload.Operations OPERATIONS = new DailyPayload.Operations(
            List.of(new DailyPayload.CollectionStatus("quote-1s", null, null, 0)), java.util.Map.of("TIMEOUT", 0),
            List.of());

    @Test
    void addsOperationsOnlyWhenTheRequestAsksForThem() throws IOException {
        when(operationsReader.read(any(), any())).thenReturn(OPERATIONS);

        DailyPayload asked = answer(listener(), request());

        assertThat(asked.operations()).isSameAs(OPERATIONS);
        verify(operationsReader).read(SINCE, AS_OF);
    }

    @Test
    void leavesOperationsOutWhenTheFlagIsMissingOrFalse() throws IOException {
        String missing = request().replace(",\n    \"includeOperations\": true", "");
        assertThat(missing).doesNotContain("includeOperations");

        assertThat(answer(listener(), missing).operations()).isNull();
        verifyNoInteractions(operationsReader);

        publisher = mock(DailyPublisher.class);
        String off = request().replace("\"includeOperations\": true", "\"includeOperations\": false");
        assertThat(answer(listener(), off).operations()).isNull();
        verifyNoInteractions(operationsReader);
    }

    @Test
    void operationsUseTheSameClampedRangeAsTheOtherData() throws IOException {
        when(operationsReader.read(any(), any())).thenReturn(OPERATIONS);
        // 요청 하한이 안전 기간(4일)보다 오래되면 응답의 다른 자료처럼 안전 기간까지만 본다.
        String old = request().replace("2026-10-16T17:00:00+09:00", "2026-10-01T17:00:00+09:00");

        answer(listener(), old);

        verify(operationsReader).read(AS_OF.minus(Duration.ofDays(4)), AS_OF);
    }

    @Test
    void sendsTheRestOfTheAnswerWithoutOperationsWhenReadingThemFails() throws IOException {
        when(operationsReader.read(any(), any())).thenThrow(new IllegalStateException("db down"));

        DailyPayload answer = answer(listener(), request());

        assertThat(answer.operations()).isNull();
        assertThat(mapper.writeValueAsString(answer)).doesNotContain("operations");
    }
}
