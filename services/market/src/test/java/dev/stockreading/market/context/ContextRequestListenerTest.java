package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.constituent.EtfInfo;
import dev.stockreading.market.constituent.HoldingsPort;
import dev.stockreading.market.insight.OverseasCloseRate;
import dev.stockreading.market.insight.OverseasCloseRateQuery;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 저장소와 해외 등락률 조회를 대역으로 바꿔 요청 한 건의 응답 조립 전체를 돌린다. 값은 모두 가짜다. */
class ContextRequestListenerTest {

    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final String REQUEST_EVENT_ID = "7a3d9e12-4b6c-4f81-9d2e-5c8b1a0f6e37";
    // 샘플 요청의 기준 시각과 since(4일 전).
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-20T16:00:31+09:00");
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-16T16:00:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:43Z"), ZoneId.of("Asia/Seoul"));

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private BackgroundRepository repository;
    private OverseasCloseRateQuery closeRateQuery;
    private HoldingsPort holdings;
    private ContextPublisher publisher;

    @BeforeEach
    void setUp() {
        repository = mock(BackgroundRepository.class);
        closeRateQuery = mock(OverseasCloseRateQuery.class);
        holdings = mock(HoldingsPort.class);
        publisher = mock(ContextPublisher.class);
        when(repository.latest(any(), any(), any(), any())).thenReturn(Optional.empty());
        when(repository.worldIndexRows(anyString(), any(), any())).thenReturn(List.of());
        when(closeRateQuery.find(anyString(), any(), any())).thenReturn(Optional.empty());
        when(holdings.topTickers(anyString())).thenReturn(List.of());
        when(holdings.stockName(anyString())).thenReturn(Optional.empty());
    }

    private ContextRequestListener listener(ContextProperties properties) {
        return new ContextRequestListener(repository, new WorldIndexRates(mapper), closeRateQuery, holdings,
                properties, publisher, mapper);
    }

    private ContextRequestListener listener() {
        return listener(new ContextProperties(Duration.ofDays(4), 32_768, 100, List.of("C", "S"), Duration.ofDays(7)));
    }

    private static String request() throws IOException {
        return ContractSchemas.sample("market.context.requested.sample.json");
    }

    private ContextPayload answer(ContextRequestListener listener, String request) throws IOException {
        listener.onRequest(request);
        ArgumentCaptor<ContextPayload> payload = ArgumentCaptor.captor();
        verify(publisher).publish(payload.capture(), eq(REQUEST_EVENT_ID), eq(CORRELATION_ID));
        return payload.getValue();
    }

    @Test
    void echoesJobIdAndSeqAndAnswersEveryBackgroundKindInOrder() throws IOException {
        ContextPayload payload = answer(listener(), request());

        assertThat(payload.jobId()).isEqualTo(JOB_ID);
        assertThat(payload.seq()).isEqualTo(1);
        assertThat(payload.symbol()).isEqualTo("TEST01");
        assertThat(payload.background()).extracting(ContextPayload.Background::apiType, ContextPayload.Background::target)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("exchange-rate-summary", null),
                        org.assertj.core.groups.Tuple.tuple("market-summary", null),
                        org.assertj.core.groups.Tuple.tuple("world-index", "C"),
                        org.assertj.core.groups.Tuple.tuple("world-index", "S"),
                        org.assertj.core.groups.Tuple.tuple("daily-price", "TEST01"));
        assertThat(payload.background()).extracting(ContextPayload.Background::status).containsOnly("missing");
    }

    @Test
    void etfMissingFromTheMapHasNoTickersAndNoCloseRates() throws IOException {
        ContextPayload payload = answer(listener(), request());

        assertThat(payload.tickers()).isEmpty();
        assertThat(payload.closeRates()).isEmpty();
        assertThat(payload.truncated()).isFalse();
        verifyNoInteractions(closeRateQuery);
    }

    @Test
    void etfInfoIsCarriedWhenTheSymbolIsAnEtf() throws IOException {
        givenEtfInfo();

        ContextPayload full = answer(listener(), request());

        assertThat(full.etf()).isEqualTo(new ContextPayload.EtfInfo("TEST ETF", "TEST INDEX", null, List.of(
                new ContextPayload.EtfMember("US", "FAKEA", "FAKE CORP", "12.5", 1),
                new ContextPayload.EtfMember("KR", "TEST09", null, null, 2),
                new ContextPayload.EtfMember("US", "FAKEB", null, "1", 3)), false));
    }

    @Test
    void etfMembersOverTheLimitAreCutByRankAndMarkedTruncated() throws IOException {
        givenEtfInfo();

        ContextPayload cut = answer(listener(), request().replace("\"maxTickers\": 100", "\"maxTickers\": 2"));

        assertThat(cut.etf().members()).extracting(ContextPayload.EtfMember::symbol).containsExactly("FAKEA", "TEST09");
        assertThat(cut.etf().membersTruncated()).isTrue();
    }

    private void givenEtfInfo() {
        when(holdings.etfInfo("TEST01")).thenReturn(Optional.of(new EtfInfo("TEST ETF", "TEST INDEX", null, List.of(
                new EtfInfo.Member("US", "FAKEA", "FAKE CORP", new BigDecimal("12.500"), 1),
                new EtfInfo.Member("KR", "TEST09", null, null, 2),
                new EtfInfo.Member("US", "FAKEB", null, new BigDecimal("1"), 3)))));
    }

    @Test
    void nameComesFromTheEtfTableWhenTheSymbolIsAnEtf() throws IOException {
        givenEtfInfo();
        when(holdings.stockName("TEST01")).thenReturn(Optional.of("TEST STOCK"));

        assertThat(answer(listener(), request()).name()).isEqualTo("TEST ETF");
    }

    @Test
    void nameComesFromTheStockTableWhenTheSymbolIsNotAnEtf() throws IOException {
        when(holdings.stockName("TEST01")).thenReturn(Optional.of("TEST STOCK"));

        ContextPayload payload = answer(listener(), request());

        assertThat(payload.name()).isEqualTo("TEST STOCK");
        assertThat(payload.etf()).isNull();
        assertThat(mapper.writeValueAsString(payload)).contains("\"name\":\"TEST STOCK\"");
    }

    @Test
    void nameIsLeftOutWhenNeitherTableHasItOrItIsBlank() throws IOException {
        when(holdings.etfInfo("TEST01")).thenReturn(Optional.of(new EtfInfo("  ", null, null, List.of())));
        when(holdings.stockName("TEST01")).thenReturn(Optional.of(" "));

        ContextPayload payload = answer(listener(), request());

        assertThat(payload.name()).isNull();
        assertThat(mapper.readTree(mapper.writeValueAsString(payload)).has("name")).isFalse();
    }

    @Test
    void symbolThatIsNotAnEtfHasNoEtfInfo() throws IOException {
        assertThat(answer(listener(), request()).etf()).isNull();
    }

    @Test
    void tickersFollowTheMapAndATickerWithoutACandidateIsLeftOutOfCloseRates() throws IOException {
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKE-B"));
        when(closeRateQuery.find(eq("FAKEA"), eq(AS_OF), eq(SINCE))).thenReturn(Optional.of(new OverseasCloseRate(
                "FAKEA", "20261019", OffsetDateTime.parse("2026-10-19T16:00:00-04:00"), "  -1.25",
                OffsetDateTime.parse("2026-10-19T23:30:02Z"))));

        ContextPayload payload = answer(listener(), request());

        assertThat(payload.tickers()).containsExactly("FAKEA", "FAKE-B");
        assertThat(payload.closeRates()).containsExactly(new ContextPayload.CloseRate(
                "FAKEA", "20261019", "2026-10-20T05:00:00+09:00", "-1.25", "2026-10-20T08:30:02+09:00"));
    }

    @Test
    void tickersOverTheLimitAreCutAndMarkedTruncated() throws IOException {
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKEB", "FAKEC"));

        ContextPayload requested = answer(listener(), request().replace("\"maxTickers\": 100", "\"maxTickers\": 2"));
        assertThat(requested.tickers()).containsExactly("FAKEA", "FAKEB");
        assertThat(requested.truncated()).isTrue();
        verify(closeRateQuery, never()).find(eq("FAKEC"), any(), any());
    }

    @Test
    void requestedTickerLimitAboveTheSafetyLimitIsCappedAndExactlyTheLimitIsNotTruncated() throws IOException {
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKEB", "FAKEC"));

        ContextPayload capped = answer(listener(
                new ContextProperties(Duration.ofDays(4), 32_768, 3, List.of(), Duration.ofDays(7))), request());

        assertThat(capped.tickers()).hasSize(3);
        assertThat(capped.truncated()).isFalse();
    }

    @Test
    void lowerBoundIsTheLaterOfTheRequestedSinceAndTheSafetyAge() throws IOException {
        // 요청 since가 10일 전이어도 안전 상한 4일 전까지만 본다(4일 초과 자료는 없음).
        String tenDays = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"2026-10-10T16:00:31+09:00\"");
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA"));

        answer(listener(), tenDays);

        verify(repository).latest(BackgroundKind.EXCHANGE_RATE_SUMMARY, null, SINCE, AS_OF);
        // 직전 영업일 행은 요청 범위보다 이른 lookback(7일 전)부터 읽는다.
        verify(repository).worldIndexRows("C", AS_OF.minusDays(7), AS_OF);
        verify(repository).latest(BackgroundKind.DAILY_PRICE, "TEST01", SINCE, AS_OF);
        verify(closeRateQuery).find("FAKEA", AS_OF, SINCE);
    }

    @Test
    void laterRequestedSinceIsKept() throws IOException {
        OffsetDateTime oneDay = AS_OF.minusDays(1);
        String request = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"" + EventFactory.format(oneDay) + "\"");

        answer(listener(), request);

        verify(repository).latest(BackgroundKind.MARKET_SUMMARY, null, oneDay, AS_OF);
    }

    @Test
    void rawOverTheSizeLimitIsMarkedTooLargeAndExactlyTheLimitIsKept() throws IOException {
        String atLimit = "{\"a\": \"" + "x".repeat(91) + "\"}";
        assertThat(atLimit.length()).isEqualTo(100);
        // 한글은 UTF-8로 3바이트라 글자 수가 아니라 바이트로 잰다.
        String overLimit = "{\"a\": \"" + "가".repeat(31) + "\"}";
        when(repository.latest(eq(BackgroundKind.EXCHANGE_RATE_SUMMARY), any(), any(), any()))
                .thenReturn(Optional.of(stored(atLimit, "2026-10-20T15:55:01+09:00")));
        when(repository.latest(eq(BackgroundKind.MARKET_SUMMARY), any(), any(), any()))
                .thenReturn(Optional.of(stored(overLimit, "2026-10-20T06:55:03Z")));

        ContextPayload payload = answer(listener(), request().replace("\"maxResponseBytes\": 32768",
                "\"maxResponseBytes\": 100"));

        assertThat(payload.background().get(0)).isEqualTo(ContextPayload.Background.ok(
                "exchange-rate-summary", null, atLimit, "2026-10-20T15:55:01+09:00"));
        assertThat(payload.background().get(1)).isEqualTo(ContextPayload.Background.tooLarge(
                "market-summary", null, "2026-10-20T15:55:03+09:00", 102));
    }

    @Test
    void requestedSizeAboveTheSafetyLimitIsCapped() throws IOException {
        String raw = "{\"a\": \"" + "x".repeat(200) + "\"}";
        when(repository.latest(eq(BackgroundKind.DAILY_PRICE), any(), any(), any()))
                .thenReturn(Optional.of(stored(raw, "2026-10-20T15:55:01+09:00")));

        ContextPayload payload = answer(listener(
                new ContextProperties(Duration.ofDays(4), 100, 100, List.of(), Duration.ofDays(7))), request());

        assertThat(payload.background().get(2).status()).isEqualTo("too-large");
        assertThat(payload.background().get(2).responseBytes()).isEqualTo(raw.length());
    }

    @Test
    void worldIndexCarriesTheRawResponseAndTheComputedRates() throws IOException {
        String current = worldIndex("20261020", " 102.00");
        when(repository.worldIndexRows("C", AS_OF.minusDays(7), AS_OF)).thenReturn(List.of(
                stored(current, "2026-10-20T15:55:05+09:00"),
                stored(worldIndex("20261019", " 100.00"), "2026-10-19T15:55:05+09:00")));

        ContextPayload.Background world = answer(listener(), request()).background().get(2);

        assertThat(world.status()).isEqualTo("ok");
        assertThat(world.response()).isEqualTo(current);
        assertThat(world.indexRates()).containsExactly(new ContextPayload.IndexRate(
                "TESTIX", "TEST INDEX", "20261020", "102.00", "20261019", "100.00", "2.00"));
    }

    @Test
    void previousCloseReceivedBeforeTheRequestedSinceIsStillUsed() throws IOException {
        // 마감 후 판처럼 요청 범위가 짧아도 직전 영업일 행은 범위 밖에서 찾는다. 배경 원문은 범위 안의 행만 쓴다.
        OffsetDateTime oneHour = AS_OF.minusHours(1);
        String request = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"" + EventFactory.format(oneHour) + "\"");
        String current = worldIndex("20261020", " 102.00");
        when(repository.worldIndexRows("C", AS_OF.minusDays(7), AS_OF)).thenReturn(List.of(
                stored(current, "2026-10-20T15:55:05+09:00"),
                stored(worldIndex("20261019", " 100.00"), "2026-10-19T15:55:05+09:00")));

        ContextPayload.Background world = answer(listener(), request).background().get(2);

        assertThat(world.response()).isEqualTo(current);
        assertThat(world.indexRates()).containsExactly(new ContextPayload.IndexRate(
                "TESTIX", "TEST INDEX", "20261020", "102.00", "20261019", "100.00", "2.00"));
    }

    @Test
    void worldIndexRowsOnlyBeforeTheRequestedSinceAreMissing() throws IOException {
        OffsetDateTime oneHour = AS_OF.minusHours(1);
        String request = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"" + EventFactory.format(oneHour) + "\"");
        when(repository.worldIndexRows("C", AS_OF.minusDays(7), AS_OF)).thenReturn(List.of(
                stored(worldIndex("20261019", " 100.00"), "2026-10-19T15:55:05+09:00")));

        ContextPayload.Background world = answer(listener(), request).background().get(2);

        assertThat(world.status()).isEqualTo("missing");
    }

    @Test
    void previousCloseSkipsRowsOlderThanTheLookbackLaterThanAsOfAndOfTheSameBusinessDay() throws IOException {
        OffsetDateTime oneHour = AS_OF.minusHours(1);
        String request = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"" + EventFactory.format(oneHour) + "\"");
        String current = worldIndex("20261020", " 102.00");
        // 실제 저장소는 asOf 뒤 행을 주지 않지만 가짜는 그대로 주므로, 계산이 스스로 걸러내는지 본다.
        when(repository.worldIndexRows("C", AS_OF.minusDays(7), AS_OF)).thenReturn(List.of(
                stored(current, "2026-10-20T15:55:05+09:00"),
                stored(worldIndex("20261020", " 101.00"), "2026-10-20T12:00:00+09:00"),
                stored(worldIndex("20261019", "  90.00"), EventFactory.format(AS_OF.plusSeconds(1))),
                stored(worldIndex("20261019", " 100.00"), "2026-10-19T15:55:05+09:00"),
                stored(worldIndex("20261013", "  50.00"), EventFactory.format(AS_OF.minusDays(7).minusSeconds(1)))));

        ContextPayload.Background world = answer(listener(), request).background().get(2);

        assertThat(world.indexRates()).containsExactly(new ContextPayload.IndexRate(
                "TESTIX", "TEST INDEX", "20261020", "102.00", "20261019", "100.00", "2.00"));
    }

    @Test
    void onlyARowOlderThanTheLookbackGivesNoPreviousClose() throws IOException {
        OffsetDateTime oneHour = AS_OF.minusHours(1);
        String request = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"" + EventFactory.format(oneHour) + "\"");
        when(repository.worldIndexRows("C", AS_OF.minusDays(7), AS_OF)).thenReturn(List.of(
                stored(worldIndex("20261020", " 102.00"), "2026-10-20T15:55:05+09:00"),
                stored(worldIndex("20261013", "  50.00"), EventFactory.format(AS_OF.minusDays(7).minusSeconds(1)))));

        ContextPayload.Background world = answer(listener(), request).background().get(2);

        assertThat(world.indexRates()).containsExactly(new ContextPayload.IndexRate(
                "TESTIX", "TEST INDEX", "20261020", "102.00", null, null, null));
    }

    @Test
    void responseBuiltFromEveryStatusMatchesTheSchema() throws IOException {
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKEB"));
        when(closeRateQuery.find(eq("FAKEA"), any(), any())).thenReturn(Optional.of(new OverseasCloseRate(
                "FAKEA", "20261019", OffsetDateTime.parse("2026-10-19T16:00:00-04:00"), " 0.50",
                OffsetDateTime.parse("2026-10-19T23:30:02Z"))));
        when(repository.latest(eq(BackgroundKind.EXCHANGE_RATE_SUMMARY), any(), any(), any()))
                .thenReturn(Optional.of(stored("{\"a\": 1}", "2026-10-20T15:55:01+09:00")));
        when(repository.latest(eq(BackgroundKind.MARKET_SUMMARY), any(), any(), any()))
                .thenReturn(Optional.of(stored("{\"a\": \"" + "x".repeat(40_000) + "\"}", "2026-10-20T15:55:03+09:00")));
        when(repository.worldIndexRows(eq("C"), any(), any())).thenReturn(List.of(
                stored(worldIndex("20261020", " 102.00"), "2026-10-20T15:55:05+09:00")));

        ContextPayload payload = answer(listener(), request());
        String json = mapper.writeValueAsString(EventFactory.create(CLOCK, "market.context.responded",
                OffsetDateTime.now(CLOCK), CORRELATION_ID, payload));

        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", json)).isEmpty();
        // 직전 행이 없는 지수도 키는 null로 실려 "없음"이 드러난다.
        assertThat(mapper.readTree(json).at("/payload/background/2/indexRates/0").has("rate")).isTrue();
        assertThat(mapper.readTree(json).at("/payload/background/2/indexRates/0/rate").isNull()).isTrue();
    }

    @Test
    void invalidRequestIsRejectedWithoutQueryingOrAnswering() throws IOException {
        String zeroBytes = request().replace("\"maxResponseBytes\": 32768", "\"maxResponseBytes\": 0");
        String zeroTickers = request().replace("\"maxTickers\": 100", "\"maxTickers\": 0");
        String sinceAfterAsOf = request().replace("\"since\": \"2026-10-16T16:00:31+09:00\"",
                "\"since\": \"2026-10-20T16:00:32+09:00\"");

        for (String invalid : List.of(zeroBytes, zeroTickers, sinceAfterAsOf)) {
            assertThatThrownBy(() -> listener().onRequest(invalid)).isInstanceOf(IllegalArgumentException.class);
        }
        verifyNoInteractions(repository, closeRateQuery, publisher);
    }

    @Test
    void sameRequestDeliveredTwiceGetsTheSameAnswer() throws IOException {
        ContextRequestListener listener = listener();

        listener.onRequest(request());
        listener.onRequest(request());

        ArgumentCaptor<ContextPayload> payloads = ArgumentCaptor.captor();
        verify(publisher, org.mockito.Mockito.times(2))
                .publish(payloads.capture(), eq(REQUEST_EVENT_ID), eq(CORRELATION_ID));
        assertThat(payloads.getAllValues().get(0)).isEqualTo(payloads.getAllValues().get(1));
    }

    private static BackgroundRepository.StoredResponse stored(String response, String fetchedAt) {
        return new BackgroundRepository.StoredResponse(response, OffsetDateTime.parse(fetchedAt));
    }

    private static String worldIndex(String day, String close) {
        return "{\"dataBody\": {\"out2\": [{\"is_cd\": \"TESTIX\", \"is_cd_nm\": \"TEST INDEX\", \"bsnss_dy\": \""
                + day + "\", \"cls_prc_p2\": \"" + close + "\", \"bdy_cmpr_r_p2\": \"0.00\"}]}}";
    }
}
