package dev.stockreading.collector.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** '받을 차례' 규칙과 ETF마다 한 건씩 발행하는 것을 가짜 출처·가짜 Kafka 로 본다. 이 규칙은 시세처리기에서 옮겨 온 것과 같다. */
class MembersPublisherTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    // 2026-11-09는 월요일, 2026-11-10은 화요일이다(한국 시간 08:00).
    private static final Clock MONDAY = Clock.fixed(Instant.parse("2026-11-08T23:00:00Z"), KST);
    private static final Clock TUESDAY = Clock.fixed(Instant.parse("2026-11-09T23:00:00Z"), KST);
    private static final LocalDate MONDAY_DATE = LocalDate.parse("2026-11-09");
    private static final LocalDate TUESDAY_DATE = LocalDate.parse("2026-11-10");
    private static final String CORR = "collect-etf-members-2026-11-09T08:00:00+09:00";

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private MemberSourcePort source;
    private MemberTargetsPort targets;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    @BeforeEach
    void setUp() {
        source = mock(MemberSourcePort.class);
        targets = mock(MemberTargetsPort.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private MembersPublisher publisher(String template, Clock clock) {
        return new MembersPublisher(new MemberSourceProperties(template, null, 10, Duration.ZERO, null,
                DayOfWeek.MONDAY), targets, source, kafka, mapper, clock);
    }

    private static List<Member> members(String... symbols) {
        List<Member> list = new ArrayList<>();
        for (String symbol : symbols) {
            list.add(new Member("US", symbol, null, "1.0", list.size() + 1));
        }
        return list;
    }

    private void tracked(LocalDate today, MemberTargetsPort.Target... list) {
        when(targets.targets(today)).thenReturn(List.of(list));
    }

    private static MemberTargetsPort.Target target(String symbol, String fundId, LocalDate lastSeenOn,
            boolean rebalanceMonth) {
        return new MemberTargetsPort.Target(symbol, fundId, lastSeenOn, rebalanceMonth);
    }

    /** 출처가 해석 전에 원문을 넘기고(실제 어댑터처럼) 구성을 돌려주게 한다. */
    private void answers(String fundId, int status, String body, List<Member> result) {
        when(source.fetchTop(eq(fundId), any(), any())).thenAnswer(invocation -> {
            invocation.<MemberSourcePort.RawRecorder>getArgument(2).record(status, body);
            return result;
        });
    }

    private List<JsonNode> sent() {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, org.mockito.Mockito.atLeast(0)).send(eq(Topics.ETF_MEMBERS_FETCHED), anyString(), value.capture());
        return value.getAllValues().stream().map(mapper::readTree).toList();
    }

    @Test
    void aFetchedCompositionIsPublishedWithItsRawAndPassesTheContract() throws IOException {
        tracked(MONDAY_DATE, target("TEST01", "F1", null, false));
        answers("F1", 200, " {\"pdf\": 1}\n", members("AAA", "BBB"));

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.ETF_MEMBERS_FETCHED), eq("TEST01"), value.capture());
        assertThat(ContractSchemas.validate("etf.members.fetched.v1.schema.json", value.getValue())).isEmpty();
        JsonNode event = mapper.readTree(value.getValue());
        assertThat(event.path("correlationId").asString()).isEqualTo(CORR);
        JsonNode payload = event.path("payload");
        assertThat(payload.path("etfSymbol").asString()).isEqualTo("TEST01");
        // 기준일은 오늘 직전 평일(월요일이면 금요일), 조회한 날은 한국 시간 오늘이다.
        assertThat(payload.path("baseDate").asString()).isEqualTo("2026-11-06");
        assertThat(payload.path("fetchedOn").asString()).isEqualTo("2026-11-09");
        // 원문은 앞뒤 공백도 걷지 않고 그대로 싣는다.
        assertThat(payload.path("response").asString()).isEqualTo(" {\"pdf\": 1}\n");
        assertThat(payload.path("members")).hasSize(2);
        assertThat(payload.path("failure").isNull()).isTrue();
        verify(source).fetchTop(eq("F1"), eq(LocalDate.parse("2026-11-06")), any());
    }

    @Test
    void aFailedResponseIsPublishedWithItsRawAndNoMembersSoThePreviousCompositionStays() throws IOException {
        tracked(MONDAY_DATE, target("TEST01", "F1", null, false), target("TEST02", "F2", null, false));
        when(source.fetchTop(eq("F1"), any(), any())).thenAnswer(invocation -> {
            invocation.<MemberSourcePort.RawRecorder>getArgument(2).record(503, "down");
            throw new MemberSourcePort.MemberSourceException("unexpected status 503");
        });
        answers("F2", 200, "{}", members("CCC"));

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        List<JsonNode> events = sent();
        assertThat(events).hasSize(2);
        for (JsonNode event : events) {
            assertThat(ContractSchemas.validate("etf.members.fetched.v1.schema.json", event.toString())).isEmpty();
        }
        JsonNode failed = events.get(0).path("payload");
        assertThat(failed.path("httpStatus").asInt()).isEqualTo(503);
        assertThat(failed.path("response").asString()).isEqualTo("down");
        assertThat(failed.path("members").isNull()).isTrue();
        assertThat(failed.path("failure").asString()).isEqualTo("unexpected status 503");
        // 한 ETF의 실패가 다음 ETF를 막지 않는다.
        assertThat(events.get(1).path("payload").path("etfSymbol").asString()).isEqualTo("TEST02");
    }

    @Test
    void noResponseAtAllIsNotPublishedAndTheNextEtfStillRuns() {
        tracked(MONDAY_DATE, target("TEST01", "F1", null, false), target("TEST02", "F2", null, false));
        when(source.fetchTop(eq("F1"), any(), any()))
                .thenThrow(new MemberSourcePort.MemberSourceException("request failed"));
        answers("F2", 200, "{}", members("CCC"));

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        verify(kafka, never()).send(anyString(), eq("TEST01"), anyString());
        verify(kafka).send(eq(Topics.ETF_MEMBERS_FETCHED), eq("TEST02"), anyString());
    }

    @Test
    void anUnexpectedErrorAfterTheResponseIsPublishedAsAFailure() {
        tracked(MONDAY_DATE, target("TEST01", "F1", null, false));
        when(source.fetchTop(eq("F1"), any(), any())).thenAnswer(invocation -> {
            invocation.<MemberSourcePort.RawRecorder>getArgument(2).record(200, "{}");
            throw new IllegalStateException("bug");
        });

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        assertThat(sent()).singleElement().satisfies(event -> assertThat(event.path("payload").path("failure").asString())
                .isEqualTo("unexpected error IllegalStateException"));
    }

    @Test
    void onANonFetchDayNothingIsFetchedUnlessTheIndexRebalancesThisMonth() {
        tracked(TUESDAY_DATE, target("TEST01", "F1", MONDAY_DATE, true), target("TEST02", "F2", MONDAY_DATE, false));
        answers("F1", 200, "{}", members("AAA"));

        publisher("http://x/{fundId}", TUESDAY).publishFetched(CORR);

        verify(source).fetchTop(eq("F1"), eq(MONDAY_DATE), any());
        verify(source, never()).fetchTop(eq("F2"), any(), any());
    }

    @Test
    void anEtfWithNoCompositionYetIsFetchedWithoutWaitingForTheFetchDay() {
        tracked(TUESDAY_DATE, target("TEST01", "F1", null, false));
        answers("F1", 200, "{}", members("AAA"));

        publisher("http://x/{fundId}", TUESDAY).publishFetched(CORR);

        verify(kafka).send(eq(Topics.ETF_MEMBERS_FETCHED), eq("TEST01"), anyString());
    }

    @Test
    void anEtfNotSeenForMoreThanSevenDaysIsFetchedOnAnyWeekday() {
        // 8일 전이면 받고, 7일 전이면 아직 받지 않는다.
        tracked(TUESDAY_DATE, target("TEST01", "F1", TUESDAY_DATE.minusDays(8), false),
                target("TEST02", "F2", TUESDAY_DATE.minusDays(7), false));
        answers("F1", 200, "{}", members("AAA"));

        publisher("http://x/{fundId}", TUESDAY).publishFetched(CORR);

        verify(source).fetchTop(eq("F1"), any(), any());
        verify(source, never()).fetchTop(eq("F2"), any(), any());
    }

    @Test
    void onTheFetchDayEveryEtfWithASourceIdIsFetchedOncePerRequest() {
        tracked(MONDAY_DATE, target("TEST01", "F1", MONDAY_DATE.minusDays(7), false),
                target("TEST02", "F2", MONDAY_DATE.minusDays(1), false));
        answers("F1", 200, "{}", members("AAA"));
        answers("F2", 200, "{}", members("BBB"));

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        verify(source).fetchTop(eq("F1"), any(), any());
        verify(source).fetchTop(eq("F2"), any(), any());
        assertThat(sent()).hasSize(2);
    }

    @Test
    void aSymbolThatBreaksTheContractIsNotFetched() {
        tracked(MONDAY_DATE, target("BAD", "F1", null, false));

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        verifyNoInteractions(source, kafka);
    }

    @Test
    void doesNothingWithoutASourceTemplateOrTargets() {
        publisher("", MONDAY).publishFetched(CORR);
        verifyNoInteractions(source, targets, kafka);

        tracked(MONDAY_DATE);
        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);
        verifyNoInteractions(source, kafka);
    }

    @Test
    void unreadableTargetsStopTheRoundWithoutThrowing() {
        when(targets.targets(any())).thenThrow(new IllegalStateException("db down"));

        publisher("http://x/{fundId}", MONDAY).publishFetched(CORR);

        verifyNoInteractions(source, kafka);
    }

    @Test
    void theBaseDateIsTheWeekdayBeforeToday() {
        assertThat(MembersPublisher.baseDate(LocalDate.parse("2026-11-09"))).isEqualTo(LocalDate.parse("2026-11-06"));
        assertThat(MembersPublisher.baseDate(LocalDate.parse("2026-11-10"))).isEqualTo(LocalDate.parse("2026-11-09"));
        assertThat(MembersPublisher.baseDate(LocalDate.parse("2026-11-14"))).isEqualTo(LocalDate.parse("2026-11-13"));
        assertThat(MembersPublisher.baseDate(LocalDate.parse("2026-11-15"))).isEqualTo(LocalDate.parse("2026-11-13"));
    }

    @Test
    void callsAreSpacedByTheIntervalAndOnlyBetweenRequests() {
        tracked(MONDAY_DATE, target("TEST01", "F1", null, false), target("TEST02", "F2", null, false));
        List<Long> at = new ArrayList<>();
        when(source.fetchTop(anyString(), any(), any())).thenAnswer(invocation -> {
            at.add(System.nanoTime());
            invocation.<MemberSourcePort.RawRecorder>getArgument(2).record(200, "{}");
            return members("AAA");
        });
        MembersPublisher spaced = new MembersPublisher(new MemberSourceProperties("http://x/{fundId}", null, 10,
                Duration.ofMillis(200), null, DayOfWeek.MONDAY), targets, source, kafka, mapper, MONDAY);

        spaced.publishFetched(CORR);

        assertThat(at).hasSize(2);
        assertThat(Duration.ofNanos(at.get(1) - at.get(0))).isGreaterThanOrEqualTo(Duration.ofMillis(200));
    }
}
