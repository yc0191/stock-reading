package dev.stockreading.news.searchquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.stockreading.news.event.HoldingsEnvelope.HoldingsPayload;
import dev.stockreading.news.event.HoldingsEnvelope.Instrument;
import dev.stockreading.news.event.HoldingsEnvelope.Membership;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커와 이름)만 쓴다. */
class GeneratedSearchQueriesTest {

    private final SearchQueryRepository repository = mock(SearchQueryRepository.class);
    private final GeneratedSearchQueries generated = new GeneratedSearchQueries(repository);

    private static Instrument etf(String symbol, String relation) {
        return new Instrument("ETF", "KR", symbol, relation, null);
    }

    private static Instrument stock(String market, String symbol, String name) {
        return new Instrument("STOCK", market, symbol, "NONE", name);
    }

    private static Membership member(String etf, String market, String symbol) {
        return new Membership("KR", etf, market, symbol, 1);
    }

    @SuppressWarnings("unchecked")
    private Map<String, ? extends java.util.Collection<String>> generate(HoldingsPayload payload) {
        generated.generate(payload);
        ArgumentCaptor<Map<String, java.util.Collection<String>>> captor = ArgumentCaptor.captor();
        verify(repository).insertGenerated(captor.capture());
        return captor.getValue();
    }

    @Test
    void usMembersOfHeldOrWatchedEtfsGetAQueryNamedAfterTheInstrument() {
        Map<String, ? extends java.util.Collection<String>> result = generate(new HoldingsPayload("h", List.of(
                etf("TEST01", "HELD"), etf("TEST02", "WATCH"),
                stock("US", "FAKE", "Fake Corp"), stock("US", "FAKEB", " Fake Two Inc. ")), List.of(
                member("TEST01", "US", "FAKE"), member("TEST02", "US", "FAKEB"))));

        assertThat(result).containsOnlyKeys("Fake Corp", "Fake Two Inc.");
        assertThat(result.get("Fake Corp")).containsExactly("FAKE");
    }

    @Test
    void sharesOfTheSameCompanyShareOneQueryAndInstrumentsWithoutANameGetNone() {
        Map<String, ? extends java.util.Collection<String>> result = generate(new HoldingsPayload("h", List.of(
                etf("TEST01", "HELD"),
                stock("US", "FAKE-A", "Fake Corp"), stock("US", "FAKE-B", "Fake Corp"),
                stock("US", "FAKEC", null), stock("US", "FAKED", "  ")), List.of(
                member("TEST01", "US", "FAKE-A"), member("TEST01", "US", "FAKE-B"),
                member("TEST01", "US", "FAKEC"), member("TEST01", "US", "FAKED"))));

        assertThat(result).containsOnlyKeys("Fake Corp");
        assertThat(result.get("Fake Corp")).containsExactlyInAnyOrder("FAKE-A", "FAKE-B");
    }

    @Test
    void membersOfEtfsThatAreNotTrackedAndDomesticMembersGetNoQuery() {
        Map<String, ? extends java.util.Collection<String>> result = generate(new HoldingsPayload("h", List.of(
                etf("TEST01", "HELD"), etf("TEST02", "NONE"),
                stock("US", "FAKE", "Fake Corp"), stock("US", "FAKEB", "Fake Two"), stock("KR", "TEST09", "테스트 회사")), List.of(
                member("TEST01", "KR", "TEST09"), member("TEST02", "US", "FAKEB"), member("TEST01", "US", "FAKE"))));

        assertThat(result).containsOnlyKeys("Fake Corp");
    }

    @Test
    void anEmptyListAsksTheRepositoryForNothing() {
        assertThat(generate(new HoldingsPayload("h", List.of(), List.of()))).isEmpty();
    }
}
