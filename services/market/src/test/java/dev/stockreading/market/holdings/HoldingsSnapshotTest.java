package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.HoldingsPayload.Instrument;
import dev.stockreading.market.event.HoldingsPayload.Membership;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class HoldingsSnapshotTest {

    private static final Instrument ETF = new Instrument("ETF", "KR", "TEST01", "HELD", "테스트 ETF");
    private static final Instrument FAKE_A = new Instrument("STOCK", "US", "FAKEA", "NONE", null);
    private static final Instrument FAKE_B = new Instrument("STOCK", "US", "FAKE-B", "NONE", "Fake B");
    private static final Membership M1 = new Membership("KR", "TEST01", "US", "FAKEA", 1);
    private static final Membership M2 = new Membership("KR", "TEST01", "US", "FAKE-B", 2);

    @Test
    void theHashIsA64CharLowerCaseHexAndDoesNotDependOnTheReadOrder() {
        HoldingsSnapshot first = new HoldingsSnapshot(List.of(ETF, FAKE_A, FAKE_B), List.of(M1, M2));
        HoldingsSnapshot shuffled = new HoldingsSnapshot(List.of(FAKE_B, ETF, FAKE_A), List.of(M2, M1));

        assertThat(first.contentHash()).matches("[0-9a-f]{64}");
        assertThat(shuffled.contentHash()).isEqualTo(first.contentHash());
        assertThat(shuffled.instruments()).containsExactlyElementsOf(first.instruments());
    }

    @Test
    void anyChangeToTheContentChangesTheHash() {
        String base = new HoldingsSnapshot(List.of(ETF, FAKE_A), List.of(M1)).contentHash();

        // 종목 추가, 관계 변경, 이름 변경, 순위 변경, 관계 제거가 모두 다른 해시를 만든다.
        assertThat(new HoldingsSnapshot(List.of(ETF, FAKE_A, FAKE_B), List.of(M1)).contentHash()).isNotEqualTo(base);
        assertThat(new HoldingsSnapshot(List.of(new Instrument("ETF", "KR", "TEST01", "WATCH", "테스트 ETF"), FAKE_A),
                List.of(M1)).contentHash()).isNotEqualTo(base);
        assertThat(new HoldingsSnapshot(List.of(new Instrument("ETF", "KR", "TEST01", "HELD", "다른 이름"), FAKE_A),
                List.of(M1)).contentHash()).isNotEqualTo(base);
        assertThat(new HoldingsSnapshot(List.of(ETF, FAKE_A),
                List.of(new Membership("KR", "TEST01", "US", "FAKEA", 2))).contentHash()).isNotEqualTo(base);
        assertThat(new HoldingsSnapshot(List.of(ETF, FAKE_A), List.of()).contentHash()).isNotEqualTo(base);
    }

    @Test
    void theMembershipCarriesNoWeightSoRefreshingWeightsNeverChangesTheHash() {
        // 구성 종목 자동 조회가 같은 구성의 비중만 고쳐도 market.holdings가 다시 나가지 않는다는 전제.
        assertThat(Membership.class.getRecordComponents()).extracting(c -> c.getName())
                .containsExactly("etfMarket", "etfSymbol", "memberMarket", "memberSymbol", "rank");
    }

    @Test
    void aMissingNameAndAnEmptyNameLookTheSameButANullRankDiffersFromRankOne() {
        // 이름이 없는 칸은 payload에서 빠지므로 소비자 입장에서 빈 이름과 구별되지 않는다.
        assertThat(new HoldingsSnapshot(List.of(FAKE_A), List.of()).contentHash())
                .isEqualTo(new HoldingsSnapshot(List.of(new Instrument("STOCK", "US", "FAKEA", "NONE", "")),
                        List.of()).contentHash());
        assertThat(new HoldingsSnapshot(List.of(ETF, FAKE_A), List.of(new Membership("KR", "TEST01", "US", "FAKEA", null)))
                .contentHash())
                .isNotEqualTo(new HoldingsSnapshot(List.of(ETF, FAKE_A), List.of(M1)).contentHash());
    }

    @Test
    void emptyListHasAStableHashAndAnEmptyPayload() {
        HoldingsSnapshot empty = new HoldingsSnapshot(List.of(), List.of());

        assertThat(empty.contentHash()).isEqualTo(new HoldingsSnapshot(List.of(), List.of()).contentHash());
        assertThat(empty.toPayload().instruments()).isEmpty();
        assertThat(empty.toPayload().memberships()).isEmpty();
    }
}
