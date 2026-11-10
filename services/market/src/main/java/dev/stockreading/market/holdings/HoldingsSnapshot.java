package dev.stockreading.market.holdings;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

import dev.stockreading.market.event.HoldingsPayload;

/**
 * 종목·구성 테이블에서 읽은 그때의 전체 목록. 읽은 순서와 상관없이 같은 내용이면 같은 해시가 나오도록
 * 종목과 구성 관계를 정렬해 담는다.
 */
record HoldingsSnapshot(List<HoldingsPayload.Instrument> instruments, List<HoldingsPayload.Membership> memberships) {

    private static final Comparator<HoldingsPayload.Instrument> INSTRUMENT_ORDER = Comparator
            .comparing(HoldingsPayload.Instrument::market).thenComparing(HoldingsPayload.Instrument::symbol);
    private static final Comparator<HoldingsPayload.Membership> MEMBERSHIP_ORDER = Comparator
            .comparing(HoldingsPayload.Membership::etfMarket).thenComparing(HoldingsPayload.Membership::etfSymbol)
            .thenComparing(HoldingsPayload.Membership::memberMarket)
            .thenComparing(HoldingsPayload.Membership::memberSymbol);

    // 항목 안의 값에 구분자가 들어 있어도 다른 목록과 같은 문자열이 되지 않도록 제어 문자로 나눈다.
    private static final char FIELD = '\u001f';
    private static final char LINE = '\u001e';

    HoldingsSnapshot {
        instruments = instruments.stream().sorted(INSTRUMENT_ORDER).toList();
        memberships = memberships.stream().sorted(MEMBERSHIP_ORDER).toList();
    }

    String contentHash() {
        StringBuilder canonical = new StringBuilder();
        for (HoldingsPayload.Instrument i : instruments) {
            canonical.append('I').append(FIELD).append(i.kind()).append(FIELD).append(i.market()).append(FIELD)
                    .append(i.symbol()).append(FIELD).append(i.relation()).append(FIELD)
                    .append(i.name() == null ? "" : i.name()).append(LINE);
        }
        for (HoldingsPayload.Membership m : memberships) {
            canonical.append('M').append(FIELD).append(m.etfMarket()).append(FIELD).append(m.etfSymbol())
                    .append(FIELD).append(m.memberMarket()).append(FIELD).append(m.memberSymbol()).append(FIELD)
                    .append(m.rank() == null ? "" : m.rank()).append(LINE);
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    HoldingsPayload toPayload() {
        return new HoldingsPayload(contentHash(), instruments, memberships);
    }
}
