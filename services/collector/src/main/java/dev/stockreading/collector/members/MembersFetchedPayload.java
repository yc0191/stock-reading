package dev.stockreading.collector.members;

import java.util.List;

/** etf.members.fetched 의 payload. response 는 운용사 응답 본문 원문이고, 실패하면 members 는 null, failure 는 사유다. */
record MembersFetchedPayload(String etfSymbol, String baseDate, String fetchedOn, String fetchedAt, int httpStatus,
        String response, List<Member> members, String failure) {
}
