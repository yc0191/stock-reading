package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.Optional;

/** 일봉이 없는 ETF의 하루 등락 대체값에 쓸 저장된 장중 시세 조회. 테스트에서 바꿀 수 있게 포트로 둔다. */
interface IntradayQuotePort {

    /** 시세 시각과 받은 시각이 모두 [since, asOf] 안인 시세 중 가장 늦게 받은 것. */
    Optional<Quote> lastReceived(String symbol, OffsetDateTime since, OffsetDateTime asOf);

    record Quote(OffsetDateTime receivedAt, long price, String raw) {
    }
}
