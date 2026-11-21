package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.List;

/** 그래프용 장중 흐름에 쓸 저장된 시세 조회. 테스트에서 바꿀 수 있게 포트로 둔다. */
interface IntradaySeriesPort {

    /**
     * 시세 시각이 [since, asOf] 안이고 받은 시각이 asOf 이하인 가격 0 초과 시세를 1분마다 그 분의 마지막 시세 한 건으로 솎아,
     * 시세 시각이 늦은 것부터 limit건까지 돌려준다.
     */
    List<Point> minuteLasts(String symbol, OffsetDateTime since, OffsetDateTime asOf, int limit);

    record Point(OffsetDateTime asOf, long price) {
    }
}
