package dev.stockreading.market.insight;

import java.time.OffsetDateTime;

/**
 * 직전 미국 정규장 마감의 등락률 한 건.
 *
 * @param ticker    조회한 티커
 * @param date      고른 일봉 레코드의 일자(dt) 원문. 기대한 장보다 오래된 장이 골렸는지 쓰는 쪽이 알 수 있게 함께 준다
 * @param closeAt   그 일자의 정규장 마감 시각(뉴욕 16:00)
 * @param rate      등락율 필드(up_dwn_r_p2) 원문. 공백이나 부호를 손대지 않는다
 * @param fetchedAt 이 값을 담은 응답을 받은 시각
 */
public record OverseasCloseRate(
        String ticker,
        String date,
        OffsetDateTime closeAt,
        String rate,
        OffsetDateTime fetchedAt) {
}
