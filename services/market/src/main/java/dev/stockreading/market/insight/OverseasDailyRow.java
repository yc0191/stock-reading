package dev.stockreading.market.insight;

import java.time.OffsetDateTime;

/** overseas_daily_price에 저장된 응답 원문 한 건과 받은 시각. */
record OverseasDailyRow(String response, OffsetDateTime fetchedAt) {
}
