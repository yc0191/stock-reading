package dev.stockreading.market.event;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public final class EventFactory {

    // ISO_OFFSET_DATE_TIME은 초를 항상 출력한다. OffsetDateTime.toString()은 초가 0이면 생략해서
    // 스키마가 요구하는 RFC 3339 "date-time" 형식을 어기게 된다.
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    // 응답에 담는 시각은 KST로 통일한다. DB 드라이버는 UTC로 돌려주므로 같은 시각을 +09:00 표기로 바꾼다.
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private EventFactory() {
    }

    public static <T> Envelope<T> create(
            Clock clock, String eventType, OffsetDateTime occurredAt, String correlationId, T payload) {
        return new Envelope<>(
                UUID.randomUUID().toString(),
                eventType,
                1,
                format(occurredAt),
                format(OffsetDateTime.now(clock)),
                correlationId,
                payload);
    }

    public static String format(OffsetDateTime time) {
        return ISO.format(time);
    }

    public static String formatKst(OffsetDateTime time) {
        return ISO.format(time.atZoneSameInstant(KST).toOffsetDateTime());
    }
}
