package dev.stockreading.collector.schedule;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/** 한국 시간 평일의 시작·끝 시각(양끝 포함) 안인지 본다. 휴장일은 판정하지 않는다. */
final class WeekdayHours {

    private static final ZoneId KOREA = ZoneId.of("Asia/Seoul");

    private WeekdayHours() {
    }

    // 타이머가 몇 초 늦게 불려도 끝 시각 회차가 빠지지 않게 분 단위로 비교한다. 끝이 "15:30" 이면 15:30:59 까지 포함한다.
    static boolean contains(OffsetDateTime at, LocalTime start, LocalTime end) {
        ZonedDateTime korea = at.atZoneSameInstant(KOREA);
        DayOfWeek day = korea.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime time = korea.toLocalTime().truncatedTo(ChronoUnit.MINUTES);
        return !time.isBefore(start) && !time.isAfter(end);
    }
}
