package dev.stockreading.market.snapshot;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 저장 직전에 as_of가 속한 날짜(Asia/Seoul 자정 경계)의 파티션이 있도록 보장한다.
 * 별도 스케줄러 없이 파티션이 없는 날의 이벤트도 저장 실패 없이 받기 위해서다.
 */
@Component
class PricePartitions {

    private static final Logger log = LoggerFactory.getLogger(PricePartitions.class);
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter SUFFIX = DateTimeFormatter.BASIC_ISO_DATE;

    private final PartitionPort port;
    // 이미 확인한 날짜. 재시작하면 비워지며, 그때는 DB에서 있음을 확인하고 건너뛴다.
    private final Set<LocalDate> known = ConcurrentHashMap.newKeySet();

    PricePartitions(PartitionPort port) {
        this.port = port;
    }

    void ensure(OffsetDateTime asOf) {
        LocalDate day = asOf.atZoneSameInstant(ZONE).toLocalDate();
        if (known.contains(day)) {
            return;
        }
        OffsetDateTime from = day.atStartOfDay(ZONE).toOffsetDateTime();
        OffsetDateTime to = day.plusDays(1).atStartOfDay(ZONE).toOffsetDateTime();
        port.createIfAbsent("price_snapshot_p" + SUFFIX.format(day), from, to);
        known.add(day);
        log.debug("partition ready day={}", day);
    }
}
