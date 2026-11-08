package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;

/** 시세처리기 운영 상태를 계약 모양으로 만든다. 수집 종류 4개와 알려진 시세 실패 종류는 0건이어도 빠뜨리지 않는다. */
@Component
class OperationsReader {

    /** 응답에 항상 담는 수집 종류와 순서. */
    static final List<String> COLLECTION_KINDS = List.of("pre-open", "quote-1s", "intraday", "post-close");

    /** 건수가 0이어도 항상 담는 시세 실패 종류. market.daily.responded v2 스키마의 required와 같다. */
    static final List<String> QUOTE_FAILURE_KINDS = List.of("TOKEN_ISSUE_FAILED", "HTTP_ERROR", "RESULT_CODE_FAILED",
            "EMPTY_PRICE", "PRICE_PARSE_FAILED", "CONNECTION_FAILED", "TIMEOUT", "FIELD_MISMATCH",
            "TICK_SKIPPED_RATE_LIMITED", "TICK_SKIPPED_SUPERSEDED", "TICK_SKIPPED_OTHER");

    private final OperationsRepository repository;

    OperationsReader(OperationsRepository repository) {
        this.repository = repository;
    }

    DailyPayload.Operations read(OffsetDateTime since, OffsetDateTime asOf) {
        Map<String, OperationsRepository.CollectionRow> byKind = repository.collections(since, asOf).stream()
                .collect(Collectors.toMap(OperationsRepository.CollectionRow::kind, row -> row));
        List<DailyPayload.CollectionStatus> collections = COLLECTION_KINDS.stream().map(kind -> {
            OperationsRepository.CollectionRow row = byKind.get(kind);
            return row == null
                    ? new DailyPayload.CollectionStatus(kind, null, null, 0)
                    : new DailyPayload.CollectionStatus(kind, EventFactory.formatKst(row.firstAt()),
                            EventFactory.formatKst(row.lastAt()), row.count());
        }).toList();

        Map<String, Integer> failures = new LinkedHashMap<>();
        QUOTE_FAILURE_KINDS.forEach(kind -> failures.put(kind, 0));
        // 모르는 종류는 이름 그대로 더해, 새 실패 종류가 조용히 사라지지 않게 한다.
        failures.putAll(repository.quoteFailures(since, asOf));

        List<DailyPayload.DroppedTopic> dropped = repository.droppedMessages(since, asOf).stream()
                .map(row -> new DailyPayload.DroppedTopic(row.topic(), row.count())).toList();
        return new DailyPayload.Operations(collections, failures, dropped);
    }
}
