package dev.stockreading.market.members;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** 수집기가 받은 ETF 구성 종목 결과(etf.members.fetched)를 받아 원문 저장·비교·반영에 넘긴다. */
@Component
class MembersFetchedListener {

    private static final Logger log = LoggerFactory.getLogger(MembersFetchedListener.class);

    private final MemberApplyService service;
    private final ObjectMapper mapper;

    MembersFetchedListener(MemberApplyService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.ETF_MEMBERS_FETCHED, groupId = "market-members")
    void onFetched(String message) {
        MembersFetchedEnvelope event;
        UUID eventId;
        try {
            event = mapper.readValue(message, MembersFetchedEnvelope.class);
            eventId = event == null || event.eventId() == null ? null : UUID.fromString(event.eventId());
        } catch (JacksonException | IllegalArgumentException e) {
            // 다시 읽어도 같은 결과라 재시도하지 않고 버린다. 원문이 없으니 이전 구성이 그대로 남는다.
            log.warn("unreadable member result ignored errorType={}", e.getClass().getSimpleName());
            return;
        }
        if (eventId == null || event.payload() == null) {
            log.warn("unreadable member result ignored errorType=missing-field");
            return;
        }
        // DB 오류는 던져서 리스너 컨테이너가 다시 시도하게 한다(원문과 반영은 한 트랜잭션이라 반쯤 남지 않는다).
        service.apply(eventId, event.payload());
    }
}
