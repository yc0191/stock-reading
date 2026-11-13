package dev.stockreading.market.members;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 수집기가 받은 구성 종목 결과(etf.members.fetched)를 원문 저장 → 앞 구성과 비교 → 반영한다. 운용사 사이트는 부르지 않는다.
 * 원문 저장과 반영을 한 트랜잭션에 묶어, 반영 중에 실패하면 원문도 남지 않아 다시 전달된 이벤트가 처음부터 다시 처리된다.
 * 같은 eventId 는 원문의 유일 제약으로 한 번만 반영한다. 받기에 실패한 결과는 원문만 남기고 이전 구성을 그대로 둔다.
 */
@Component
class MemberApplyService {

    enum Result { CHANGED, FIRST, SAME, FAILED_KEPT, DUPLICATE, UNKNOWN_ETF }

    private static final Logger log = LoggerFactory.getLogger(MemberApplyService.class);

    private final MemberRepository repository;
    private final TransactionTemplate transaction;

    MemberApplyService(MemberRepository repository, TransactionTemplate transaction) {
        this.repository = repository;
        this.transaction = transaction;
    }

    Result apply(UUID eventId, MembersFetchedEnvelope.Payload payload) {
        Result result = transaction.execute(status -> applyInTransaction(eventId, payload));
        // 종목코드는 보유 종목이 드러나므로 남기지 않는다.
        if (result == Result.FAILED_KEPT) {
            log.warn("member result kept previous composition eventId={} reason={}", eventId, payload.failure());
        } else {
            log.info("member result {} eventId={}", result, eventId);
        }
        return result;
    }

    private Result applyInTransaction(UUID eventId, MembersFetchedEnvelope.Payload payload) {
        Optional<Long> etf = repository.etfId(payload.etfSymbol());
        if (etf.isEmpty()) {
            return Result.UNKNOWN_ETF;
        }
        long etfId = etf.get();
        if (!repository.saveRaw(eventId, etfId, LocalDate.parse(payload.baseDate()), payload.httpStatus(),
                payload.response(), OffsetDateTime.parse(payload.fetchedAt()))) {
            return Result.DUPLICATE;
        }
        List<Member> members = payload.members();
        if (members == null || members.isEmpty()) {
            return Result.FAILED_KEPT;
        }
        LocalDate fetchedOn = LocalDate.parse(payload.fetchedOn());
        Optional<MemberRepository.Latest> latest = repository.latest(etfId);
        if (latest.isPresent() && sameComposition(latest.get().members(), members)) {
            repository.touch(latest.get().id(), etfId, members, fetchedOn);
            return Result.SAME;
        }
        repository.record(etfId, members, fetchedOn);
        // 처음 받은 구성은 비교할 앞 구성이 없어 바뀐 것으로 세지 않는다.
        return latest.isPresent() ? Result.CHANGED : Result.FIRST;
    }

    /** 순위까지 같을 때만 같은 구성이다. 비중과 이름은 비교하지 않는다(비중은 touch가 따로 맞춘다). */
    static boolean sameComposition(List<Member> before, List<Member> after) {
        if (before.size() != after.size()) {
            return false;
        }
        for (int i = 0; i < before.size(); i++) {
            Member b = before.get(i);
            Member a = after.get(i);
            if (b.rank() != a.rank() || !MemberChanges.key(b).equals(MemberChanges.key(a))) {
                return false;
            }
        }
        return true;
    }
}
