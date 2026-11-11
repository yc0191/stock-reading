package dev.stockreading.market.members;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 출처 번호가 있는 ETF의 상위 구성 종목을 받아 바뀐 것만 반영한다. 평소에는 한 주에 한 번(fetchDay), 지수 교체일이 든 달에는
 * 매일 받는다. 받기만 하고 바뀐 게 없으면 이력의 마지막 확인일만 고친다.
 * 받기에 실패한 ETF는 이전 구성을 그대로 두고 다음 ETF로 넘어간다(한 ETF의 실패가 다른 ETF를 막지 않는다).
 */
@Component
class MemberFetchService {

    private static final Logger log = LoggerFactory.getLogger(MemberFetchService.class);

    private static final long STALE_DAYS = 7;

    private final MemberSourceProperties properties;
    private final MemberSourcePort source;
    private final MemberRepository repository;

    MemberFetchService(MemberSourceProperties properties, MemberSourcePort source, MemberRepository repository) {
        this.properties = properties;
        this.source = source;
        this.repository = repository;
    }

    /** 오늘(한국 시간) 받을 차례인 ETF의 구성을 받아 반영한다. */
    void run(LocalDate today) {
        if (!properties.enabled()) {
            log.debug("member source is not configured; skipping");
            return;
        }
        int fetched = 0;
        int changed = 0;
        int failed = 0;
        List<MemberRepository.Target> targets = repository.targets();
        if (targets.isEmpty()) {
            // 구성 종목을 받을 펀드 번호(constituent_source_id)를 가진 ETF가 없으면 조용히 멈추므로 한 줄 남긴다.
            log.info("no ETF has a constituent source id; nothing to fetch");
            return;
        }
        for (MemberRepository.Target target : targets) {
            Optional<MemberRepository.Latest> latest = repository.latest(target.etfId());
            if (!isDue(target, latest, today)) {
                continue;
            }
            if (fetched > 0) {
                pause();
            }
            fetched++;
            try {
                if (apply(target, latest, target.fundId(), today)) {
                    changed++;
                }
            } catch (RuntimeException e) {
                // 이전 구성을 그대로 둔다. 종목코드와 주소가 드러나지 않게 오류 종류와 사유만 남긴다.
                failed++;
                log.warn("member fetch failed errorType={} reason={}", e.getClass().getSimpleName(),
                        e instanceof MemberSourcePort.MemberSourceException ? e.getMessage() : "-");
            }
        }
        log.info("member fetch done date={} fetched={} changed={} failed={}", today, fetched, changed, failed);
    }

    /**
     * 조회 요일이거나 지수 교체 달이면 받는다. 마지막 확인일이 7일 넘게 지난 ETF는 요일과 상관없이 받는다.
     * 조회 요일이 공휴일 다음이라 출처가 비어 와 실패하면 다음 주까지 낡은 구성이 남기 때문이다.
     */
    private boolean isDue(MemberRepository.Target target, Optional<MemberRepository.Latest> latest, LocalDate today) {
        // 처음 받는 ETF는 구성이 하나도 없으니 조회 요일까지 기다리지 않는다.
        if (latest.isEmpty() || today.getDayOfWeek() == properties.fetchDay()) {
            return true;
        }
        if (latest.isPresent() && ChronoUnit.DAYS.between(latest.get().lastSeenOn(), today) > STALE_DAYS) {
            return true;
        }
        return repository.inRebalanceMonth(target.tracks(), today);
    }

    /** @return 구성이 바뀌어 반영했으면 true */
    private boolean apply(MemberRepository.Target target, Optional<MemberRepository.Latest> latest, String fundId,
            LocalDate today) {
        LocalDate baseDate = baseDate(today);
        List<Member> members = source.fetchTop(fundId, baseDate,
                (status, body) -> repository.saveRaw(target.etfId(), baseDate, status, body));
        if (latest.isPresent() && sameComposition(latest.get().members(), members)) {
            repository.touch(latest.get().id(), target.etfId(), members, today);
            return false;
        }
        repository.record(target.etfId(), members, today);
        // 처음 받은 구성은 비교할 앞 구성이 없어 바뀐 것으로 세지 않는다.
        return latest.isPresent();
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

    /** 구성을 받을 기준일: 오늘 직전의 평일. 공휴일은 따로 보지 않고, 그 날 자료가 없으면 출처가 비어 와 실패로 처리된다. */
    static LocalDate baseDate(LocalDate today) {
        LocalDate date = today.minusDays(1);
        while (date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY) {
            date = date.minusDays(1);
        }
        return date;
    }

    private void pause() {
        long millis = properties.callInterval().toMillis();
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
