package dev.stockreading.collector.members;

import java.time.LocalDate;
import java.util.List;

/** 구성 종목을 자동으로 받을 ETF와 "받을 차례"를 정하는 데 쓰는 값을 시세처리기 표에서 읽는다. 읽기만 한다. */
interface MemberTargetsPort {

    /**
     * @param today 조회하는 날(한국 시간). 지수 교체 달인지 이 날이 든 달로 판단한다
     * @return 출처 번호가 있는 국내 ETF(종목코드 순)
     */
    List<Target> targets(LocalDate today);

    /**
     * @param fundId 출처가 ETF를 부르는 번호(etf.constituent_source_id)
     * @param lastSeenOn 가장 최근 구성을 마지막으로 확인한 날. 받은 구성이 아직 없으면 null
     * @param rebalanceMonth today가 든 달에 이 ETF가 따르는 지수의 교체일이 있으면 true
     */
    record Target(String symbol, String fundId, LocalDate lastSeenOn, boolean rebalanceMonth) {
    }
}
