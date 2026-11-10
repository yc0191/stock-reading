package dev.stockreading.collector.holdings;

import java.util.List;

/**
 * 관심 ETF가 담은 미국 상위 종목 목록. 공시 수집과 해외 시세 수집이 어느 종목을 볼지 이 목록으로 정한다.
 * 목록은 시세처리기가 알리는 market.holdings 이벤트로만 받고, 구현은 값을 로그에 남기지 않는다.
 */
public interface HoldingsPort {

    /**
     * 관심 ETF(HELD 또는 WATCH)가 담은 미국 종목 티커를 중복 없이 모은 목록(ETF 코드 순, ETF 안에서는 비중 순위 순).
     * 여러 ETF가 같은 종목을 담아도 한 번만 조회하기 위한 것이다. 목록을 아직 받지 못했으면 빈 목록이다.
     */
    List<String> allTickers();

    /**
     * 그 미국 티커를 구성 종목으로 담은 관심 ETF 코드 목록(코드 순). 담은 ETF가 없거나 목록을 아직 받지 못했으면 빈 목록이다.
     * 미국 티커로 건 뉴스 검색이 제대로 되지 않았을 때, 그 기사가 함께 나가는 ETF의 수집도 불완전으로 알리는 데 쓴다.
     */
    List<String> etfsHolding(String ticker);
}
