package dev.stockreading.market.holdings;

import java.util.List;

/**
 * 관심 ETF가 담은 미국 상위 종목 목록. 배경 자료 응답이 변동 종목에 담긴 미국 종목을 이 목록으로 찾는다.
 * 목록은 보유 종목이 드러나므로 설정(.env)으로만 받고, 구현은 값을 로그에 남기지 않는다.
 */
public interface HoldingsPort {

    /** ETF 하나의 상위 종목 티커 목록(설정 순서). 설정에 없는 ETF 코드면 빈 목록이다. */
    List<String> topTickers(String etfCode);
}
