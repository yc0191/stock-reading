package dev.stockreading.collector.holdings;

import java.util.List;

/**
 * 관심 ETF가 담은 미국 상위 종목 목록. 공시 수집과 해외 시세 수집이 어느 종목을 볼지 이 목록으로 정한다.
 * 목록은 보유 종목이 드러나므로 설정(.env)으로만 받고, 구현은 값을 로그에 남기지 않는다.
 */
public interface HoldingsPort {

    /** 설정에 있는 ETF 코드 목록(설정 순서). */
    List<String> etfCodes();

    /** ETF 하나의 상위 종목 티커 목록(설정 순서). 설정에 없는 ETF 코드면 빈 목록이다. */
    List<String> topTickers(String etfCode);

    /** 모든 ETF의 티커를 중복 없이 모은 목록(처음 나온 순서). 여러 ETF가 같은 종목을 담아도 한 번만 조회하기 위한 것이다. */
    List<String> allTickers();
}
