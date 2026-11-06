package dev.stockreading.market.holdings;

import java.util.List;
import java.util.Optional;

/**
 * 관심 ETF가 담은 종목 목록. 배경 자료 응답이 변동 종목에 담긴 미국 종목과 ETF 정보를 이 목록으로 찾는다.
 * 목록은 보유 종목이 드러나므로 값은 저장소 밖 파일로만 받고, 구현은 값을 로그에 남기지 않는다.
 */
public interface HoldingsPort {

    /** ETF 하나의 미국 구성 종목 티커 목록(비중 순위 순). 모르는 ETF 코드면 빈 목록이다. */
    List<String> topTickers(String etfCode);

    /** ETF 하나의 이름·추종 대상·업종과 구성 종목. 목록에 없는 ETF 코드면 비어 있다. */
    Optional<EtfInfo> etfInfo(String etfCode);
}
