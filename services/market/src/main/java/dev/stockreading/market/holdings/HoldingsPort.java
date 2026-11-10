package dev.stockreading.market.holdings;

import java.util.List;
import java.util.Optional;

/**
 * 관심 ETF가 담은 종목 목록. 배경 자료 응답이 변동 종목에 담긴 미국 종목과 ETF 정보를 이 목록으로 찾는다.
 * 목록은 보유 종목이 드러나므로 값은 저장소에 두지 않고 DB에서만 읽으며, 구현은 값을 로그에 남기지 않는다.
 */
public interface HoldingsPort {

    /** ETF 하나의 미국 구성 종목 티커 목록(비중 순위 순). 모르는 ETF 코드면 빈 목록이다. */
    List<String> topTickers(String etfCode);

    /** 개인 포트폴리오에 담긴 국내 ETF와 그 관계(종목코드 순, 한 명이라도 보유면 HELD). 아무도 담지 않은 ETF는 빠진다. */
    List<TrackedEtf> trackedEtfs();

    /** 관심 ETF 하나. relation은 HELD 또는 WATCH다. */
    record TrackedEtf(String code, String relation) {
    }

    /** ETF 하나의 이름·추종 대상·업종과 구성 종목. 목록에 없는 ETF 코드면 비어 있다. */
    Optional<EtfInfo> etfInfo(String etfCode);
}
