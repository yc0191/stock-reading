package dev.stockreading.market.members;

import java.time.OffsetDateTime;
import java.util.Optional;

/** 데일리 판이 ETF의 상위 구성 종목 변경을 읽는 길. 저장 상태를 바꾸지 않는다. */
public interface MemberChangesPort {

    /**
     * @param etfCode 국내 ETF 종목코드
     * @param since 판 요청 시간 범위의 시작
     * @param asOf 판 요청 시간 범위의 끝
     * @return 범위 안에 반영된 변경. 없으면 빈 값
     */
    Optional<MemberChanges> changes(String etfCode, OffsetDateTime since, OffsetDateTime asOf);
}
