package dev.stockreading.market.members;

import java.time.LocalDate;
import java.util.List;

/** 운용사 사이트처럼 ETF의 구성 종목을 공개하는 곳. 운용사마다 어댑터가 하나씩이다. */
interface MemberSourcePort {

    /**
     * ETF 하나의 상위 구성 종목(주식만, 비중 내림차순, rank 1부터).
     *
     * @param fundId 그 출처가 ETF를 부르는 번호
     * @param baseDate 구성을 받을 기준일
     * @param recorder 해석하기 전에 받은 응답 원문을 넘겨받는다. 상태가 200이 아닌 응답도 넘긴다.
     * @throws MemberSourceException 응답이 없거나 모양이 다르거나 주식 행이 하나도 없을 때. 호출한 쪽은 이전 구성을 유지한다.
     */
    List<Member> fetchTop(String fundId, LocalDate baseDate, RawRecorder recorder);

    /** 받은 응답 원문을 저장하는 쪽. */
    @FunctionalInterface
    interface RawRecorder {

        void record(int httpStatus, String body);
    }

    /** 출처 조회 실패. 메시지에 주소·종목코드를 담지 않는다. */
    class MemberSourceException extends RuntimeException {

        MemberSourceException(String message) {
            super(message);
        }

        MemberSourceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
