package dev.stockreading.collector.edgar;

import java.util.List;

/** SEC EDGAR 조회. 실패하면 종류가 담긴 {@link EdgarApiException} 을 던진다. */
interface EdgarPort {

    /** 티커를 10자리 CIK 로 바꾼다. 종류 구분 표기(점·하이픈)는 SEC 매핑 파일의 하이픈 표기로 맞춰 찾는다. */
    String cikOf(String ticker);

    /** 티커의 상장 거래소(SEC 값 그대로, 예: Nasdaq, NYSE). SEC 에 거래소가 없으면 빈 목록이다. */
    List<String> exchangesOf(String ticker);

    /** 10자리 CIK 의 공시 이력을 조회한다. */
    CompanySubmissions submissions(String cik);
}
