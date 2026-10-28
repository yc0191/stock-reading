package dev.stockreading.collector.edgar;

/** SEC EDGAR 조회. 실패하면 종류가 담긴 {@link EdgarApiException} 을 던진다. */
public interface EdgarPort {

    /** 티커를 10자리 CIK 로 바꾼다. 종류 구분 표기(점·하이픈)는 SEC 매핑 파일의 하이픈 표기로 맞춰 찾는다. */
    String cikOf(String ticker);

    /** 10자리 CIK 의 공시 이력을 조회한다. */
    CompanySubmissions submissions(String cik);
}
