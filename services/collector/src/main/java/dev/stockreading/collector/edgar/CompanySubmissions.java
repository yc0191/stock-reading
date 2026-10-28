package dev.stockreading.collector.edgar;

import java.util.List;

/**
 * 회사 한 곳의 공시 이력(submissions)에서 쓰는 부분. cik 는 앞을 0으로 채운 10자리다.
 * exchanges 는 SEC 응답의 값을 그대로 둔다(예: Nasdaq, NYSE). filings 는 최근 이력(filings.recent)만이다.
 */
public record CompanySubmissions(String cik, List<String> exchanges, List<Filing> filings) {
}
