package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import tools.jackson.databind.json.JsonMapper;

// 실제 SEC 서버로 응답 구조를 확인하는 수동 도구다. EDGAR_LIVE=1, EDGAR_USER_AGENT, EDGAR_LIVE_TICKER 가 있을 때만 돈다.
// 매핑 파일 1회, submissions 1회만 호출한다. 티커·CIK·연락처는 출력하지 않고 구조만 검사한다.
@EnabledIfEnvironmentVariable(named = "EDGAR_LIVE", matches = "1")
class EdgarClientLiveTest {

    @Test
    void realSubmissionsMatchTheParsedShape() {
        var properties = new EdgarProperties("https://data.sec.gov", "https://www.sec.gov/files/company_tickers.json",
                "https://www.sec.gov/Archives/edgar/data", System.getenv("EDGAR_USER_AGENT"), 10, 10,
                "8-K,8-K/A,6-K", 72);
        var client = new EdgarClient(properties, JsonMapper.builder().build(), Clock.systemUTC(),
                EdgarCallSpacing.perSecond(properties.maxCallsPerSecond()));

        String cik = client.cikOf(System.getenv("EDGAR_LIVE_TICKER"));
        CompanySubmissions submissions = client.submissions(cik);

        assertThat(cik).matches("[0-9]{10}");
        assertThat(submissions.exchanges()).isNotEmpty();
        assertThat(submissions.filings()).isNotEmpty().allSatisfy(filing -> {
            assertThat(filing.acceptedAt().getNano()).isZero();
            assertThat(filing.url()).startsWith("https://www.sec.gov/Archives/edgar/data/");
        });
        assertThat(submissions.filings()).filteredOn(f -> f.form().equals("8-K"))
                .allSatisfy(f -> assertThat(f.items()).isNotEmpty());
    }
}
