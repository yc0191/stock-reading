package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NewsTextTest {

    @Test
    void stripsTagsWithoutLeavingSpaces() {
        assertThat(NewsText.clean("<b>테스트</b>종목 실적")).isEqualTo("테스트종목 실적");
    }

    @Test
    void decodesNamedAndNumericEntities() {
        assertThat(NewsText.clean("&quot;3% 뛰더니&quot; &amp; &#39;급락&#39; &#x41;")).isEqualTo("\"3% 뛰더니\" & '급락' A");
    }

    @Test
    void escapedTagsStayTextInsteadOfBecomingTags() {
        // 태그를 먼저 지우고 엔티티를 되돌리므로, 글자로 쓰인 &lt;b&gt; 는 태그로 되살아나지 않는다.
        assertThat(NewsText.clean("a &lt;b&gt;x&lt;/b&gt; c")).isEqualTo("a <b>x</b> c");
    }

    @Test
    void unknownOrInvalidEntitiesAreKept() {
        assertThat(NewsText.clean("a &foo; b &#99999999999; c")).isEqualTo("a &foo; b &#99999999999; c");
    }

    @Test
    void collapsesWhitespaceAndNbsp() {
        assertThat(NewsText.clean("  가\n\n나&nbsp;&nbsp;다 라  ")).isEqualTo("가 나 다 라");
    }

    @Test
    void nullBecomesEmpty() {
        assertThat(NewsText.clean(null)).isEmpty();
    }

    @Test
    void titleKeyIgnoresLeadingBracketsPunctuationAndCase() {
        assertThat(NewsText.titleKey("[특징주] 가나·다라, AI 하락(종합)"))
                .isEqualTo(NewsText.titleKey("가나 다라 AI 하락 종합"));
        assertThat(NewsText.titleKey("[ESG][단독] A 사 B")).isEqualTo("a사b");
    }

    @Test
    void titleKeyKeepsDifferentTitlesApart() {
        assertThat(NewsText.titleKey("테스트종목 급락")).isNotEqualTo(NewsText.titleKey("테스트종목 급등"));
    }

    @Test
    void urlKeyIgnoresSchemeCaseTrackingFragmentAndTrailingSlash() {
        String base = NewsText.urlKey("https://news.example.com/a/b?id=1");
        assertThat(NewsText.urlKey("http://NEWS.example.com/a/b/?id=1&utm_source=x#top")).isEqualTo(base);
    }

    @Test
    void urlKeyKeepsRealQueryDifferences() {
        assertThat(NewsText.urlKey("https://n.example.com/a?id=1")).isNotEqualTo(NewsText.urlKey("https://n.example.com/a?id=2"));
    }
}
