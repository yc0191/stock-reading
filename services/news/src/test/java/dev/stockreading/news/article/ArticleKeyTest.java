package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ArticleKeyTest {

    private static final String BASE = "https://news.example.com/article/123";

    @Test
    void keyIsLowercaseSha256Hex() {
        assertThat(ArticleKey.of(BASE)).matches("^[0-9a-f]{64}$");
    }

    @Test
    void ignoresWwwPrefix() {
        assertThat(ArticleKey.of("https://www.news.example.com/article/123")).isEqualTo(ArticleKey.of(BASE));
    }

    @Test
    void ignoresScheme() {
        assertThat(ArticleKey.of("http://news.example.com/article/123")).isEqualTo(ArticleKey.of(BASE));
    }

    @Test
    void ignoresHostCase() {
        assertThat(ArticleKey.of("https://News.Example.COM/article/123")).isEqualTo(ArticleKey.of(BASE));
    }

    @Test
    void ignoresFragment() {
        assertThat(ArticleKey.of(BASE + "#comments")).isEqualTo(ArticleKey.of(BASE));
    }

    @Test
    void ignoresUtmParameters() {
        assertThat(ArticleKey.of(BASE + "?utm_source=a&utm_medium=b")).isEqualTo(ArticleKey.of(BASE));
        assertThat(ArticleKey.of(BASE + "?id=1&utm_campaign=c")).isEqualTo(ArticleKey.of(BASE + "?id=1"));
    }

    @Test
    void ignoresTrailingSlash() {
        assertThat(ArticleKey.of(BASE + "/")).isEqualTo(ArticleKey.of(BASE));
    }

    @Test
    void ignoresQueryOrder() {
        assertThat(ArticleKey.of(BASE + "?b=2&a=1")).isEqualTo(ArticleKey.of(BASE + "?a=1&b=2"));
    }

    @Test
    void sortsSameQueryNameByValue() {
        assertThat(ArticleKey.of(BASE + "?a=2&a=1")).isEqualTo(ArticleKey.of(BASE + "?a=1&a=2"));
    }

    @Test
    void keepsPathCase() {
        assertThat(ArticleKey.of("https://news.example.com/Article/123")).isNotEqualTo(ArticleKey.of(BASE));
    }

    @Test
    void differentArticlesGetDifferentKeys() {
        assertThat(ArticleKey.of("https://news.example.com/article/124")).isNotEqualTo(ArticleKey.of(BASE));
        assertThat(ArticleKey.of("https://other.example.com/article/123")).isNotEqualTo(ArticleKey.of(BASE));
        assertThat(ArticleKey.of(BASE + "?id=1")).isNotEqualTo(ArticleKey.of(BASE + "?id=2"));
    }

    @Test
    void nonUrlInputStillGetsStableKey() {
        assertThat(ArticleKey.of("not a url")).isEqualTo(ArticleKey.of("  not a url "));
    }
}
