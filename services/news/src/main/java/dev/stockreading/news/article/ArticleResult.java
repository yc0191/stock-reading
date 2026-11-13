package dev.stockreading.news.article;

/** 기사 이벤트 하나의 처리 결과. 값은 DB(article_event_result.result)에 그대로 남는다. */
enum ArticleResult {
    /** 이 이벤트로 새 기사가 저장됐다. */
    STORED("stored"),
    /** 같은 기사가 이미 있어 종목·검색어만 합쳤다. */
    MERGED("merged"),
    /** 재시도를 모두 소진해 저장하지 못했다. */
    FAILED("failed");

    private final String value;

    ArticleResult(String value) {
        this.value = value;
    }

    String value() {
        return value;
    }

    static ArticleResult of(String value) {
        for (ArticleResult result : values()) {
            if (result.value.equals(value)) {
                return result;
            }
        }
        throw new IllegalArgumentException("unknown article result " + value);
    }
}
