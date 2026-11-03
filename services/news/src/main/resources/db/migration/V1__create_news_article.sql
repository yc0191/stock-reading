CREATE TABLE news_article (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE,
    article_key     CHAR(64)     NOT NULL UNIQUE,
    title           TEXT         NOT NULL,
    summary         TEXT         NOT NULL,
    url             TEXT         NOT NULL,
    publisher       VARCHAR(255) NOT NULL,
    published_at    TIMESTAMPTZ  NOT NULL,
    matched_queries TEXT[]       NOT NULL DEFAULT '{}',
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_news_article_published_at ON news_article (published_at DESC);

CREATE TABLE news_article_symbol (
    article_id BIGINT      NOT NULL REFERENCES news_article (id) ON DELETE CASCADE,
    symbol     VARCHAR(6)  NOT NULL,
    PRIMARY KEY (article_id, symbol)
);

CREATE INDEX idx_news_article_symbol_symbol_article ON news_article_symbol (symbol, article_id);
