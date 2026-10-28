-- 장 마감 후 조회 응답(market.insight)을 API별로 한 건=한 행으로 보관한다. response는 해석하지 않은 원문이다.
CREATE TABLE daily_price (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    target      VARCHAR(6)  NOT NULL,
    response    JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE world_index (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    target      TEXT        NOT NULL,
    response    JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 환율종합과 시장종합은 조회 대상 구분값이 없어 target 열을 두지 않는다.
CREATE TABLE exchange_rate_summary (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    response    JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE market_summary (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    response    JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
