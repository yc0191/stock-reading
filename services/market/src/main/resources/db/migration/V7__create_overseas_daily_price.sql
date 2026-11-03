-- 보유 미국 종목의 해외 일봉 응답(market.insight, apiType overseas-daily-price)을 한 건=한 행으로 보관한다.
-- target은 미국 티커이고, 거래소 코드는 response 원문에 있다. response는 해석하지 않은 원문이다.
CREATE TABLE overseas_daily_price (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    target      VARCHAR(16) NOT NULL,
    response    JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 종목과 기간으로 읽으므로 둘을 묶어 색인한다.
CREATE INDEX overseas_daily_price_target_fetched_at ON overseas_daily_price (target, fetched_at);
