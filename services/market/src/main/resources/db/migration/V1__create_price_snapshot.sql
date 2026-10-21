CREATE TABLE price_snapshot (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID        NOT NULL UNIQUE,
    symbol      VARCHAR(6)  NOT NULL,
    as_of       TIMESTAMPTZ NOT NULL,
    price       BIGINT      NOT NULL CHECK (price >= 0),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_price_snapshot_symbol_as_of ON price_snapshot (symbol, as_of DESC);
