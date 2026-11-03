-- 시세 조회가 실패했거나 건너뛰었다는 사실(market.snapshot.failed)의 기록.
-- 컨슈머 처리 실패 메시지를 담는 news의 failed_event와 다른 용도라 이름과 구조를 따로 둔다.
-- 건수가 많지 않아 파티션하지 않는다.
CREATE TABLE quote_failure (
    id                   BIGSERIAL PRIMARY KEY,
    event_id             UUID        NOT NULL UNIQUE,
    correlation_id       TEXT        NOT NULL,
    symbol               VARCHAR(6)  NOT NULL,
    failure_type         TEXT        NOT NULL,
    occurred_at          TIMESTAMPTZ NOT NULL,
    scheduled_for        TIMESTAMPTZ NOT NULL,
    http_status          INT,
    response_code        TEXT,
    elapsed_ms           BIGINT,
    exception_type       TEXT,
    token_age_ms         BIGINT,
    token_remaining_ms   BIGINT,
    concurrent_calls     INT,
    tick_succeeded       INT,
    tick_failed          INT,
    response_raw         JSONB,
    received_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_quote_failure_symbol_occurred_at ON quote_failure (symbol, occurred_at DESC);
