-- SEC 공시(filing.collected) 한 건을 한 행으로 보관한다. 본문은 저장하지 않고 메타데이터만 둔다.
CREATE TABLE filing (
    id               BIGSERIAL PRIMARY KEY,
    event_id         UUID        NOT NULL UNIQUE,
    accession_number VARCHAR(20) NOT NULL UNIQUE,
    cik              CHAR(10)    NOT NULL,
    form             VARCHAR(20) NOT NULL,
    items            TEXT[]      NOT NULL DEFAULT '{}',
    accepted_at      TIMESTAMPTZ NOT NULL,
    filing_date      DATE        NOT NULL,
    url              TEXT        NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 같은 공시에 보유 티커가 나중에 더 연결되어도 공시는 한 행이고 티커만 늘어난다.
CREATE TABLE filing_ticker (
    filing_id BIGINT      NOT NULL REFERENCES filing (id) ON DELETE CASCADE,
    ticker    VARCHAR(16) NOT NULL,
    PRIMARY KEY (filing_id, ticker)
);

-- 종목과 접수 시각 기간으로 읽는다.
CREATE INDEX idx_filing_ticker_ticker_filing ON filing_ticker (ticker, filing_id);
CREATE INDEX idx_filing_accepted_at ON filing (accepted_at);
