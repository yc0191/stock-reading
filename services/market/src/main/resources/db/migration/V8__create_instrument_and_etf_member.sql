-- 관심 종목과 ETF 구성. 종목은 instrument 한 곳에 한 번만 적고, ETF가 어떤 종목을 얼마나 담았는지는 etf_member에만 적는다.
-- 시장(market)에는 CHECK를 두지 않아 새 시장이 생겨도 저장이 막히지 않는다.

CREATE TABLE instrument (
    id         BIGSERIAL PRIMARY KEY,
    kind       VARCHAR(8)   NOT NULL CHECK (kind IN ('ETF', 'STOCK')),
    market     VARCHAR(8)   NOT NULL,
    symbol     VARCHAR(16)  NOT NULL,
    name       VARCHAR(100),
    -- 추종 대상은 ETF에만 있다.
    tracks     VARCHAR(100),
    sector     VARCHAR(100),
    -- 내가 보유(HELD) 중인지, 지켜보는(WATCH) 종목인지, 구성 종목으로만 알고 있는(NONE) 종목인지.
    relation   VARCHAR(8)   NOT NULL DEFAULT 'NONE' CHECK (relation IN ('HELD', 'WATCH', 'NONE')),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (market, symbol),
    CHECK (kind = 'ETF' OR tracks IS NULL)
);

-- 같은 종목이 여러 ETF에 담기면 ETF마다 한 줄이다. etf_id가 ETF 종목인지는 테이블 제약으로 막지 못해 적재 코드가 지킨다.
CREATE TABLE etf_member (
    etf_id    BIGINT        NOT NULL REFERENCES instrument (id) ON DELETE CASCADE,
    member_id BIGINT        NOT NULL REFERENCES instrument (id) ON DELETE CASCADE,
    -- 비중(%)과 비중 순위. 비중을 모르는 구성 종목은 비워 둔다.
    weight    NUMERIC(7, 4) CHECK (weight >= 0 AND weight <= 100),
    rank      INTEGER       CHECK (rank >= 1),
    PRIMARY KEY (etf_id, member_id),
    CHECK (etf_id <> member_id)
);

-- 종목이 어느 ETF에 담겼는지 거꾸로 찾을 때 쓴다.
CREATE INDEX etf_member_member_id ON etf_member (member_id);
