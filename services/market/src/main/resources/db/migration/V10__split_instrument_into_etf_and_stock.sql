-- instrument 한 표를 ETF(etf)와 주식(stock)으로 나누고, ETF가 담은 종목은 연결 표(etf_stock_map)에 둔다.
-- 한 주식이 여러 ETF에 담길 수 있어 비중과 순위는 주식이 아니라 연결 표에 있다.
-- 보유(HELD)·관심(WATCH) 구분(instrument.relation)은 개인 데이터라 personal 스키마의 member_portfolio로 옮겨 가며,
-- 이 파일은 그 값을 옮기지 않는다(personal 스키마가 아직 없을 수 있고 값은 저장소에 둘 수 없다). 처음 값은 사람이 SQL로 넣는다.

CREATE TABLE etf (
    id                    BIGSERIAL PRIMARY KEY,
    market                VARCHAR(8)   NOT NULL,
    symbol                VARCHAR(16)  NOT NULL,
    name                  VARCHAR(100),
    -- 추종 지수. index_rebalance_schedule.tracks와 같은 값이다.
    tracks                VARCHAR(100),
    -- 운용사 사이트가 ETF를 부르는 번호. 비어 있으면 구성 종목을 자동으로 조회하지 않는다.
    constituent_source_id VARCHAR(64),
    -- 켜면 ETF 이름을 뉴스 검색어로 자동 사용한다.
    search_by_name        BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (market, symbol)
);

CREATE TABLE stock (
    id             BIGSERIAL PRIMARY KEY,
    market         VARCHAR(8)   NOT NULL,
    symbol         VARCHAR(16)  NOT NULL,
    name           VARCHAR(100),
    sector         VARCHAR(100),
    search_by_name BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (market, symbol)
);

-- 기존 행은 id를 그대로 가져와 이력·원문의 etf_id가 같은 ETF를 계속 가리키게 한다.
-- ETF 안에서 ETF를 구성 종목으로 담은 행은 주식 표에 옮길 수 없어 옮기지 않는다(자동 조회와 파일 모두 주식만 담았다).
INSERT INTO etf (id, market, symbol, name, tracks, created_at, updated_at)
SELECT id, market, symbol, name, tracks, updated_at, updated_at FROM instrument WHERE kind = 'ETF';

INSERT INTO stock (id, market, symbol, name, sector, created_at, updated_at)
SELECT id, market, symbol, name, sector, updated_at, updated_at FROM instrument WHERE kind = 'STOCK';

SELECT setval(pg_get_serial_sequence('etf', 'id'), GREATEST(COALESCE((SELECT max(id) FROM etf), 0), 1),
              (SELECT count(*) > 0 FROM etf));
SELECT setval(pg_get_serial_sequence('stock', 'id'), GREATEST(COALESCE((SELECT max(id) FROM stock), 0), 1),
              (SELECT count(*) > 0 FROM stock));

CREATE TABLE etf_stock_map (
    etf_id   BIGINT        NOT NULL REFERENCES etf (id) ON DELETE CASCADE,
    -- 개인 포트폴리오가 주식을 가리키므로 주식 행을 지워 연결이 끊기지 않게 CASCADE를 두지 않는다.
    stock_id BIGINT        NOT NULL REFERENCES stock (id),
    -- 비중(%)과 비중 순위. 비중을 모르는 구성 종목은 비워 둔다.
    weight   NUMERIC(7, 4) CHECK (weight >= 0 AND weight <= 100),
    rank     INTEGER       CHECK (rank >= 1),
    PRIMARY KEY (etf_id, stock_id)
);

INSERT INTO etf_stock_map (etf_id, stock_id, weight, rank)
SELECT em.etf_id, em.member_id, em.weight, em.rank
FROM etf_member em
JOIN etf ON etf.id = em.etf_id
JOIN stock ON stock.id = em.member_id;

-- 주식이 어느 ETF에 담겼는지 거꾸로 찾을 때 쓴다.
CREATE INDEX etf_stock_map_stock_id ON etf_stock_map (stock_id);

-- 이력과 원문은 instrument 대신 etf를 가리키게 하고 이름을 맞춘다.
ALTER TABLE etf_member_history DROP CONSTRAINT etf_member_history_etf_id_fkey;
ALTER TABLE etf_member_raw DROP CONSTRAINT etf_member_raw_etf_id_fkey;

ALTER TABLE etf_member_history RENAME TO etf_stock_history;
ALTER TABLE etf_member_raw RENAME TO etf_stock_raw;
ALTER INDEX etf_member_history_etf_applied RENAME TO etf_stock_history_etf_applied;
ALTER INDEX etf_member_raw_etf_fetched RENAME TO etf_stock_raw_etf_fetched;
ALTER SEQUENCE etf_member_history_id_seq RENAME TO etf_stock_history_id_seq;
ALTER SEQUENCE etf_member_raw_id_seq RENAME TO etf_stock_raw_id_seq;
ALTER TABLE etf_stock_history RENAME CONSTRAINT etf_member_history_pkey TO etf_stock_history_pkey;
ALTER TABLE etf_stock_raw RENAME CONSTRAINT etf_member_raw_pkey TO etf_stock_raw_pkey;

ALTER TABLE etf_stock_history ADD CONSTRAINT etf_stock_history_etf_id_fkey
    FOREIGN KEY (etf_id) REFERENCES etf (id) ON DELETE CASCADE;
ALTER TABLE etf_stock_raw ADD CONSTRAINT etf_stock_raw_etf_id_fkey
    FOREIGN KEY (etf_id) REFERENCES etf (id) ON DELETE CASCADE;

DROP TABLE etf_member;
DROP TABLE instrument;
