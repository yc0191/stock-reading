-- 뉴스 검색어 표. 어떤 검색어로 기사를 찾고, 찾은 기사를 어느 종목 태그로 달지를 정한다.
-- 값은 사람이 SQL로 넣고, 수집기는 시작할 때 뷰(news_search_target)를 읽는다.

CREATE TABLE news_keyword (
    id         BIGSERIAL PRIMARY KEY,
    keyword    VARCHAR(100) NOT NULL UNIQUE,
    enabled    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 거시 주제(환율·금리·미국금리). 특정 종목에 속하지 않는 검색어가 이 주제에 걸린다.
CREATE TABLE macro_keyword (
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR(50)  NOT NULL UNIQUE,
    enabled    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 검색어가 무엇에 속하는지. 한 줄은 ETF, 주식, 거시 주제 중 정확히 하나를 가리키고, 한 대상이 검색어를 여럿 가질 수 있다.
CREATE TABLE news_keyword_map (
    id               BIGSERIAL PRIMARY KEY,
    keyword_id       BIGINT NOT NULL REFERENCES news_keyword (id) ON DELETE CASCADE,
    etf_id           BIGINT REFERENCES etf (id) ON DELETE CASCADE,
    stock_id         BIGINT REFERENCES stock (id) ON DELETE CASCADE,
    macro_keyword_id BIGINT REFERENCES macro_keyword (id) ON DELETE CASCADE,
    CHECK (num_nonnulls(etf_id, stock_id, macro_keyword_id) = 1)
);

CREATE UNIQUE INDEX news_keyword_map_etf ON news_keyword_map (keyword_id, etf_id) WHERE etf_id IS NOT NULL;
CREATE UNIQUE INDEX news_keyword_map_stock ON news_keyword_map (keyword_id, stock_id) WHERE stock_id IS NOT NULL;
CREATE UNIQUE INDEX news_keyword_map_macro ON news_keyword_map (keyword_id, macro_keyword_id)
    WHERE macro_keyword_id IS NOT NULL;

-- 거시 주제가 영향을 주는 종목. 연결이 없는 주제의 기사는 시장 전체(*)에 단다.
CREATE TABLE macro_keyword_map (
    id               BIGSERIAL PRIMARY KEY,
    macro_keyword_id BIGINT NOT NULL REFERENCES macro_keyword (id) ON DELETE CASCADE,
    etf_id           BIGINT REFERENCES etf (id) ON DELETE CASCADE,
    stock_id         BIGINT REFERENCES stock (id) ON DELETE CASCADE,
    CHECK (num_nonnulls(etf_id, stock_id) = 1)
);

CREATE UNIQUE INDEX macro_keyword_map_etf ON macro_keyword_map (macro_keyword_id, etf_id) WHERE etf_id IS NOT NULL;
CREATE UNIQUE INDEX macro_keyword_map_stock ON macro_keyword_map (macro_keyword_id, stock_id)
    WHERE stock_id IS NOT NULL;

-- 수집기가 읽는 검색 대상. 한 줄은 "이 검색어로 찾은 기사에 이 태그를 단다"이다.
--   tag     : 기사에 달 종목코드. 시장 전체에 해당하면 '*'.
--   kind    : 태그 대상 종류(ETF, STOCK, MARKET). MARKET이면 market은 비어 있다.
--   origin  : NAME(search_by_name으로 종목 이름을 자동 검색) 또는 KEYWORD(검색어 표).
-- 태그 규칙: 검색어가 ETF·주식에 속하면 그 종목, 거시 주제에 속하면 주제에 연결된 종목들(연결이 없으면 시장 전체).
CREATE VIEW news_search_target AS
SELECT e.name AS query, e.symbol AS tag, e.market AS market, 'ETF' AS kind, 'NAME' AS origin
FROM etf e WHERE e.search_by_name AND e.name IS NOT NULL AND btrim(e.name) <> ''
UNION ALL
SELECT s.name, s.symbol, s.market, 'STOCK', 'NAME'
FROM stock s WHERE s.search_by_name AND s.name IS NOT NULL AND btrim(s.name) <> ''
UNION ALL
SELECT k.keyword, e.symbol, e.market, 'ETF', 'KEYWORD'
FROM news_keyword k
JOIN news_keyword_map m ON m.keyword_id = k.id
JOIN etf e ON e.id = m.etf_id
WHERE k.enabled
UNION ALL
SELECT k.keyword, s.symbol, s.market, 'STOCK', 'KEYWORD'
FROM news_keyword k
JOIN news_keyword_map m ON m.keyword_id = k.id
JOIN stock s ON s.id = m.stock_id
WHERE k.enabled
UNION ALL
SELECT k.keyword, e.symbol, e.market, 'ETF', 'KEYWORD'
FROM news_keyword k
JOIN news_keyword_map m ON m.keyword_id = k.id
JOIN macro_keyword mk ON mk.id = m.macro_keyword_id AND mk.enabled
JOIN macro_keyword_map mm ON mm.macro_keyword_id = mk.id
JOIN etf e ON e.id = mm.etf_id
WHERE k.enabled
UNION ALL
SELECT k.keyword, s.symbol, s.market, 'STOCK', 'KEYWORD'
FROM news_keyword k
JOIN news_keyword_map m ON m.keyword_id = k.id
JOIN macro_keyword mk ON mk.id = m.macro_keyword_id AND mk.enabled
JOIN macro_keyword_map mm ON mm.macro_keyword_id = mk.id
JOIN stock s ON s.id = mm.stock_id
WHERE k.enabled
UNION ALL
SELECT k.keyword, '*', NULL, 'MARKET', 'KEYWORD'
FROM news_keyword k
JOIN news_keyword_map m ON m.keyword_id = k.id
JOIN macro_keyword mk ON mk.id = m.macro_keyword_id AND mk.enabled
WHERE k.enabled
  AND NOT EXISTS (SELECT 1 FROM macro_keyword_map mm WHERE mm.macro_keyword_id = mk.id);
