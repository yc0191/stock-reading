-- 정기 뉴스 수집이 쓰는 검색어. 검색어 하나가 어느 종목의 기사로 태그될지는 search_query_symbol에 적는다.
-- 종목 자리의 "*"는 시장 전체 이슈다. 테이블을 직접 고치면 수집기가 재시작 없이 새 검색어로 검색한다.
CREATE TABLE search_query (
    id         BIGSERIAL PRIMARY KEY,
    query      VARCHAR(200) NOT NULL UNIQUE CHECK (btrim(query) <> ''),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 종목 칸은 국내 종목코드와 미국 티커(종류 구분 포함)가 모두 들어가도록 보유 종목 테이블(instrument.symbol)과 같은 길이다.
-- 기사 태그 칸(news_article_symbol.symbol)은 6자라서 이 칸과 길이가 다르다.
CREATE TABLE search_query_symbol (
    query_id BIGINT      NOT NULL REFERENCES search_query (id) ON DELETE CASCADE,
    symbol   VARCHAR(16) NOT NULL CHECK (symbol = '*' OR symbol ~ '^[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?$'),
    PRIMARY KEY (query_id, symbol)
);
