-- 기사 태그에 미국 티커(종류 구분 포함)도 담도록 검색어 종목 칸(search_query_symbol.symbol)과 같은 길이·형식으로 넓힌다.
-- 기존 행은 6자 이하라 모두 새 조건을 통과한다.
ALTER TABLE news_article_symbol ALTER COLUMN symbol TYPE VARCHAR(16);
ALTER TABLE news_article_symbol
    ADD CONSTRAINT news_article_symbol_symbol_check
    CHECK (symbol = '*' OR symbol ~ '^[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?$');
