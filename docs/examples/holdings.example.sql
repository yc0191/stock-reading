-- 업무 데이터(종목·구성 종목·검색어·멤버·포트폴리오)를 DB에 처음 넣는 SQL 예시. 값은 모두 가짜(TEST·FAKE 접두)다.
-- 실제 값은 이 저장소에 두지 않고 저장소 밖 비공개 SQL로만 넣는다(docs/local-development.md의 "처음 값 넣기").
-- 실행 계정이 다르다: 1~3은 시세처리기 계정(market 스키마 주인), 4는 personal 쓰기 계정이다.
-- psql -v ON_ERROR_STOP=1 -U <계정> -d <데이터베이스> -f <파일> 로 계정마다 해당 부분만 따로 실행한다.
-- 시세처리기 계정의 기본 검색 경로가 market이라 market 표는 스키마 이름 없이 쓴다.

-- 1. [market 계정] ETF와 주식. market은 시장 코드(KR, US), symbol은 종목코드·티커다.
--    constituent_source_id를 넣은 ETF만 구성 종목을 자동 조회한다(출처가 ETF를 부르는 번호). search_by_name을 켜면 이름을 뉴스 검색어로 쓴다.
INSERT INTO etf (market, symbol, name, tracks, constituent_source_id, search_by_name) VALUES
    ('KR', 'TEST01', 'TEST ETF ONE', 'TEST INDEX', 'FUND-ID-1', TRUE),
    ('KR', 'TEST02', 'TEST ETF TWO', NULL, NULL, FALSE);
INSERT INTO stock (market, symbol, name, sector, search_by_name) VALUES
    ('US', 'FAKEA', 'FAKE CORP A', 'TEST SECTOR', FALSE),
    ('US', 'FAKE-B', 'FAKE CORP B', NULL, FALSE),
    ('KR', 'TEST09', 'TEST CORP KR', NULL, TRUE);

-- 2. [market 계정] ETF가 담은 종목. 한 주식이 여러 ETF에 담길 수 있고, 비중(%)과 순위는 이 연결 줄에 둔다.
--    자동 조회 대상 ETF는 조회가 이 줄들을 맞추므로 직접 넣지 않아도 된다.
INSERT INTO etf_stock_map (etf_id, stock_id, weight, rank)
SELECT e.id, s.id, v.weight, v.rank
FROM (VALUES ('TEST01', 'FAKEA', 12.5, 1), ('TEST01', 'FAKE-B', 8.25, 2), ('TEST02', 'FAKEA', 30, 1)) AS v (etf, stock, weight, rank)
JOIN etf e ON e.symbol = v.etf
JOIN stock s ON s.symbol = v.stock;

-- 3. [market 계정] 뉴스 검색어. 한 줄은 ETF, 주식, 거시 주제 중 정확히 하나에 속한다.
--    거시 주제에 연결된 종목이 없으면 그 기사는 시장 전체(*) 태그가 붙는다.
INSERT INTO news_keyword (keyword) VALUES ('test keyword'), ('test rate keyword');
INSERT INTO macro_keyword (name) VALUES ('TEST MACRO');
INSERT INTO news_keyword_map (keyword_id, etf_id)
SELECT k.id, e.id FROM news_keyword k, etf e WHERE k.keyword = 'test keyword' AND e.symbol = 'TEST01';
INSERT INTO news_keyword_map (keyword_id, macro_keyword_id)
SELECT k.id, m.id FROM news_keyword k, macro_keyword m WHERE k.keyword = 'test rate keyword' AND m.name = 'TEST MACRO';
INSERT INTO macro_keyword_map (macro_keyword_id, etf_id)
SELECT m.id, e.id FROM macro_keyword m, etf e WHERE m.name = 'TEST MACRO' AND e.symbol = 'TEST01';

-- 4. [personal 쓰기 계정] 멤버, 메일 수신 설정, 포트폴리오. relation은 HELD(보유) 또는 WATCH(관심)다.
--    메일 주소는 개인정보라 예시도 example.com 가짜 주소만 쓴다.
INSERT INTO personal.member (name) VALUES ('TEST MEMBER');
INSERT INTO personal.member_mail (member_id, email, receive_move, receive_morning, receive_post_close)
SELECT id, 'member@example.com', TRUE, TRUE, TRUE FROM personal.member WHERE name = 'TEST MEMBER';
INSERT INTO personal.member_portfolio (member_id, etf_id, relation)
SELECT m.id, e.id, 'HELD' FROM personal.member m, market.etf e WHERE m.name = 'TEST MEMBER' AND e.symbol = 'TEST01';
INSERT INTO personal.member_portfolio (member_id, stock_id, relation)
SELECT m.id, s.id, 'WATCH' FROM personal.member m, market.stock s WHERE m.name = 'TEST MEMBER' AND s.symbol = 'TEST09';
