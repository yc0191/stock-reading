-- 뉴스 검색어를 market 스키마의 검색어 표(news_keyword 등)로 옮기면서 이 서비스의 검색어 표를 없앤다.
-- 수집기가 검색어를 이벤트로 받던 방식도 함께 없어졌다.
DROP TABLE search_query_symbol;
DROP TABLE search_query;
