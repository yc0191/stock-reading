-- personal 스키마(멤버·메일·포트폴리오)와 그 계정, 권한을 만든다. 개인 데이터는 개인화 서비스 자리(ADR-006)에 두며 지금은 서비스가 없다.
-- 여러 번 실행해도 같다: 이미 있는 계정과 스키마는 그대로 두고(비밀번호도 바꾸지 않는다) 권한만 다시 맞춘다.
-- market 표(etf, stock, etf_stock_map, etf_stock_history, index_rebalance_schedule, news_search_target)가 먼저 만들어져 있어야 한다(market 마이그레이션 → 이 스크립트 → personal 마이그레이션).
-- 값(계정 이름·비밀번호)은 SQL 텍스트가 아니라 psql을 실행한 환경변수에서 \getenv로 읽는다.
--   실행: psql -v ON_ERROR_STOP=1 -d <데이터베이스> -f infra/db/personal/init.sql   (관리자 계정으로)
--
-- 계정과 권한:
--   owner     personal 스키마의 주인. 마이그레이션(Flyway)이 이 계정으로 표를 만든다.
--   writer    값을 넣고 고치는 계정(작업자 SQL, 나중에 CUD 기능). personal 전체를 읽고 쓴다.
--   collector 수집기. 수집 대상 뷰(personal.collect_target)와 뉴스 검색 대상 뷰(market.news_search_target), market의 구성 종목 표(etf, stock, etf_stock_map),
--             구성 종목 조회 차례를 정하는 표(etf_stock_history, index_rebalance_schedule)만 읽는다.
--   notifier  발송기. 받는 사람을 고르려고 personal의 표를 읽는다(member_mail 포함).
--   briefing  브리핑 서비스(기존 계정). 데일리 판을 멤버마다 짜려고 멤버 번호·사용 여부·수신 설정·포트폴리오와 market.etf의 종목코드, market.stock의 종목코드·이름만 읽는다. 메일 주소 열(member_mail.email)은 읽지 못하고 personal에 쓰지도 못한다. 표 권한은 personal 마이그레이션이 준다.
--   news      뉴스 서비스(기존 계정). market의 구성 종목 표(etf, stock, etf_stock_map)만 읽는다. personal은 읽지 못한다.
--   market    시세처리기(기존 계정). personal.active_portfolio 뷰만 읽는다(보유·관심 구분 계산). member, member_mail, member_portfolio는 읽지 못한다.
\set ON_ERROR_STOP on

\getenv owner_user PERSONAL_OWNER_DB_USER
\getenv owner_password PERSONAL_OWNER_DB_PASSWORD
\getenv writer_user PERSONAL_WRITER_DB_USER
\getenv writer_password PERSONAL_WRITER_DB_PASSWORD
\getenv collector_user COLLECTOR_DB_USER
\getenv collector_password COLLECTOR_DB_PASSWORD
\getenv notifier_user NOTIFIER_DB_USER
\getenv notifier_password NOTIFIER_DB_PASSWORD
\getenv market_user MARKET_DB_USER
\getenv briefing_user BRIEFING_DB_USER
\getenv news_user NEWS_DB_USER

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'owner_user', :'owner_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'owner_user') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'writer_user', :'writer_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'writer_user') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'collector_user', :'collector_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'collector_user') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'notifier_user', :'notifier_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'notifier_user') \gexec

CREATE SCHEMA IF NOT EXISTS personal AUTHORIZATION :"owner_user";

GRANT USAGE ON SCHEMA personal TO :"writer_user", :"collector_user", :"notifier_user", :"market_user", :"briefing_user";

-- 기본 권한: 마이그레이션이 만드는 새 표(뷰 포함)에 읽기·쓰기를 자동으로 준다. 수집기와 시세처리기는 표마다 마이그레이션이 따로 준다.
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_user" IN SCHEMA personal
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"writer_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_user" IN SCHEMA personal
    GRANT USAGE, SELECT ON SEQUENCES TO :"writer_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_user" IN SCHEMA personal
    GRANT SELECT ON TABLES TO :"notifier_user";

-- 이미 있는 표에도 같은 권한을 준다(기본 권한은 앞으로 만들 표에만 적용되므로, 이 스크립트를 다시 돌릴 때를 위해).
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA personal TO :"writer_user";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA personal TO :"writer_user";
GRANT SELECT ON ALL TABLES IN SCHEMA personal TO :"notifier_user";

-- market 표 읽기. 포트폴리오가 market.etf·market.stock을 가리키는 외래 키와 수집 대상 뷰가 이 권한을 쓴다.
GRANT USAGE ON SCHEMA market TO :"owner_user", :"writer_user", :"notifier_user", :"collector_user";
GRANT SELECT, REFERENCES ON market.etf, market.stock TO :"owner_user";
GRANT SELECT ON market.etf_stock_map TO :"owner_user";
-- writer는 포트폴리오를 넣을 때 종목코드로 id를 찾고, notifier는 종목코드로 급변동 수신자를 고른다.
GRANT SELECT ON market.etf, market.stock TO :"writer_user", :"notifier_user";
GRANT SELECT ON market.news_search_target TO :"collector_user";
-- 구성 종목 읽기. 뉴스는 ETF 기사를 구성 종목 기사로 넓히려고, 수집기는 미국 티커를 담은 ETF를 찾으려고 읽는다. 읽기만 주고 쓰기는 주지 않는다.
GRANT USAGE ON SCHEMA market TO :"news_user";
GRANT SELECT ON market.etf, market.stock, market.etf_stock_map TO :"news_user", :"collector_user";
-- 수집기는 구성 종목을 받을 차례인 ETF를 고르려고 마지막 확인일(이력)과 지수 교체 일정을 읽는다. 원문 저장과 반영은 시세처리기가 한다.
GRANT SELECT ON market.etf_stock_history, market.index_rebalance_schedule TO :"collector_user";
-- 브리핑은 멤버의 포트폴리오에서 ETF 종목코드를 찾으려고 market.etf의 id·시장·종목코드 열만 읽는다(이름·그 밖의 열은 읽지 못한다).
GRANT USAGE ON SCHEMA market TO :"briefing_user";
GRANT SELECT (id, market, symbol) ON market.etf TO :"briefing_user";
-- 마감 후 판의 그날 급변동에 멤버 포트폴리오의 국내 주식을 이름과 함께 보이려고 market.stock의 id·시장·종목코드·이름 열만 읽는다.
GRANT SELECT (id, market, symbol, name) ON market.stock TO :"briefing_user";
