-- ops 스키마(확인기의 실행 장부)와 확인기 계정, 권한을 만든다. 확인기는 이슈 조치 효과를 날마다 재는 서버의 파이썬 서비스다.
-- 여러 번 실행해도 같다: 이미 있는 계정과 스키마는 그대로 두고(비밀번호도 바꾸지 않는다) 권한만 다시 맞춘다.
-- market·briefing 스키마가 먼저 있어야 한다(market 마이그레이션, briefing 마이그레이션 → 이 스크립트 → ops 마이그레이션).
-- 값(계정 이름·비밀번호)은 SQL 텍스트가 아니라 psql을 실행한 환경변수에서 \getenv로 읽는다.
--   실행: psql -v ON_ERROR_STOP=1 -d <데이터베이스> -f infra/db/ops/init.sql   (관리자 계정으로)
--
-- 확인기 계정(ops_checker):
--   ops       주인이며 읽고 쓴다. 마이그레이션(Flyway)이 이 계정으로 장부 표를 만든다.
--   market    모든 표·뷰를 읽기만 한다. 앞으로 market 계정이 만들 표도 자동으로 읽는다(기본 권한).
--   briefing  모든 표·뷰를 읽기만 한다. 앞으로 briefing 계정이 만들 표도 자동으로 읽는다(기본 권한).
--   personal  권한이 없다. 스키마 사용 권한도 주지 않는다(멤버 메일 주소 같은 개인정보를 지표가 읽을 수 없게).
--   그 밖의 스키마도 주지 않는다. market·briefing의 표를 바꾸는 권한은 어디에도 없다.
\set ON_ERROR_STOP on

\getenv ops_user OPS_CHECKER_DB_USER
\getenv ops_password OPS_CHECKER_DB_PASSWORD
\getenv market_user MARKET_DB_USER
\getenv briefing_user BRIEFING_DB_USER

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'ops_user', :'ops_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'ops_user') \gexec

CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION :"ops_user";

GRANT USAGE ON SCHEMA market, briefing TO :"ops_user";

-- 이미 있는 표(뷰, 파티션 포함)를 읽게 한다.
GRANT SELECT ON ALL TABLES IN SCHEMA market, briefing TO :"ops_user";

-- 앞으로 만들 표도 읽게 한다. 기본 권한은 "그 표를 만드는 계정"별로 정해지므로 market 표는 market 계정이,
-- briefing 표는 briefing 계정이 만든다는 전제다. 다른 계정이 만든 표가 생기면 그 계정으로도 이 줄을 더한다.
ALTER DEFAULT PRIVILEGES FOR ROLE :"market_user" IN SCHEMA market GRANT SELECT ON TABLES TO :"ops_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"briefing_user" IN SCHEMA briefing GRANT SELECT ON TABLES TO :"ops_user";
