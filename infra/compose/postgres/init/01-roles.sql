-- Creates one role + one schema per service (ADR-004). Runs once on first start.
-- Values come from the container environment via \getenv, never from SQL text in git.
\getenv market_user MARKET_DB_USER
\getenv market_password MARKET_DB_PASSWORD

CREATE ROLE :"market_user" LOGIN PASSWORD :'market_password';
CREATE SCHEMA market AUTHORIZATION :"market_user";
ALTER ROLE :"market_user" SET search_path = market;
