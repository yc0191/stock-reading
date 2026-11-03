-- news 서비스 전용 계정과 스키마를 만든다(ADR-004, 01-roles.sql과 같은 방식). DB 볼륨을 처음 만들 때만 실행된다.
-- 값은 SQL 텍스트가 아니라 컨테이너 환경변수에서 \getenv로 읽는다.
\getenv news_user NEWS_DB_USER
\getenv news_password NEWS_DB_PASSWORD

CREATE ROLE :"news_user" LOGIN PASSWORD :'news_password';
CREATE SCHEMA news AUTHORIZATION :"news_user";
ALTER ROLE :"news_user" SET search_path = news;
