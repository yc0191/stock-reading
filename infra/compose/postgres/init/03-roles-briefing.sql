-- briefing 서비스 전용 계정과 스키마를 만든다(ADR-004, 01-roles.sql과 같은 방식). DB 볼륨을 처음 만들 때만 실행된다.
-- 값은 SQL 텍스트가 아니라 컨테이너 환경변수에서 \getenv로 읽는다.
\getenv briefing_user BRIEFING_DB_USER
\getenv briefing_pw BRIEFING_DB_PASSWORD

CREATE ROLE :"briefing_user" LOGIN PASSWORD :'briefing_pw';
CREATE SCHEMA briefing AUTHORIZATION :"briefing_user";
ALTER ROLE :"briefing_user" SET search_path = briefing;
