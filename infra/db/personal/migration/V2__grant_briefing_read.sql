-- 브리핑 서비스가 데일리 판을 멤버마다 짜려고 읽는 personal 표의 열만 연다.
-- 이 파일의 ${briefing_user}는 Flyway 자리 표시자로, 실행할 때 브리핑 계정 이름으로 바뀐다. 스키마 사용 권한과 market.etf 열 권한은 init.sql이 준다.
-- 열마다 명시한다: 메일 주소(member_mail.email)와 멤버 이름은 읽지 못하고, 어떤 표에도 쓰지 못한다.
-- 기본 권한에 기대지 않는다. 앞으로 만들 표에는 자동으로 권한이 가지 않으므로 필요할 때 마이그레이션에서 따로 연다.
GRANT SELECT (id, enabled) ON member TO "${briefing_user}";
GRANT SELECT (member_id, enabled, receive_morning, receive_post_close) ON member_mail TO "${briefing_user}";
GRANT SELECT (member_id, etf_id, relation) ON member_portfolio TO "${briefing_user}";
