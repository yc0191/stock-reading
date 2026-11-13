-- 브리핑 서비스가 마감 후 판의 '그날 급변동'을 멤버마다 그 멤버의 ETF와 국내 주식으로 거르려고 포트폴리오의 주식 열을 더 읽는다(#65).
-- 이 파일의 ${briefing_user}는 Flyway 자리 표시자로, 실행할 때 브리핑 계정 이름으로 바뀐다. market.stock 열 권한은 init.sql이 준다.
-- V2와 같이 열만 연다: 메일 주소와 멤버 이름은 여전히 읽지 못하고, 어떤 표에도 쓰지 못한다.
GRANT SELECT (stock_id) ON member_portfolio TO "${briefing_user}";
