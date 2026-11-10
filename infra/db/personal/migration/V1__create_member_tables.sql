-- 개인 데이터: 멤버, 멤버의 메일 수신 설정, 멤버의 포트폴리오. 표는 personal 스키마 주인 계정이 만들고, 값은 사람이 SQL로 넣는다.
-- market 표가 먼저 만들어져 있어야 하며(외래 키), 계정과 기본 권한은 infra/db/personal/init.sql이 만든다.
-- 이 파일의 ${market_user}, ${collector_user}, ${notifier_user}, ${writer_user}는 Flyway 자리 표시자로, 실행할 때 계정 이름으로 바뀐다.

CREATE TABLE member (
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    enabled    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 메일 주소는 개인정보라 멤버와 따로 두고 읽는 계정을 좁힌다: 발송기와 쓰기 계정만 읽는다(기본 권한).
-- 시세처리기와 수집기에는 이 표의 권한을 주지 않는다.
CREATE TABLE member_mail (
    id                 BIGSERIAL PRIMARY KEY,
    member_id          BIGINT       NOT NULL UNIQUE REFERENCES member (id) ON DELETE CASCADE,
    email              VARCHAR(254) NOT NULL UNIQUE,
    receive_move       BOOLEAN      NOT NULL DEFAULT TRUE,
    receive_morning    BOOLEAN      NOT NULL DEFAULT TRUE,
    receive_post_close BOOLEAN      NOT NULL DEFAULT TRUE,
    enabled            BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 기본 권한에만 기대지 않고 읽는 계정을 명시한다: 발송기는 읽기만, 쓰기 계정은 읽고 쓴다. 다른 계정에는 주지 않는다.
GRANT SELECT ON member_mail TO "${notifier_user}";
GRANT SELECT, INSERT, UPDATE, DELETE ON member_mail TO "${writer_user}";

-- 멤버가 담은 ETF 또는 주식. 한 줄은 market.etf와 market.stock 중 정확히 하나를 가리킨다(스키마를 넘는 외래 키).
-- market 쪽 행은 지워지지 않게 CASCADE를 두지 않는다. 구성 종목 자동 조회가 stock 행을 지우지 않는 것도 이 때문이다.
CREATE TABLE member_portfolio (
    id         BIGSERIAL PRIMARY KEY,
    member_id  BIGINT      NOT NULL REFERENCES member (id) ON DELETE CASCADE,
    etf_id     BIGINT      REFERENCES market.etf (id),
    stock_id   BIGINT      REFERENCES market.stock (id),
    relation   VARCHAR(8)  NOT NULL CHECK (relation IN ('HELD', 'WATCH')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (num_nonnulls(etf_id, stock_id) = 1)
);

CREATE UNIQUE INDEX member_portfolio_etf ON member_portfolio (member_id, etf_id) WHERE etf_id IS NOT NULL;
CREATE UNIQUE INDEX member_portfolio_stock ON member_portfolio (member_id, stock_id) WHERE stock_id IS NOT NULL;
CREATE INDEX member_portfolio_etf_id ON member_portfolio (etf_id) WHERE etf_id IS NOT NULL;
CREATE INDEX member_portfolio_stock_id ON member_portfolio (stock_id) WHERE stock_id IS NOT NULL;

-- 사용 중인 멤버의 포트폴리오만 보여 주는 뷰. 멤버 번호는 내보내지 않는다.
-- 수집 대상(collect_target)과 시세처리기의 보유·관심 계산이 같은 기준(사용 중인 멤버)을 쓰도록 이 뷰 하나를 거친다.
CREATE VIEW active_portfolio AS
SELECT p.etf_id, p.stock_id, p.relation
FROM member_portfolio p
JOIN member m ON m.id = p.member_id
WHERE m.enabled;

-- 수집기가 읽는 수집 대상: 사용 중인 멤버의 포트폴리오(active_portfolio)를 합친 것(보유·관심 모두).
--   KR_QUOTE : 국내 1초 시세를 받을 종목. 포트폴리오의 국내 ETF와 국내 주식.
--   US_DAILY : 미국 일봉을 받을 종목. 포트폴리오의 미국 주식과, 포트폴리오 ETF가 담은 미국 구성 종목.
--   kind     : 그 종목이 ETF인지 주식인지. UNION이 같은 줄을 한 줄로 합친다.
CREATE VIEW collect_target AS
SELECT 'KR_QUOTE' AS purpose, e.market, e.symbol, 'ETF' AS kind
FROM active_portfolio a JOIN market.etf e ON e.id = a.etf_id
WHERE e.market = 'KR'
UNION
SELECT 'KR_QUOTE', s.market, s.symbol, 'STOCK'
FROM active_portfolio a JOIN market.stock s ON s.id = a.stock_id
WHERE s.market = 'KR'
UNION
SELECT 'US_DAILY', s.market, s.symbol, 'STOCK'
FROM active_portfolio a JOIN market.stock s ON s.id = a.stock_id
WHERE s.market = 'US'
UNION
SELECT 'US_DAILY', s.market, s.symbol, 'STOCK'
FROM active_portfolio a
JOIN market.etf_stock_map em ON em.etf_id = a.etf_id
JOIN market.stock s ON s.id = em.stock_id
WHERE s.market = 'US';

-- 시세처리기는 보유·관심 구분을 계산하려고 사용 중인 포트폴리오 뷰만 읽는다(member, member_mail, member_portfolio는 읽지 못한다).
-- 수집기는 수집 대상 뷰만 읽는다.
GRANT SELECT ON active_portfolio TO "${market_user}";
GRANT SELECT ON collect_target TO "${collector_user}";
