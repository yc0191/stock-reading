-- 확인기 장부. 확인기 계정(ops 스키마 주인)이 만든다(init.sql 이 계정과 스키마를 먼저 만든다).
-- 값은 비율·판정·버전·지문뿐이다. 건수, 종목, 서버 주소, SQL 본문, DB 원본은 저장하지 않는다.

-- 확인기 실행 한 번에 한 줄. 같은 수집기 알림(event_id)은 한 번만 실행하려고 event_id 가 유니크다.
-- 알림을 받으면 먼저 이 줄을 넣어 차지하고(outcome 비어 있음) 끝나면 finished_at·outcome 을 채워도 되도록 두 칸은 비울 수 있다.
CREATE TABLE check_run (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    day            DATE         NOT NULL,
    edition        VARCHAR(16)  NOT NULL CHECK (edition IN ('morning', 'post-close')),
    event_id       VARCHAR(64)  NOT NULL UNIQUE,
    market_status  VARCHAR(16)  NOT NULL CHECK (market_status IN ('collected', 'market-closed')),
    deploy_version VARCHAR(128) NOT NULL,
    started_at     TIMESTAMPTZ  NOT NULL,
    finished_at    TIMESTAMPTZ,
    outcome        VARCHAR(16)  CHECK (outcome IN ('ok', 'holiday', 'error')),
    -- 실패 종류만 쓴다(값·종목·주소 없음).
    error          VARCHAR(200),
    CHECK (outcome IS DISTINCT FROM 'error' OR error IS NOT NULL),
    -- 실행 중(둘 다 비어 있음)이거나 끝남(둘 다 있음)이어야 한다. 한쪽만 채워진 줄은 막는다.
    CHECK ((finished_at IS NULL) = (outcome IS NULL))
);
CREATE INDEX check_run_day_edition_idx ON check_run (day, edition);

-- 지표 하나에 한 줄. 휴장 실행에는 줄이 없다.
-- value 는 0~1 비율이며 계산할 수 없으면(hold, error) 비운다. 건수·종목은 저장하지 않는다.
-- verdict: pass/fail 은 값을 비교한 결과, hold 는 값이 비어 계산 불가(분모 0 등), error 는 SQL 실패·10초 초과·결과 모양 틀림.
-- 형식이 틀린 기준 파일은 그 파일만 error 로 기록한다. 그때 이슈 번호·시도는 파일 이름에서, metric 은 자리 값으로, threshold 는 비운다.
CREATE TABLE check_result (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        BIGINT           NOT NULL REFERENCES check_run (id) ON DELETE CASCADE,
    issue         INTEGER          NOT NULL,
    attempt       VARCHAR(64)      NOT NULL,
    -- 기준 파일 바이트의 SHA-256(소문자 16진수 64자)
    criteria_hash CHAR(64)         NOT NULL CHECK (criteria_hash ~ '^[0-9a-f]{64}$'),
    metric        VARCHAR(128)     NOT NULL,
    value         DOUBLE PRECISION CHECK (value >= 0 AND value <= 1),
    -- 기준 파일의 통과선 문자열(예 '< 0.01')
    threshold     VARCHAR(64),
    verdict       VARCHAR(8)       NOT NULL CHECK (verdict IN ('pass', 'fail', 'hold', 'error')),
    -- hold·error 일 때 종류(값·종목·주소 없음)
    reason        VARCHAR(200),
    UNIQUE (run_id, issue, attempt, metric),
    CHECK ((verdict IN ('pass', 'fail')) = (value IS NOT NULL)),
    CHECK (verdict IN ('pass', 'fail') OR reason IS NOT NULL)
);
