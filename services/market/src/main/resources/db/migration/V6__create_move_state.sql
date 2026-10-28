-- 급변동 재발행 판단용 상태. 종목과 감지 유형마다 한 행이며, 감지 조건이 풀리면 지운다.
-- price_snapshot의 날짜 파티션을 지워도 남아야 하므로 시세 이력과 따로 둔 비파티션 테이블이다.
-- 감지 유형에는 CHECK를 두지 않아 새 유형이 생겨도 저장이 막히지 않는다.

CREATE TABLE move_state (
    symbol               VARCHAR(6)  NOT NULL,
    detection_type       VARCHAR(16) NOT NULL,
    last_published_price BIGINT      NOT NULL CHECK (last_published_price >= 0),
    move_started_at      TIMESTAMPTZ NOT NULL,
    last_published_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, detection_type)
);
