-- 휴장 알림을 받은 날의 데일리 판. 브리핑이 재시작해도(휴장 알림은 다시 오지 않음) 그날 그 판과 한도 시각을 끈 채로 둔다.
-- 판 작업이 아니므로 job_run에 넣지 않는다. 서버 상태 절이 job_run을 판·실패 건수로 세기 때문이다.
-- 판 값 제한은 코드 enum(job_log.py의 JobKind 중 데일리 판 둘)과 같아야 한다. 어긋나면 test_job_log_schema.py가 실패한다.
CREATE TABLE market_closed_edition (
    edition_day DATE NOT NULL,
    edition     VARCHAR(16) NOT NULL CHECK (edition IN ('morning', 'post-close')),
    event_id    VARCHAR(64) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (edition_day, edition)
);
