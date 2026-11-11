-- 작업 기록(job_run, job_log)과 수집 완료 알림(collection_completed). briefing 스키마에서 실행한다.
-- 입력·대상·관련 데이터·받은 원문은 키 순서와 표기를 바꾸는 JSONB가 아니라 JSON으로 둔다.
-- 종류·상태·작성 방식·기록 종류의 값 제한은 코드 enum(job_log.py)과 같아야 한다. 어긋나면 test_job_log_schema.py가 실패한다.
CREATE TABLE job_run (
    job_id          VARCHAR(64) PRIMARY KEY,
    job_kind        VARCHAR(16) NOT NULL CHECK (job_kind IN ('move', 'morning', 'post-close')),
    input           JSON NOT NULL,
    status          VARCHAR(16) NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    failure_reason  VARCHAR(48),
    generated_by    VARCHAR(16) CHECK (generated_by IN ('llm', 'rule-based')),
    target          JSON NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    final_text      TEXT,
    weak_count      INTEGER NOT NULL DEFAULT 0 CHECK (weak_count >= 0),
    research_count  INTEGER NOT NULL DEFAULT 0 CHECK (research_count >= 0),
    result_event_id VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_run_result_event_id_idx ON job_run (result_event_id);
-- 시작할 때 끝나지 않고 남은 작업을 찾는 질문과 서버 상태 절의 범위 조회에 쓴다.
CREATE INDEX job_run_status_started_at_idx ON job_run (status, started_at);

CREATE TABLE job_log (
    id          BIGSERIAL PRIMARY KEY,
    job_id      VARCHAR(64) NOT NULL REFERENCES job_run (job_id),
    entry_type  VARCHAR(24) NOT NULL CHECK (entry_type IN (
                    'report-written', 'llm-rejected', 'query-written', 'research-requested', 'articles-received',
                    'sentence-rewritten', 'check-round', 'resent', 'missing-items', 'step-failed', 'moves-grouped',
                    'etf-outlook')),
    stage       VARCHAR(48) NOT NULL,
    sentence_no INTEGER,
    round       INTEGER,
    before      TEXT,
    after       TEXT,
    reason      VARCHAR(48) NOT NULL,
    error_type  VARCHAR(64),
    attempts    INTEGER NOT NULL CHECK (attempts >= 0),
    data        JSON NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_log_job_id_idx ON job_log (job_id, id);

-- 받은 원문(raw)은 키 순서와 표기를 바꾸지 않도록 JSON(JSONB 아님)으로 그대로 둔다.
CREATE TABLE collection_completed (
    event_id           VARCHAR(64) PRIMARY KEY,
    job_type           VARCHAR(48) NOT NULL,
    scheduled_for      TIMESTAMPTZ NOT NULL,
    started_at         TIMESTAMPTZ NOT NULL,
    finished_at        TIMESTAMPTZ NOT NULL,
    published_count    INTEGER NOT NULL CHECK (published_count >= 0),
    incomplete_symbols JSON NOT NULL,
    research_job_id    VARCHAR(64),
    raw                JSON NOT NULL,
    received_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX collection_completed_finished_at_idx ON collection_completed (finished_at);
