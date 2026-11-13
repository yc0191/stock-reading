-- 단계별 실행 기록. 브리핑이 한 모든 호출(시세처리기·뉴스처리기 내부 조회, LLM)을 정상까지 포함해 시도마다 한 줄 남긴다.
-- job_log(특이한 일·판단)와 따로 두어, 메일 하나의 추적 ID로 호출이 언제 시작해 얼마나 걸렸고 몇 번째 시도에 어떻게 끝났는지 본다.
-- 결과 값 제한은 코드 enum(job_step.py의 StepResult)과 같아야 한다. 어긋나면 test_job_log_schema.py가 실패한다.
CREATE TABLE job_step (
    id             BIGSERIAL PRIMARY KEY,
    job_id         VARCHAR(64) NOT NULL REFERENCES job_run (job_id),
    step           VARCHAR(48) NOT NULL,
    target         VARCHAR(64) NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ NOT NULL,
    result         VARCHAR(16) NOT NULL CHECK (result IN ('succeeded', 'failed')),
    failure_kind   VARCHAR(64),
    attempt        INTEGER NOT NULL CHECK (attempt >= 1),
    correlation_id VARCHAR(256),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_step_job_id_idx ON job_step (job_id, id);
-- 메일 하나의 추적 ID로 호출 기록을 찾는다.
CREATE INDEX job_step_correlation_id_idx ON job_step (correlation_id);
