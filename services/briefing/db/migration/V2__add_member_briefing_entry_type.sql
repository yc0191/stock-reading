-- 데일리 판을 멤버마다 따로 발행하므로, 작업 한 건 안에서 어느 멤버 몫을 냈는지 남기는 기록 종류 member-briefing을 더한다.
-- 값 제한은 코드 enum(job_log.py)과 같아야 한다. 어긋나면 test_job_log_schema.py가 실패한다.
ALTER TABLE job_log DROP CONSTRAINT job_log_entry_type_check;
ALTER TABLE job_log ADD CONSTRAINT job_log_entry_type_check CHECK (entry_type IN (
    'report-written', 'llm-rejected', 'query-written', 'research-requested', 'articles-received',
    'sentence-rewritten', 'check-round', 'resent', 'missing-items', 'step-failed', 'moves-grouped',
    'etf-outlook', 'member-briefing'));
