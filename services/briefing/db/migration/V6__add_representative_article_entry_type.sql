-- 레벨 중 급변동이 고른 대표 기사의 기록 종류 하나를 더한다. representative-article는 고른 기사(제목·언론사·발행 시각)나
-- 관련 기사 없음과 고른 근거를 data에 남기고, 마감 후 판의 그날 급변동 줄이 읽는다.
-- 값 제한은 코드 enum(job_log.py)과 같아야 한다. 어긋나면 test_job_log_schema.py가 실패한다.
ALTER TABLE job_log DROP CONSTRAINT job_log_entry_type_check;
ALTER TABLE job_log ADD CONSTRAINT job_log_entry_type_check CHECK (entry_type IN (
    'report-written', 'llm-rejected', 'query-written', 'research-requested', 'articles-received',
    'sentence-rewritten', 'check-round', 'resent', 'missing-items', 'step-failed', 'moves-grouped',
    'etf-outlook', 'member-briefing', 'final-check', 'sentence-fixed', 'representative-article'));
