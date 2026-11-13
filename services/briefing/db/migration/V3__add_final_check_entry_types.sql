-- 발행 직전 마지막 검사의 기록 종류 둘을 더한다. final-check는 코드 검사·LLM 검토·최종으로 고른 본문과 이유,
-- sentence-fixed는 검토로 고친 문장의 전후다(재검색으로 고친 문장은 그대로 sentence-rewritten).
-- 값 제한은 코드 enum(job_log.py)과 같아야 한다. 어긋나면 test_job_log_schema.py가 실패한다.
ALTER TABLE job_log DROP CONSTRAINT job_log_entry_type_check;
ALTER TABLE job_log ADD CONSTRAINT job_log_entry_type_check CHECK (entry_type IN (
    'report-written', 'llm-rejected', 'query-written', 'research-requested', 'articles-received',
    'sentence-rewritten', 'check-round', 'resent', 'missing-items', 'step-failed', 'moves-grouped',
    'etf-outlook', 'member-briefing', 'final-check', 'sentence-fixed'));
