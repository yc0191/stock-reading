import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import StoreSettings
from briefing.job_log import (
    EntryType,
    GeneratedBy,
    JobKind,
    JobLogEntry,
    JobResult,
    JobStart,
    RunStatus,
)
from briefing.job_log_store import (
    FAIL_UNFINISHED_SQL,
    FINISH_RUN_SQL,
    INSERT_SQL,
    LOG_TABLE,
    RUN_TABLE,
    SELECT_MOVES_SQL,
    SELECT_OUTLOOKS_SQL,
    START_RUN_SQL,
    PostgresJobLog,
)

JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
AT = datetime(2026, 10, 20, 16, 0, 31, tzinfo=timezone(timedelta(hours=9)))
RAW_RESPONSE = '{"b": 1,  "a": [1.50, 2.0]}'  # 키 순서와 공백, 소수 표기가 원문 그대로 남아야 한다
RAW_INPUT = '{"eventId":  "x", "payload": {"b": 1,  "a": [1.50]}}'


class FakeDb:
    """실행된 SQL과 값을 모으는 가짜 DB. 앞의 fail_times번 연결은 실패시킨다.

    returning은 UPDATE ... RETURNING이 돌려줄 줄이다.
    """

    def __init__(self, fail_times=0, returning=(), answer=None):
        # answer는 읽기 질문(SELECT)에 돌려줄 줄들이다. 없으면 UPDATE ... RETURNING의 줄(returning)을 돌려준다.
        self.answer = answer
        self.rows = []
        self.statements = []
        self.fail_times = fail_times
        self.returning = [(job_id,) for job_id in returning]
        self.connects = 0
        self.closed = 0
        self.commits = 0

    def connect(self):
        self.connects += 1
        if self.connects <= self.fail_times:
            raise ConnectionError("connection refused for user secret-user password=hunter2")
        return FakeConnection(self)


class FakeConnection:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        self.db.commits += 1

    def close(self):
        self.db.closed += 1


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params):
        self.db.statements.append((sql, params))
        if sql is INSERT_SQL:
            self.db.rows.append(params)

    def fetchall(self):
        return list(self.db.answer) if self.db.answer is not None else self.db.returning


def entry(job_id=JOB_ID, data=None, **changes):
    values = dict(job_id=job_id, entry_type=EntryType.RESENT, stage="market.history", reason="resent-after-timeout",
                  occurred_at=AT, error_type="ResponseTimeout", attempts=2,
                  data=data if data is not None else {"response": RAW_RESPONSE, "한글": "원문"})
    values.update(changes)
    return JobLogEntry(**values)


@pytest.fixture
def sleeps():
    return []


def store_for(db, sleeps, attempts=3):
    return PostgresJobLog(db.connect, StoreSettings(record_attempts=attempts), sleep=sleeps.append)


def test_start_inserts_the_run_line_as_running_with_the_original_event_text(sleeps):
    db = FakeDb()
    run = JobStart(JOB_ID, JobKind.MOVE, RAW_INPUT, {"symbol": "TEST01", "moveAt": AT.isoformat()}, AT)

    assert store_for(db, sleeps).start(run) is True

    ((sql, params),) = db.statements
    assert sql is START_RUN_SQL and "ON CONFLICT (job_id) DO NOTHING" in sql
    job_id, kind, raw_input, status, target, started_at = params
    assert (job_id, kind, status, started_at) == (JOB_ID, "move", "running", AT)
    assert raw_input == RAW_INPUT  # 시작시킨 이벤트 원문을 고쳐 쓰지 않는다
    assert json.loads(target) == {"symbol": "TEST01", "moveAt": AT.isoformat()}


def test_finish_updates_the_run_line_with_its_end_state_and_the_mail_briefing_id(sleeps):
    db = FakeDb()
    result = JobResult(JOB_ID, RunStatus.SUCCEEDED, AT, failure_reason=None, generated_by=GeneratedBy.LLM,
                       final_text="최종 본문", weak_count=1, research_count=3, result_event_id="evt-1")

    assert store_for(db, sleeps).finish(result) is True

    ((sql, params),) = db.statements
    assert sql is FINISH_RUN_SQL
    assert params == ("succeeded", None, "llm", AT, "최종 본문", 1, 3, "evt-1", JOB_ID)
    failed = JobResult(JOB_ID, RunStatus.FAILED, AT, failure_reason="llm_error")
    store_for(db, sleeps).finish(failed)
    assert db.statements[-1][1] == ("failed", "llm_error", None, AT, None, 0, 0, None, JOB_ID)


def test_record_inserts_one_row_with_every_field_and_keeps_the_related_data_as_given(sleeps):
    db = FakeDb()
    sentence = entry(entry_type=EntryType.SENTENCE_REWRITTEN, sentence_no=2, round=1, before="고치기 전", after="고친 뒤")

    assert store_for(db, sleeps).record(sentence) is True

    ((sql, params),) = db.statements
    assert sql is INSERT_SQL
    job_id, entry_type, stage, sentence_no, round_no, before, after, reason, error_type, attempts, data, occurred_at \
        = params
    assert (job_id, entry_type, stage, sentence_no, round_no, before, after) == (
        JOB_ID, "sentence-rewritten", "market.history", 2, 1, "고치기 전", "고친 뒤")
    assert (reason, error_type, attempts, occurred_at) == ("resent-after-timeout", "ResponseTimeout", 2, AT)
    assert json.loads(data) == {"response": RAW_RESPONSE, "한글": "원문"}
    assert "한글" in data  # 한글이 \u 이스케이프로 바뀌지 않는다
    assert db.commits == 1 and db.closed == 1 and sleeps == []


def test_data_is_stored_without_cutting_it_short(sleeps):
    db = FakeDb()
    big = "x" * 100_000

    assert store_for(db, sleeps).record(entry(data={"response": big})) is True

    assert json.loads(db.rows[0][10]) == {"response": big}


def test_record_retries_a_failed_connection_and_then_succeeds(sleeps):
    db = FakeDb(fail_times=2)
    store = store_for(db, sleeps)

    assert store.record(entry()) is True

    assert db.connects == 3 and len(db.rows) == 1 and len(sleeps) == 2
    assert store.has_failed(JOB_ID) is False


def test_record_that_keeps_failing_returns_false_logs_an_error_and_remembers_the_job(sleeps, caplog):
    db = FakeDb(fail_times=99)
    store = store_for(db, sleeps)

    with caplog.at_level(logging.DEBUG, logger="briefing.job_log_store"):
        assert store.record(entry()) is False

    assert db.connects == 3 and db.rows == []
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and JOB_ID in errors[0].getMessage()
    assert store.has_failed(JOB_ID) is True and store.has_failed("other-job") is False


def test_a_final_result_that_cannot_be_saved_logs_an_error_and_marks_the_job_failed(sleeps, caplog):
    db = FakeDb(fail_times=99)
    store = store_for(db, sleeps)

    with caplog.at_level(logging.DEBUG, logger="briefing.job_log_store"):
        assert store.finish(JobResult(JOB_ID, RunStatus.SUCCEEDED, AT)) is False

    assert [r.levelno for r in caplog.records if r.levelno == logging.ERROR] == [logging.ERROR]
    assert store.has_failed(JOB_ID) is True


def test_logs_never_contain_connection_secrets_or_the_symbol(sleeps, caplog):
    db = FakeDb(fail_times=99)

    with caplog.at_level(logging.DEBUG, logger="briefing.job_log_store"):
        store_for(db, sleeps).record(entry(data={"symbol": "TEST01"}))

    assert "hunter2" not in caplog.text and "secret-user" not in caplog.text
    assert "ConnectionError" in caplog.text
    assert "TEST01" not in caplog.text


def test_failed_job_memory_is_bounded(sleeps, monkeypatch):
    import briefing.job_log_store as module

    monkeypatch.setattr(module, "REMEMBERED_FAILED_JOBS", 2)
    store = store_for(FakeDb(fail_times=99), sleeps, attempts=1)

    for job in ("a", "b", "c"):
        store.record(entry(job_id=job))

    assert [store.has_failed(job) for job in ("a", "b", "c")] == [False, True, True]


def test_default_store_settings_keep_the_receive_loop_from_stalling():
    settings = StoreSettings()
    assert settings.record_attempts * settings.record_retry_interval.total_seconds() <= 3


# ---- 시작할 때 끝나지 않고 남은 작업 정리 ----

def test_unfinished_runs_of_the_previous_run_are_failed_with_the_restart_reason_and_each_gets_a_log_line(sleeps):
    db = FakeDb(returning=("job-b", "job-a"))

    interrupted = store_for(db, sleeps).fail_unfinished(AT)

    assert sorted(interrupted) == ["job-a", "job-b"]
    [(sql, params), *logged] = db.statements
    assert sql is FAIL_UNFINISHED_SQL
    assert "WHERE status = %s AND started_at < %s" in sql
    # 진행 중인 것만 고르고, 이번 실행이 시작한 뒤에 만든 작업은 건드리지 않도록 시작 시각을 상한으로 둔다.
    assert params == ("failed", "interrupted_by_restart", AT, "running", AT)
    assert [p[0] for _, p in logged] == ["job-a", "job-b"]
    assert {p[1] for _, p in logged} == {"step-failed"}
    assert {p[2] for _, p in logged} == {"restart"}
    assert {p[7] for _, p in logged} == {"interrupted_by_restart"}
    assert db.commits == 3 and db.closed == db.connects


def test_nothing_is_written_when_no_run_was_left_unfinished(sleeps):
    db = FakeDb()

    assert store_for(db, sleeps).fail_unfinished(AT) == []
    assert [sql for sql, _ in db.statements] == [FAIL_UNFINISHED_SQL]


def test_an_unreachable_database_stops_the_startup_cleanup_instead_of_hiding_it(sleeps):
    db = FakeDb(fail_times=1)

    with pytest.raises(ConnectionError):
        store_for(db, sleeps).fail_unfinished(AT)


# ---- 읽기: 그날 아침 예상과 급변동 ----

def test_morning_outlooks_ask_for_the_korean_day_newest_first_and_return_each_data(sleeps):
    newer, older = {"outlooks": [{"symbol": "TEST01"}], "n": 2}, {"outlooks": [], "n": 1}
    db = FakeDb(answer=[(json.dumps(newer),), (json.dumps(older),)])
    assert store_for(db, sleeps).morning_outlooks(AT.date()) == [newer, older]
    [(sql, params)] = db.statements
    assert sql is SELECT_OUTLOOKS_SQL and "ORDER BY l.occurred_at DESC" in sql
    assert params == ("etf-outlook", "morning", datetime(2026, 10, 20, 0, 0, tzinfo=AT.tzinfo),
                      datetime(2026, 10, 21, 0, 0, tzinfo=AT.tzinfo))
    assert db.closed == 1


def test_moves_between_returns_parsed_rows_and_skips_a_row_that_cannot_be_read(sleeps):
    good = ("m1", json.dumps({"payload": {"changePercent": 2.1}}),
            json.dumps({"symbol": "TEST01", "moveAt": "2026-10-20T10:15:00+09:00"}), "succeeded", "llm", None)
    broken = ("m2", "{}", "not json", "running", None, None)
    db = FakeDb(answer=[good, broken])
    since, until = AT.replace(hour=0, minute=0), AT
    [run] = store_for(db, sleeps).moves_between(since, until)
    assert (run.job_id, run.symbol, run.change_percent, run.status, run.generated_by) == (
        "m1", "TEST01", 2.1, "succeeded", "llm")
    assert db.statements == [(SELECT_MOVES_SQL, ("move", since, until))]


def test_a_failed_read_is_none_and_the_log_names_only_the_error_kind(sleeps, caplog):
    db = FakeDb(fail_times=1)
    with caplog.at_level(logging.WARNING):
        assert store_for(db, sleeps).morning_outlooks(AT.date()) is None
        assert store_for(FakeDb(fail_times=1), sleeps).moves_between(AT, AT) is None
    assert "ConnectionError" in caplog.text and "hunter2" not in caplog.text and "secret-user" not in caplog.text
    # 읽기는 다시 시도하지 않는다(판 발행을 늦추지 않는다).
    assert sleeps == []
