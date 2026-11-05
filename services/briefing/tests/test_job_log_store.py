import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import StoreSettings
from briefing.job_log import JobLogEntry
from briefing.job_log_store import (
    CREATE_INDEX_SQL,
    CREATE_TABLE_SQL,
    INSERT_SQL,
    TABLE,
    PostgresJobLog,
)

JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
AT = datetime(2026, 10, 20, 16, 0, 31, tzinfo=timezone(timedelta(hours=9)))
RAW_RESPONSE = '{"b": 1,  "a": [1.50, 2.0]}'  # 키 순서와 공백, 소수 표기가 원문 그대로 남아야 한다


class FakeDb:
    """실행된 SQL과 값을 모으는 가짜 DB. 앞의 fail_times번 연결은 실패시킨다."""

    def __init__(self, fail_times=0):
        self.rows = []
        self.statements = []
        self.fail_times = fail_times
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


def entry(job_id=JOB_ID, data=None):
    return JobLogEntry("move", False, job_id, {"symbol": "TEST01", "moveAt": AT.isoformat()}, "market.history",
                       "resent", "resent-after-timeout", "ResponseTimeout", 2,
                       data if data is not None else {"response": RAW_RESPONSE, "한글": "원문"}, AT)


@pytest.fixture
def sleeps():
    return []


def store_for(db, sleeps, attempts=3):
    return PostgresJobLog(db.connect, StoreSettings(record_attempts=attempts), sleep=sleeps.append)


def test_create_table_makes_the_log_table_and_its_index_and_can_run_twice():
    db = FakeDb()
    store = PostgresJobLog(db.connect)

    store.create_table()
    store.create_table()

    sqls = [sql for sql, _ in db.statements]
    assert sqls == [CREATE_TABLE_SQL, CREATE_INDEX_SQL] * 2
    assert f"CREATE TABLE IF NOT EXISTS {TABLE}" in CREATE_TABLE_SQL and TABLE == "briefing.job_log"
    assert "failed_jobs" not in CREATE_TABLE_SQL
    assert db.closed == 4 and db.commits == 4


def test_record_inserts_one_row_with_every_field_and_keeps_the_related_data_as_given(sleeps):
    db = FakeDb()

    assert store_for(db, sleeps).record(entry()) is True

    ((sql, params),) = db.statements
    assert sql is INSERT_SQL
    kind, researched, job_id, target, stage, entry_type, reason, error_type, attempts, data, occurred_at = params
    assert (kind, researched, job_id, stage, entry_type, reason, error_type, attempts, occurred_at) == (
        "move", False, JOB_ID, "market.history", "resent", "resent-after-timeout", "ResponseTimeout", 2, AT)
    assert json.loads(target) == {"symbol": "TEST01", "moveAt": AT.isoformat()}
    assert json.loads(data) == {"response": RAW_RESPONSE, "한글": "원문"}
    assert "한글" in data  # 한글이 \u 이스케이프로 바뀌지 않는다
    assert db.commits == 1 and db.closed == 1 and sleeps == []


def test_data_is_stored_without_cutting_it_short(sleeps):
    db = FakeDb()
    big = "x" * 100_000

    assert store_for(db, sleeps).record(entry(data={"response": big})) is True

    assert json.loads(db.rows[0][9]) == {"response": big}


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


def test_logs_never_contain_connection_secrets_or_the_symbol(sleeps, caplog):
    db = FakeDb(fail_times=99)

    with caplog.at_level(logging.DEBUG, logger="briefing.job_log_store"):
        store_for(db, sleeps).record(entry())

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
