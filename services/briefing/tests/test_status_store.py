"""서버 상태 저장소(PostgresStatusStore)를 가짜 DB로 확인한다. 실제 Postgres는 부르지 않는다."""
import json
import logging
from datetime import datetime, timedelta, timezone

from briefing.config import StoreSettings
from briefing.status import CollectionRecord
from briefing.status_store import (
    INSERT_COLLECTION_SQL,
    SELECT_COLLECTIONS_SQL,
    SELECT_RUNS_SQL,
    PostgresStatusStore,
)

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 16, 15, 42, tzinfo=KST)
RAW = '{"b": 1,  "payload": {"x": 1.50}}'  # 키 순서와 공백, 소수 표기가 원문 그대로 남아야 한다


class FakeDb:
    def __init__(self, fail_times=0, rows=()):
        self.statements = []
        self.fail_times = fail_times
        self.rows = list(rows)
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

    def fetchall(self):
        return self.db.rows


def record(**changes):
    values = dict(event_id="event-1", job_type="collect-news", scheduled_for=AT, started_at=AT, finished_at=AT,
                  published_count=12, incomplete_symbols=("TEST02",), research_job_id=None, raw=RAW)
    values.update(changes)
    return CollectionRecord(**values)


def make(db):
    return PostgresStatusStore(db.connect, StoreSettings(record_attempts=3), sleep=lambda _s: None)


def test_a_collection_is_saved_with_its_raw_text_and_the_insert_is_idempotent_by_event_id():
    db = FakeDb()
    assert make(db).save_collection(record(research_job_id="r-1")) is True
    [(sql, params)] = db.statements
    assert sql is INSERT_COLLECTION_SQL and "ON CONFLICT (event_id) DO NOTHING" in sql
    assert params[0] == "event-1" and params[5] == 12 and json.loads(params[6]) == ["TEST02"]
    assert params[7] == "r-1" and params[8] == RAW  # 원문 그대로
    assert db.commits == 1 and db.closed == db.connects


def test_a_failed_save_is_retried_and_then_reported_without_leaking_the_error_text(caplog):
    db = FakeDb(fail_times=2)
    assert make(db).save_collection(record()) is True and db.connects == 3
    db = FakeDb(fail_times=99)
    with caplog.at_level(logging.INFO):
        assert make(db).save_collection(record()) is False
    assert db.connects == 3
    assert "hunter2" not in caplog.text and "secret-user" not in caplog.text and "TEST02" not in caplog.text


def test_collections_are_read_by_the_finished_time_range():
    row = ("event-1", "collect-news", AT, AT, AT, 12, '["TEST02"]', None, RAW)
    db = FakeDb(rows=[row])
    result = make(db).collections_between(AT - timedelta(hours=1), AT)
    assert db.statements[0] == (SELECT_COLLECTIONS_SQL, (AT - timedelta(hours=1), AT))
    assert "finished_at >= %s AND finished_at <= %s" in SELECT_COLLECTIONS_SQL
    assert result == [record()]


def test_job_runs_are_read_by_the_started_time_range():
    row = ("job-1", "move", "failed", "llm_error", "rule-based", AT)
    db = FakeDb(rows=[row])
    [run] = make(db).job_runs_between(AT - timedelta(hours=1), AT)
    assert db.statements[0][0] == SELECT_RUNS_SQL and "started_at >= %s AND started_at <= %s" in SELECT_RUNS_SQL
    assert (run.job_id, run.job_kind, run.status, run.failure_reason, run.generated_by, run.started_at) == row


def test_a_failed_read_returns_none_without_leaking_the_error_text(caplog):
    db = FakeDb(fail_times=99)
    with caplog.at_level(logging.INFO):
        assert make(db).collections_between(AT, AT) is None
        assert make(db).job_runs_between(AT, AT) is None
    assert "hunter2" not in caplog.text
