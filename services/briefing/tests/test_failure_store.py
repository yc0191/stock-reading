import logging
import sys

import pytest

from briefing.config import StoreSettings
from briefing.failure_store import CREATE_TABLE_SQL, DETAIL_MAX_LENGTH, INSERT_SQL, PostgresFailureStore
from briefing.jobs import Failure, FailureReason

JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"


class FakeDb:
    """job_id 유니크와 ON CONFLICT DO NOTHING을 흉내 내는 가짜 DB. 앞의 fail_times번 연결은 실패시킨다."""

    def __init__(self, fail_times=0):
        self.rows = {}
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
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params):
        self.db.statements.append((sql, params))
        if sql is INSERT_SQL:
            job_id = params[0]
            if job_id in self.db.rows:
                self.rowcount = 0
            else:
                self.db.rows[job_id] = params
                self.rowcount = 1


def failure(detail="no market.history.responded within the time limit", job_id=JOB_ID):
    return Failure(job_id, "TEST01", FailureReason.MARKET_NO_RESPONSE, detail, 3)


@pytest.fixture
def sleeps():
    return []


def store_for(db, sleeps):
    return PostgresFailureStore(connect=db.connect, sleep=sleeps.append)


def test_records_one_row_with_reason_code_and_attempts(sleeps):
    db = FakeDb()

    store_for(db, sleeps).record(failure())

    assert db.rows == {JOB_ID: (JOB_ID, "TEST01", "market_no_response",
                                "no market.history.responded within the time limit", 3)}
    assert db.commits == 1
    assert db.closed == db.connects == 1
    assert sleeps == []


def test_same_job_id_is_recorded_only_once(sleeps, caplog):
    db = FakeDb()
    store = store_for(db, sleeps)
    caplog.set_level(logging.INFO, logger="briefing.failure_store")

    store.record(failure())
    store.record(Failure(JOB_ID, "TEST01", FailureReason.NEWS_UNSTABLE, "later duplicate", 5))

    assert len(db.rows) == 1
    assert db.rows[JOB_ID][2] == "market_no_response"
    assert "already recorded" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_insert_skips_conflicting_job_id_in_sql():
    assert "ON CONFLICT (job_id) DO NOTHING" in INSERT_SQL


def test_retries_then_succeeds_without_error_log(sleeps, caplog):
    db = FakeDb(fail_times=2)

    store_for(db, sleeps).record(failure())

    assert JOB_ID in db.rows
    assert db.connects == 3
    assert sleeps == [0.5, 0.5]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_gives_up_after_three_attempts_and_logs_only_ids(sleeps, caplog):
    db = FakeDb(fail_times=99)
    secret_detail = "기사 본문 내용 user@example.com"

    store_for(db, sleeps).record(failure(detail=secret_detail))

    assert db.connects == 3
    assert len(sleeps) == 2
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert [r.getMessage() for r in errors] == [
        f"could not record failure: job={JOB_ID} symbol=TEST01 reason=market_no_response"]
    # 설명(기사 내용이 섞일 수 있음)과 예외 메시지(접속 정보가 섞일 수 있음)는 어느 로그에도 남지 않는다.
    assert secret_detail not in caplog.text
    assert "hunter2" not in caplog.text and "secret-user" not in caplog.text


def test_attempts_and_interval_come_from_settings(sleeps):
    db = FakeDb(fail_times=99)
    store = PostgresFailureStore(connect=db.connect, sleep=sleeps.append,
                                 settings=StoreSettings(record_attempts=5))

    store.record(failure())

    assert db.connects == 5
    assert len(sleeps) == 4


def test_long_detail_is_cut_to_the_column_limit(sleeps):
    db = FakeDb()

    store_for(db, sleeps).record(failure(detail="x" * (DETAIL_MAX_LENGTH + 100)))

    assert len(db.rows[JOB_ID][3]) == DETAIL_MAX_LENGTH
    assert f"detail     VARCHAR({DETAIL_MAX_LENGTH})" in CREATE_TABLE_SQL


def test_detail_within_limit_is_kept_as_is(sleeps):
    db = FakeDb()
    exact = "y" * DETAIL_MAX_LENGTH

    store_for(db, sleeps).record(failure(detail=exact))

    assert db.rows[JOB_ID][3] == exact


def test_create_table_is_idempotent_and_allows_only_the_five_reasons(sleeps):
    db = FakeDb()

    store_for(db, sleeps).create_table()

    assert db.statements == [(CREATE_TABLE_SQL, None)]
    assert "CREATE TABLE IF NOT EXISTS briefing.failed_jobs" in CREATE_TABLE_SQL
    assert "job_id     VARCHAR(64) PRIMARY KEY" in CREATE_TABLE_SQL
    for column in ("symbol", "reason", "detail", "attempts", "created_at"):
        assert f"\n    {column} " in CREATE_TABLE_SQL
    for reason in FailureReason:
        assert f"'{reason.value}'" in CREATE_TABLE_SQL
    assert len(FailureReason) == 5


def test_works_without_a_database_driver_installed():
    # 드라이버는 실제 연결 함수 안에서만 불러오므로 모듈을 불러와도 드라이버가 로드되지 않는다.
    assert "psycopg" not in sys.modules


def test_core_llm_failure_is_written_through_the_store(sleeps):
    from datetime import datetime, timedelta, timezone

    from briefing.jobs import BriefingCore, ReportInput

    db = FakeDb()
    noop = type("Noop", (), {"send": lambda self, e: None, "on_ready": lambda self, r: None})()
    core = BriefingCore(requests=noop, failures=store_for(db, sleeps), ready=noop)
    as_of = datetime(2026, 10, 20, 16, 0, 31, tzinfo=timezone(timedelta(hours=9)))
    report_input = ReportInput(JOB_ID, "TEST01", "corr", as_of, 100, 103, 3.0, (), ())

    core.fail_llm_error(report_input, "invalid JSON from model", 3)

    assert db.rows[JOB_ID] == (JOB_ID, "TEST01", "llm_error", "invalid JSON from model", 3)
