"""작업 기록을 실제 Postgres에 저장한 뒤 다시 읽어 확인한다.

BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD가 모두 있을 때만 실행하고, 없으면 건너뛴다.
대상은 briefing 스키마가 비어 있는 테스트 전용 DB라고 가정한다.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from briefing.job_log import JobLogEntry
from briefing.job_log_store import TABLE, PostgresJobLog, psycopg_connector

_VARS = ("BRIEFING_IT_DB_URL", "BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD")

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="실제 DB 테스트: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD 가 없음",
)

AT = datetime(2026, 10, 20, 16, 0, 31, tzinfo=timezone(timedelta(hours=9)))
RAW = '{"b": 1,  "a": [1.50, 2.0]}'


@pytest.fixture(scope="module")
def connect():
    from psycopg.conninfo import make_conninfo

    conninfo = make_conninfo(os.environ["BRIEFING_IT_DB_URL"], user=os.environ["BRIEFING_IT_DB_USER"],
                             password=os.environ["BRIEFING_IT_DB_PASSWORD"])
    return psycopg_connector(conninfo)


@pytest.fixture(scope="module")
def store(connect):
    # 서비스가 시작할 때와 같이 테이블을 만든다. 두 번 불러도 오류가 나지 않아야 한다.
    store = PostgresJobLog(connect=connect, sleep=lambda _seconds: None)
    store.create_table()
    store.create_table()
    return store


def fetch(connect, sql, params=None):
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
    finally:
        connection.close()


def entry(job_id, entry_type="resent", data=None, **changes):
    values = dict(job_kind="move", researched=False, job_id=job_id, target={"symbol": "ITB001", "moveAt": AT.isoformat()},
                  stage="market.history", entry_type=entry_type, reason="resent-after-timeout",
                  error_type="ResponseTimeout", attempts=2, data=data if data is not None else {"k": RAW},
                  occurred_at=AT)
    values.update(changes)
    return JobLogEntry(**values)


def test_startup_creates_the_log_table_in_the_briefing_schema_and_not_the_old_one(store, connect):
    rows = fetch(connect, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'briefing' "
                          "AND table_name IN ('job_log', 'failed_jobs') ORDER BY table_name")

    assert rows == [("job_log",)]


def test_recorded_line_is_read_back_with_every_field(store, connect):
    assert store.record(entry("it-log-1", data={"response": RAW, "한글": "원문", "n": None})) is True

    rows = fetch(connect, f"SELECT job_kind, researched, job_id, target::text, stage, entry_type, reason, error_type, "
                          f"attempts, data::text, occurred_at, created_at IS NOT NULL FROM {TABLE} "
                          "WHERE job_id = 'it-log-1'")

    ((kind, researched, job_id, target, stage, entry_type, reason, error_type, attempts, data, occurred_at, has_created),
     ) = rows
    assert (kind, researched, job_id, stage, entry_type, reason, error_type, attempts, has_created) == (
        "move", False, "it-log-1", "market.history", "resent", "resent-after-timeout", "ResponseTimeout", 2, True)
    assert json.loads(target) == {"symbol": "ITB001", "moveAt": AT.isoformat()}
    assert json.loads(data) == {"response": RAW, "한글": "원문", "n": None}
    assert occurred_at == AT


def test_many_lines_of_one_job_are_kept_in_order_under_the_same_job_id(store, connect):
    for entry_type in ("resent", "check-round", "llm-rejected", "llm-succeeded", "result"):
        store.record(entry("it-log-2", entry_type, error_type=None))

    rows = fetch(connect, f"SELECT entry_type, error_type FROM {TABLE} WHERE job_id = 'it-log-2' ORDER BY id")

    assert rows == [("resent", None), ("check-round", None), ("llm-rejected", None), ("llm-succeeded", None),
                    ("result", None)]


def test_a_long_response_is_stored_whole(store, connect):
    big = "가" * 200_000
    store.record(entry("it-log-3", data={"response": big}))

    ((data,),) = fetch(connect, f"SELECT data::text FROM {TABLE} WHERE job_id = 'it-log-3'")

    assert json.loads(data) == {"response": big}


def test_researched_and_daily_lines_use_the_same_table(store, connect):
    store.record(entry("it-log-4", researched=True))
    store.record(entry("it-log-5", job_kind="morning",
                       target={"rangeStart": AT.isoformat(), "rangeEnd": AT.isoformat()}))

    rows = fetch(connect, f"SELECT job_id, job_kind, researched FROM {TABLE} "
                          "WHERE job_id IN ('it-log-4', 'it-log-5') ORDER BY job_id")

    assert rows == [("it-log-4", "move", True), ("it-log-5", "morning", False)]
