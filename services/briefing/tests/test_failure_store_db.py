"""실패 기록을 실제 Postgres에서 저장 후 다시 읽어 확인한다.

BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD가 모두 있을 때만 실행하고, 없으면 건너뛴다.
대상은 briefing 스키마가 비어 있는 테스트 전용 DB라고 가정한다.
"""
import os

import pytest

from briefing.failure_store import INSERT_SQL, TABLE, PostgresFailureStore, psycopg_connector
from briefing.ports import Failure, FailureReason

_VARS = ("BRIEFING_IT_DB_URL", "BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD")

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="실제 DB 테스트: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD 가 없음",
)


@pytest.fixture(scope="module")
def connect():
    from psycopg.conninfo import make_conninfo

    conninfo = make_conninfo(os.environ["BRIEFING_IT_DB_URL"], user=os.environ["BRIEFING_IT_DB_USER"],
                             password=os.environ["BRIEFING_IT_DB_PASSWORD"])
    return psycopg_connector(conninfo)


@pytest.fixture(scope="module")
def store(connect):
    # 서비스가 시작할 때와 같이 테이블을 만든다. 두 번 불러도 오류가 나지 않아야 한다.
    store = PostgresFailureStore(connect=connect, sleep=lambda _seconds: None)
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


def test_startup_creates_the_table_in_the_briefing_schema(store, connect):
    rows = fetch(connect, "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'failed_jobs'")

    assert rows == [("briefing", "failed_jobs")]


def test_recorded_failure_is_read_back(store, connect):
    store.record(Failure("it-job-1", "ITB001", FailureReason.NEWS_NO_RESPONSE, "no news.articles.responded", 3))

    rows = fetch(connect, f"SELECT job_id, symbol, reason, detail, attempts, created_at IS NOT NULL FROM {TABLE} "
                          "WHERE job_id = 'it-job-1'")

    assert rows == [("it-job-1", "ITB001", "news_no_response", "no news.articles.responded", 3, True)]


def test_same_job_id_recorded_twice_keeps_the_first_row(store, connect):
    store.record(Failure("it-job-2", "ITB002", FailureReason.MARKET_NO_RESPONSE, "first", 1))
    store.record(Failure("it-job-2", "ITB002", FailureReason.LLM_ERROR, "second", 2))

    rows = fetch(connect, f"SELECT reason, detail, attempts FROM {TABLE} WHERE job_id = 'it-job-2'")

    assert rows == [("market_no_response", "first", 1)]


def test_every_allowed_reason_is_accepted(store, connect):
    for index, reason in enumerate(FailureReason):
        store.record(Failure(f"it-job-3-{index}", "ITB003", reason, "allowed", 0))

    rows = fetch(connect, f"SELECT reason FROM {TABLE} WHERE symbol = 'ITB003' ORDER BY reason")

    assert sorted(row[0] for row in rows) == sorted(reason.value for reason in FailureReason)


def test_reason_outside_the_allowed_values_is_rejected(store, connect):
    import psycopg

    connection = connect()
    try:
        with pytest.raises(psycopg.errors.CheckViolation):
            with connection.cursor() as cursor:
                cursor.execute(INSERT_SQL, ("it-job-4", "ITB004", "unknown_reason", "bad", 1))
        connection.rollback()
    finally:
        connection.close()

    assert fetch(connect, f"SELECT count(*) FROM {TABLE} WHERE job_id = 'it-job-4'") == [(0,)]
