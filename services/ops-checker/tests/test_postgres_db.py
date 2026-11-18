"""실제 Postgres 로 읽기 전용 실행과 장부 쓰기를 확인한다. OPS_CHECKER_TEST_DB_URL(호스트:포트/DB)·OPS_CHECKER_TEST_DB_USER·OPS_CHECKER_TEST_DB_PASSWORD 가 있고
ops 마이그레이션이 적용된 데이터베이스에서만 돈다. 끝나면 이 테스트가 쓴 줄만 지운다."""
import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from ops_checker.metrics import MetricError
from ops_checker.store import PostgresRunStore, PostgresScalarQuery, ResultRow, RunRecord, connector

URL, USER, PASSWORD = (os.environ.get(k) for k in ("OPS_CHECKER_TEST_DB_URL", "OPS_CHECKER_TEST_DB_USER", "OPS_CHECKER_TEST_DB_PASSWORD"))
pytestmark = pytest.mark.skipif(not (URL and USER and PASSWORD), reason="OPS_CHECKER_TEST_DB_URL·OPS_CHECKER_TEST_DB_USER·OPS_CHECKER_TEST_DB_PASSWORD 없음")

TEST_EVENT = "99999999-9999-4999-8999-999999999999"


def connect():
    hostport, dbname = URL.split("/", 1)
    host, _, port = hostport.partition(":")
    return connector(host, int(port or 5432), dbname, USER, PASSWORD)()


def test_reads_one_cell_and_rejects_writes_and_slow_queries():
    query = PostgresScalarQuery(connect)
    assert query.scalar("SELECT 0.25::numeric") == [(Decimal("0.25"),)]
    with pytest.raises(MetricError) as write:
        query.scalar("CREATE TABLE ops.should_not_exist (x int)")
    assert write.value.reason in ("read-only-violation", "sql-failed")
    with pytest.raises(MetricError) as broken:
        query.scalar("SELECT 1 FROM no_such_table")
    assert broken.value.reason == "sql-failed"
    with pytest.raises(MetricError) as slow:
        query.scalar("SELECT pg_sleep(11)")
    assert slow.value.reason == "timeout"
    assert query.scalar("SELECT 1") == [(1,)]   # 실패 뒤에도 연결이 되살아 있다


def test_run_and_results_are_written_once_per_event_id():
    store = PostgresRunStore(connect)
    now = datetime(2026, 11, 18, 7, 31, tzinfo=timezone.utc)
    run = RunRecord(date(2026, 11, 18), "post-close", TEST_EVENT, "collected", "v", now, now, "ok")
    row = ResultRow(999999, "it", "h" * 64, "m", Decimal("0.5"), "< 0.9", "pass")
    try:
        assert not store.exists(TEST_EVENT)
        assert store.record(run, [row]) is True
        assert store.exists(TEST_EVENT)
        assert store.record(run, [row]) is False
    finally:
        with connect() as conn:
            conn.execute("DELETE FROM ops.check_result WHERE run_id IN (SELECT id FROM ops.check_run WHERE event_id = %s)",
                         (TEST_EVENT,))
            conn.execute("DELETE FROM ops.check_run WHERE event_id = %s", (TEST_EVENT,))
