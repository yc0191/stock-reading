"""서버 상태 저장소를 실제 Postgres에 저장한 뒤 다시 읽어 확인한다.

BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD가 모두 있을 때만 실행하고, 없으면 건너뛴다.
대상은 briefing 스키마가 비어 있는 테스트 전용 DB라고 가정한다.
"""
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from briefing.job_log import JobKind, JobResult, JobStart, RunStatus
from briefing.job_log_store import PostgresJobLog, psycopg_connector
from briefing.status import CollectionRecord
from briefing.status_store import COLLECTION_TABLE, PostgresStatusStore

_VARS = ("BRIEFING_IT_DB_URL", "BRIEFING_IT_DB_USER", "BRIEFING_IT_DB_PASSWORD")

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="실제 DB 테스트: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD 가 없음",
)

KST = timezone(timedelta(hours=9))
# 다른 DB 테스트의 행과 섞이지 않도록 먼 과거의 범위를 쓴다.
BASE = datetime(2001, 3, 5, 10, 0, tzinfo=KST)
RAW = '{"b": 1,  "a": [1.50, 2.0]}'


@pytest.fixture(scope="module")
def connect():
    from psycopg.conninfo import make_conninfo

    conninfo = make_conninfo(os.environ["BRIEFING_IT_DB_URL"], user=os.environ["BRIEFING_IT_DB_USER"],
                             password=os.environ["BRIEFING_IT_DB_PASSWORD"])
    return psycopg_connector(conninfo)


@pytest.fixture(scope="module")
def stores(connect):
    # 서비스가 시작할 때와 같이 테이블을 만든다. 두 번 불러도 오류가 나지 않아야 한다.
    log_store = PostgresJobLog(connect=connect, sleep=lambda _s: None)
    log_store.create_table()
    status = PostgresStatusStore(connect=connect, sleep=lambda _s: None)
    status.create_table()
    status.create_table()
    return log_store, status


def collection(finished, job_type="collect-news", research=None, incomplete=("TEST02",)):
    return CollectionRecord(str(uuid.uuid4()), job_type, finished, finished, finished, 5, tuple(incomplete),
                            research, RAW)


def test_a_collection_is_saved_once_per_event_id_and_read_back_by_range_with_both_ends(stores, connect):
    _, status = stores
    inside = collection(BASE + timedelta(hours=1), research=str(uuid.uuid4()), job_type="research-news")
    edge_low, edge_high = collection(BASE), collection(BASE + timedelta(hours=2))
    outside = collection(BASE + timedelta(hours=2, seconds=1))
    for record in (inside, edge_low, edge_high, outside):
        assert status.save_collection(record) is True
    assert status.save_collection(inside) is True  # 같은 eventId는 한 줄만 남는다
    found = status.collections_between(BASE, BASE + timedelta(hours=2))
    assert [r.event_id for r in found] == [edge_low.event_id, inside.event_id, edge_high.event_id]
    read = found[1]
    assert read.research_job_id == inside.research_job_id and read.is_research()
    assert read.incomplete_symbols == ("TEST02",) and read.published_count == 5
    assert read.raw == RAW  # 키 순서·공백·소수 표기가 원문 그대로
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {COLLECTION_TABLE} WHERE event_id = %s", (inside.event_id,))
            assert cursor.fetchone()[0] == 1
    finally:
        connection.close()


def test_job_runs_are_read_by_the_started_time_range(stores):
    log_store, status = stores
    start = BASE + timedelta(days=1)
    ids = {}
    for kind, offset in ((JobKind.MORNING, 0), (JobKind.MOVE, 1), (JobKind.MOVE, 4)):
        job_id = str(uuid.uuid4())
        ids[(kind, offset)] = job_id
        assert log_store.start(JobStart(job_id, kind, json.dumps({"eventId": job_id}), {"k": "v"},
                                        start + timedelta(hours=offset))) is True
    failed = ids[(JobKind.MOVE, 1)]
    assert log_store.finish(JobResult(failed, RunStatus.FAILED, start + timedelta(hours=1, minutes=1),
                                      failure_reason="llm_error")) is True
    rows = status.job_runs_between(start, start + timedelta(hours=2))
    assert [(r.job_kind, r.status, r.failure_reason, r.generated_by) for r in rows] == [
        ("morning", "running", None, None), ("move", "failed", "llm_error", None)]
