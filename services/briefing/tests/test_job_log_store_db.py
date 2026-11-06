"""작업 기록(job_run·job_log)을 실제 Postgres에 저장한 뒤 다시 읽어 확인한다.

BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD가 모두 있을 때만 실행하고, 없으면 건너뛴다.
대상은 briefing 스키마가 비어 있는 테스트 전용 DB라고 가정한다.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from briefing.job_log import EntryType, GeneratedBy, JobKind, JobLogEntry, JobResult, JobStart, RunStatus
from briefing.job_log_store import LOG_TABLE, RUN_TABLE, PostgresJobLog, psycopg_connector

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


def start(store, job_id, kind=JobKind.MOVE, raw=RAW):
    run = JobStart(job_id, kind, raw, {"symbol": "ITB001", "moveAt": AT.isoformat()}, AT)
    assert store.start(run) is True
    return run


def entry(job_id, entry_type=EntryType.RESENT, data=None, **changes):
    values = dict(job_id=job_id, entry_type=entry_type, stage="market.history", reason="resent-after-timeout",
                  occurred_at=AT, error_type="ResponseTimeout", attempts=2,
                  data=data if data is not None else {"k": RAW})
    values.update(changes)
    return JobLogEntry(**values)


def test_startup_creates_the_run_and_log_tables_in_the_briefing_schema(store, connect):
    rows = fetch(connect, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'briefing' "
                          "AND table_name IN ('job_run', 'job_log', 'failed_jobs') ORDER BY table_name")

    assert rows == [("job_log",), ("job_run",)]


def test_job_run_line_is_made_running_and_updated_when_the_job_ends(store, connect):
    start(store, "it-run-1", raw=RAW)

    ((status, finished_at, raw_input, target),) = fetch(
        connect, f"SELECT status, finished_at, input::text, target::text FROM {RUN_TABLE} WHERE job_id = 'it-run-1'")
    assert (status, finished_at) == ("running", None)
    assert raw_input == RAW  # JSON 칸은 시작시킨 이벤트 원문을 표기 그대로 보존한다
    assert json.loads(target) == {"symbol": "ITB001", "moveAt": AT.isoformat()}

    result = JobResult("it-run-1", RunStatus.SUCCEEDED, AT + timedelta(minutes=3), generated_by=GeneratedBy.LLM,
                       final_text="최종 본문 한글", weak_count=2, research_count=4, result_event_id="evt-it-1")
    assert store.finish(result) is True

    rows = fetch(connect, "SELECT job_kind, status, failure_reason, generated_by, finished_at, final_text, "
                          f"weak_count, research_count, result_event_id FROM {RUN_TABLE} WHERE job_id = 'it-run-1'")
    assert rows == [("move", "succeeded", None, "llm", AT + timedelta(minutes=3), "최종 본문 한글", 2, 4, "evt-it-1")]


def test_starting_the_same_job_twice_keeps_one_run_line(store, connect):
    start(store, "it-run-2")
    start(store, "it-run-2")

    assert fetch(connect, f"SELECT count(*) FROM {RUN_TABLE} WHERE job_id = 'it-run-2'") == [(1,)]


def test_recorded_line_is_read_back_with_every_field(store, connect):
    start(store, "it-log-1")
    sentence = entry("it-log-1", EntryType.SENTENCE_REWRITTEN, data={"response": RAW, "한글": "원문", "n": None},
                     sentence_no=3, round=2, before="고치기 전 문장", after="고친 문장")
    assert store.record(sentence) is True

    rows = fetch(connect, f"SELECT job_id, entry_type, stage, sentence_no, round, before, after, reason, error_type, "
                          f"attempts, data::text, occurred_at, created_at IS NOT NULL FROM {LOG_TABLE} "
                          "WHERE job_id = 'it-log-1'")

    ((job_id, entry_type, stage, sentence_no, round_no, before, after, reason, error_type, attempts, data,
      occurred_at, has_created),) = rows
    assert (job_id, entry_type, stage, sentence_no, round_no, before, after) == (
        "it-log-1", "sentence-rewritten", "market.history", 3, 2, "고치기 전 문장", "고친 문장")
    assert (reason, error_type, attempts, has_created) == ("resent-after-timeout", "ResponseTimeout", 2, True)
    assert json.loads(data) == {"response": RAW, "한글": "원문", "n": None}
    assert occurred_at == AT


def test_many_lines_of_one_job_are_kept_in_order_under_the_same_job_id(store, connect):
    start(store, "it-log-2")
    kinds = (EntryType.RESENT, EntryType.CHECK_ROUND, EntryType.LLM_REJECTED, EntryType.REPORT_WRITTEN,
             EntryType.STEP_FAILED)
    for kind in kinds:
        store.record(entry("it-log-2", kind, error_type=None))

    rows = fetch(connect, f"SELECT entry_type FROM {LOG_TABLE} WHERE job_id = 'it-log-2' ORDER BY id")

    assert rows == [(kind.value,) for kind in kinds]


def test_a_long_response_is_stored_whole(store, connect):
    start(store, "it-log-3")
    big = "가" * 200_000
    store.record(entry("it-log-3", data={"response": big}))

    ((data,),) = fetch(connect, f"SELECT data::text FROM {LOG_TABLE} WHERE job_id = 'it-log-3'")

    assert json.loads(data) == {"response": big}


def test_every_job_kind_uses_the_same_two_tables(store, connect):
    start(store, "it-log-4", JobKind.MORNING)
    start(store, "it-log-5", JobKind.POST_CLOSE)

    rows = fetch(connect, f"SELECT job_id, job_kind FROM {RUN_TABLE} WHERE job_id IN ('it-log-4', 'it-log-5') "
                          "ORDER BY job_id")

    assert rows == [("it-log-4", "morning"), ("it-log-5", "post-close")]


def test_value_checks_reject_the_old_entry_types_and_unknown_kinds(store, connect):
    import psycopg

    start(store, "it-check-1")
    for sql, params in (
            (f"INSERT INTO {LOG_TABLE} (job_id, entry_type, stage, reason, attempts, data, occurred_at) "
             "VALUES ('it-check-1', 'result', 's', 'r', 0, '{}', now())", None),
            (f"INSERT INTO {LOG_TABLE} (job_id, entry_type, stage, reason, attempts, data, occurred_at) "
             "VALUES ('it-check-1', 'llm-succeeded', 's', 'r', 0, '{}', now())", None),
            (f"INSERT INTO {RUN_TABLE} (job_id, job_kind, input, status, target, started_at) "
             "VALUES ('it-check-2', 'noon', '{}', 'running', '{}', now())", None)):
        connection = connect()
        try:
            with pytest.raises(psycopg.errors.CheckViolation):
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
        finally:
            connection.close()


def test_a_log_line_needs_its_run_line(store, connect):
    import psycopg

    connection = connect()
    try:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with connection.cursor() as cursor:
                cursor.execute(f"INSERT INTO {LOG_TABLE} (job_id, entry_type, stage, reason, attempts, data, "
                               "occurred_at) VALUES ('it-no-run', 'resent', 's', 'r', 0, '{}', now())")
    finally:
        connection.close()
    # 저장소는 같은 일을 false로 돌려주고 그 작업을 실패로 기억한다.
    assert store.record(entry("it-no-run")) is False and store.has_failed("it-no-run") is True
