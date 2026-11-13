"""작업 기록(job_run·job_log)을 실제 Postgres에 저장한 뒤 다시 읽어 확인한다.

BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD가 모두 있을 때만 실행하고, 없으면 건너뛴다.
대상은 Flyway가 briefing 스키마에 services/briefing/db/migration을 적용해 둔 테스트 전용 DB라고 가정한다(scripts/test-all.sh가 준비한다).
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
    return PostgresJobLog(connect=connect, sleep=lambda _seconds: None)


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


def test_flyway_created_the_briefing_tables_and_recorded_versions_1_to_3(store, connect):
    rows = fetch(connect, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'briefing' "
                          "AND table_name IN ('job_run', 'job_log', 'collection_completed', 'failed_jobs') "
                          "ORDER BY table_name")
    applied = fetch(connect, "SELECT version, success FROM briefing.flyway_schema_history WHERE version IS NOT NULL")

    assert rows == [("collection_completed",), ("job_log",), ("job_run",)]
    assert applied == [("1", True), ("2", True), ("3", True)]


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


def test_unfinished_runs_are_failed_once_with_the_restart_reason_and_a_log_line_and_newer_runs_are_left(store, connect):
    now = AT + timedelta(days=1)
    for job_id, started_at in (("it-restart-old", AT), ("it-restart-done", AT), ("it-restart-new", now + timedelta(hours=1))):
        assert store.start(JobStart(job_id, JobKind.MOVE, RAW, {"symbol": "ITB001", "moveAt": AT.isoformat()},
                                    started_at)) is True
    assert store.finish(JobResult("it-restart-done", RunStatus.SUCCEEDED, AT, generated_by=GeneratedBy.LLM)) is True

    interrupted = store.fail_unfinished(now)

    assert "it-restart-old" in interrupted
    assert "it-restart-done" not in interrupted and "it-restart-new" not in interrupted
    runs = dict((row[0], row[1:]) for row in fetch(
        connect, f"SELECT job_id, status, failure_reason, finished_at, final_text FROM {RUN_TABLE} "
                 "WHERE job_id LIKE 'it-restart-%'"))
    assert runs["it-restart-old"] == ("failed", "interrupted_by_restart", now, None)
    assert runs["it-restart-done"][0] == "succeeded"  # 이미 끝난 작업은 그대로
    assert runs["it-restart-new"] == ("running", None, None, None)  # 이번 실행이 만든 작업은 그대로
    lines = fetch(connect, f"SELECT entry_type, stage, reason FROM {LOG_TABLE} WHERE job_id = 'it-restart-old'")
    assert lines == [("step-failed", "restart", "interrupted_by_restart")]
    # 다시 불러도 같은 작업을 또 고치거나 기록하지 않는다.
    assert "it-restart-old" not in store.fail_unfinished(now)
    assert len(fetch(connect, f"SELECT 1 FROM {LOG_TABLE} WHERE job_id = 'it-restart-old'")) == 1


def test_a_moves_grouped_line_is_accepted_and_read_back_with_every_grouped_move(store, connect):
    start(store, "it-group-1")
    moves = [{"eventId": "it-g1", "price": 10000, "changePercent": 1.5}, {"eventId": "it-g2", "price": 10100}]
    assert store.record(entry("it-group-1", EntryType.MOVES_GROUPED, stage="move-group", reason="grouped-by-symbol",
                              error_type=None, attempts=2, data={"moves": moves})) is True

    ((entry_type, attempts, data),) = fetch(
        connect, f"SELECT entry_type, attempts, data::text FROM {LOG_TABLE} WHERE job_id = 'it-group-1'")
    assert (entry_type, attempts) == ("moves-grouped", 2) and json.loads(data) == {"moves": moves}


def test_a_member_briefing_line_says_which_member_the_board_was_for(store, connect):
    # V2가 기록 종류에 member-briefing을 더했다. 멤버 번호와 결과 이벤트 ID가 JSON 칸에 그대로 남는다.
    start(store, "it-member-1", kind=JobKind.POST_CLOSE)
    data = {"memberId": 41, "resultEventId": "it-event-41", "etfCount": 2, "missingItems": []}
    assert store.record(entry("it-member-1", EntryType.MEMBER_BRIEFING, stage="compose", reason="member_published",
                              error_type=None, attempts=0, data=data)) is True

    ((entry_type, stored),) = fetch(
        connect, f"SELECT entry_type, data::text FROM {LOG_TABLE} WHERE job_id = 'it-member-1'")
    assert entry_type == "member-briefing" and json.loads(stored) == data



def test_final_check_and_sentence_fixed_lines_are_accepted_and_read_back(store, connect):
    # V3가 기록 종류에 final-check와 sentence-fixed를 더했다. 고친 문장의 before·after가 그대로 남는다.
    start(store, "it-final-1")
    check = {"codeCheck": {"fixable": [], "weakOnly": [2]}, "review": {"problems": []}, "chosen": "original"}
    assert store.record(entry("it-final-1", EntryType.FINAL_CHECK, stage="final-check", reason="no-problem",
                              error_type=None, attempts=1, data=check)) is True
    assert store.record(entry("it-final-1", EntryType.SENTENCE_FIXED, stage="final-check", reason="contradiction",
                              error_type=None, attempts=1, sentence_no=4, round=1, before="고치기 전 문장 한글",
                              after="검토로 고친 문장 한글", data={"problem": "모순"})) is True

    rows = fetch(connect, f"SELECT entry_type, stage, reason, sentence_no, round, before, after, data::text "
                          f"FROM {LOG_TABLE} WHERE job_id = 'it-final-1' ORDER BY id")

    ((check_type, check_stage, check_reason, check_no, check_round, check_before, check_after, check_data),
     (fixed_type, fixed_stage, fixed_reason, fixed_no, fixed_round, fixed_before, fixed_after, fixed_data)) = rows
    assert (check_type, check_stage, check_reason, check_no, check_round, check_before, check_after) == (
        "final-check", "final-check", "no-problem", None, None, None, None)
    assert json.loads(check_data) == check
    assert (fixed_type, fixed_stage, fixed_reason, fixed_no, fixed_round, fixed_before, fixed_after) == (
        "sentence-fixed", "final-check", "contradiction", 4, 1, "고치기 전 문장 한글", "검토로 고친 문장 한글")
    assert json.loads(fixed_data) == {"problem": "모순"}

# ---- 마감 후 판이 읽는 그날 아침 예상과 급변동 ----

KST = timezone(timedelta(hours=9))
# 다른 테스트의 자료(2026-10-20)와 겹치지 않는 날짜와 종목으로 정한다.
REVIEW_DAY = datetime(2026, 3, 3, tzinfo=KST)


def start_at(store, job_id, kind, at, raw="{}", target=None):
    assert store.start(JobStart(job_id, kind, raw, target or {"symbol": "ITR001", "moveAt": at.isoformat()}, at))


def test_the_morning_outlook_of_the_korean_day_is_read_back_newest_first(store):
    morning = REVIEW_DAY.replace(hour=8, minute=45)
    start_at(store, "it-review-morning-1", JobKind.MORNING, morning)
    start_at(store, "it-review-morning-prev", JobKind.MORNING, morning - timedelta(days=1))
    start_at(store, "it-review-post", JobKind.POST_CLOSE, morning)
    for job_id, at, n in (("it-review-morning-1", morning, 1), ("it-review-morning-1", morning + timedelta(minutes=5), 2),
                          ("it-review-morning-prev", morning - timedelta(days=1), 3), ("it-review-post", morning, 4)):
        store.record(entry(job_id, EntryType.ETF_OUTLOOK, data={"n": n, "outlooks": []}, occurred_at=at))

    # 자정 경계의 기록은 한국 시간 날짜로 가른다(UTC 날짜로 가르면 같은 날로 잘못 잡힌다).
    start_at(store, "it-review-morning-night", JobKind.MORNING, REVIEW_DAY + timedelta(days=1, minutes=30))
    store.record(entry("it-review-morning-night", EntryType.ETF_OUTLOOK, data={"n": 5, "outlooks": []},
                       occurred_at=REVIEW_DAY + timedelta(days=1, minutes=30)))

    found = store.morning_outlooks(REVIEW_DAY.date())

    assert [d["n"] for d in found] == [2, 1]


def test_the_moves_started_in_the_range_are_read_back_with_their_results(store):
    day = REVIEW_DAY + timedelta(days=10)
    event = json.dumps({"payload": {"symbol": "ITR001", "changePercent": 2.1}})
    first, second, other_day = day.replace(hour=10, minute=15), day.replace(hour=13, minute=20), day - timedelta(days=1)
    start_at(store, "it-review-move-1", JobKind.MOVE, first, event)
    start_at(store, "it-review-move-2", JobKind.MOVE, second, event.replace("2.1", "-1.8"))
    start_at(store, "it-review-move-3", JobKind.MOVE, other_day, event)
    start_at(store, "it-review-move-4", JobKind.MORNING, first)
    store.finish(JobResult("it-review-move-1", RunStatus.SUCCEEDED, first + timedelta(minutes=2),
                           generated_by=GeneratedBy.LLM))
    store.finish(JobResult("it-review-move-2", RunStatus.FAILED, second + timedelta(minutes=2),
                           failure_reason="test-failure"))

    runs = store.moves_between(day, day.replace(hour=17))

    assert [(r.job_id, r.symbol, r.move_at, r.change_percent, r.status, r.generated_by, r.failure_reason)
            for r in runs] == [
        ("it-review-move-1", "ITR001", first, 2.1, "succeeded", "llm", None),
        ("it-review-move-2", "ITR001", second, -1.8, "failed", None, "test-failure")]
