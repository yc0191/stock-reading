"""job_history.py(브리핑 작업 하나의 기록 보기)를 가짜 연결로 확인한다. 실제 DB를 부르지 않는다."""
import io
from datetime import datetime, timedelta, timezone

import pytest

import job_history as jh

KST = timezone(timedelta(hours=9))
T0 = datetime(2026, 11, 5, 10, 0, 0, tzinfo=KST)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
BRIEFING_ID = "9b1f5a2c-0d3e-5f47-8a6b-1c2d3e4f5a6b"

RUN = {
    "job_id": JOB_ID, "result_event_id": BRIEFING_ID, "job_kind": "move", "status": "succeeded",
    "failure_reason": None, "generated_by": "llm", "target": {"symbol": "TEST01", "moveAt": T0.isoformat()},
    "started_at": T0, "finished_at": T0 + timedelta(minutes=4), "final_text": "최종 본문 첫 줄\n최종 본문 둘째 줄",
    "weak_count": 0, "research_count": 2, "input": '{"eventId": "x"}',
}


def log(no, minutes, entry_type, stage="llm-report", reason="r", **changes):
    row = {"id": no, "entry_type": entry_type, "stage": stage, "sentence_no": None, "round": None, "before": None,
           "after": None, "reason": reason, "error_type": None, "attempts": 0, "data": {},
           "occurred_at": T0 + timedelta(minutes=minutes)}
    row.update(changes)
    return row


# 일부러 시간 순서와 다르게 둔다.
LOG = [
    log(3, 3, "sentence-rewritten", "rewrite", "sentence_rewritten", sentence_no=2, round=1,
        before="고치기 전 문장", after="고친 뒤 문장"),
    log(1, 0, "report-written", reason="llm-output-accepted", attempts=1),
    log(2, 1, "query-written", "query", "query_accepted", sentence_no=2, round=1, attempts=1,
        data={"queries": ["검색어 하나", "검색어 둘"]}),
    log(4, 2, "articles-received", "filter", "articles_received", sentence_no=2, round=1, data={
        "usable": [{"articleId": "r1", "title": "쓴 기사", "publishedAt": "2026-11-05T09:00:00+09:00"}],
        "dropped": [{"articleId": "r2", "title": "버린 기사", "publishedAt": "2026-11-01T09:00:00+09:00",
                     "reason": "outside_range"}],
        "collectorCompletion": {"jobType": "research-news", "publishedCount": 7, "incompleteSymbols": []}}),
    log(5, 2, "llm-rejected", reason="llm-output-rejected", sentence_no=2, round=1, attempts=1,
        error_type="InvalidOutput", data={"rejection": "cause cites 1 ref(s) that were not given"}),
]


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params):
        self.db.executed.append((sql, params))
        self.sql, self.params = sql, params

    def fetchall(self):
        if "FROM briefing.job_run" in self.sql:
            key = self.params[0]
            return [tuple(self.db.run[c] for c in jh.RUN_COLUMNS)] \
                if key in (self.db.run["job_id"], self.db.run["result_event_id"]) else []
        return [tuple(row[c] for c in jh.LOG_COLUMNS) for row in self.db.log]


class FakeConn:
    def __init__(self, run=RUN, log_rows=LOG):
        self.run, self.log, self.executed, self.closed = run, log_rows, [], False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def run_cli(key, conn=None, *extra):
    conn = conn or FakeConn()
    out = io.StringIO()
    code = jh.main([key, *extra], env={}, out=out, connect_fn=lambda env: conn)
    return code, out.getvalue(), conn


def test_history_shows_the_run_line_and_the_log_lines_in_time_order():
    code, text, conn = run_cli(JOB_ID)

    assert code == 0 and conn.closed
    assert f"작업 {JOB_ID}" in text and f"브리핑 ID(메일에 보이는 값): {BRIEFING_ID}" in text
    assert "종류 move  상태 succeeded  작성 llm" in text and "재검색 요청 2건" in text
    assert "최종 본문:\n  최종 본문 첫 줄\n  최종 본문 둘째 줄" in text
    # 작업 기록은 입력 순서가 아니라 시간 순서(같은 시각이면 id 순서)로 나온다.
    positions = [text.index(kind) for kind in ("report-written", "query-written", "articles-received",
                                               "llm-rejected", "sentence-rewritten")]
    assert positions == sorted(positions)
    assert "작업 기록 5줄 (시간 순서)" in text


def test_the_same_history_is_found_by_the_job_id_and_by_the_briefing_id_in_the_mail():
    _, by_job, _ = run_cli(JOB_ID)
    code, by_mail, conn = run_cli(BRIEFING_ID)

    assert code == 0 and by_mail == by_job
    # 두 ID 모두 같은 질문 하나로 찾는다. 질문은 job_id와 브리핑 ID 칸을 함께 본다.
    ((sql, params),) = conn.executed[:1]
    assert "job_id = %s OR result_event_id = %s" in sql and params == (BRIEFING_ID, BRIEFING_ID)


def test_sentence_lines_show_the_sentence_number_round_queries_articles_and_before_after_sentences():
    _, text, _ = run_cli(JOB_ID)

    assert "문장 2, 회차 1" in text
    assert "검색어: 검색어 하나, 검색어 둘" in text
    assert "쓸 기사:\n" in text and "r1 '쓴 기사'" in text
    assert "버린 기사:" in text and "r2 '버린 기사'" in text and "← outside_range" in text
    assert "수집기 완료 알림: 발행 7건, 못 모은 종목 0개" in text
    assert "거부 이유: cause cites 1 ref(s) that were not given" in text and "오류 InvalidOutput" in text
    assert "before(고치기 전):\n          고치기 전 문장" in text
    assert "after(고친 뒤):\n          고친 뒤 문장" in text


def test_full_option_appends_the_raw_data_of_each_line():
    _, plain, _ = run_cli(JOB_ID)
    _, full, _ = run_cli(JOB_ID, None, "--full")

    assert "data: " not in plain
    assert full.count("data: ") == len(LOG) and '"queries": ["검색어 하나", "검색어 둘"]' in full


def test_a_failed_run_shows_its_reason_and_an_unpublished_run_has_no_briefing_id():
    failed = {**RUN, "status": "failed", "failure_reason": "llm_error", "result_event_id": None, "generated_by": None,
              "final_text": None, "finished_at": None}
    code, text, _ = run_cli(JOB_ID, FakeConn(failed, []))

    assert code == 0
    assert "상태 failed" in text and "실패 사유 llm_error" in text and "(발행하지 않음)" in text
    assert "아직 끝나지 않음" in text and "(줄이 없습니다)" in text and "최종 본문:" not in text


def test_unknown_id_prints_a_message_and_returns_one(capsys):
    code, text, conn = run_cli("no-such-id")

    assert (code, text) == (1, "") and conn.closed
    assert "찾지 못했습니다" in capsys.readouterr().err


def test_the_tool_only_reads_the_tables():
    source = open(jh.__file__, encoding="utf-8").read().upper()
    for write in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP ", "ALTER ", "TRUNCATE"):
        assert write not in source.replace("# ", "")
    _, _, conn = run_cli(JOB_ID)
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _ in conn.executed)
    assert "ORDER BY occurred_at, id" in jh.LOG_SQL


def test_missing_connection_settings_name_the_variables_without_printing_values():
    with pytest.raises(SystemExit) as stop:
        jh.connect({"JOB_HISTORY_DB_PASSWORD": "secret-value"})

    assert "JOB_HISTORY_DB_URL" in str(stop.value) and "secret-value" not in str(stop.value)
