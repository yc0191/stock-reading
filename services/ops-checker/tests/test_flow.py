"""확인기 흐름 시험(가짜 알림·가짜 DB·가짜 S3). 테스트 이름의 b11~b20 은 수락 기준 번호다."""
import hashlib
import json
from datetime import date, datetime

import pytest

from fakes import EVENT_ID, KST, FakeQuery, FakeResults, FakeStore, criteria_file, metric_block, notification
from ops_checker.flow import DUPLICATE, FAILED, HOLIDAY, IGNORED, OK, STALE, Checker, MalformedNotification
from ops_checker.metrics import MetricError
from ops_checker.results import build_result, result_key
from ops_checker.store import RunRecord

NOW = datetime(2026, 11, 18, 16, 31, tzinfo=KST)
VERSION = "0123456789abcdef0123456789abcdef01234567"


def setup(tmp_path, answers=None, fail_upload=False):
    store, results = FakeStore(), FakeResults(fail=fail_upload)
    query = FakeQuery(answers or {})
    checker = Checker(store, query, results, tmp_path, VERSION, clock=lambda: NOW)
    return checker, store, query, results


def body(results: FakeResults) -> dict:
    return json.loads(results.puts[-1][1])


def test_b11_morning_notification_does_nothing(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT ratio"))
    checker, store, query, results = setup(tmp_path)
    assert checker.handle(notification(edition="morning")) == IGNORED
    assert not store.runs and not results.puts and not query.calls


def test_b12_closed_post_close_records_holiday_without_computing(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT ratio"))
    checker, store, query, results = setup(tmp_path)
    assert checker.handle(notification(status="market-closed")) == HOLIDAY
    assert not query.calls and not store.rows
    assert [r.outcome for r in store.runs] == ["holiday"] and store.runs[0].market_status == "market-closed"
    key, _ = results.puts[0]
    assert key == "results/2026-11-18/post-close.json"
    assert body(results)["status"] == "market-closed" and body(results)["checks"] == []


def test_b13_collected_post_close_computes_every_criteria_file(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("one", "SELECT one") + "\n" + metric_block("two", "SELECT two"))
    criteria_file(tmp_path, 901, "b", metrics=metric_block("three", "SELECT three", ">= 0.5"))
    checker, store, query, results = setup(tmp_path, {"one": [(0.1,)], "two": [(0.9,)], "three": [(0.6,)]})
    assert checker.handle(notification()) == OK
    run = store.runs[0]
    assert (run.outcome, run.error, run.deploy_version, run.day, run.edition) == ("ok", None, VERSION, date(2026, 11, 18), "post-close")
    assert [(r.issue, r.metric, r.verdict) for r in store.rows] == [(900, "one", "pass"), (900, "two", "fail"), (901, "three", "pass")]
    assert all(r.criteria_hash and r.threshold for r in store.rows)
    checks = body(results)["checks"]
    assert [c["issue"] for c in checks] == [900, 901] 


def test_b14_same_event_id_twice_records_and_uploads_once(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT m"))
    checker, store, query, results = setup(tmp_path, {"SELECT m": [(0.1,)]})
    assert checker.handle(notification()) == OK
    assert checker.handle(notification()) == DUPLICATE
    assert len(store.runs) == 1 and len(results.puts) == 1 and len(query.calls) == 1


def test_b14_a_run_recorded_between_the_check_and_the_write_is_not_duplicated(tmp_path):
    checker, store, _, _ = setup(tmp_path)
    store.exists = lambda event_id: False   # 확인할 때는 없었는데 쓰기 전에 같은 알림이 먼저 기록된 경우
    store.runs.append(RunRecord(date(2026, 11, 18), "post-close", EVENT_ID, "collected", VERSION, NOW, NOW, "ok"))
    assert checker.handle(notification()) == DUPLICATE
    assert len(store.runs) == 1


def test_b15_verdicts_pass_fail_hold_error_with_reasons(tmp_path):
    metrics = "\n".join(metric_block(k, f"SELECT {k}") for k in ("ok", "bad", "empty", "boom", "slow", "wide", "big"))
    criteria_file(tmp_path, 900, "a", metrics=metrics)
    answers = {"SELECT ok": [(0.1,)], "SELECT bad": [(0.9,)], "SELECT empty": [(None,)],
               "SELECT boom": MetricError("sql-failed"), "SELECT slow": MetricError("timeout"),
               "SELECT wide": [(0.1,), (0.2,)], "SELECT big": [(7,)]}
    checker, store, _, results = setup(tmp_path, answers)
    checker.handle(notification())
    got = {r.metric: (r.verdict, r.reason) for r in store.rows}
    assert got == {"ok": ("pass", None), "bad": ("fail", None), "empty": ("hold", "no-value"),
                   "boom": ("error", "sql-failed"), "slow": ("error", "timeout"),
                   "wide": ("error", "bad-shape"), "big": ("error", "out-of-range")}
    assert store.runs[0].outcome == "ok"   # 지표 error 는 그 지표의 판정이고 실행 자체는 끝났다


def test_b16_query_that_tries_to_write_is_an_error_and_other_metrics_continue(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("w", "SELECT w") + "\n" + metric_block("r", "SELECT r"))
    checker, store, _, _ = setup(tmp_path, {"SELECT w": MetricError("read-only-violation"), "SELECT r": [(0.1,)]})
    checker.handle(notification())
    assert [(r.metric, r.verdict, r.reason) for r in store.rows] == [
        ("w", "error", "read-only-violation"), ("r", "pass", None)]


def test_b17_malformed_criteria_file_is_only_that_files_error(tmp_path):
    (tmp_path / "902-a.yml").write_text("issue: [", encoding="utf-8")
    criteria_file(tmp_path, 903, "a", metrics=metric_block("m", "SELECT m"))
    checker, store, query, results = setup(tmp_path, {"SELECT m": [(0.1,)]})
    assert checker.handle(notification()) == OK
    assert [(r.issue, r.metric, r.verdict, r.reason) for r in store.rows] == [
        (902, "_format", "error", "invalid-yaml"), (903, "m", "pass", None)]
    assert len(query.calls) == 1


def test_b18_upload_failure_makes_the_run_an_error(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT m"))
    checker, store, _, _ = setup(tmp_path, {"SELECT m": [(0.1,)]}, fail_upload=True)
    assert checker.handle(notification()) == FAILED
    assert (store.runs[0].outcome, store.runs[0].error) == ("error", "upload-failed")


def test_b18_upload_failure_on_a_holiday_is_also_an_error(tmp_path):
    checker, store, _, _ = setup(tmp_path, fail_upload=True)
    assert checker.handle(notification(status="market-closed")) == FAILED
    assert store.runs[0].outcome == "error"


def test_b19_result_file_has_only_the_agreed_fields_and_the_file_hash(tmp_path):
    path = criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT secret_table_name"), days=3)
    checker, store, _, results = setup(tmp_path, {"secret_table_name": [(0.25,)]})
    checker.handle(notification())
    doc = body(results)
    assert set(doc) == {"day", "edition", "status", "deploy_version", "checks"}
    assert (doc["day"], doc["edition"], doc["status"], doc["deploy_version"]) == ("2026-11-18", "post-close", "collected", VERSION)
    check = doc["checks"][0]
    assert set(check) == {"issue", "attempt", "criteria_hash", "comment", "days", "metrics"}
    assert check["criteria_hash"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert check["metrics"] == [{"key": "m", "value": 0.25, "pass": "< 0.5", "verdict": "pass", "reason": None}]
    assert "secret_table_name" not in results.puts[0][1].decode()


def test_b20_error_and_reason_hold_only_kinds_not_values(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT m"))
    checker, store, _, _ = setup(tmp_path, {"SELECT m": MetricError("sql-failed")}, fail_upload=True)
    checker.handle(notification())
    assert store.runs[0].error == "upload-failed" and store.rows[0].reason == "sql-failed"


def test_day_is_the_korean_date_of_scheduled_for(tmp_path):
    checker, store, _, results = setup(tmp_path)
    checker.handle(notification(status="market-closed", scheduled="2026-11-17T15:30:00Z"))   # 한국 시각 11-18 00:30
    assert store.runs[0].day == date(2026, 11, 18) and results.puts[0][0] == result_key(date(2026, 11, 18))


def test_notification_of_another_day_leaves_nothing(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT m"))
    checker, store, query, results = setup(tmp_path)   # 지금은 한국 시각 11-18
    assert checker.handle(notification(scheduled="2026-11-17T16:30:00+09:00")) == STALE
    assert checker.handle(notification(status="market-closed", scheduled="2026-11-19T16:30:00+09:00")) == STALE
    assert not store.runs and not results.puts and not query.calls


@pytest.mark.parametrize("bad", [b"not json", b"{}", notification(event_id=""), notification(status="weird"),
                                 notification(scheduled="2026-11-18T16:30:00")])
def test_malformed_notifications_are_rejected_without_side_effects(tmp_path, bad):
    checker, store, _, results = setup(tmp_path)
    with pytest.raises(MalformedNotification):
        checker.handle(bad)
    assert not store.runs and not results.puts


def test_build_result_without_checks_is_valid_json():
    doc = json.loads(build_result(date(2026, 11, 18), "market-closed", "v", []))
    assert doc["checks"] == []


class CountingTrigger:
    def __init__(self) -> None:
        self.calls = 0

    def fire(self) -> bool:
        self.calls += 1
        return True


def test_verdict_is_requested_only_after_the_result_file_is_uploaded(tmp_path):
    criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT m"))
    for status, expected in (("collected", OK), ("market-closed", HOLIDAY)):
        trigger = CountingTrigger()
        checker = Checker(FakeStore(), FakeQuery({"SELECT m": [(0.1,)]}), FakeResults(), tmp_path, VERSION,
                          clock=lambda: NOW, trigger=trigger)
        assert checker.handle(notification(status=status)) == expected
        assert trigger.calls == 1
        assert checker.handle(notification(status=status)) == DUPLICATE   # 같은 알림을 다시 받아도 다시 부르지 않는다
        assert trigger.calls == 1

    trigger = CountingTrigger()
    checker = Checker(FakeStore(), FakeQuery({"SELECT m": [(0.1,)]}), FakeResults(fail=True), tmp_path, VERSION,
                      clock=lambda: NOW, trigger=trigger)
    assert checker.handle(notification()) == FAILED
    assert checker.handle(notification(edition="morning")) == IGNORED
    assert checker.handle(notification(scheduled="2026-11-17T16:30:00+09:00")) == STALE
    assert trigger.calls == 0
