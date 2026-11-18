"""ops_verdict.py(조치 판정)를 가짜 결과 파일·가짜 git 저장소·가짜 이슈 정보로 확인한다. GitHub·S3는 부르지 않는다."""
import hashlib
import json
import os
import subprocess
from datetime import date

import pytest

import ops_verdict as ov

MON, TUE, WED = "2026-11-16", "2026-11-17", "2026-11-18"
SQL_MARKER = "zz_secret_sql_marker"
GIT_ENV = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com", "PATH": os.environ["PATH"]}


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
                          env=GIT_ENV).stdout.strip()


class World:
    """기준 파일 폴더, 결과 폴더, 이슈 정보를 담은 임시 세계. 이슈 번호는 8xx 가짜 값만 쓴다."""

    def __init__(self, tmp_path):
        self.repo = tmp_path / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q")
        git(self.repo, "commit", "-q", "--allow-empty", "-m", "base")
        base = git(self.repo, "rev-parse", "HEAD")
        git(self.repo, "commit", "-q", "--allow-empty", "-m", "fix")
        self.fix = git(self.repo, "rev-parse", "HEAD")
        git(self.repo, "commit", "-q", "--allow-empty", "-m", "deploy")
        self.deploy = git(self.repo, "rev-parse", "HEAD")
        git(self.repo, "checkout", "-q", "-b", "other", base)
        git(self.repo, "commit", "-q", "--allow-empty", "-m", "unrelated")
        self.unrelated = git(self.repo, "rev-parse", "HEAD")
        self.criteria_dir = tmp_path / "criteria"
        self.results_dir = tmp_path / "results"
        self.criteria_dir.mkdir()
        self.results_dir.mkdir()
        self.issues = {}
        self.hashes = {}

    def criteria(self, issue, days=2, comment=True, attempt="t1"):
        text = (f"issue: {issue}\nattempt: \"{attempt}\"\ncomment: {'true' if comment else 'false'}\n"
                f"fix_commit: \"{self.fix}\"\ndays: {days}\nmetrics:\n  - key: m1\n"
                f"    sql: SELECT 0.1 AS {SQL_MARKER}\n    pass: \"< 0.5\"\nhuman:\n  - \"메일 확인\"\n").encode()
        (self.criteria_dir / f"{issue}-{attempt}.yml").write_bytes(text)
        self.hashes[(issue, attempt)] = hashlib.sha256(text).hexdigest()
        self.issues.setdefault(str(issue), {"open": True, "labels": ["verifying"], "comments": []})

    def result(self, day, issue=None, verdict="pass", value=0.1, deploy=None, hash_=None, reason=None,
               status="collected", attempt="t1"):
        checks = []
        if issue is not None and status == "collected":
            checks = [{"issue": issue, "attempt": attempt, "criteria_hash": hash_ or self.hashes[(issue, attempt)],
                       "comment": True, "days": 2,
                       "metrics": [{"key": "m1", "value": value, "pass": "< 0.5", "verdict": verdict, "reason": reason}],
                       "human": ["메일 확인"]}]
        (self.results_dir / day).mkdir(exist_ok=True)
        (self.results_dir / day / "post-close.json").write_text(json.dumps(
            {"day": day, "edition": "post-close", "status": status, "deploy_version": deploy or self.deploy,
             "checks": checks}))

    def run(self, today, alert_since=None):
        return ov.run(self.results_dir, self.criteria_dir, self.issues, self.repo, date.fromisoformat(today), alert_since)


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def test_two_passing_days_post_day_comments_final_comment_and_swap_label(w):
    w.criteria(801)
    w.result(MON, 801)
    w.result(TUE, 801, value=0.2)
    r = w.run(TUE)
    t = r.todo[801]
    assert r.failures == []
    assert len(t.comments) == 3
    assert "1/2일째" in t.comments[0] and "2/2일째" in t.comments[1]
    assert "10.00%" in t.comments[0] and "< 0.5" in t.comments[0] and "사람 확인 필요 1개" in t.comments[0]
    assert t.remove_labels == ["verifying"] and t.add_labels == ["check-passed"]


def test_any_failing_day_makes_check_failed(w):
    w.criteria(801)
    w.result(MON, 801, verdict="fail", value=0.9)
    w.result(TUE, 801)
    assert w.run(TUE).todo[801].add_labels == ["check-failed"]


def test_days_already_in_comments_are_not_counted_or_commented_again(w):
    w.criteria(801)
    w.result(MON, 801)
    first = w.run(MON).todo[801]
    assert len(first.comments) == 1 and first.add_labels == []
    w.issues["801"]["comments"] = list(first.comments)
    assert w.run(MON).todo[801].comments == []
    w.result(TUE, 801)
    second = w.run(TUE).todo[801]
    assert "2/2일째" in second.comments[0] and not any("3/2" in c for c in second.comments)
    assert second.add_labels == ["check-passed"]


def test_final_verdict_uses_comment_marks_even_if_result_file_is_gone(w):
    w.criteria(801)
    w.result(MON, 801, verdict="fail", value=0.9)
    w.issues["801"]["comments"] = list(w.run(MON).todo[801].comments)
    (w.results_dir / MON / "post-close.json").unlink()
    (w.results_dir / MON).rmdir()
    w.result(TUE, 801)
    assert w.run(TUE).todo[801].add_labels == ["check-failed"]


def test_label_is_retried_without_second_final_comment(w):
    w.criteria(801, days=1)
    w.result(MON, 801)
    w.issues["801"]["comments"] = list(w.run(MON).todo[801].comments)
    t = w.run(MON).todo[801]
    assert t.comments == [] and t.add_labels == ["check-passed"] and t.remove_labels == ["verifying"]


def test_changed_criteria_hash_is_not_counted(w):
    w.criteria(801, days=1)
    w.result(MON, 801, hash_="0" * 64)
    r = w.run(MON)
    assert r.failures == [] and not r.todo[801].add_labels and any("지문" in s for s in r.summary)


def test_fix_commit_not_in_deployed_version_is_not_counted(w):
    w.criteria(801, days=1)
    w.result(MON, 801, deploy=w.unrelated)
    r = w.run(MON)
    assert not r.todo[801].add_labels and any("배포 전" in s for s in r.summary)


def test_fix_commit_equal_to_deployed_version_is_counted(w):
    w.criteria(801, days=1)
    w.result(MON, 801, deploy=w.fix)
    assert w.run(MON).todo[801].add_labels == ["check-passed"]


def test_unresolvable_deploy_version_fails_the_job(w):
    w.criteria(801, days=1)
    w.result(MON, 801, deploy="deadbeef")
    r = w.run(MON)
    assert any("배포 버전 확인 불가" in f for f in r.failures) and not r.todo[801].add_labels


def test_hold_is_not_counted_and_not_a_failure(w):
    w.criteria(801, days=1)
    w.result(MON, 801, verdict="hold", value=None, reason="empty")
    r = w.run(MON)
    assert r.failures == [] and not r.todo[801].add_labels


def test_error_metric_is_not_counted_and_fails_the_job_with_kind_only(w):
    w.criteria(801, days=1)
    w.result(MON, 801, verdict="error", value=None, reason="timeout")
    r = w.run(MON)
    assert r.failures == [f"{MON} 이슈 #801 시도 t1 error 지표(timeout)"] and not r.todo[801].add_labels


def test_missing_weekday_result_fails_with_fixed_message(w):
    w.criteria(801, days=5)
    w.result(MON, 801)
    w.result(WED, 801)
    assert w.run(WED).failures == [f"{ov.MISSING_RESULT} ({TUE})"]


def test_missing_today_result_fails(w):
    w.criteria(801, days=5)
    w.result(MON, 801)
    assert w.run(TUE).failures == [f"{ov.MISSING_RESULT} ({TUE})"]


def test_empty_results_folder_fails(w):
    w.criteria(801)
    assert w.run(MON).failures and ov.MISSING_RESULT in w.run(MON).failures[0]


def test_weekend_files_are_not_expected(w):
    w.criteria(801, days=5)
    w.result("2026-11-20", 801)
    w.result("2026-11-23", 801)
    assert w.run("2026-11-23").failures == []


def test_market_closed_weekday_is_ok_and_not_counted(w):
    w.criteria(801)
    w.result(MON, 801)
    w.result(TUE, status="market-closed")
    r = w.run(TUE)
    assert r.failures == [] and len(r.todo[801].comments) == 1 and not r.todo[801].add_labels


@pytest.mark.parametrize("raw", ["{not json", "[]", json.dumps({"day": MON, "status": "x", "checks": []})])
def test_unreadable_or_malformed_result_file_fails_without_leaking_content(w, raw):
    w.criteria(801)
    (w.results_dir / MON).mkdir()
    (w.results_dir / MON / "post-close.json").write_text(raw)
    r = w.run(MON)
    assert len(r.failures) == 1 and r.failures[0].startswith(MON) and raw not in r.failures[0]


@pytest.mark.parametrize("setup", ["comment-false", "not-verifying", "closed"])
def test_no_comment_or_label_when_comment_false_or_issue_not_open_verifying(w, setup):
    w.criteria(801, days=1, comment=setup != "comment-false")
    if setup == "not-verifying":
        w.issues["801"]["labels"] = []
    if setup == "closed":
        w.issues["801"]["open"] = False
    w.result(MON, 801)
    r = w.run(MON)
    assert r.failures == []
    assert not (r.todo[801].comments or r.todo[801].add_labels or r.todo[801].remove_labels)
    assert r.summary


def test_missing_comment_field_defaults_to_commenting(w):
    w.criteria(801, days=1)
    path = w.criteria_dir / "801-t1.yml"
    path.write_text(path.read_text().replace("comment: true\n", ""))
    w.hashes[(801, "t1")] = hashlib.sha256(path.read_bytes()).hexdigest()
    w.result(MON, 801)
    assert len(w.run(MON).todo[801].comments) == 2


def test_sql_body_never_appears_in_comments_or_summary(w):
    w.criteria(801, days=1)
    w.result(MON, 801)
    r = w.run(MON)
    assert SQL_MARKER not in json.dumps([r.todo[801].comments, r.summary, r.failures])


def test_malformed_criteria_file_fails_but_others_continue(w):
    w.criteria(801, days=1)
    (w.criteria_dir / "802-t1.yml").write_text("issue: abc\n")
    w.result(MON, 801)
    r = w.run(MON)
    assert r.failures == ["기준 파일 형식 오류"] and r.todo[801].add_labels == ["check-passed"]


def test_cli_writes_todo_file_and_exit_code(w, tmp_path):
    w.criteria(801, days=1)
    w.result(MON, 801)
    issues, out = tmp_path / "issues.json", tmp_path / "out.json"
    issues.write_text(json.dumps(w.issues))
    args = ["--results-dir", str(w.results_dir), "--criteria-dir", str(w.criteria_dir), "--issues", str(issues),
            "--repo", str(w.repo), "--today", MON, "--out", str(out)]
    assert ov.main(args) == 0
    assert json.loads(out.read_text())["issues"]["801"]["add_labels"] == ["check-passed"]
    issues.write_text("{broken")
    assert ov.main(args) == 1


def test_unsafe_reason_key_and_pass_from_result_file_are_not_published(w):
    w.criteria(801, days=1)
    w.result(MON, 801, verdict="error", value=None, reason="secret=abc123 한글")
    r = w.run(MON)
    assert r.failures == [f"{MON} 이슈 #801 시도 t1 error 지표(unknown)"]
    w.result(MON, 801)
    path = w.results_dir / MON / "post-close.json"
    doc = json.loads(path.read_text())
    doc["checks"][0]["metrics"][0].update({"key": "evil|key\n[x](http://e)", "pass": "< 0.5 | ![i](http://e)"})
    path.write_text(json.dumps(doc))
    comment = w.run(MON).todo[801].comments[0]
    assert "evil" not in comment and "http" not in comment and "| - | 10.00% | - | 통과 |" in comment


def test_trailing_newline_in_reason_key_and_pass_is_not_accepted(w):
    w.criteria(801, days=1)
    w.result(MON, 801, verdict="error", value=None, reason="ab\n")
    assert w.run(MON).failures == [f"{MON} 이슈 #801 시도 t1 error 지표(unknown)"]
    w.result(MON, 801)
    path = w.results_dir / MON / "post-close.json"
    doc = json.loads(path.read_text())
    doc["checks"][0]["metrics"][0].update({"key": "ab\n", "pass": "< 0.5\n"})
    path.write_text(json.dumps(doc))
    assert "| - | 10.00% | - | 통과 |" in w.run(MON).todo[801].comments[0]


def test_well_formed_reason_key_and_pass_are_kept(w):
    w.criteria(801, days=5)
    w.result(MON, 801)
    assert "| m1 | 10.00% | < 0.5 | 통과 |" in w.run(MON).todo[801].comments[0]
    w.result(TUE, 801, verdict="error", value=None, reason="timeout")
    assert w.run(TUE).failures[0].endswith("error 지표(timeout)")


def test_alert_since_moves_old_error_and_missing_day_to_summary_only(w):
    w.criteria(801, days=5)
    w.result(MON, 801, verdict="error", value=None, reason="timeout")
    w.result(WED, 801)
    r = w.run(WED, alert_since=date.fromisoformat(TUE))
    assert r.failures == []
    assert any("error 지표(timeout)" in s for s in r.summary) and any(f"({TUE})" in s for s in r.summary)


def test_alert_since_still_fails_for_days_after_it(w):
    w.criteria(801, days=5)
    w.result(MON, 801, verdict="error", value=None, reason="timeout")
    w.result(WED, 801)
    r = w.run(WED, alert_since=date.fromisoformat(MON))
    assert r.failures == [f"{ov.MISSING_RESULT} ({TUE})"]


def test_without_alert_since_behaviour_is_unchanged(w):
    w.criteria(801, days=5)
    w.result(MON, 801, verdict="error", value=None, reason="timeout")
    w.result(WED, 801)
    assert len(w.run(WED).failures) == 2


def test_manual_run_does_not_fail_for_missing_today_but_scheduled_does(w):
    w.criteria(801, days=5)
    w.result(MON, 801)
    r = ov.run(w.results_dir, w.criteria_dir, w.issues, w.repo, date.fromisoformat(TUE), require_today=False)
    assert r.failures == []
    assert w.run(TUE).failures == [f"{ov.MISSING_RESULT} ({TUE})"]


def test_manual_run_still_fails_for_missing_past_weekday(w):
    w.criteria(801, days=5)
    w.result(MON, 801)
    w.result(WED, 801)
    r = ov.run(w.results_dir, w.criteria_dir, w.issues, w.repo, date.fromisoformat(WED), require_today=False)
    assert r.failures == [f"{ov.MISSING_RESULT} ({TUE})"]


def test_cli_alert_since_and_manual_flags(w, tmp_path):
    w.criteria(801, days=5)
    w.result(MON, 801, verdict="error", value=None, reason="timeout")
    issues, out = tmp_path / "issues.json", tmp_path / "out.json"
    issues.write_text(json.dumps(w.issues))
    args = ["--results-dir", str(w.results_dir), "--criteria-dir", str(w.criteria_dir), "--issues", str(issues),
            "--repo", str(w.repo), "--today", TUE, "--out", str(out)]
    assert ov.main(args) == 1
    assert ov.main(args + ["--alert-since", MON, "--manual"]) == 0
    assert ov.main(args + ["--alert-since", "bad"]) == 1
