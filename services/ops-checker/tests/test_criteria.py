import hashlib
from pathlib import Path

import pytest

from fakes import criteria_file, metric_block
from ops_checker.criteria import fingerprint, load_all, load_file, parse_pass

REPO_CRITERIA = Path(__file__).resolve().parents[3] / "ops-checks"


def test_fingerprint_is_sha256_of_the_file_bytes(tmp_path):
    path = criteria_file(tmp_path, 900, "a", metrics=metric_block("m", "SELECT 0.1"))
    assert load_file(path).criteria_hash == hashlib.sha256(path.read_bytes()).hexdigest()
    assert fingerprint(b"") == hashlib.sha256(b"").hexdigest()


def test_valid_file_is_read_with_defaults(tmp_path):
    path = criteria_file(tmp_path, 901, "t1", metrics=metric_block("m", "SELECT 0.1"), human="human:\n  - 사람 확인")
    c = load_file(path)
    assert c.error is None and (c.issue, c.attempt, c.days, c.comment) == (901, "t1", 2, True)
    assert c.human == ("사람 확인",) and c.metrics[0].key == "m"


def test_comment_false_is_kept(tmp_path):
    path = criteria_file(tmp_path, 902, "a", metrics=metric_block("m", "SELECT 0.1"), comment="false")
    assert load_file(path).comment is False


@pytest.mark.parametrize("sql, expr, reason", [
    ("UPDATE market.etf SET x = 1", "< 0.5", "bad-sql"),
    ("SELECT 1 FROM personal.member", "< 0.5", "bad-sql"),
    ("SELECT 1; COMMIT; DELETE FROM market.etf", "< 0.5", "bad-sql"),
    ("SELECT 0.1", "<> 0.5", "bad-pass-expression"),
    ("SELECT 0.1", "< 1.5", "bad-pass-expression"),
])
def test_bad_metric_marks_only_that_file_as_error(tmp_path, sql, expr, reason):
    path = criteria_file(tmp_path, 903, "a", metrics=metric_block("m", sql, expr))
    c = load_file(path)
    assert c.error == reason and c.criteria_hash == fingerprint(path.read_bytes())


def test_trailing_semicolon_is_allowed(tmp_path):
    assert load_file(criteria_file(tmp_path, 904, "a", metrics=metric_block("m", "SELECT 0.1;"))).error is None


def test_issue_and_attempt_must_match_the_file_name(tmp_path):
    path = criteria_file(tmp_path, 905, "a", metrics=metric_block("m", "SELECT 0.1"))
    path.rename(tmp_path / "906-a.yml")
    assert load_file(tmp_path / "906-a.yml").error == "issue-mismatch"


def test_duplicate_keys_invalid_yaml_and_missing_days_are_errors(tmp_path):
    dup = criteria_file(tmp_path, 907, "a", metrics=metric_block("m", "SELECT 0.1") + "\n" + metric_block("m", "SELECT 0.2"))
    assert load_file(dup).error == "bad-metric-key"
    (tmp_path / "908-a.yml").write_text("issue: [", encoding="utf-8")
    assert load_file(tmp_path / "908-a.yml").error == "invalid-yaml"
    nodays = criteria_file(tmp_path, 909, "a", metrics=metric_block("m", "SELECT 0.1"), days=0)
    assert load_file(nodays).error == "bad-days"


def test_files_that_are_not_criteria_names_are_skipped(tmp_path):
    criteria_file(tmp_path, 910, "a", metrics=metric_block("m", "SELECT 0.1"))
    (tmp_path / "README.md").write_text("x")
    (tmp_path / "notes.yml").write_text("x")
    assert [c.issue for c in load_all(tmp_path)] == [910]
    assert load_all(tmp_path / "missing") == []


def test_pass_expression_needs_a_zero_to_one_number():
    assert parse_pass(">= 0.05")[0] == ">="
    with pytest.raises(ValueError):
        parse_pass("< abc")


def test_criteria_files_in_the_repository_are_valid():
    loaded = load_all(REPO_CRITERIA)
    assert loaded, "ops-checks/ 에 기준 파일이 있어야 한다"
    assert [(c.issue, c.attempt, c.error) for c in loaded if c.error] == []
    t1 = next(c for c in loaded if (c.issue, c.attempt) == (74, "t1"))
    assert t1.comment is False


def test_numeric_attempt_in_yaml_matches_the_file_name(tmp_path):
    (tmp_path / "74-1.yml").write_text(
        "issue: 74\nattempt: 1\nfix_commit: \"abc1234\"\ndays: 2\nmetrics:\n"
        "  - key: m\n    sql: \"SELECT 0.1\"\n    pass: \"< 0.5\"\n", encoding="utf-8")
    c = load_file(tmp_path / "74-1.yml")
    assert c.error is None and c.attempt == "1"
