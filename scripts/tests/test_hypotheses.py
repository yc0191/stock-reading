import json
import subprocess
from datetime import datetime, timezone

import pytest

import hypotheses as hy

RULE = {"kind": "proportion", "target": 0.95, "confidence": 0.95, "min_coverage": 0.6, "min_total": 100,
        "measures": ["J1"]}

ACTIVE_PROPORTION = """
id = "H001"
title = "제목과 요약의 충분성"
claim = "제목과 요약만으로 정확도 95% 이상"
state = "active"

[rule]
kind = "proportion"
target = 0.95
confidence = 0.95
min_coverage = 0.6
min_total = 100
measures = ["J1"]

[verify]
command = "python3 evaluate.py"
needs = "local-data"
"""

ACTIVE_TEST = """
id = "H002"
title = "URL 변형이 같은 키가 된다"
claim = "URL 변형 4종이 같은 articleId 를 만든다"
state = "active"

[rule]
kind = "test"

[verify]
command = "true"
needs = "none"
"""

DRAFT = """
id = "H003"
title = "초안 가설"
claim = "아직 기준이 없다"
state = "draft"
"""

README_TEXT = "# x\n\n<!-- hypotheses:start -->\nold\n<!-- hypotheses:end -->\n\n끝\n"
NOW = datetime(2026, 10, 30, 10, 0, 0, tzinfo=timezone.utc)


def make_repo(tmp_path, hypotheses):
    hyp = tmp_path / "docs" / "hypotheses"
    hyp.mkdir(parents=True)
    (tmp_path / "README.md").write_text(README_TEXT, encoding="utf-8")
    for folder, text in hypotheses.items():
        (hyp / folder).mkdir()
        (hyp / folder / "hypothesis.toml").write_text(text, encoding="utf-8")
    return tmp_path


def items(repo):
    return hy.discover(repo / "docs" / "hypotheses")


def write_result(item, **fields):
    results = item["folder"] / "results"
    results.mkdir(exist_ok=True)
    data = {"run_at": "2026-10-30T10:00:00+00:00", "commit": "abc1234", "verdict": "supported",
            "rule_sha256": hy.rule_hash(item["def"]["rule"]), **fields}
    (results / f"{data['run_at']}.json").write_text(json.dumps(data), encoding="utf-8")


def fake_run(stdout="", returncode=0):
    def run(cmd, **kwargs):
        if cmd[0] == "git":
            return subprocess.CompletedProcess(cmd, 0, "abc1234\n", "")
        return subprocess.CompletedProcess(cmd, returncode, stdout, "")
    return run


MEASURE = {"name": "J1", "n_total": 300, "n_answered": 200, "n_correct": 196, "coverage": 0.667,
           "accuracy": 0.98, "lower_bound": 0.955, "verdict": "supported"}


# ---------------------------------------------------------------- 판정 계산

@pytest.mark.parametrize("n,max_errors", [(59, 0), (100, 1), (150, 2), (200, 4), (300, 8), (400, 12)])
def test_lower_bound_reproduces_the_sample_size_table_in_the_hypothesis_docs(n, max_errors):
    assert hy.lower_bound(n - max_errors, n, 0.05) >= 0.95
    assert hy.lower_bound(n - max_errors - 1, n, 0.05) < 0.95


def test_supported_when_lower_bound_and_coverage_pass():
    m = hy.evaluate_measure(RULE, n_answered=200, n_correct=196, n_total=300)
    assert m["verdict"] == "supported" and m["lower_bound"] >= 0.95


def test_inconclusive_when_observed_meets_target_but_lower_bound_does_not():
    m = hy.evaluate_measure(RULE, n_answered=100, n_correct=97, n_total=120)
    assert m["accuracy"] >= 0.95 and m["lower_bound"] < 0.95
    assert m["verdict"] == "inconclusive"


def test_rejected_when_accuracy_is_below_target():
    assert hy.evaluate_measure(RULE, 200, 180, 300)["verdict"] == "rejected"


def test_rejected_when_coverage_is_below_minimum_even_if_accurate():
    assert hy.evaluate_measure(RULE, 100, 100, 400)["verdict"] == "rejected"


def test_combine_requires_every_measure_to_be_supported():
    assert hy.combine(["supported", "supported"]) == "supported"
    assert hy.combine(["supported", "inconclusive"]) == "inconclusive"
    assert hy.combine(["supported", "rejected"]) == "rejected"


# ---------------------------------------------------------------- 노출 상태

def test_hypothesis_with_incomplete_criteria_is_hidden(tmp_path):
    missing_rule = 'id = "H004"\ntitle = "t"\nclaim = "c"\nstate = "active"\n'
    no_min_total = ACTIVE_PROPORTION.replace("min_total = 100\n", "")
    repo = make_repo(tmp_path, {"H003-d": DRAFT, "H004-m": missing_rule, "H001-n": no_min_total})
    assert {i["def"]["id"]: hy.status_of(i)["label"] for i in items(repo)} == \
        {"H003": "hidden", "H004": "hidden", "H001": "hidden"}


def test_complete_hypothesis_without_result_is_in_progress(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION})
    assert hy.status_of(items(repo)[0])["label"] == "in_progress"


def test_result_decides_the_label(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION})
    write_result(items(repo)[0], verdict="rejected")
    assert hy.status_of(items(repo)[0])["label"] == "rejected"


def test_changing_the_rule_after_a_result_invalidates_it(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION})
    write_result(items(repo)[0], verdict="supported")
    changed = ACTIVE_PROPORTION.replace("target = 0.95", "target = 0.90")
    (repo / "docs/hypotheses/H001-x/hypothesis.toml").write_text(changed, encoding="utf-8")
    assert hy.status_of(items(repo)[0])["label"] == "in_progress"


# ---------------------------------------------------------------- README 표

def test_table_shows_evidence_for_decided_and_nothing_for_in_progress_and_drafts(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION, "H002-y": ACTIVE_TEST, "H003-d": DRAFT})
    write_result(items(repo)[0], measures=[MEASURE])
    table = hy.render_table(items(repo))
    h1 = next(line for line in table.splitlines() if "H001" in line)
    h2 = next(line for line in table.splitlines() if "H002" in line)
    assert "증명됨" in h1 and "95.5%" in h1 and "커밋 `abc1234`" in h1 and "재현: 로컬 데이터 필요" in h1
    title_cell, status_cell, evidence_cell = [c.strip() for c in h2.strip().strip("|").split("|")]
    assert status_cell == "증명 진행 중" and evidence_cell == ""
    assert "H003" not in table and "아직 기준이 없다" not in table


def test_rejected_and_inconclusive_are_shown_with_their_real_status(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION, "H002-y": ACTIVE_TEST})
    a, b = items(repo)
    write_result(a, verdict="inconclusive", measures=[{**MEASURE, "verdict": "inconclusive"}])
    write_result(b, verdict="rejected", exit_code=1)
    table = hy.render_table(items(repo))
    assert "판정 보류" in table and "기각됨" in table and "테스트 실패" in table


def test_render_replaces_only_the_marked_section_and_is_idempotent(tmp_path):
    repo = make_repo(tmp_path, {"H002-y": ACTIVE_TEST})
    hyp, readme = repo / "docs/hypotheses", repo / "README.md"
    assert hy.render_readme(hyp, readme, check=False) is True
    text = readme.read_text(encoding="utf-8")
    assert text.startswith("# x\n\n<!-- hypotheses:start -->") and text.endswith("<!-- hypotheses:end -->\n\n끝\n")
    assert "old" not in text and "H002" in text
    assert hy.render_readme(hyp, readme, check=False) is False


def test_check_detects_a_hand_edited_table_without_writing(tmp_path):
    repo = make_repo(tmp_path, {"H002-y": ACTIVE_TEST})
    hyp, readme = repo / "docs/hypotheses", repo / "README.md"
    hy.render_readme(hyp, readme, check=False)
    edited = readme.read_text(encoding="utf-8").replace("증명 진행 중", "증명됨")
    readme.write_text(edited, encoding="utf-8")
    assert hy.render_readme(hyp, readme, check=True) is True
    assert readme.read_text(encoding="utf-8") == edited


def test_cli_render_check_fails_on_drift_and_passes_after_render(tmp_path, capsys):
    repo = make_repo(tmp_path, {"H002-y": ACTIVE_TEST})
    assert hy.main(["render", "--check"], root=repo) == 1
    assert hy.main(["render"], root=repo) == 0
    assert hy.main(["render", "--check"], root=repo) == 0


# ---------------------------------------------------------------- 검증 실행

def test_verify_test_kind_takes_the_verdict_from_the_exit_code(tmp_path):
    repo = make_repo(tmp_path, {"H002-y": ACTIVE_TEST})
    hy.verify(items(repo)[0], repo, run=fake_run(returncode=0), now=NOW)
    assert hy.status_of(items(repo)[0])["label"] == "supported"
    hy.verify(items(repo)[0], repo, run=fake_run(returncode=1), now=datetime(2026, 11, 1, tzinfo=timezone.utc))
    assert hy.status_of(items(repo)[0])["label"] == "rejected"


def test_verify_computes_the_verdict_itself_and_ignores_what_the_command_claims(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION})
    out = json.dumps({"verdict": "supported",
                      "measures": {"J1": {"n_total": 300, "n_answered": 200, "n_correct": 180},
                                   "title": "기사 제목은 저장되면 안 된다"}})
    path = hy.verify(items(repo)[0], repo, run=fake_run(stdout=out), now=NOW)
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    assert data["verdict"] == "rejected" and data["measures"][0]["accuracy"] == pytest.approx(0.9)
    assert data["rule_sha256"] == hy.rule_hash(items(repo)[0]["def"]["rule"])
    assert "기사 제목" not in text


def test_failed_command_or_missing_measure_writes_nothing(tmp_path):
    repo = make_repo(tmp_path, {"H001-x": ACTIVE_PROPORTION})
    with pytest.raises(RuntimeError):
        hy.verify(items(repo)[0], repo, run=fake_run(returncode=2), now=NOW)
    with pytest.raises(RuntimeError):
        hy.verify(items(repo)[0], repo, run=fake_run(stdout=json.dumps({"measures": {}})), now=NOW)
    assert not (repo / "docs/hypotheses/H001-x/results").exists()
