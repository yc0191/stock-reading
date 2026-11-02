"""전체 실행 스크립트(scripts/test-all.sh)의 옵션 처리와 결과 표 생성(run_report.py)을 가짜 실행 결과로 확인한다.

실제로 Gradle·pytest·Docker 를 띄우지 않는다. 옵션 처리는 아무것도 띄우지 않는 --plan, --help, 틀린 옵션만 실행해 본다.
"""
import io
import json
import subprocess
from pathlib import Path

import check_test_map as ctm
import run_report as rr

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "test-all.sh"
HEADER = "| 기능 | 대상 | 단위 | 가상 | 실제 |\n|---|---|---|---|---|\n"
NA = "해당 없음 - 외부 의존 없음"


def run_script(*args):
    return subprocess.run(["bash", str(SCRIPT), *args], capture_output=True, text=True, timeout=30)


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def map_text(rows) -> str:
    body = "".join(f"| {' | '.join(cells)} |\n" for cells in rows)
    return f"# 지도\n\n{HEADER}{body}\n테스트 도우미: 없음\n"


def make_maps(root: Path, market_rows=None, scripts_rows=None) -> None:
    """지도 6개. 시험마다 market·scripts 행만 바꾸고 나머지는 결과가 필요 없는 행으로 둔다."""
    quiet = [("조용한 기능", "Quiet", "없음 - 아직 없음", NA, NA)]
    for map_file in ctm.MAP_FILES:
        write(root / "docs/test-map" / map_file, map_text(quiet))
    if market_rows is not None:
        write(root / "docs/test-map/market.md", map_text(market_rows))
    if scripts_rows is not None:
        write(root / "docs/test-map/scripts.md", map_text(scripts_rows))


def java_xml(results: Path, phase: str, service: str, cls: str, statuses) -> None:
    cases = []
    for i, status in enumerate(statuses):
        child = {"passed": "", "skipped": "<skipped/>", "failed": "<failure message='x'/>"}[status]
        cases.append(f"<testcase name='t{i}()' classname='dev.x.{cls}'>{child}</testcase>")
    write(results / phase / "java" / service / f"TEST-dev.x.{cls}.xml",
          f"<?xml version='1.0'?><testsuite name='dev.x.{cls}'>{''.join(cases)}</testsuite>")


def py_xml(results: Path, phase: str, group: str, cases) -> None:
    body = []
    for classname, status in cases:
        child = {"passed": "", "skipped": "<skipped message='m'/>", "failed": "<failure message='x'/>"}[status]
        body.append(f"<testcase classname='{classname}' name='test_a'>{child}</testcase>")
    write(results / phase / "py" / f"{group}.xml",
          f"<?xml version='1.0'?><testsuites><testsuite name='pytest'>{''.join(body)}</testsuite></testsuites>")


def stages(results: Path, *lines) -> None:
    write(results / "stages.tsv", "".join("\t".join(line) + "\n" for line in lines))


OK_STAGES = (("java", "Java 전체", "ok", "", "250", "main/java"),
             ("live", "시세 흐름 실제", "skip", rr.LIVE_SKIP_REASON, "0", ""))

MARKET_ROWS = [
    ("시세 저장", "PriceStore", "StoreTest", "StoreFlowTest, LoadTest [부하, 켜는 조건: QUOTE_LOAD=1]",
     "StoreDbTest [DB, 켜는 조건: MARKET_IT_DB_URL·MARKET_IT_DB_USER·MARKET_IT_DB_PASSWORD]"),
    ("시세 조회", "PriceQuery", "QueryTest", NA, "없음 - 나중에 채움 [외부 API]"),
    ("시세 묶음", "PriceBatch", "QueryTest; 해당 없음 - 섞인 칸", NA, NA),
    ("발행 모양", "Envelope", "ContractTest", NA, NA),
]
SCRIPTS_ROWS = [
    ("전체 실행과 결과 표", "test-all.sh, run_report.py", "test_run_report.py", "test_run_report.py",
     "test_quote_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live]"),
]


def passing_results(results: Path) -> None:
    for cls in ("StoreTest", "StoreFlowTest", "QueryTest", "ContractTest"):
        java_xml(results, "main", "market", cls, ["passed", "passed"])
    java_xml(results, "main", "market", "LoadTest", ["skipped"])
    java_xml(results, "main", "market", "StoreDbTest", ["skipped", "skipped"])
    java_xml(results, "db", "market", "StoreDbTest", ["passed", "passed"])
    py_xml(results, "main", "scripts", [("tests.test_run_report", "passed"),
                                       ("tests.test_quote_flow_live", "skipped")])
    stages(results, *OK_STAGES)


def render(root: Path, results: Path, live=False):
    out = io.StringIO()
    code = rr.report(root, results, live, out=out)
    return code, out.getvalue()


def row_of(output: str, feature: str) -> str:
    return next(line for line in output.splitlines() if line.startswith(f"| {feature} |"))


# ---------------------------------------------------------------- 옵션 처리

def test_plan_without_live_lists_steps_and_skips_the_live_flow():
    result = run_script("--plan")
    assert result.returncode == 0
    assert "Java 전체" in result.stdout and "실제 DB 테스트" in result.stdout
    assert f"건너뜀({rr.LIVE_SKIP_REASON})" in result.stdout
    assert "일회용 Kafka" not in result.stdout


def test_plan_with_live_includes_the_live_flow():
    result = run_script("--live", "--plan")
    assert result.returncode == 0
    assert "일회용 Kafka" in result.stdout
    assert rr.LIVE_SKIP_REASON not in result.stdout


def test_unknown_option_is_refused_before_doing_anything():
    result = run_script("--liv")
    assert result.returncode == 64
    assert "알 수 없는 옵션" in result.stderr


def test_help_shows_the_options():
    result = run_script("--help")
    assert result.returncode == 0
    assert "--live" in result.stdout and "--plan" in result.stdout


def test_script_reads_the_map_for_the_table_and_cleans_up_on_exit():
    text = SCRIPT.read_text(encoding="utf-8")
    assert "run_report.py report" in text and "run_report.py db-tests" in text
    assert "trap on_exit EXIT" in text


# ---------------------------------------------------------------- 칸 해석

def test_parse_item_reads_label_and_condition():
    (item,) = rr.parse_item("SmokeTest [외부 API, 켜는 조건: SMOKE=1, GUARD=1]")
    assert item.tests == ("SmokeTest",)
    assert item.label == "외부 API"
    assert item.condition == "SMOKE=1, GUARD=1"
    assert item.phase == "main"


def test_parse_item_knows_db_and_live_phases():
    assert rr.parse_item("StoreDbTest [DB, 켜는 조건: MARKET_IT_DB_URL·MARKET_IT_DB_USER]")[0].phase == "db"
    assert rr.parse_item("test_quote_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live]")[0].phase == "live"
    assert rr.parse_item("없음 - 나중에 [DB]")[0].kind == "none"
    assert rr.parse_item("해당 없음 - 필요 없음")[0].kind == "na"


def test_bracket_belongs_only_to_the_name_right_before_it():
    always, load = rr.parse_item("FlowTest, LoadTest [부하, 켜는 조건: QUOTE_LOAD=1]")
    assert (always.tests, always.condition) == (("FlowTest",), "")
    assert (load.tests, load.label, load.condition) == (("LoadTest",), "부하", "QUOTE_LOAD=1")


def test_db_tests_are_chosen_from_the_map(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    write(tmp_path / "docs/test-map/briefing.md", map_text([
        ("실패 기록", "failure_store.py", NA, "test_failure_store.py",
         "test_failure_store_db.py [DB, 켜는 조건: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD]")]))
    assert rr.db_tests(tmp_path) == {"market": ["StoreDbTest"], "briefing": ["test_failure_store_db.py"]}


# ---------------------------------------------------------------- 결과 표

def test_default_run_table_shows_ran_skipped_and_live_reason(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)

    code, output = render(tmp_path, results)

    assert code == 0
    assert row_of(output, "시세 저장") == ("| 시세 저장 | 돌았음 | 돌았음 + 건너뜀(부하, 켜는 조건 없음: QUOTE_LOAD=1) "
                                       "| 돌았음(DB) |")
    assert row_of(output, "시세 조회") == "| 시세 조회 | 돌았음 | - | 없음 |"
    assert row_of(output, "시세 묶음") == "| 시세 묶음 | 돌았음 | - | - |"
    assert row_of(output, "전체 실행과 결과 표") == (
        f"| 전체 실행과 결과 표 | 돌았음 | 돌았음 | 건너뜀(서비스 흐름, {rr.LIVE_SKIP_REASON}) |")
    assert "| Java 전체 | 통과 (실행 8, 실패 0, 건너뜀 3) | 4분 10초 |" in output
    assert f"| 시세 흐름 실제 | 건너뜀 ({rr.LIVE_SKIP_REASON}) | 0초 |" in output
    assert "실제 호출: 0회" in output
    for feature in ("시세 저장", "시세 조회", "발행 모양", "조용한 기능"):
        assert f"| {feature} |" in output


def test_failed_test_makes_the_cell_and_exit_code_fail(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)
    java_xml(results, "main", "market", "QueryTest", ["passed", "failed"])

    code, output = render(tmp_path, results)

    assert code == 1
    assert row_of(output, "시세 조회").startswith("| 시세 조회 | 실패 |")
    assert "결과: 실패" in output


def test_missing_result_of_an_always_on_test_is_a_failure(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)
    (results / "main/java/market/TEST-dev.x.ContractTest.xml").unlink()

    code, output = render(tmp_path, results)

    assert code == 1
    assert "| 발행 모양 | 실패(결과 없음: ContractTest) |" in output


def test_db_test_skipped_in_the_db_phase_is_a_failure(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)
    java_xml(results, "db", "market", "StoreDbTest", ["skipped", "skipped"])

    code, output = render(tmp_path, results)

    assert code == 1
    assert row_of(output, "시세 저장").endswith("| 실패(DB, 켰는데 건너뜀) |")


def test_db_test_without_db_phase_result_is_a_failure_even_if_main_phase_skipped_it(tmp_path):
    # DB 준비가 실패하면 db 단계 결과가 없다. 기본 단계의 건너뜀을 건너뜀으로 보여 주면 실패가 숨는다.
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)
    (results / "db/java/market/TEST-dev.x.StoreDbTest.xml").unlink()
    stages(results, *OK_STAGES, ("dbprep", "실제 DB 준비", "fail", "공유 .env 에 MARKET_DB_USER 가 없음", "1", ""))

    code, output = render(tmp_path, results)

    assert code == 1
    assert row_of(output, "시세 저장").endswith("| 실패(DB, 결과 없음: StoreDbTest) |")
    assert "| 실제 DB 준비 | 실패 (공유 .env 에 MARKET_DB_USER 가 없음) | 1초 |" in output


def test_failed_stage_alone_makes_the_exit_code_fail(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)
    stages(results, *OK_STAGES, ("cleanup", "정리", "fail", "남은 것이 있음", "2", ""))

    code, output = render(tmp_path, results)

    assert code == 1
    assert "| 정리 | 실패 (남은 것이 있음) | 2초 |" in output


def test_live_run_reads_the_live_phase_and_counts_real_calls(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)
    py_xml(results, "live", "scripts", [("tests.test_quote_flow_live", "passed")])
    write(results / "live/counts.json", json.dumps({"snapshots": 1, "with_raw": 1, "failures": {}}))

    code, output = render(tmp_path, results, live=True)

    assert code == 0
    assert row_of(output, "전체 실행과 결과 표").endswith("| 돌았음(서비스 흐름) |")
    assert "실제 호출: 토큰 1, 시세 1" in output


def test_live_run_without_live_result_is_a_failure(tmp_path):
    make_maps(tmp_path, market_rows=MARKET_ROWS, scripts_rows=SCRIPTS_ROWS)
    results = tmp_path / "results"
    passing_results(results)

    code, output = render(tmp_path, results, live=True)

    assert code == 1
    assert row_of(output, "전체 실행과 결과 표").endswith("| 실패(서비스 흐름, 결과 없음: test_quote_flow_live.py) |")
    assert "실제 호출: 알 수 없음" in output


def test_live_calls_do_not_count_skipped_ticks_and_count_token_failure_once():
    assert rr.live_calls({"snapshots": 0, "failures": {"TICK_SKIPPED": 1}}) == "토큰 0, 시세 0"
    assert rr.live_calls({"snapshots": 0, "failures": {"TOKEN_ISSUE_FAILED": 1}}) == "토큰 1, 시세 0"
    assert rr.live_calls({"snapshots": 1, "failures": {"HTTP_ERROR": 1}}) == "토큰 1, 시세 2"


def test_real_repository_map_is_readable_and_has_the_live_row():
    rows = rr.map_rows(REPO_ROOT, "scripts.md")
    live = [r for r in rows if "test-all.sh" in r.cells["대상"]]
    assert len(live) == 1
    assert rr.parse_cell(live[0].cells["실제"])[0].phase == "live"
    assert rr.db_tests(REPO_ROOT).keys() >= {"market", "news", "briefing"}


def _is_jdk21(java_home: Path) -> bool:
    text = SCRIPT.read_text(encoding="utf-8")
    func = "is_jdk21() {" + text.split("is_jdk21() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
    result = subprocess.run(["bash", "-c", func + 'is_jdk21 "$1"', "bash", str(java_home)], capture_output=True, timeout=10)
    return result.returncode == 0


def fake_java(root: Path, version: str) -> Path:
    java = root / "bin" / "java"
    write(java, f'#!/bin/sh\necho \'openjdk version "{version}" 2025-01-01\' >&2\n')
    java.chmod(0o755)
    return root


def test_jdk_check_refuses_another_version_that_java_home_falls_back_to(tmp_path):
    # /usr/libexec/java_home -v 21 은 21이 없으면 17 같은 다른 버전을 돌려준다.
    assert _is_jdk21(fake_java(tmp_path / "jdk21", "21.0.9"))
    assert not _is_jdk21(fake_java(tmp_path / "jdk17", "17.0.15"))
    assert not _is_jdk21(fake_java(tmp_path / "jdk210", "210.0.1"))
    assert not _is_jdk21(tmp_path / "missing")
