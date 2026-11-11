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
        ("작업 기록", "job_log_store.py", NA, "test_job_log_store.py",
         "test_job_log_store_db.py [DB, 켜는 조건: BRIEFING_IT_DB_URL·BRIEFING_IT_DB_USER·BRIEFING_IT_DB_PASSWORD]")]))
    assert rr.db_tests(tmp_path) == {"market": ["StoreDbTest"], "briefing": ["test_job_log_store_db.py"]}


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


# ---------------------------------------------------------------- --live 브리핑·메일

BRIEF_LIVE = "test_briefing_flow_live.py [서비스 흐름, 외부 API, 켜는 조건: test-all.sh --live]"
LIVE_SCRIPTS_ROWS = [
    ("전체 실행과 결과 표", "test-all.sh, run_report.py, live_briefing.py", "test_run_report.py", "test_run_report.py",
     "test_quote_flow_live.py [서비스 흐름, 켜는 조건: test-all.sh --live]; " + BRIEF_LIVE),
]
BRIEFING_ROWS = [
    ("리포트 생성", "report.py", "test_report.py", NA,
     "scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live]"),
]
LIVE_STAGES = (("live", "시세 흐름 실제", "ok", "", "90", "live/py/scripts.xml"),
               ("livebrief", "브리핑·메일 실제", "ok", "리포트 파일 build/test-all-live/briefing-R.txt", "100",
                "live/py/scripts-briefing.xml"))


def live_brief_results(results: Path, brief_status="passed", counts=None, brief_stage=None) -> None:
    passing_results(results)
    py_xml(results, "main", "briefing", [("tests.test_report", "passed")])
    py_xml(results, "main", "scripts", [("tests.test_run_report", "passed"), ("tests.test_quote_flow_live", "skipped"),
                                        ("tests.test_briefing_flow_live", "skipped")])
    py_xml(results, "live", "scripts", [("tests.test_quote_flow_live", "passed")])
    py_xml(results, "live", "scripts-briefing", [("tests.test_briefing_flow_live", brief_status)])
    write(results / "live/counts.json", json.dumps({"snapshots": 1, "with_raw": 1, "failures": {}}))
    if counts is not None:
        write(results / "live/briefing-counts.json", json.dumps(counts))
    stages(results, OK_STAGES[0], LIVE_STAGES[0], brief_stage or LIVE_STAGES[1])


def make_live_maps(root: Path) -> None:
    make_maps(root, market_rows=MARKET_ROWS, scripts_rows=LIVE_SCRIPTS_ROWS)
    write(root / "docs/test-map/briefing.md", map_text(BRIEFING_ROWS))


def test_parse_item_reads_a_test_of_another_map():
    item = rr.parse_item("scripts/test_briefing_flow_live.py [외부 API, 켜는 조건: test-all.sh --live]")[0]
    assert item.group == "scripts"
    assert item.tests == ("test_briefing_flow_live.py",)
    assert item.phase == "live"
    assert rr.parse_item("test_report.py")[0].group == ""


def test_live_run_with_briefing_shows_llm_and_mail_calls_and_the_report_file(tmp_path):
    make_live_maps(tmp_path)
    results = tmp_path / "results"
    live_brief_results(results, counts={"briefings": 1, "generatedBy": "llm", "llm": 1, "mail": 1})

    code, output = render(tmp_path, results, live=True)

    assert code == 0, output
    # briefing 지도의 칸이 scripts 지도의 테스트 결과로 판정된다.
    assert row_of(output, "리포트 생성").endswith("| 돌았음(외부 API) |")
    assert row_of(output, "전체 실행과 결과 표").endswith("| 돌았음(서비스 흐름) + 돌았음(서비스 흐름, 외부 API) |")
    assert "| 브리핑·메일 실제 | 통과 (실행 1, 실패 0, 건너뜀 0, 리포트 파일 build/test-all-live/briefing-R.txt) |" in output
    assert "| 시세 흐름 실제 | 통과 (실행 1, 실패 0, 건너뜀 0) |" in output
    assert "실제 호출: 토큰 1, 시세 1, LLM 1, 메일 1" in output


def test_llm_retries_are_visible_in_the_call_line(tmp_path):
    make_live_maps(tmp_path)
    results = tmp_path / "results"
    live_brief_results(results, counts={"briefings": 1, "generatedBy": "llm", "llm": 2, "mail": 1})

    code, output = render(tmp_path, results, live=True)

    assert code == 0
    assert "실제 호출: 토큰 1, 시세 1, LLM 2, 메일 1" in output


def test_rule_based_briefing_fails_the_cell_and_the_stage(tmp_path):
    make_live_maps(tmp_path)
    results = tmp_path / "results"
    failed_stage = ("livebrief", "브리핑·메일 실제", "fail", "리포트·메일 확인 실패", "100", "live/py/scripts-briefing.xml")
    live_brief_results(results, brief_status="failed", brief_stage=failed_stage,
                       counts={"briefings": 1, "generatedBy": "rule-based", "llm": 0, "mail": 1})

    code, output = render(tmp_path, results, live=True)

    assert code == 1
    assert row_of(output, "리포트 생성").endswith("| 실패(외부 API) |")
    assert "| 브리핑·메일 실제 | 실패 (실행 1, 실패 1, 건너뜀 0, 리포트·메일 확인 실패) |" in output
    assert "실제 호출: 토큰 1, 시세 1, LLM 0, 메일 1" in output


def test_briefing_not_reached_means_no_llm_or_mail_calls(tmp_path):
    make_live_maps(tmp_path)
    results = tmp_path / "results"
    not_run = ("livebrief", "브리핑·메일 실제", "fail", "시세 흐름 실제가 통과하지 않아 돌리지 않음", "0", "")
    live_brief_results(results, brief_stage=not_run)
    (results / "live/py/scripts-briefing.xml").unlink()

    code, output = render(tmp_path, results, live=True)

    assert code == 1
    assert row_of(output, "리포트 생성").endswith("| 실패(외부 API, 결과 없음: test_briefing_flow_live.py) |")
    assert "실제 호출: 토큰 1, 시세 1, LLM 0, 메일 0" in output


def test_briefing_started_without_counts_is_reported_as_unknown(tmp_path):
    make_live_maps(tmp_path)
    results = tmp_path / "results"
    failed_stage = ("livebrief", "브리핑·메일 실제", "fail", "news 가 시작 중에 끝남", "30", "live/py/scripts-briefing.xml")
    live_brief_results(results, brief_stage=failed_stage)

    code, output = render(tmp_path, results, live=True)

    assert code == 1
    assert "LLM·메일 알 수 없음" in output


def test_default_run_skips_the_briefing_cell_of_another_map(tmp_path):
    make_live_maps(tmp_path)
    results = tmp_path / "results"
    passing_results(results)
    py_xml(results, "main", "briefing", [("tests.test_report", "passed")])

    code, output = render(tmp_path, results)

    assert code == 0, output
    assert row_of(output, "리포트 생성").endswith(f"| 건너뜀(외부 API, {rr.LIVE_SKIP_REASON}) |")
    assert "실제 호출: 0회" in output


def test_plan_with_live_lists_the_briefing_and_mail_step():
    result = run_script("--live", "--plan")
    assert result.returncode == 0
    assert "브리핑·메일 실제" in result.stdout and "[테스트]" in result.stdout


def test_script_checks_every_live_key_before_calling_anything():
    text = SCRIPT.read_text(encoding="utf-8")
    keys = text.split('LIVE_KEYS="', 1)[1].split('"', 1)[0].split()
    assert {"BROKER_APP_KEY", "TEST_ALL_LIVE_SYMBOL", "ANTHROPIC_API_KEY", "SMTP_HOST", "TEST_ALL_LIVE_RECIPIENT"} <= set(keys)
    # 브리핑 재검색에 수집기가 실제 뉴스 검색으로 답하도록 뉴스 검색 키도 미리 확인하고 수집기에만 넘긴다.
    # 비밀값(ID·시크릿)은 Parameter Store, 비밀이 아닌 주소·헤더 이름은 실행 환경변수나 .env 에서 읽어 둘 다 미리 확인한다.
    secrets = {"NEWS_SEARCH_CLIENT_ID", "NEWS_SEARCH_CLIENT_SECRET"}
    settings = {"NEWS_SEARCH_BASE_URL", "NEWS_SEARCH_ID_HEADER", "NEWS_SEARCH_SECRET_HEADER"}
    check = text.split("missing_live_settings() {", 1)[1].split("\n}\n", 1)[0]
    assert secrets <= set(keys) and not settings & set(keys)
    assert all(f"$(env_or_dotenv {key})" in check for key in settings)
    collector = text.split('echo "collector 기동', 1)[1].split("COLLECTOR_PID=$!", 1)[0]
    assert all(f'{key}="$(live_value {key})"' in collector for key in secrets)
    assert all(f'{key}="$(env_or_dotenv {key})"' in collector for key in settings)
    # 키 확인이 bootJar·Kafka 기동보다 앞에 있어야 한다.
    body = text.split("run_live() {", 1)[1]
    assert body.index("fetch_live_params") < body.index("missing_live_keys") < body.index("missing_live_settings") < body.index("bootJar") < body.index("docker run")


def _live_functions(text: str) -> str:
    return "".join(name + text.split(name, 1)[1].split("\n}\n", 1)[0] + "\n}\n"
                   for name in ("fetch_live_params() {", "live_value() {", "keep_loader_log() {"))


def test_live_keys_come_only_from_parameter_store_each_run(tmp_path):
    text = SCRIPT.read_text(encoding="utf-8")
    # 실제 호출에 쓰는 키는 .env·실행 환경변수 대신 매번 Parameter Store 에서 읽은 값만 쓴다.
    keys = text.split('LIVE_KEYS="', 1)[1].split('"', 1)[0].split()
    for key in keys + ["SMTP_PORT"]:
        assert f"$(env_or_dotenv {key})" not in text
        assert f'env_or_dotenv "$key"' not in text
    # 가짜 로더로 로더가 --env-file 의 임시 파일에만 쓰고, 값이 그 파일에서만 읽히는지 확인한다.
    loader = tmp_path / "fake_python"
    write(loader, '#!/bin/sh\nfor a; do [ "$prev" = --env-file ] && f=$a; prev=$a; done\n'
                  'printf \'TEST_ALL_LIVE_SYMBOL=TEST01\\n\' > "$f"\necho "$@"\n')
    loader.chmod(0o755)
    env = {"PATH": "/usr/bin:/bin", "TMP": str(tmp_path), "PY": str(loader), "ROOT": str(REPO_ROOT),
           "TEST_ALL_LIVE_SYMBOL": "TEST99", "TEST_ALL_AWS_PROFILE": "test-profile"}
    script = "set -u\n" + _live_functions(text) + \
        'LIVE_PARAMS=""; fetch_live_params || exit 9; live_value TEST_ALL_LIVE_SYMBOL; echo; live_value SMTP_HOST\n'
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["TEST01", ""]
    log = (tmp_path / "load_config.log").read_text(encoding="utf-8")
    assert "--profile test-profile" in log and "live-params.env" in log


def test_live_needs_a_named_aws_profile_and_keeps_the_loader_log(tmp_path):
    text = SCRIPT.read_text(encoding="utf-8")
    body = text.split("run_live() {", 1)[1]
    # 프로필이 없으면 기본 자격증명으로 넘어가지 않고 Parameter Store 를 부르기 전에 멈춘다.
    assert body.index('-z "${TEST_ALL_AWS_PROFILE:-}"') < body.index("fetch_live_params")
    assert "--profile \"$TEST_ALL_AWS_PROFILE\"" in text.split("fetch_live_params() {", 1)[1].split("\n}\n", 1)[0]
    # 읽기 실패 로그는 지워지는 임시 폴더 대신 결과 폴더에 권한 600 으로 남긴다.
    (tmp_path / "tmp").mkdir()
    write(tmp_path / "tmp" / "load_config.log", "중단: Parameter Store 읽기 실패\n")
    env = {"PATH": "/usr/bin:/bin", "TMP": str(tmp_path / "tmp"), "ROOT": str(tmp_path),
           "LIVE_OUT_DIR": str(tmp_path / "build" / "test-all-live"), "RUN_ID": "TESTRUN"}
    script = _live_functions(text) + "keep_loader_log\n"
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env, timeout=10)
    assert result.stdout == "build/test-all-live/load_config-TESTRUN.log"
    kept = tmp_path / "build" / "test-all-live" / "load_config-TESTRUN.log"
    assert kept.read_text(encoding="utf-8").startswith("중단:")
    assert kept.stat().st_mode & 0o777 == 0o600


def test_script_turns_on_real_mail_only_inside_the_test_notifier():
    text = SCRIPT.read_text(encoding="utf-8")
    assert text.count("NOTIFIER_SEND_ENABLED=true") == 1
    notifier_block = text.split('echo "notifier 기동', 1)[1].split("NOTIFIER_PID=$!", 1)[0]
    assert "NOTIFIER_SEND_ENABLED=true" in notifier_block
    assert 'NOTIFIER_SUBJECT_PREFIX="[테스트]"' in notifier_block and "PER_HOUR" not in notifier_block
    # .env 에 쓰는 명령이 없어야 한다(읽기만 한다).
    assert ">> \"$ENV_FILE\"" not in text and "> \"$ENV_FILE\"" not in text


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


def test_service_ready_patterns_match_the_kafka_client_log_format():
    # Kafka 클라이언트는 "Adding newly assigned partitions: [collector.command-0]"처럼 대괄호로 감싸 찍는다.
    # 대괄호 없이 토픽 이름을 기다리면 서비스가 떠도 끝까지 맞지 않아 대기 시간 초과로 실패한다.
    import re
    text = SCRIPT.read_text(encoding="utf-8")
    patterns = re.findall(r'wait_for_log "[^"]+" "([^"]*Adding newly assigned partitions:[^"]*)"', text)
    assert len(patterns) == 4
    log_line = ("[Consumer clientId=consumer-collector-command-2, groupId=collector-command] "
                "Adding newly assigned partitions: [collector.command-0]")
    assert any(p in log_line for p in patterns)
    for p in patterns:
        rest = p.split("Adding newly assigned partitions:", 1)[1]
        assert rest == "" or rest.startswith(" ["), p


def test_live_checks_count_only_rows_saved_after_this_run_started():
    # 실제 DB 테스트가 같은 테스트 DB 에 남긴 가짜 행(조회 실패 3종 등)을 이번 실행의 결과로 세면 안 된다.
    text = SCRIPT.read_text(encoding="utf-8")
    body = text.split("run_live() {", 1)[1]
    assert body.index('LIVE_SINCE=$(echo "SELECT now();" | admin_sql)') < body.index('echo "수동 실행 명령 collect 1건 발행"')
    assert 'QUOTE_FLOW_LIVE_SINCE="$LIVE_SINCE"' in text and 'BRIEFING_FLOW_LIVE_SINCE="$LIVE_SINCE"' in text
    quote = (REPO_ROOT / "scripts" / "tests" / "test_quote_flow_live.py").read_text(encoding="utf-8")
    # 앞 단계 실제 DB 테스트가 미래 날짜로 넣은 가짜 행이 섞이지 않게 시작 시각 뒤, 지금 이전만 센다.
    bounded = "received_at >= %s::timestamptz AND received_at <= now()"
    assert quote.count("FROM market.") == quote.count(bounded) >= 2 and 'os.environ["QUOTE_FLOW_LIVE_SINCE"]' in quote
    brief = (REPO_ROOT / "scripts" / "tests" / "test_briefing_flow_live.py").read_text(encoding="utf-8")
    assert "created_at >= %s::timestamptz" in brief and 'env["BRIEFING_FLOW_LIVE_SINCE"]' in brief
    # 재검색이 근거를 못 찾고 접은 단계 기록(step-failed)이 아니라 작업 실패(job_run)로 판정한다.
    failed_sql = brief.split("def _failed_jobs", 1)[1].split("\ndef ", 1)[0].split("cur.execute", 1)[1]
    assert "FROM briefing.job_run WHERE status = 'failed'" in failed_sql and "step-failed" not in failed_sql
    seed = (REPO_ROOT / "scripts" / "live_briefing.py").read_text(encoding="utf-8")
    assert seed.count("FROM market.price_snapshot") == seed.count(bounded) == 2
    assert 'LIVE_BRIEFING_SINCE="$LIVE_SINCE"' in text


def test_live_stops_before_calling_when_broker_base_url_is_not_an_https_address():
    text = SCRIPT.read_text(encoding="utf-8")
    body = text.split("run_live() {", 1)[1]
    guard = 'if ! [[ "$base_url" =~ ^https://[^/[:space:]]+$ ]]'
    assert body.index(guard) < body.index("bootJar")
    check = "check() { " + guard + "; then echo no; else echo yes; fi; }; "
    for value, ok in [("https://example.com:443", "yes"), ("https://example.com", "yes"),
                      ("someone@example.com", "no"), ("example.com:443", "no"),
                      ("https://example.com:443/", "no"), ("https://example.com :443", "no"), ("", "no")]:
        result = subprocess.run(["bash", "-c", check + 'base_url="$1"; check', "bash", value],
                                capture_output=True, text=True, timeout=10)
        assert result.stdout.strip() == ok, value
