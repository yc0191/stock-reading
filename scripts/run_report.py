"""전체 실행 스크립트(scripts/test-all.sh)의 도우미. 표준 라이브러리만 쓴다.

- db-tests: 테스트 지도에서 실제 DB 테스트(켜는 조건이 *_IT_DB_* 인 것)를 골라 출력한다.
- report: 지도(docs/test-map)와 테스트 결과 파일(JUnit XML)을 읽어 기능별 '돌았음 / 건너뜀(이유) / 실패' 표와
  단계 요약을 출력한다. 실패가 하나라도 있으면 1로 끝난다.

결과 폴더 모양(test-all.sh 가 만든다):
  <results>/<단계>/java/<서비스>/TEST-*.xml   Gradle 결과
  <results>/<단계>/py/<briefing|scripts>[-<이름>].xml  pytest --junitxml 결과('-' 앞이 지도 이름)
  <results>/stages.tsv                         단계마다 한 줄(키, 이름, 상태, 설명, 초, 결과 폴더)
  <results>/live/counts.json                   시세 흐름 실제 테스트가 남긴 행 개수(값은 없음)
  <results>/live/briefing-counts.json          브리핑·메일 실제 테스트가 남긴 개수(LLM 호출, 메일 발송 등)
단계는 main(Java 전체·브리핑·설정 로더), db(실제 DB 테스트), live(--live 시세 흐름과 브리핑·메일)다.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import check_test_map as ctm

LIVE_SKIP_REASON = "실제 호출 옵션 없음"
LIVE_CONDITION = "test-all.sh --live"
DB_CONDITION = re.compile(r"\b[A-Z]+_IT_DB_[A-Z]+\b")
PY_FILE = re.compile(r"^test_\w+$")
NAME_WITH_BRACKET = re.compile(r"([A-Za-z_][A-Za-z0-9_.\-/]*)\s*(\[[^\]]*\])?")
SKIPPED_FAILURE_TYPES = ("TICK_SKIPPED",)
TOKEN_FAILURE_TYPES = ("TOKEN_ISSUE_FAILED",)

PASSED, SKIPPED, FAILED = "passed", "skipped", "failed"


@dataclass(frozen=True)
class Item:
    """칸 안의 ';' 로 나뉜 한 덩어리."""
    kind: str  # tests | none | na
    tests: tuple
    label: str  # 대괄호의 종류 표시(DB, 외부 API 등). 없으면 빈 문자열
    condition: str  # 켜는 조건. 없으면 빈 문자열
    group: str = ""  # 다른 지도의 테스트를 가리킬 때('scripts/test_x.py') 그 지도 이름. 없으면 칸이 있는 지도

    @property
    def phase(self) -> str:
        if LIVE_CONDITION in self.condition:
            return "live"
        if DB_CONDITION.search(self.condition):
            return "db"
        return "main"


def _bracket(bracket: str) -> tuple:
    label, condition = [], ""
    for part in (p.strip() for p in bracket[1:-1].split(",")):
        if part.startswith("켜는 조건:"):
            condition = part[len("켜는 조건:"):].strip()
        elif condition:
            # '켜는 조건: A=1, B=1' 처럼 쉼표 뒤에 이어지는 조건
            condition = f"{condition}, {part}"
        elif part:
            label.append(part)
    return ", ".join(label), condition


def parse_item(text: str) -> list:
    """';' 로 나뉜 한 덩어리를 테스트 이름마다 하나씩 나눈다. 대괄호는 바로 앞 이름에만 붙는다
    (예: 'ATest, LoadTest [부하, 켜는 조건: X=1]' 에서 ATest 는 늘 돌고 LoadTest 만 조건부)."""
    text = text.strip()
    if text.startswith("해당 없음"):
        return [Item("na", (), "", "")]
    if text.startswith("없음"):
        return [Item("none", (), "", "")]
    items = []
    for part in NAME_WITH_BRACKET.finditer(text):
        label, condition = _bracket(part.group(2)) if part.group(2) else ("", "")
        group, _, name = part.group(1).rpartition("/")
        items.append(Item("tests", (name,), label, condition, group))
    return items


def parse_cell(cell: str) -> list:
    return [item for part in cell.split(";") if part.strip() for item in parse_item(part)]


# ---------------------------------------------------------------- 결과 파일 읽기

def _case_status(case) -> str:
    if case.find("failure") is not None or case.find("error") is not None:
        return FAILED
    if case.find("skipped") is not None:
        return SKIPPED
    return PASSED


def _java_name(suite_name: str) -> str:
    # 중첩 클래스(@Nested)는 바깥 클래스의 결과로 친다.
    return suite_name.split("$", 1)[0].rsplit(".", 1)[-1]


def _py_name(classname: str) -> str | None:
    for part in classname.split("."):
        if PY_FILE.match(part):
            return part
    return None


def load_results(phase_dir: Path) -> dict:
    """{(지도 이름, 테스트 이름): [상태, ...]}. 지도 이름은 서비스 이름 또는 briefing, scripts."""
    found: dict = {}
    if not phase_dir.is_dir():
        return found
    for xml in sorted((phase_dir / "java").glob("*/*.xml")):
        group = xml.parent.name
        for suite in _suites(xml):
            name = _java_name(suite.get("name", ""))
            found.setdefault((group, name), []).extend(_case_status(c) for c in suite.iter("testcase"))
    for xml in sorted((phase_dir / "py").glob("*.xml")):
        # 한 단계에서 같은 지도의 pytest 를 여러 번 돌리면 'scripts-briefing.xml' 처럼 이름을 붙여 따로 남긴다.
        group = xml.stem.split("-", 1)[0]
        for suite in _suites(xml):
            for case in suite.iter("testcase"):
                name = _py_name(case.get("classname", ""))
                if name:
                    found.setdefault((group, name), []).append(_case_status(case))
    return found


def _suites(xml: Path) -> list:
    try:
        root = ET.parse(xml).getroot()
    except ET.ParseError:
        return []
    return [root] if root.tag == "testsuite" else list(root.iter("testsuite"))


def totals(statuses) -> dict:
    counts = {PASSED: 0, SKIPPED: 0, FAILED: 0}
    for status in statuses:
        counts[status] += 1
    return counts


# ---------------------------------------------------------------- 칸 판정

def _with_label(word: str, item: Item, reason: str = "") -> str:
    inside = ", ".join(p for p in (item.label, reason) if p)
    return f"{word}({inside})" if inside else word


def judge_item(item: Item, group: str, results: dict, live: bool) -> tuple:
    """(표시 문자열, 실패 여부)."""
    if item.kind == "none":
        return "없음", False
    if item.kind == "na":
        return "-", False
    if item.phase == "live" and not live:
        return _with_label("건너뜀", item, LIVE_SKIP_REASON), False
    phase_results = results.get(item.phase, {})
    statuses, missing = [], []
    for name in item.tests:
        key = (item.group or group, name[:-3] if name.endswith(".py") else name)
        if key in phase_results:
            statuses.extend(phase_results[key])
        else:
            missing.append(name)
    counts = totals(statuses)
    if counts[FAILED]:
        return _with_label("실패", item), True
    # 기본 단계의 조건부 테스트는 결과가 없거나 모두 건너뛰면 켜는 조건이 없는 것으로 본다.
    conditional_main = item.phase == "main" and item.condition
    if missing and not conditional_main:
        return _with_label("실패", item, "결과 없음: " + ", ".join(missing)), True
    if counts[PASSED]:
        return _with_label("돌았음", item), False
    if conditional_main:
        return _with_label("건너뜀", item, "켜는 조건 없음: " + item.condition), False
    if item.phase in ("db", "live"):
        # 이 단계는 켜는 환경변수를 넣고 돌렸으므로 건너뛰었다면 준비가 잘못된 것이다.
        return _with_label("실패", item, "켰는데 건너뜀"), True
    return _with_label("건너뜀", item, "테스트가 스스로 건너뜀"), False


def judge_cell(cell: str, group: str, results: dict, live: bool) -> tuple:
    shown, failed = [], False
    for item in parse_cell(cell):
        text, bad = judge_item(item, group, results, live)
        failed = failed or bad
        if text not in shown:
            shown.append(text)
    if len(shown) > 1 and "-" in shown:
        # 테스트와 '해당 없음'이 한 칸에 섞이면 결과만 보여 준다.
        shown.remove("-")
    return " + ".join(shown) or "실패(칸이 비었음)", failed or not shown


def map_rows(root: Path, map_file: str) -> list:
    path = root / ctm.MAP_DIR / map_file
    if not path.is_file():
        return []
    errors: list = []
    rows, _ = ctm.parse_map(path, errors, map_file)
    return rows


# ---------------------------------------------------------------- 실제 DB 테스트 고르기

def db_tests(root: Path) -> dict:
    """{지도 이름: [테스트 이름, ...]} 중 켜는 조건이 *_IT_DB_* 인 것."""
    chosen: dict = {}
    for map_file in ctm.MAP_FILES:
        group = map_file[:-3]
        for row in map_rows(root, map_file):
            for col in ctm.TEST_COLUMNS:
                for item in parse_cell(row.cells[col]):
                    if item.kind == "tests" and item.phase == "db":
                        for name in item.tests:
                            if name not in chosen.setdefault(group, []):
                                chosen[group].append(name)
    return chosen


# ---------------------------------------------------------------- 단계 요약과 호출 횟수

@dataclass(frozen=True)
class Stage:
    key: str
    label: str
    status: str  # ok | fail | skip
    detail: str
    seconds: str
    results: str


def read_stages(path: Path) -> list:
    stages = []
    if not path.is_file():
        return stages
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = (line.split("\t") + [""] * 6)[:6]
        if parts[0]:
            stages.append(Stage(*parts))
    return stages


def _duration(seconds: str) -> str:
    if not seconds.isdigit():
        return "-"
    s = int(seconds)
    return f"{s // 60}분 {s % 60}초" if s >= 60 else f"{s}초"


def stage_counts(results_dir: Path, rel: str) -> str:
    if not rel:
        return ""
    target = results_dir / rel
    statuses = []
    xmls = [target] if target.is_file() else sorted(target.rglob("*.xml")) if target.is_dir() else []
    for xml in xmls:
        for suite in _suites(xml):
            statuses.extend(_case_status(c) for c in suite.iter("testcase"))
    if not statuses:
        return "결과 없음"
    c = totals(statuses)
    return f"실행 {c[PASSED] + c[FAILED]}, 실패 {c[FAILED]}, 건너뜀 {c[SKIPPED]}"


def live_calls(counts: dict | None) -> str:
    """시세 흐름 실제 테스트가 남긴 행 개수로 실제 호출 횟수를 센다.

    토큰은 메모리에만 두므로 새로 띄운 collector 가 처음 한 번 발급한다. 건너뜀 기록은 호출하지 않은 것이다.
    """
    if not counts:
        return "알 수 없음(행 개수 기록 없음)"
    failures = counts.get("failures", {})
    quote = counts.get("snapshots", 0) + sum(n for t, n in failures.items()
                                             if t not in SKIPPED_FAILURE_TYPES + TOKEN_FAILURE_TYPES)
    token = 1 if quote or any(failures.get(t, 0) for t in TOKEN_FAILURE_TYPES) else 0
    return f"토큰 {token}, 시세 {quote}"


def briefing_calls(counts: dict | None) -> str:
    """브리핑·메일 실제 테스트가 남긴 개수로 LLM 호출과 메일 발송 횟수를 적는다. 1이 아니면 그대로 보인다."""
    if not counts:
        return "LLM·메일 알 수 없음(브리핑 확인 기록 없음)"
    return f"LLM {counts.get('llm', 0)}, 메일 {counts.get('mail', 0)}"


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def report(root: Path, results_dir: Path, live: bool, out=sys.stdout) -> int:
    results = {phase: load_results(results_dir / phase) for phase in ("main", "db", "live")}
    failed = False
    mode = "--live 실행" if live else f"기본 실행, {LIVE_SKIP_REASON}"
    print(f"== 테스트 지도 결과 ({mode}) ==", file=out)
    for map_file in ctm.MAP_FILES:
        group = map_file[:-3]
        print(f"\n[{group}]", file=out)
        print("| 기능 | 단위 | 가상 | 실제 |", file=out)
        print("|---|---|---|---|", file=out)
        rows = map_rows(root, map_file)
        if not rows:
            print(f"| (지도 {map_file} 를 읽지 못함) | 실패 | 실패 | 실패 |", file=out)
            failed = True
        for row in rows:
            shown = []
            for col in ctm.TEST_COLUMNS:
                text, bad = judge_cell(row.cells[col], group, results, live)
                failed = failed or bad
                shown.append(text)
            print(f"| {row.cells['기능']} | {' | '.join(shown)} |", file=out)

    print("\n== 단계 ==", file=out)
    print("| 단계 | 결과 | 시간 |", file=out)
    print("|---|---|---|", file=out)
    stages = read_stages(results_dir / "stages.tsv")
    if not stages:
        print("| (단계 기록 없음) | 실패 | - |", file=out)
        failed = True
    for stage in stages:
        word = {"ok": "통과", "fail": "실패", "skip": "건너뜀"}.get(stage.status, "실패")
        failed = failed or stage.status not in ("ok", "skip")
        extra = ", ".join(p for p in (stage_counts(results_dir, stage.results), stage.detail) if p)
        print(f"| {stage.label} | {word}{f' ({extra})' if extra else ''} | {_duration(stage.seconds)} |", file=out)

    if live:
        calls = live_calls(_read_json(results_dir / "live" / "counts.json"))
        # 브리핑·메일 단계에 가지 못했으면(시세 흐름 실패 등) LLM·메일은 부르지 않은 것이다.
        reached = any(s.key == "livebrief" and s.status != "skip" and s.results for s in stages)
        brief = _read_json(results_dir / "live" / "briefing-counts.json")
        calls += ", " + (briefing_calls(brief) if reached or brief else "LLM 0, 메일 0")
        print(f"\n실제 호출: {calls}", file=out)
    else:
        print("\n실제 호출: 0회", file=out)
    print(f"결과: {'실패 (종료 코드 1)' if failed else '통과 (종료 코드 0)'}", file=out)
    return 1 if failed else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="전체 실행 스크립트 도우미")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent, help="저장소 루트")
    sub = parser.add_subparsers(dest="command", required=True)
    db = sub.add_parser("db-tests", help="실제 DB 테스트 이름을 '지도 이름 이름 ...' 줄로 출력")
    db.set_defaults(command="db-tests")
    rep = sub.add_parser("report", help="기능별 결과 표 출력")
    rep.add_argument("--results", type=Path, required=True, help="결과 폴더")
    rep.add_argument("--live", action="store_true", help="--live 실행의 결과")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if args.command == "db-tests":
        for group, names in db_tests(root).items():
            print(group, *names)
        return 0
    return report(root, args.results, args.live)


if __name__ == "__main__":
    sys.exit(main())
