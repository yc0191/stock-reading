"""조치 판정: 확인기가 남긴 결과 파일을 읽어 이슈에 달 댓글과 라벨 변경을 '할 일 파일'로 만든다.

사용:
  python3 scripts/ops_verdict.py --results-dir R --criteria-dir C --issues I --repo G --today YYYY-MM-DD --out O

- R: <YYYY-MM-DD>/post-close.json 들이 든 폴더(S3 results/ 아래를 받아 둔 것)
- C: 기준 파일 폴더(ops-checks/). 지문은 파일 바이트의 SHA-256
- I: {"<이슈 번호>": {"open": bool, "labels": [str], "comments": [str]}} (comments 는 이 작업이 단 댓글만)
- G: git 저장소 경로. 조치 커밋이 그날 운영 버전의 조상인지 본다
- O: {"issues": {"<이슈>": {"comments": [...], "remove_labels": [...], "add_labels": [...]}}, "summary": [...]}
- 선택: --alert-since YYYY-MM-DD 를 주면 그 날짜 이하의 '빠진 평일'과 'error 지표가 있는 날'은 이미 알린 실패로 보고
  종료 코드에 넣지 않고 summary 에만 적는다(날마다 같은 실패가 반복 알림되지 않게). 주지 않으면 모두 실패로 친다.
- 선택: --manual 은 손 실행용이다. 오늘 결과 파일이 아직 없어도 실패로 치지 않는다(오늘은 파일이 있을 때만 처리).
- 종료 코드: 0 정상, 1 실패(결과 파일 없음·오류, error 지표 등). 실패해도 할 일 파일은 쓴다.

GitHub·S3에는 접속하지 않는다. 실제 댓글·라벨은 워크플로가 할 일 파일대로 한다.
이미 센 날은 이 작업이 단 댓글에 넣은 보이지 않는 표시로 알아낸다(결과 파일이 지워져도 유지된다).
공개 로그와 댓글에는 비율·판정·날짜·버전·이슈 번호·실패 종류만 쓴다. 기준 파일의 SQL 본문은 읽지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

MISSING_RESULT = "결과 파일 없음: 확인기 또는 수집기 알림 확인"
VERDICTS = {"pass", "fail", "hold", "error"}
VERDICT_LABEL = {"pass": "통과", "fail": "실패", "hold": "판정 보류", "error": "오류"}
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
VERSION_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._/-]{0,99}$")
# 결과 파일 값은 공개 로그·댓글에 나가므로 모양이 맞을 때만 쓴다.
REASON_RE = re.compile(r"^[a-z0-9_-]{1,40}$")
KEY_RE = re.compile(r"^[A-Za-z0-9_.-]{1,60}$")
PASS_RE = re.compile(r"^(<=|>=|==|<|>)\s*(0(\.\d{1,6})?|1(\.0{1,6})?)$")
DAY_MARK = re.compile(r"<!-- ops-check day=(\d{4}-\d{2}-\d{2}) attempt=(\S+?) verdict=(pass|fail) -->")
FINAL_MARK = re.compile(r"<!-- ops-check-final attempt=(\S+?) verdict=(pass|fail) -->")


class ResultFileError(Exception):
    """결과 파일 하나를 읽거나 해석할 수 없다. 메시지에는 종류만 쓴다."""


@dataclass(frozen=True)
class Criteria:
    issue: int
    attempt: str
    fix_commit: str
    days: int
    comment: bool
    sha256: str


@dataclass
class IssueTodo:
    comments: list[str] = field(default_factory=list)
    remove_labels: list[str] = field(default_factory=list)
    add_labels: list[str] = field(default_factory=list)


@dataclass
class Report:
    todo: dict[int, IssueTodo] = field(default_factory=dict)
    summary: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def for_issue(self, issue: int) -> IssueTodo:
        return self.todo.setdefault(issue, IssueTodo())


def top_level_scalars(text: str) -> dict[str, str]:
    """기준 파일 맨 앞 칸의 한 줄 값만 읽는다. 지표의 SQL 본문이 있는 칸은 읽지 않는다."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        m = re.match(r"^(issue|attempt|comment|fix_commit|days):[ \t]*(.*?)[ \t]*$", line)
        if not m:
            continue
        raw = m.group(2)
        quoted = re.match(r'^"([^"]*)"|^\'([^\']*)\'', raw)
        values[m.group(1)] = (quoted.group(1) if quoted.group(1) is not None else quoted.group(2)) if quoted \
            else re.sub(r"\s+#.*$", "", raw)
    return values


def load_criteria(criteria_dir: Path, report: Report) -> dict[tuple[int, str], Criteria]:
    found: dict[tuple[int, str], Criteria] = {}
    for path in sorted(criteria_dir.glob("*.yml")):
        data = path.read_bytes()
        v = top_level_scalars(data.decode("utf-8", errors="replace"))
        try:
            issue, days = int(v["issue"]), int(v["days"])
            attempt, fix = v["attempt"], v["fix_commit"]
            comment = {"true": True, "false": False}[v.get("comment", "true").lower()]
            if days < 1 or not attempt or not VERSION_RE.match(fix):
                raise ValueError
        except (KeyError, ValueError):
            report.failures.append("기준 파일 형식 오류")
            continue
        found[(issue, attempt)] = Criteria(issue, attempt, fix, days, comment, hashlib.sha256(data).hexdigest())
    return found


def read_result(path: Path, day: str) -> dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ResultFileError("결과 파일 읽기 실패") from e
    if not isinstance(doc, dict) or doc.get("day") != day or doc.get("status") not in ("collected", "market-closed") \
            or not isinstance(doc.get("checks"), list):
        raise ResultFileError("결과 파일 형식 오류")
    if doc["status"] == "collected" and not VERSION_RE.match(str(doc.get("deploy_version", ""))):
        raise ResultFileError("결과 파일 형식 오류")
    for check in doc["checks"]:
        if not isinstance(check, dict) or not isinstance(check.get("issue"), int) \
                or not isinstance(check.get("attempt"), str) or not isinstance(check.get("criteria_hash"), str) \
                or not isinstance(check.get("metrics"), list):
            raise ResultFileError("결과 파일 형식 오류")
        for m in check["metrics"]:
            if not isinstance(m, dict) or m.get("verdict") not in VERDICTS or not isinstance(m.get("key"), str):
                raise ResultFileError("결과 파일 형식 오류")
    return doc


def load_results(results_dir: Path, report: Report) -> dict[str, dict[str, Any] | None]:
    """날짜별 결과. 읽을 수 없는 날은 None 으로 두고(파일은 있다) 실패로 기록한다."""
    days: dict[str, dict[str, Any] | None] = {}
    for folder in sorted(p for p in results_dir.iterdir() if p.is_dir()) if results_dir.is_dir() else []:
        if not DAY_RE.match(folder.name):
            report.failures.append("결과 폴더 이름 형식 오류")
            continue
        path = folder / "post-close.json"
        if not path.is_file():
            continue
        try:
            days[folder.name] = read_result(path, folder.name)
        except ResultFileError as e:
            days[folder.name] = None
            report.failures.append(f"{folder.name} {e}")
    return days


def safe(value: Any, pattern: re.Pattern[str], fallback: str) -> str:
    return value if isinstance(value, str) and pattern.fullmatch(value) else fallback


def is_alerted(day: str, alert_since: date | None) -> bool:
    return alert_since is not None and date.fromisoformat(day) <= alert_since


def check_expected_days(days: dict[str, Any], today: date, report: Report, alert_since: date | None = None,
                        require_today: bool = True) -> None:
    """가장 오래된 결과 파일 날짜부터 오늘까지의 평일에는 파일이 있어야 한다(휴장 평일도).
    이미 알린 날(alert_since 이하)은 summary 에만 적고, require_today 가 아니면 오늘 파일이 없어도 넘어간다."""
    if not days:
        report.failures.append(MISSING_RESULT + " (결과 폴더 비어 있음)")
        return
    day = date.fromisoformat(min(days))
    while day <= today:
        if day.weekday() < 5 and day.isoformat() not in days and (require_today or day != today):
            line = f"{MISSING_RESULT} ({day.isoformat()})"
            if is_alerted(day.isoformat(), alert_since):
                report.summary.append(f"{line} - 이미 알림")
            else:
                report.failures.append(line)
        day += timedelta(days=1)


def git_ok(repo: Path, *args: str) -> int:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True).returncode


def deployed(repo: Path, fix: str, version: str) -> bool | None:
    """조치 커밋이 운영 버전의 조상(같아도 됨)이면 True. 커밋을 찾을 수 없으면 None."""
    for rev in (fix, version):
        if git_ok(repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{rev}^{{commit}}") != 0:
            return None
    code = git_ok(repo, "merge-base", "--is-ancestor", fix, version)
    return {0: True, 1: False}.get(code)


def pct(value: Any) -> str:
    return f"{value * 100:.2f}%" if isinstance(value, (int, float)) else "-"


def day_comment(c: Criteria, day: str, verdict: str, nth: int, metrics: list[dict], human_count: int) -> str:
    rows = "\n".join(f"| {safe(m['key'], KEY_RE, '-')} | {pct(m.get('value'))} | {safe(m.get('pass'), PASS_RE, '-')} "
                     f"| {VERDICT_LABEL[m['verdict']]} |" for m in metrics)
    lines = [f"<!-- ops-check day={day} attempt={c.attempt} verdict={verdict} -->",
             f"**조치 판정** 이슈 #{c.issue} 시도 {c.attempt}, {day} 마감 후 판 ({nth}/{c.days}일째: {VERDICT_LABEL[verdict]})",
             "", "| 지표 | 비율 | 통과선 | 판정 |", "|---|---|---|---|", rows]
    if human_count:
        lines += ["", f"사람 확인 필요 {human_count}개: 기준 파일의 human 항목은 자동으로 잴 수 없어 사람이 확인합니다."]
    return "\n".join(lines)


def final_comment(c: Criteria, verdict: str) -> str:
    result = "통과" if verdict == "pass" else "실패"
    detail = "측정일 모두 통과선을 지켰습니다." if verdict == "pass" else "통과선을 지키지 못한 날이 있습니다."
    return "\n".join([
        f"<!-- ops-check-final attempt={c.attempt} verdict={verdict} -->",
        f"**조치 판정 결과: {result}** 이슈 #{c.issue} 시도 {c.attempt}, 측정일 {c.days}일 완료. {detail}",
        f"`verifying` 라벨을 떼고 `check-{'passed' if verdict == 'pass' else 'failed'}` 라벨을 붙였습니다. 이슈는 닫지 않았으니 확인 뒤 사람이 닫습니다."])


def judge_criteria(c: Criteria, days: dict[str, dict[str, Any] | None], info: dict[str, Any] | None,
                   repo: Path, report: Report, alert_since: date | None = None) -> None:
    tag = f"이슈 #{c.issue} 시도 {c.attempt}"
    actionable = bool(info and info.get("open") and "verifying" in info.get("labels", []) and c.comment)
    comments = list(info.get("comments", [])) if info else []
    counted = {m.group(1): m.group(3) for text in comments for m in DAY_MARK.finditer(text) if m.group(2) == c.attempt}
    has_final = any(m.group(1) == c.attempt for text in comments for m in FINAL_MARK.finditer(text))
    todo = report.for_issue(c.issue)

    for day in sorted(days):
        doc = days[day]
        if doc is None or doc["status"] == "market-closed":
            continue
        entry = next((e for e in doc["checks"] if e["issue"] == c.issue and e["attempt"] == c.attempt), None)
        if entry is None or day in counted:
            continue
        if actionable and len(counted) >= c.days:
            break
        if entry["criteria_hash"] != c.sha256:
            report.summary.append(f"{tag} {day}: 기준 파일 지문이 달라 세지 않음")
            continue
        is_deployed = deployed(repo, c.fix_commit, doc["deploy_version"])
        if is_deployed is None:
            report.failures.append(f"{day} {tag} 배포 버전 확인 불가")
            continue
        if not is_deployed:
            report.summary.append(f"{tag} {day}: 조치 커밋이 운영 버전에 없어(배포 전) 세지 않음")
            continue
        verdicts = [m["verdict"] for m in entry["metrics"]]
        if "error" in verdicts:
            reasons = sorted({safe(m.get("reason"), REASON_RE, "unknown") for m in entry["metrics"] if m["verdict"] == "error"})
            line = f"{day} {tag} error 지표({', '.join(reasons)})"
            if is_alerted(day, alert_since):
                report.summary.append(f"{line} - 이미 알림")
            else:
                report.failures.append(line)
            continue
        if "hold" in verdicts or not verdicts:
            report.summary.append(f"{tag} {day}: 판정 보류 지표가 있어 세지 않음")
            continue
        verdict = "pass" if all(v == "pass" for v in verdicts) else "fail"
        if not actionable:
            report.summary.append(f"{tag} {day}: {VERDICT_LABEL[verdict]} (댓글·라벨 대상 아님)")
            continue
        counted[day] = verdict
        report.summary.append(f"{tag} {day}: {VERDICT_LABEL[verdict]} ({len(counted)}/{c.days}일째)")
        todo.comments.append(day_comment(c, day, verdict, len(counted), entry["metrics"], len(entry.get("human") or [])))

    if actionable and len(counted) >= c.days:
        verdict = "pass" if all(v == "pass" for v in counted.values()) else "fail"
        if not has_final:
            todo.comments.append(final_comment(c, verdict))
        todo.remove_labels.append("verifying")
        todo.add_labels.append("check-passed" if verdict == "pass" else "check-failed")
        report.summary.append(f"{tag}: 측정일 {c.days}일 완료, {VERDICT_LABEL[verdict]}")


def run(results_dir: Path, criteria_dir: Path, issues: dict[str, Any], repo: Path, today: date,
        alert_since: date | None = None, require_today: bool = True) -> Report:
    report = Report()
    criteria = load_criteria(criteria_dir, report)
    days = load_results(results_dir, report)
    check_expected_days(days, today, report, alert_since, require_today)
    for day in sorted(d for d, doc in days.items() if doc):
        for entry in days[day]["checks"]:
            if (entry["issue"], entry["attempt"]) not in criteria:
                report.summary.append(f"이슈 #{entry['issue']} 시도 {entry['attempt']} {day}: 기준 파일이 없어 세지 않음")
    for c in sorted(criteria.values(), key=lambda x: (x.issue, x.attempt)):
        judge_criteria(c, days, issues.get(str(c.issue)), repo, report, alert_since)
    # 라벨 변경은 같은 이슈의 여러 시도에서 겹칠 수 있어 중복만 없앤다.
    for todo in report.todo.values():
        todo.remove_labels = list(dict.fromkeys(todo.remove_labels))
        todo.add_labels = list(dict.fromkeys(todo.add_labels))
    return report


def write_output(report: Report, out: Path) -> None:
    issues = {str(n): {"comments": t.comments, "remove_labels": t.remove_labels, "add_labels": t.add_labels}
              for n, t in sorted(report.todo.items()) if t.comments or t.remove_labels or t.add_labels}
    out.write_text(json.dumps({"issues": issues, "summary": report.summary}, ensure_ascii=False, indent=2) + "\n",
                   encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--criteria-dir", type=Path, required=True)
    ap.add_argument("--issues", type=Path, required=True)
    ap.add_argument("--repo", type=Path, required=True)
    ap.add_argument("--today", help="YYYY-MM-DD(KST). 없으면 지금 KST 날짜")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--alert-since", help="YYYY-MM-DD. 이 날짜 이하의 빠진 평일·error 지표는 실패로 치지 않음")
    ap.add_argument("--manual", action="store_true", help="손 실행: 오늘 결과 파일이 없어도 실패로 치지 않음")
    a = ap.parse_args(argv)
    today = date.fromisoformat(a.today) if a.today else datetime.now(ZoneInfo("Asia/Seoul")).date()
    try:
        alert_since = date.fromisoformat(a.alert_since) if a.alert_since else None
    except ValueError:
        print("실패: --alert-since 날짜 형식 오류", file=sys.stderr)
        return 1
    try:
        issues = json.loads(a.issues.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        print("실패: 이슈 정보 파일 읽기 실패", file=sys.stderr)
        return 1
    report = run(a.results_dir, a.criteria_dir, issues, a.repo, today, alert_since, not a.manual)
    write_output(report, a.out)
    for line in report.summary:
        print(line)
    for line in report.failures:
        print(f"실패: {line}", file=sys.stderr)
    return 1 if report.failures else 0


if __name__ == "__main__":
    sys.exit(main())
