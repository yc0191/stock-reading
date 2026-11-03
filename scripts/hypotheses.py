"""가설 레지스트리: 검증 실행(verify)과 README 표 생성(render).

원본은 docs/hypotheses/H###-*/hypothesis.toml 과 그 아래 results/*.json 이다.
README 의 가설 표는 이 파일들에서 만든 사본이며, 판정(증명됨/기각됨/판정 보류)은 규칙에서 코드가 계산한다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import shlex
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
START = "<!-- hypotheses:start -->"
END = "<!-- hypotheses:end -->"
LABEL_TEXT = {"supported": "증명됨", "rejected": "기각됨", "inconclusive": "판정 보류", "in_progress": "증명 진행 중"}


# ---------------------------------------------------------------- 판정

def lower_bound(successes: int, n: int, alpha: float) -> float:
    """정확 이항(Clopper-Pearson) 단측 하한: P(X >= successes | p) = alpha 인 p."""
    if n <= 0 or successes <= 0:
        return 0.0

    def tail(p: float) -> float:
        return sum(math.exp(math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1)
                            + i * math.log(p) + (n - i) * math.log1p(-p)) for i in range(successes, n + 1))

    lo, hi = 0.0, 1.0
    for _ in range(80):
        mid = (lo + hi) / 2
        lo, hi = (lo, mid) if tail(mid) > alpha else (mid, hi)
    return lo


def evaluate_measure(rule: dict, n_answered: int, n_correct: int, n_total: int) -> dict:
    coverage = n_answered / n_total if n_total else 0.0
    accuracy = n_correct / n_answered if n_answered else 0.0
    lb = lower_bound(n_correct, n_answered, 1 - rule["confidence"])
    if n_total < rule["min_total"]:
        verdict = "inconclusive"
    elif coverage < rule["min_coverage"]:
        verdict = "rejected"
    elif lb >= rule["target"]:
        verdict = "supported"
    elif accuracy >= rule["target"]:
        verdict = "inconclusive"
    else:
        verdict = "rejected"
    return {"n_total": n_total, "n_answered": n_answered, "n_correct": n_correct,
            "coverage": coverage, "accuracy": accuracy, "lower_bound": lb, "verdict": verdict}


def combine(verdicts: list[str]) -> str:
    if "rejected" in verdicts:
        return "rejected"
    return "supported" if verdicts and all(v == "supported" for v in verdicts) else "inconclusive"


# ---------------------------------------------------------------- 로딩과 상태

def rule_hash(rule: dict) -> str:
    return hashlib.sha256(json.dumps(rule, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def discover(hyp_dir: Path) -> list[dict]:
    items = []
    for folder in sorted(p for p in hyp_dir.glob("H*-*") if (p / "hypothesis.toml").is_file()):
        with open(folder / "hypothesis.toml", "rb") as f:
            definition = tomllib.load(f)
        results = [json.loads(p.read_text(encoding="utf-8")) for p in sorted((folder / "results").glob("*.json"))]
        items.append({"folder": folder, "def": definition, "results": results})
    return items


def is_complete(h: dict) -> bool:
    """판정 기준이 완성되어 있는가. 하나라도 비면 README 에 노출하지 않는다."""
    if h.get("state") != "active" or not all(str(h.get(k, "")).strip() for k in ("id", "title", "claim")):
        return False
    rule, verify = h.get("rule", {}), h.get("verify", {})
    if not str(verify.get("command", "")).strip():
        return False
    if rule.get("kind") == "test":
        return True
    if rule.get("kind") == "proportion":
        return (all(k in rule for k in ("target", "confidence", "min_coverage", "min_total", "measures"))
                and bool(rule["measures"]) and 0 < rule["target"] < 1 and 0 < rule["confidence"] < 1)
    return False


def status_of(item: dict) -> dict:
    """label: hidden | in_progress | supported | rejected | inconclusive.

    판정 기준(rule)이 바뀐 뒤에는 이전 결과를 쓰지 않는다. 결과에 기준의 해시가 있어 알 수 있다.
    """
    h = item["def"]
    if not is_complete(h):
        return {"label": "hidden", "result": None}
    current = [r for r in item["results"] if r.get("rule_sha256") == rule_hash(h["rule"])]
    if not current:
        return {"label": "in_progress", "result": None}
    latest = max(current, key=lambda r: r["run_at"])
    return {"label": latest["verdict"], "result": latest}


# ---------------------------------------------------------------- README 표

def evidence_text(h: dict, result: dict) -> str:
    if h["rule"]["kind"] == "proportion":
        parts = [f"{m['name']}: 정확도 {m['accuracy']:.1%} (신뢰하한 {m['lower_bound']:.1%}, "
                 f"답한 {m['n_answered']}건), 커버리지 {m['coverage']:.0%}" for m in result["measures"]]
    else:
        parts = ["테스트 통과" if result["exit_code"] == 0 else "테스트 실패"]
    parts.append(f"실행 {result['run_at'][:10]}, 커밋 `{result['commit']}`")
    if h.get("verify", {}).get("needs") == "local-data":
        parts.append("재현: 로컬 데이터 필요")
    else:
        parts.append(f"재현: `{h['verify']['command']}`")
    return "<br>".join(parts)


def render_table(items: list[dict]) -> str:
    rows = []
    for item in items:
        status, h = status_of(item), item["def"]
        if status["label"] == "hidden":
            continue
        title = f"**{h['id']}** {h['title']}<br>{h['claim']}"
        evidence = "" if status["label"] == "in_progress" else evidence_text(h, status["result"])
        rows.append(f"| {title} | {LABEL_TEXT[status['label']]} | {evidence} |")
    if not rows:
        return "아직 공개할 가설이 없습니다."
    return "\n".join(["| 가설 | 상태 | 근거 |", "|---|---|---|", *rows])


def render_readme(hyp_dir: Path, readme: Path, check: bool) -> bool:
    """README 의 표시 구간을 다시 만든다. 내용이 달라졌으면 True (check 이면 쓰지 않는다)."""
    old = readme.read_text(encoding="utf-8")
    if START not in old or END not in old or old.index(START) > old.index(END):
        raise ValueError(f"{readme} 에 {START} 와 {END} 표시가 필요합니다")
    head, rest = old.split(START, 1)
    _, tail = rest.split(END, 1)
    new = f"{head}{START}\n{render_table(discover(hyp_dir))}\n{END}{tail}"
    if new != old and not check:
        readme.write_text(new, encoding="utf-8")
    return new != old


# ---------------------------------------------------------------- 검증 실행

def verify(item: dict, root: Path, run=subprocess.run, now: datetime | None = None) -> Path:
    """검증 명령을 실행하고, 판정을 코드로 계산해 results/ 에 집계만 저장한다."""
    h = item["def"]
    if not is_complete(h):
        raise ValueError("판정 기준이 완성된 active 가설만 검증할 수 있습니다")
    rule, now = h["rule"], now or datetime.now(timezone.utc)
    proc = run(shlex.split(h["verify"]["command"]), capture_output=True, text=True, cwd=root, timeout=3600)
    commit = run(["git", "-C", str(root), "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
    record = {"hypothesis": h["id"], "run_at": now.isoformat(timespec="seconds"),
              "commit": commit.stdout.strip() or "unknown", "rule_sha256": rule_hash(rule)}
    if rule["kind"] == "test":
        record["exit_code"] = proc.returncode
        record["verdict"] = "supported" if proc.returncode == 0 else "rejected"
    else:
        if proc.returncode != 0:
            raise RuntimeError(f"검증 명령이 실패했습니다 (종료 코드 {proc.returncode})")
        reported = json.loads(proc.stdout).get("measures", {})
        missing = [n for n in rule["measures"] if n not in reported]
        if missing:
            raise RuntimeError(f"검증 명령의 출력에 측정이 없습니다: {missing}")
        record["measures"] = [{"name": n, **evaluate_measure(rule, **{k: reported[n][k] for k in ("n_answered", "n_correct", "n_total")})}
                              for n in rule["measures"]]
        record["verdict"] = combine([m["verdict"] for m in record["measures"]])
    results = item["folder"] / "results"
    results.mkdir(exist_ok=True)
    path = results / f"{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------- 명령줄

def main(argv: list[str] | None = None, root: Path = ROOT, run=subprocess.run) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("render", help="README 의 가설 표를 다시 만든다")
    r.add_argument("--check", action="store_true", help="쓰지 않고 표가 최신인지만 확인한다 (최신이 아니면 종료 코드 1)")
    v = sub.add_parser("verify", help="가설의 검증 명령을 실행하고 판정을 기록한다")
    v.add_argument("id")
    args = parser.parse_args(argv)
    hyp_dir, readme = root / "docs" / "hypotheses", root / "README.md"

    if args.cmd == "render":
        changed = render_readme(hyp_dir, readme, check=args.check)
        if args.check and changed:
            print("README 의 가설 표가 최신이 아닙니다: python3 scripts/hypotheses.py render")
            return 1
        print("갱신" if changed and not args.check else "변경 없음")
        return 0
    item = next((i for i in discover(hyp_dir) if i["def"].get("id") == args.id), None)
    if item is None:
        print("없는 가설:", args.id)
        return 1
    try:
        path = verify(item, root, run)
    except (ValueError, RuntimeError) as error:
        print("중단:", error)
        return 1
    print("결과 저장:", path.relative_to(root), "→ python3 scripts/hypotheses.py render")
    return 0


if __name__ == "__main__":
    sys.exit(main())
