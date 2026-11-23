"""기준 파일(ops-checks/<이슈>-<시도>.yml) 읽기, 지문, 형식 검사. 이미지에 실려 배포되며 Kafka·DB는 모른다."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

# 판정 쪽(scripts/ops_verdict.py, ops-verdict 워크플로)은 .yml 만 읽는다. .yaml 은 재더라도 판정 댓글이 붙지 않으므로 형식 오류로 둔다.
FILE_NAME = re.compile(r"^(\d+)-([^/]+)\.(ya?ml)$")
PASS_EXPR = re.compile(r"^\s*(<=|>=|<|>|==)\s*(\d+(?:\.\d+)?)\s*$")
SQL_START = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
PERSONAL_SCHEMA = re.compile(r"\bpersonal\s*\.", re.IGNORECASE)


@dataclass(frozen=True)
class Metric:
    key: str
    sql: str
    pass_expr: str


@dataclass(frozen=True)
class Criteria:
    """파일 하나가 말하는 기준. error 가 있으면 형식이 틀린 파일이고 나머지 칸은 기본값이다."""
    issue: int
    attempt: str
    criteria_hash: str
    comment: bool = True
    days: int | None = None
    metrics: tuple[Metric, ...] = ()
    human: tuple[str, ...] = ()
    error: str | None = None


def fingerprint(data: bytes) -> str:
    """지문 = 파일 바이트의 SHA-256(16진 소문자)."""
    return hashlib.sha256(data).hexdigest()


def parse_pass(expr: str) -> tuple[str, Decimal]:
    """통과식('< 0.01')을 (연산자, 기준값)으로. 모양이 틀리거나 0~1 밖이면 ValueError."""
    match = PASS_EXPR.match(expr)
    if not match:
        raise ValueError("bad-pass-expression")
    limit = Decimal(match.group(2))
    if limit > 1:
        raise ValueError("bad-pass-expression")
    return match.group(1), limit


def _problem(doc: object, issue: int, attempt: str) -> str | None:
    """형식이 틀린 이유 종류(값 없이 이름만). 맞으면 None."""
    if not isinstance(doc, dict):
        return "not-a-mapping"
    if type(doc.get("issue")) is not int or doc["issue"] != issue:
        return "issue-mismatch"
    # YAML 이 `attempt: 1` 을 숫자로 읽으므로 파일 이름의 문자열과 문자열로 비교한다.
    raw_attempt = doc.get("attempt")
    if isinstance(raw_attempt, bool) or not isinstance(raw_attempt, (str, int)) or str(raw_attempt) != attempt:
        return "attempt-mismatch"
    if "comment" in doc and not isinstance(doc["comment"], bool):
        return "bad-comment"
    if not isinstance(doc.get("fix_commit"), str) or not doc["fix_commit"]:
        return "bad-fix-commit"
    if type(doc.get("days")) is not int or doc["days"] <= 0:
        return "bad-days"
    metrics = doc.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        return "no-metrics"
    keys = set()
    for metric in metrics:
        if not isinstance(metric, dict):
            return "bad-metric"
        key, sql, expr = metric.get("key"), metric.get("sql"), metric.get("pass")
        if not isinstance(key, str) or not key or key in keys:
            return "bad-metric-key"
        keys.add(key)
        if not isinstance(sql, str) or not SQL_START.match(sql) or PERSONAL_SCHEMA.search(sql):
            return "bad-sql"
        # 읽기 전용 트랜잭션을 SQL 안의 COMMIT 으로 벗어나지 못하게 문장 하나만 받는다.
        if ";" in sql.strip().rstrip(";"):
            return "bad-sql"
        if not isinstance(expr, str):
            return "bad-pass-expression"
        try:
            parse_pass(expr)
        except (ValueError, InvalidOperation):
            return "bad-pass-expression"
    human = doc.get("human", [])
    if human is None:
        human = []
    if not isinstance(human, list) or not all(isinstance(h, str) for h in human):
        return "bad-human"
    return None


def load_file(path: Path) -> Criteria | None:
    """기준 파일 하나. 이름이 <이슈>-<시도>.yml 꼴이 아니면 기준 파일이 아니므로 None."""
    match = FILE_NAME.match(path.name)
    if not match:
        return None
    issue, attempt = int(match.group(1)), match.group(2)
    data = path.read_bytes()
    digest = fingerprint(data)
    if match.group(3) != "yml":
        return Criteria(issue, attempt, digest, error="bad-extension")
    try:
        doc = yaml.safe_load(data)
    except yaml.YAMLError:
        return Criteria(issue, attempt, digest, error="invalid-yaml")
    problem = _problem(doc, issue, attempt)
    if problem:
        return Criteria(issue, attempt, digest, error=problem)
    return Criteria(
        issue=issue, attempt=attempt, criteria_hash=digest,
        comment=doc.get("comment", True), days=doc["days"],
        metrics=tuple(Metric(m["key"], m["sql"], m["pass"]) for m in doc["metrics"]),
        human=tuple(doc.get("human") or ()),
    )


def load_all(directory: Path) -> list[Criteria]:
    """폴더의 기준 파일 전부(이름순). 폴더가 없으면 빈 목록."""
    if not directory.is_dir():
        return []
    loaded = (load_file(p) for p in sorted(directory.iterdir()) if p.is_file())
    return [c for c in loaded if c is not None]
