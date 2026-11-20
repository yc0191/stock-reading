"""지표 하나를 계산해 판정한다. SQL 실행은 ScalarQuery 뒤에 두어 DB 없이 시험한다."""
from __future__ import annotations

import operator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Protocol

from ops_checker.criteria import Metric, parse_pass

PASS, FAIL, HOLD, ERROR = "pass", "fail", "hold", "error"
# 급변동마다 앞뒤 시세를 모으는 지표가 장 마감 직후 10초를 넘었다. 지표가 모두 상한까지 걸려도
# Kafka 소비자의 poll 간격 기본 상한(300초) 안에 끝나도록 지표 수를 생각해 30초로 둔다.
SQL_TIMEOUT_SECONDS = 30

_COMPARE = {"<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge, "==": operator.eq}


class MetricError(Exception):
    """SQL 이 실패했다. reason 은 종류 이름뿐이고 SQL 본문·값은 담지 않는다(sql-failed, timeout, read-only-violation)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class ScalarQuery(Protocol):
    def scalar(self, sql: str) -> list[tuple]:
        """SQL 을 읽기 전용 트랜잭션에서 돌려 행 목록을 돌려준다. 실패하면 MetricError."""


@dataclass(frozen=True)
class MetricResult:
    key: str
    threshold: str
    verdict: str
    value: Decimal | None = None
    reason: str | None = None


def judge(metric: Metric, rows: list[tuple]) -> MetricResult:
    """결과 모양이 한 행 한 칸의 0~1 비율이면 통과식과 비교하고, 비었으면 hold, 모양이 틀리면 error."""
    if len(rows) != 1 or len(rows[0]) != 1:
        return MetricResult(metric.key, metric.pass_expr, ERROR, reason="bad-shape")
    raw = rows[0][0]
    if raw is None:
        return MetricResult(metric.key, metric.pass_expr, HOLD, reason="no-value")
    if isinstance(raw, bool):
        return MetricResult(metric.key, metric.pass_expr, ERROR, reason="bad-shape")
    try:
        value = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except InvalidOperation:
        return MetricResult(metric.key, metric.pass_expr, ERROR, reason="bad-shape")
    if not value.is_finite() or not Decimal(0) <= value <= Decimal(1):
        return MetricResult(metric.key, metric.pass_expr, ERROR, reason="out-of-range")
    op, limit = parse_pass(metric.pass_expr)
    verdict = PASS if _COMPARE[op](value, limit) else FAIL
    return MetricResult(metric.key, metric.pass_expr, verdict, value=value)


def evaluate(metric: Metric, query: ScalarQuery) -> MetricResult:
    try:
        rows = query.scalar(metric.sql)
    except MetricError as error:
        return MetricResult(metric.key, metric.pass_expr, ERROR, reason=error.reason)
    return judge(metric, rows)
