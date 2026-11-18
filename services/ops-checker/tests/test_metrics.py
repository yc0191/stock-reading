from decimal import Decimal

import pytest

from fakes import FakeQuery
from ops_checker.criteria import Metric
from ops_checker.metrics import ERROR, FAIL, HOLD, PASS, MetricError, evaluate, judge

LT = Metric("m", "SELECT x", "< 0.01")
GE = Metric("m", "SELECT x", ">= 0.05")


def test_value_meeting_the_expression_passes_and_the_boundary_is_exact():
    assert judge(LT, [(Decimal("0.0099"),)]).verdict == PASS
    assert judge(LT, [(Decimal("0.01"),)]).verdict == FAIL      # '<' 는 같은 값을 통과시키지 않는다
    assert judge(GE, [(Decimal("0.05"),)]).verdict == PASS      # '>=' 는 같은 값을 통과시킨다
    assert judge(GE, [(0.04,)]).verdict == FAIL


def test_float_values_do_not_pick_up_binary_noise():
    assert judge(Metric("m", "x", "== 0.1"), [(0.1,)]).verdict == PASS


def test_empty_value_holds_with_a_reason():
    result = judge(LT, [(None,)])
    assert (result.verdict, result.reason, result.value) == (HOLD, "no-value", None)


@pytest.mark.parametrize("rows, reason", [
    ([], "bad-shape"),
    ([(0.1,), (0.2,)], "bad-shape"),
    ([(0.1, 0.2)], "bad-shape"),
    ([("abc",)], "bad-shape"),
    ([(True,)], "bad-shape"),
    ([(1.5,)], "out-of-range"),
    ([(-0.1,)], "out-of-range"),
    ([(float("nan"),)], "out-of-range"),
])
def test_wrong_shape_or_range_is_an_error(rows, reason):
    result = judge(LT, rows)
    assert (result.verdict, result.reason) == (ERROR, reason)


def test_sql_failure_and_timeout_become_errors_with_only_the_kind():
    for kind in ("sql-failed", "timeout", "read-only-violation"):
        result = evaluate(LT, FakeQuery({"SELECT": MetricError(kind)}))
        assert (result.verdict, result.reason, result.value) == (ERROR, kind, None)


def test_pass_result_keeps_value_and_threshold():
    result = evaluate(LT, FakeQuery({"SELECT": [(Decimal("0"),)]}))
    assert (result.verdict, result.value, result.threshold) == (PASS, Decimal("0"), "< 0.01")
