"""마감 후 판의 아침 예상 대조와 그날 급변동 요약에 쓰는 값. 맞음·빗나감은 코드가 정하고 Claude는 이유만 쓴다. Kafka와 DB를 모른다."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from briefing.daily_inputs import decimal_rate
from briefing.daily_prompts import DIRECTION_DOWN, DIRECTION_FLAT, DIRECTION_UNSURE, DIRECTION_UP, DIRECTIONS
from briefing.job_log import MoveRun

VERDICT_RIGHT = "맞음"
VERDICT_WRONG = "빗나감"
# 방향을 정하지 못한 ETF는 맞고 틀림을 가릴 수 없어 대조하지 않는다.
NOT_COMPARED = "대조 안 함(예상 어려움)"

# 대조를 뺀 이유(본문에 보이고 작업 기록에도 남는다)
NO_MORNING_CALLS = "같은 날 아침 판의 ETF별 예상 기록이 없어 대조하지 못했습니다."
MORNING_READ_FAILED = "아침 판의 ETF별 예상 기록을 읽지 못해 대조하지 못했습니다."
NO_ACTUAL_RATES = "실제 등락 자료가 없어 대조하지 못했습니다."


@dataclass(frozen=True)
class MorningCall:
    """같은 날 아침 판이 쓴 ETF 하나의 방향 예상."""

    symbol: str
    direction: str
    reason: str


@dataclass(frozen=True)
class ReviewInput:
    """마감 후 판이 작업 기록에서 읽은 값. calls가 비면 note가 대조를 뺀 이유이고, moves가 None이면 읽지 못한 것이다."""

    calls: tuple[MorningCall, ...]
    note: str | None
    moves: tuple[MoveRun, ...] | None


@dataclass(frozen=True)
class Review:
    """ETF 하나의 대조 결과. verdict가 None이면 대조하지 않은 것(예상 어려움)이고 그때는 rate가 없을 수 있다."""

    symbol: str
    call: MorningCall
    rate: Decimal | None
    verdict: str | None


def parse_calls(records: Iterable[Mapping[str, Any]]) -> tuple[MorningCall, ...]:
    """etf-outlook 줄의 data(기록이 늦은 것부터)에서 가장 최근의 쓸 수 있는 예상. 방향이나 종목이 깨진 항목은 버리고, 남는 것이 없으면 앞 기록으로 넘어간다."""
    for data in records:
        outlooks = data.get("outlooks")
        calls = []
        for raw in outlooks if isinstance(outlooks, list) else ():
            if isinstance(raw, dict) and isinstance(raw.get("symbol"), str) and raw.get("direction") in DIRECTIONS \
                    and isinstance(raw.get("reason"), str):
                calls.append(MorningCall(raw["symbol"], raw["direction"], raw["reason"]))
        if calls:
            return tuple(calls)
    return ()


def verdict_of(direction: str, rate: Decimal, flat_band_percent: float) -> str | None:
    """상승·하락 예상은 실제 등락의 부호로, 보합 예상은 등락 절댓값이 flat_band_percent 이하인지로 가른다."""
    if direction == DIRECTION_UNSURE:
        return None
    if direction == DIRECTION_UP:
        hit = rate > 0
    elif direction == DIRECTION_DOWN:
        hit = rate < 0
    elif direction == DIRECTION_FLAT:
        hit = abs(rate) <= Decimal(str(flat_band_percent))
    else:
        return None
    return VERDICT_RIGHT if hit else VERDICT_WRONG


def review_rows(etfs: Iterable[Any], calls: Iterable[MorningCall], flat_band_percent: float) -> tuple[Review, ...]:
    """ETF 순서대로 아침 예상과 그날 실제 등락을 대조한다. 예상이 없는 ETF와, 일봉이 없어 실제 등락을 모르는 ETF는 뺀다."""
    by_symbol: dict[str, MorningCall] = {}
    for call in calls:
        by_symbol.setdefault(call.symbol, call)
    rows = []
    for etf in etfs:
        call = by_symbol.get(etf.symbol)
        if call is None:
            continue
        if call.direction == DIRECTION_UNSURE:
            rows.append(Review(etf.symbol, call, None, None))
            continue
        rate = decimal_rate(etf.rate) if etf.has_day() else None
        if rate is not None:
            rows.append(Review(etf.symbol, call, rate, verdict_of(call.direction, rate, flat_band_percent)))
    return tuple(rows)
