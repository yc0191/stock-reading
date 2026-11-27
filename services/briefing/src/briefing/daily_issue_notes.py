"""ETF 이유 끝의 '(금리 이슈 참고)'가 내용 없는 금리 이슈 칸을 가리키지 않게 한다.

LLM이 ETF 이유에는 금리 칸을 보라고 붙여 놓고 금리 칸에는 기사가 없다고 쓰는 일이 있다. 금리 칸이 근거 번호 없이 나가는 판이면
보라는 표시만 지운다. ETF 이유 문장 자체는 바꾸지 않는다.
"""
from __future__ import annotations

import re
from dataclasses import replace

from briefing.daily_output import POINT_RATE, DailyOutput, Point
from briefing.llm_output import MERGED_ISSUE_NOTE

FX_NOTE = "(환율 이슈 참고)"
_RATE_NOTE = re.compile(r"\s*\(금리 이슈 참고\)")


def settle_rate_notes(output: DailyOutput) -> DailyOutput:
    """금리 이슈 칸이 없거나 근거 번호가 없으면 ETF 이유의 금리 참고 표시를 지우고, 환율·금리 표시는 환율만 남긴다."""
    rate = next((p for p in output.issues if p.kind == POINT_RATE), None)
    if rate is not None and rate.refs:
        return output
    return replace(output, points=_without(output.points), outlooks=_without(output.outlooks),
                   reviews=_without(output.reviews))


def _without(points: tuple[Point, ...]) -> tuple[Point, ...]:
    return tuple(replace(p, statement=_strip(p.statement)) for p in points)


def _strip(text: str) -> str:
    """표시만 있던 문장은 지우면 비므로 그대로 둔다."""
    stripped = _RATE_NOTE.sub("", text.replace(MERGED_ISSUE_NOTE, FX_NOTE))
    return stripped if stripped.strip() else text
