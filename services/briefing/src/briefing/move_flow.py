"""'오늘 흐름': 그날 같은 종목의 앞선 급변동을 시각순으로 모은다. 코드가 작업 기록에서 만들어 메일 본문과 리포트 LLM 입력에 넣는다.

Kafka와 DB를 모르며 작업 기록에서 읽은 급변동(MoveRun)을 인자로 받는다. 상은 메일을 보냈고 중은 기록만 남겼다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Sequence

from briefing.compose import KST
from briefing.job_log import LEVEL_HIGH, MoveRun

FLOW_TITLE = "오늘 흐름"
LEVEL_NAMES = {LEVEL_HIGH: "상, 메일 보냄"}
LEVEL_MID_NAME = "중, 기록만"


@dataclass(frozen=True)
class FlowMove:
    """앞선 급변동 한 건. rate는 그때의 오늘 등락률(%, 전일 종가 대비)이고 모르면 변동률이다."""

    at: datetime
    rate: float
    level: str

    def line(self) -> str:
        return f"{self.at.astimezone(KST):%H:%M} {self.rate:+.2f}% ({LEVEL_NAMES.get(self.level, LEVEL_MID_NAME)})"


def day_range(as_of: datetime) -> tuple[datetime, datetime]:
    """변동 시각이 속한 한국 날짜의 [00:00, 다음 날 00:00). 작업은 변동 시각 뒤에 시작하므로 끝을 하루 끝까지 둔다."""
    start = as_of.astimezone(KST).replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def earlier_moves(runs: Sequence[MoveRun] | None, symbol: str, as_of: datetime, job_id: str) -> tuple[FlowMove, ...]:
    """같은 종목에서 이 변동보다 앞선 변동을 시각순으로. 읽지 못했으면(None) 비어 있다. 같은 시각의 변동은 앞선 것이 아니다."""
    found = []
    for run in runs or ():
        if run.symbol != symbol or run.job_id == job_id or run.move_at >= as_of:
            continue
        rate = run.today_change_percent if run.today_change_percent is not None else run.change_percent
        if rate is not None:
            found.append(FlowMove(run.move_at, rate, run.level))
    return tuple(sorted(found, key=lambda m: m.at))


def flow_input(moves: Sequence[FlowMove]) -> list[dict[str, Any]]:
    """LLM 입력의 todayFlow. 근거 번호가 없어 인용하지 않는 참고 자료다."""
    return [{"at": f"{m.at.astimezone(KST):%H:%M}", "todayChangePercent": round(m.rate, 2),
             "level": "high" if m.level == LEVEL_HIGH else "mid"} for m in moves]
