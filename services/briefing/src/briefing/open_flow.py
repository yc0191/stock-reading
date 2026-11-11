"""아침 판의 개장 흐름(전일 종가 대비 시가와 장 시작 뒤 판 시작 시각까지의 움직임). Kafka를 모른다.

값은 시세처리기가 저장한 시세로 계산해 market.daily.responded의 ETF 선택 칸 openFlow로 준다. 이 모듈은 그 칸을 읽고,
읽을 시세 범위를 정하고, 받은 값이 범위 안인지 다시 검사한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from briefing.compose import KST
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import parse_timestamp

OPEN_SOURCE_RAW = "raw"
OPEN_SOURCE_FIRST_SNAPSHOT = "first-snapshot"
STATUS_PRESENT = "present"
OPEN_SOURCE_TEXT = {OPEN_SOURCE_RAW: "시세 원문의 시가", OPEN_SOURCE_FIRST_SNAPSHOT: "장 시작 뒤 첫 시세(시가 칸 없음)"}


@dataclass(frozen=True)
class OpenFlow:
    """ETF 하나의 개장 흐름. 값이 없으면 None이고, 범위 안 시세가 없으면 present가 False다."""

    present: bool
    prev_close: str | None = None
    open: str | None = None
    open_source: str | None = None
    open_rate: str | None = None
    start_at: datetime | None = None
    start_price: str | None = None
    end_at: datetime | None = None
    end_price: str | None = None
    flow_rate: str | None = None
    rate_vs_prev_close: str | None = None


def open_flow_start(scheduled_for: datetime, settings: CoreSettings = DEFAULT_SETTINGS) -> datetime | None:
    """개장 시세를 읽을 구간의 시작. 판 날짜의 장 시작 시각이며 판 시각에서 거슬러 오르는 길이는 상한까지만이다.
    판 시각이 장 시작 전이면 읽을 시세가 없으므로 None이다."""
    local = scheduled_for.astimezone(KST)
    start = max(datetime.combine(local.date(), settings.open_flow_start, KST), local - settings.open_flow_max_window)
    return start if start <= local else None


def parse_open_flow(raw: dict[str, Any] | None) -> OpenFlow | None:
    """응답의 openFlow 칸. 칸이 없으면(이전 형식 응답, 아침 판이 아닌 요청) None."""
    if raw is None:
        return None

    def at(key: str) -> datetime | None:
        value = raw.get(key)
        return parse_timestamp(value) if value is not None else None

    return OpenFlow(raw["status"] == STATUS_PRESENT, raw.get("prevClose"), raw.get("open"), raw.get("openSource"),
                    raw.get("openRate"), at("startAt"), raw.get("startPrice"), at("endAt"), raw.get("endPrice"),
                    raw.get("flowRate"), raw.get("rateVsPrevClose"))


def in_range(flow: OpenFlow, start: datetime, as_of: datetime) -> bool:
    """시세 시각이 [구간 시작, 판 시각] 안인가. 값이 없는 개장 흐름은 버릴 것이 없으므로 안이다."""
    if not flow.present:
        return True
    return flow.start_at is not None and flow.end_at is not None \
        and start <= flow.start_at <= flow.end_at <= as_of


def signed(rate: str | None) -> str:
    """등락률 문자열에 부호를 붙여 보인다. 모르면 자료 없음."""
    if rate is None:
        return "자료 없음"
    return f"+{rate}%" if not rate.startswith(("+", "-")) and rate not in ("0", "0.00") else f"{rate}%"
