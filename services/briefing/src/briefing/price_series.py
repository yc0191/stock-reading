"""데일리 판 그래프용 가격 자료(장중 흐름, 기간 종가). Kafka를 모른다.

값은 시세처리기가 저장한 시세·국내 일봉으로 만들어 데일리 자료 조회 응답의 ETF 선택 칸 intradaySeries·dailyCloses로 준다.
이 모듈은 그 칸을 읽고, 읽을 장중 시세 범위를 정하고, 받은 값이 범위 안인지 다시 검사한다. 그래프에만 쓰며 LLM 입력에는 넣지 않는다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from briefing.compose import KST
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import parse_timestamp

STATUS_PRESENT = "present"


@dataclass(frozen=True)
class IntradayPoint:
    """장중 흐름의 한 점(1분에 한 점, 그 분의 마지막 시세). 등락률은 전일 종가를 모르면 None이다."""

    at: datetime
    price: str
    rate_vs_prev_close: str | None


@dataclass(frozen=True)
class IntradaySeries:
    """ETF 하나의 장중 흐름. 범위 안 시세가 없으면 present가 False이고 points가 비어 있다. points는 시각 오름차순이다."""

    present: bool
    prev_close: str | None = None
    truncated: bool = False
    points: tuple[IntradayPoint, ...] = ()


@dataclass(frozen=True)
class DailyClose:
    """기간 종가의 하루. 값은 일봉 원문 그대로이고 date는 yyyyMMdd다."""

    date: str
    close: str
    rate: str | None


@dataclass(frozen=True)
class DailyCloses:
    """ETF 하나의 기간 종가. 쓸 일봉이 없으면 present가 False이고 points가 비어 있다. points는 일자 오름차순이다.
    fetched_at은 쓴 일봉 원문 중 가장 늦게 받은 것의 받은 시각이다."""

    present: bool
    fetched_at: datetime | None = None
    truncated: bool = False
    points: tuple[DailyClose, ...] = ()


def intraday_series_start(scheduled_for: datetime, settings: CoreSettings = DEFAULT_SETTINGS) -> datetime | None:
    """장중 흐름을 읽을 구간의 시작. 판 날짜의 장 시작 시각이며 판 시각에서 거슬러 오르는 길이는 상한까지만이다.
    판 시각이 장 시작 전이면 읽을 시세가 없으므로 None이다."""
    local = scheduled_for.astimezone(KST)
    start = max(datetime.combine(local.date(), settings.intraday_series_start, KST),
                local - settings.intraday_series_max_window)
    return start if start <= local else None


def parse_intraday_series(raw: dict[str, Any] | None) -> IntradaySeries | None:
    """응답의 intradaySeries 칸. 칸이 없으면(이전 형식 응답, 요청하지 않은 판) None. 형식이 틀리면 KeyError 등."""
    if raw is None:
        return None
    points = tuple(IntradayPoint(parse_timestamp(p["at"]), p["price"], p.get("rateVsPrevClose"))
                   for p in raw["points"])
    return IntradaySeries(raw["status"] == STATUS_PRESENT, raw.get("prevClose"), bool(raw["truncated"]), points)


def parse_daily_closes(raw: dict[str, Any] | None) -> DailyCloses | None:
    """응답의 dailyCloses 칸. 칸이 없으면(이전 형식 응답, 요청하지 않은 판) None. 형식이 틀리면 KeyError 등."""
    if raw is None:
        return None
    fetched = raw.get("fetchedAt")
    points = tuple(DailyClose(p["date"], p["close"], p.get("rate")) for p in raw["points"])
    return DailyCloses(raw["status"] == STATUS_PRESENT, parse_timestamp(fetched) if fetched is not None else None,
                       bool(raw["truncated"]), points)


def intraday_in_range(series: IntradaySeries, start: datetime, as_of: datetime, max_points: int) -> bool:
    """점이 모두 [구간 시작, 판 시각] 안이고 시각 오름차순이며 상한 이하인가. 값이 없는 흐름은 버릴 것이 없으므로 안이다."""
    if not series.present:
        return not series.points
    if not series.points or len(series.points) > max_points:
        return False
    times = [p.at for p in series.points]
    return start <= times[0] and times[-1] <= as_of and all(a < b for a, b in zip(times, times[1:]))


def closes_in_range(closes: DailyCloses, range_start: datetime, as_of: datetime, max_points: int) -> bool:
    """쓴 일봉을 받은 시각이 [판 범위 시작, 판 시각] 안이고, 일자가 판 날짜 이하로 겹치지 않게 오름차순이며 상한 이하인가.
    상태형 자료라 판 시각 뒤에 받은 것과 판 범위보다 오래된 것은 쓰지 않는다. 값이 없는 종가는 버릴 것이 없으므로 안이다."""
    if not closes.present:
        return not closes.points
    if closes.fetched_at is None or not range_start <= closes.fetched_at <= as_of:
        return False
    if not closes.points or len(closes.points) > max_points:
        return False
    dates = [p.date for p in closes.points]
    last_date = as_of.astimezone(KST).strftime("%Y%m%d")
    return dates[-1] <= last_date and all(a < b for a, b in zip(dates, dates[1:]))
