"""급변동 리포트에 넣는 배경 숫자와 비교 숫자. Kafka를 모르며 리포트 입력을 인자로 받는다.

배경 원문(환율종합, 시장종합)에서 원달러 환율과 코스피·코스닥 숫자를 꺼내고, 변동률과의 비교 숫자를 코드가 계산한다.
LLM은 이 숫자를 새로 계산하지 않고 근거 번호로 인용만 한다. 저장 원문은 바꾸지 않고 꺼낸 값은 파생값으로만 쓴다.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Sequence

from briefing.compose import KST
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.sources import BACKGROUND_OK, MARK_MISSING, Background, CloseRate

if TYPE_CHECKING:
    from briefing.jobs import ReportInput

EXCHANGE_RATE_API = "exchange-rate-summary"
MARKET_SUMMARY_API = "market-summary"

# 원문에서 행을 찾는 표시 이름. 제공자 고유 코드값이 아니라 사람이 읽는 이름으로 찾는다.
USD_KRW_NAME = "원/달러"
KOSPI_NAME = "KOSPI종합"
KOSDAQ_NAME = "KOSDAQ종합"

# 방향 칸 값. 1 상한·2 상승은 오름, 4 하한·5 하락은 내림, 3은 보합이다.
_DIRECTION_SIGN = {"1": 1, "2": 1, "3": 0, "4": -1, "5": -1}

# 비교 숫자는 LLM 입력의 changePercent와 같이 소수 둘째 자리에서 반올림한다(사사오입).
CENT = Decimal("0.01")


@dataclass(frozen=True)
class ExchangeRateFigure:
    """원달러 환율. 값이 없는 칸은 None이다."""

    value: Decimal | None
    change: Decimal | None
    rate: Decimal | None
    # 최종 수신 시각 HH:mm:ss. 원문의 6자리를 읽기 쉽게 나눈다.
    received_time: str | None
    fetched_at: datetime | None


@dataclass(frozen=True)
class IndexFigure:
    """국내 지수(코스피종합, 코스닥종합). 전일대비는 방향 칸으로 부호를 붙인 값이다."""

    name: str
    value: Decimal | None
    change: Decimal | None
    rate: Decimal | None
    fetched_at: datetime | None


@dataclass(frozen=True)
class UsCloseSummary:
    """담긴 미국 종목의 직전 정규장 등락률 집계. 숫자로 읽은 등락률만 센다."""

    up: int
    down: int
    flat: int
    average: Decimal
    latest_close_at: datetime


@dataclass(frozen=True)
class MoveFigures:
    exchange_rate: ExchangeRateFigure | None
    kospi: IndexFigure | None
    kosdaq: IndexFigure | None
    # 비교한 등락률 - 지수 등락률(%p). 필요한 값이 없으면 None이다.
    vs_kospi: Decimal | None
    vs_kosdaq: Decimal | None
    us_close: UsCloseSummary | None
    # 지수(시장종합)를 변동 시각보다 기준 넘게 앞서 받았으면 그 받은 시각. 아니면 None이다.
    stale_index_at: datetime | None = None


def to_decimal(text: Any) -> Decimal | None:
    """원문 숫자 문자열을 읽는다. 앞뒤 공백만 없애고, 비었거나 숫자가 아니면 None."""
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    if not stripped:
        return None
    try:
        value = Decimal(stripped)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def round_cent(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _rows(item: Background | None) -> list[dict]:
    """배경 원문의 본문 목록(out2). 원문이 없거나 모양이 다르면 빈 목록."""
    if item is None or item.status != BACKGROUND_OK or item.response is None:
        return []
    try:
        data = json.loads(item.response)
    except (json.JSONDecodeError, TypeError):
        return []
    body = data.get("dataBody") if isinstance(data, dict) else None
    rows = body.get("out2") if isinstance(body, dict) else None
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def _find(rows: list[dict], name_field: str, name: str) -> dict | None:
    for row in rows:
        value = row.get(name_field)
        if isinstance(value, str) and value.strip() == name:
            return row
    return None


def _received_time(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    if len(stripped) != 6 or not stripped.isdigit():
        return None
    return f"{stripped[0:2]}:{stripped[2:4]}:{stripped[4:6]}"


def _signed_change(row: dict) -> Decimal | None:
    """시장종합의 전일대비는 부호 없이 오고 방향은 방향 칸에만 있다. 방향을 모르면 None."""
    change = to_decimal(row.get("bdy_cmpr_p2"))
    sign = _DIRECTION_SIGN.get(str(row.get("bdy_cmpr_ccd", "")).strip())
    if change is None or sign is None:
        return None
    return abs(change) * sign


def _background(background: Sequence[Background], api_type: str) -> Background | None:
    return next((b for b in background if b.api_type == api_type), None)


def exchange_rate_figure(background: Sequence[Background]) -> ExchangeRateFigure | None:
    """환율종합 원문의 원/달러 행. 원문이나 행이 없으면 None."""
    item = _background(background, EXCHANGE_RATE_API)
    row = _find(_rows(item), "crncy_cd_nm", USD_KRW_NAME)
    if row is None:
        return None
    return ExchangeRateFigure(
        value=to_decimal(row.get("cls_prc_p4")),
        change=to_decimal(row.get("bdy_cmpr_p4")),
        rate=to_decimal(row.get("bdy_cmpr_r_p2")),
        received_time=_received_time(row.get("fnl_rcp_tm")),
        fetched_at=item.fetched_at,
    )


def index_figure(background: Sequence[Background], name: str) -> IndexFigure | None:
    """시장종합 원문의 지수 행. 원문이나 행이 없으면 None."""
    item = _background(background, MARKET_SUMMARY_API)
    row = _find(_rows(item), "indx_nm", name)
    if row is None:
        return None
    return IndexFigure(
        name=name,
        value=to_decimal(row.get("now_indx_p2")),
        change=_signed_change(row),
        rate=to_decimal(row.get("up_dwn_r_p2")),
        fetched_at=item.fetched_at,
    )


def versus(change_percent: float | None, index: IndexFigure | None) -> Decimal | None:
    """비교한 등락률과 지수 등락률의 차이(%p). 어느 쪽이든 없으면 None."""
    if change_percent is None or index is None or index.rate is None:
        return None
    # float의 이진 오차가 반올림을 바꾸지 않게 표기 그대로 Decimal로 바꾼다.
    return round_cent(Decimal(repr(change_percent)) - index.rate)


def us_close_summary(close_rates: Sequence[CloseRate]) -> UsCloseSummary | None:
    """오름·내림·보합 개수와 평균 등락률. 숫자로 읽은 등락률이 하나도 없으면 None."""
    read = [(r, to_decimal(r.rate)) for r in close_rates]
    read = [(r, v) for r, v in read if v is not None]
    if not read:
        return None
    values = [v for _, v in read]
    return UsCloseSummary(
        up=sum(1 for v in values if v > 0),
        down=sum(1 for v in values if v < 0),
        flat=sum(1 for v in values if v == 0),
        average=round_cent(sum(values) / len(values)),
        latest_close_at=max(r.close_at for r, _ in read),
    )


def stale_index_at(background: Sequence[Background], as_of: datetime, stale_after: timedelta) -> datetime | None:
    """시장종합을 변동 시각보다 stale_after 넘게 앞서 받았으면 그 받은 시각. 딱 stale_after 앞이면 오래된 것으로 보지 않는다."""
    item = _background(background, MARKET_SUMMARY_API)
    if item is None or item.status != BACKGROUND_OK or item.fetched_at is None:
        return None
    return item.fetched_at if as_of - item.fetched_at > stale_after else None


def move_figures(compared_percent: float | None, background: Sequence[Background],
                 close_rates: Sequence[CloseRate], *, as_of: datetime | None = None,
                 stale_after: timedelta | None = None) -> MoveFigures:
    """compared_percent는 지수 등락률과 같은 기간의 등락률이다. 없으면 비교 숫자를 만들지 않는다."""
    kospi = index_figure(background, KOSPI_NAME)
    kosdaq = index_figure(background, KOSDAQ_NAME)
    stale = stale_index_at(background, as_of, stale_after) if as_of is not None and stale_after is not None else None
    return MoveFigures(
        exchange_rate=exchange_rate_figure(background),
        kospi=kospi,
        kosdaq=kosdaq,
        vs_kospi=versus(compared_percent, kospi),
        vs_kosdaq=versus(compared_percent, kosdaq),
        us_close=us_close_summary(close_rates),
        stale_index_at=stale,
    )


def signed(value: Decimal) -> str:
    """부호를 붙여 쓴다(+1.23, -0.40, 0.00)."""
    return f"+{value}" if value > 0 else str(value)


def display(value: Decimal | int | str | None, sign: bool = False) -> str:
    """LLM 입력과 근거 제목에 쓰는 글자. 값이 없으면 그 값만 '없음'이다."""
    if value is None:
        return MARK_MISSING
    if sign and isinstance(value, Decimal):
        return signed(value)
    return str(value)


# ---- 리포트 입력의 비교 숫자와 머리말 값 ----

# 시장과 비교하는 등락률의 이름. 지수 등락률은 하루(전일 대비) 값이라 같은 기간의 등락률과만 비교한다.
COMPARED_MOVE = "변동률"
COMPARED_TODAY = "오늘 등락률"


def compares_today(report_input: ReportInput) -> bool:
    """짧은 시간 변동의 변동률은 몇 분 사이의 값이라, 그 종목의 오늘 등락률(전일 종가 대비)로 시장과 비교한다."""
    return report_input.detection_type == "window"


def compared_label(report_input: ReportInput) -> str:
    return COMPARED_TODAY if compares_today(report_input) else COMPARED_MOVE


def report_figures(report_input: ReportInput, settings: CoreSettings = DEFAULT_SETTINGS) -> MoveFigures:
    """비교 숫자. 짧은 시간 변동에 오늘 등락률이 없으면 비교 숫자를 만들지 않는다(짧은 변동률로 대신하지 않는다)."""
    i = report_input
    compared = i.today_change_percent if compares_today(i) else i.change_percent
    return move_figures(compared, i.background, i.close_rates, as_of=i.as_of, stale_after=settings.market_stale_after)


def comparison_facts(report_input: ReportInput, settings: CoreSettings = DEFAULT_SETTINGS) -> list[str]:
    """코드가 계산한 비교 숫자를 문장으로 바꾼다. 값이 없는 비교는 쓰지 않는다."""
    i = report_input
    f = report_figures(i, settings)
    # '변동률', '오늘 등락률' 모두 받침으로 끝나 '이'가 붙는다.
    subject = f"{compared_label(i)}이"
    facts = []
    for label, diff in (("코스피종합", f.vs_kospi), ("코스닥종합", f.vs_kosdaq)):
        if diff is None:
            continue
        received = f", {label}은 {f.stale_index_at.astimezone(KST):%H:%M}에 받은 값" if f.stale_index_at else ""
        if diff == 0:
            facts.append(f"{subject} {label} 등락률과 같습니다 (차이 0.00%p{received}, 시세 자료로 코드가 계산).")
        else:
            side = "높습니다" if diff > 0 else "낮습니다"
            facts.append(f"{subject} {label} 등락률보다 {abs(diff)}%p {side} "
                         f"(차이 {signed(diff)}%p{received}, 시세 자료로 코드가 계산).")
    us = f.us_close
    if us is not None:
        facts.append(f"담긴 미국 종목은 직전 정규장에서 {us.up}개 오르고 {us.down}개 내렸으며 {us.flat}개는 보합이었고, "
                     f"평균 등락률은 {signed(us.average)}%입니다 (해외 시세 자료로 코드가 계산).")
    return facts


def window_minutes(report_input: ReportInput) -> int | None:
    """짧은 시간 변동이 변동 시각부터 감지까지 몇 분 사이였는지. 1분이 안 되면 1분으로 센다. 알 수 없으면 None."""
    i = report_input
    if i.detection_type != "window" or i.detected_at is None:
        return None
    return max(1, int((i.detected_at - i.as_of).total_seconds() // 60))


def today_first(report_input: ReportInput,
                settings: CoreSettings = DEFAULT_SETTINGS) -> tuple[float, int | None] | None:
    """머리말과 메일 제목에 오늘 등락을 먼저 쓸 때의 (오늘 등락률, 분). 짧은 시간 변동이고 둘 다 알 때만 있다.

    분이 짧은 시간 변동 구간 상한을 넘으면(다시 보낸 변동) 분을 None으로 두어 '짧은 시간'으로 쓰게 한다.
    """
    minutes = window_minutes(report_input)
    if minutes is None or report_input.today_change_percent is None:
        return None
    return report_input.today_change_percent, minutes if minutes <= settings.window_move_max_minutes else None
