"""급변동 리포트에 넣는 배경 숫자와 비교 숫자. Kafka를 모르며 리포트 입력을 인자로 받는다.

배경 원문(환율종합, 시장종합)에서 원달러 환율과 코스피·코스닥 숫자를 꺼내고, 변동률과의 비교 숫자를 코드가 계산한다.
LLM은 이 숫자를 새로 계산하지 않고 근거 번호로 인용만 한다. 저장 원문은 바꾸지 않고 꺼낸 값은 파생값으로만 쓴다.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Sequence

from briefing.sources import BACKGROUND_OK, MARK_MISSING, Background, CloseRate

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
    # 변동률 - 지수 등락률(%p). 필요한 값이 없으면 None이다.
    vs_kospi: Decimal | None
    vs_kosdaq: Decimal | None
    us_close: UsCloseSummary | None


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


def versus(change_percent: float, index: IndexFigure | None) -> Decimal | None:
    """변동률과 지수 등락률의 차이(%p)."""
    if index is None or index.rate is None:
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


def move_figures(change_percent: float, background: Sequence[Background],
                 close_rates: Sequence[CloseRate]) -> MoveFigures:
    kospi = index_figure(background, KOSPI_NAME)
    kosdaq = index_figure(background, KOSDAQ_NAME)
    return MoveFigures(
        exchange_rate=exchange_rate_figure(background),
        kospi=kospi,
        kosdaq=kosdaq,
        vs_kospi=versus(change_percent, kospi),
        vs_kosdaq=versus(change_percent, kosdaq),
        us_close=us_close_summary(close_rates),
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
