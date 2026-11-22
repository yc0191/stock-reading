"""레벨 상 메일의 장중 가격 차트 그림(PNG). Kafka를 모르며 리포트 입력(ReportInput)을 인자로 받는다.

그날 09:00부터 변동 5분 뒤까지 1분 간격 가격을 선으로 그리고, 전일 종가를 점선으로, 앞선 급변동과 이 변동을 세로선으로 표시한다.
한글 글꼴과 한 가지 색 규칙은 데일리 차트(daily_chart)와 같다. 코스피 비교선은 넣지 않는다.
"""
from __future__ import annotations

import io
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from briefing.compose import KST
from briefing.daily_chart import DOWN_COLOR, FLAT_COLOR, UP_COLOR, load_fonts
from briefing.move_flow import FlowMove

if TYPE_CHECKING:
    from briefing.jobs import ReportInput

CHART_CONTENT_ID = "chart-move"
CHART_ALT = "장중 가격 차트"
CHART_TITLE = "장중 가격 (원)"
PREV_CLOSE_LABEL = "전일 종가"
THIS_LABEL = "이 변동"
# 차트를 그리는 데 필요한 가장 적은 점 수(선 하나)
MIN_POINTS = 2

WIDTH_INCHES = 7.2
HEIGHT_INCHES = 3.4
DPI = 100


@dataclass(frozen=True)
class ChartInput:
    points: tuple[tuple[datetime, int], ...]
    prev_close: float | None
    earlier: tuple[FlowMove, ...]
    move_at: datetime
    move_rate: float


def prev_close_of(i: ReportInput) -> float | None:
    """전일 종가. 전일 종가 대비 유형은 이벤트의 기준가가 그 값이고, 그 밖은 현재가와 오늘 등락률에서 거꾸로 구한다(근사값)."""
    if i.detection_type == "prevClose":
        return float(i.previous_price)
    if i.today_change_percent is None or i.today_change_percent <= -100:
        return None
    return i.price / (1 + i.today_change_percent / 100)


def chart_input(i: ReportInput) -> ChartInput | None:
    """그릴 점이 없으면 None. 이 변동 시각 이전 점만이 아니라 받은 점 전부(변동 5분 뒤까지)를 쓴다."""
    points = tuple(sorted((p.as_of, p.price) for p in i.chart_points))
    if len(points) < MIN_POINTS:
        return None
    rate = i.today_change_percent if i.today_change_percent is not None else i.change_percent
    return ChartInput(points, prev_close_of(i), i.earlier_moves, i.as_of, rate)


def render_move_chart(c: ChartInput) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure

    regular_font, bold_font = load_fonts()
    with warnings.catch_warnings():
        # 글꼴이 없으면 한글 글자마다 경고가 나온다. 글꼴 경고는 load_fonts가 한 번 남겼다.
        warnings.filterwarnings("ignore", message="Glyph .* missing from font")
        figure = Figure(figsize=(WIDTH_INCHES, HEIGHT_INCHES), dpi=DPI)
        axes = figure.subplots()
        times = [t.astimezone(KST) for t, _ in c.points]
        prices = [p for _, p in c.points]
        axes.plot(times, prices, color="#424242", linewidth=1.4)
        if c.prev_close is not None:
            axes.axhline(c.prev_close, color=FLAT_COLOR, linestyle=":", linewidth=1.1)
            axes.annotate(PREV_CLOSE_LABEL, (times[0], c.prev_close), xytext=(2, 3), textcoords="offset points",
                          fontproperties=regular_font, fontsize=8, color=FLAT_COLOR)
        low, high = min(prices), max(prices)
        if c.prev_close is not None:
            low, high = min(low, c.prev_close), max(high, c.prev_close)
        pad = (high - low) * 0.12 or max(1.0, high * 0.01)
        axes.set_ylim(low - pad, high + pad)
        top = high + pad
        for move in c.earlier:
            at = move.at.astimezone(KST)
            axes.axvline(at, color="#9e9e9e", linestyle="--", linewidth=1.0)
            axes.annotate(f"{at:%H:%M}", (at, top), xytext=(2, -10), textcoords="offset points",
                          fontproperties=regular_font, fontsize=8, color="#616161")
        move_at = c.move_at.astimezone(KST)
        color = UP_COLOR if c.move_rate > 0 else DOWN_COLOR if c.move_rate < 0 else FLAT_COLOR
        axes.axvline(move_at, color=color, linewidth=1.8)
        # 변동 시각이 그림 오른쪽 끝에 가까워 글자를 선 왼쪽에 오른쪽 정렬로 쓴다. 앞선 급변동 글자(선 오른쪽, 한 줄 위)와 겹치지 않는다.
        axes.annotate(f"{THIS_LABEL} {move_at:%H:%M}", (move_at, top), xytext=(-3, -22), textcoords="offset points",
                      ha="right", fontproperties=bold_font, fontsize=9, color=color)
        axes.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=KST))
        axes.set_title(CHART_TITLE, fontproperties=bold_font, fontsize=12, loc="left")
        for tick in axes.get_xticklabels() + axes.get_yticklabels():
            tick.set_fontproperties(regular_font)
        for spine in ("top", "right"):
            axes.spines[spine].set_visible(False)
        figure.tight_layout()
        buffer = io.BytesIO()
        figure.savefig(buffer, format="png")
        return buffer.getvalue()

