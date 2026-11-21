"""데일리 판 그래프 그림(PNG). Kafka를 모르며 모은 자료(DailyInput)를 인자로 받는다.

마감 후 판의 하루 등락은 ETF마다 장중 최저~최고 범위(회색)와 종가 등락(점)으로 그린다. 자료 없는 ETF는 빼고 이름은
호출한 쪽이 그림 아래에 적는다. 장중 흐름(아침 판 개장 흐름, 마감 후 판 하루)과 기간 흐름(1개월, 6개월)은 선 그래프다.
한글 글꼴(Noto Sans KR)은 scripts/install_font.py 로 내려받는다. 글꼴 파일이 없으면 경고를 남기고 기본 글꼴로 그린다
(한글이 네모로 보일 수 있다).
"""
from __future__ import annotations

import io
import logging
import math
from datetime import date, timedelta
import os
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from briefing.compose import KST
from briefing.daily_report import RELATION_WATCH, DailyInput, EtfDay, etf_groups

log = logging.getLogger("briefing.daily_chart")

FONT_DIR_ENV = "BRIEFING_FONT_DIR"
DEFAULT_FONT_DIR = Path(__file__).resolve().parents[2] / "fonts"
REGULAR_FONT = "NotoSansKR-Regular.otf"
BOLD_FONT = "NotoSansKR-Bold.otf"

# 한국 증시 관례대로 오름은 빨강, 내림은 파랑이다. HTML의 등락률 색과 같다.
UP_COLOR = "#d32f2f"
DOWN_COLOR = "#1565c0"
FLAT_COLOR = "#9e9e9e"
WATCH_SUFFIX = " (관심)"
CHART_TITLE = "하루 등락과 장중 범위 (회색: 장중 최저~최고, 점: 종가, %)"
# 보유/관심 구분이 없는 응답에서도 같이 쓰므로 alt에는 구분 말을 넣지 않는다.
CHART_ALT = "ETF 하루 등락과 장중 범위 그래프"
RANGE_COLOR = "#bdbdbd"

DPI = 100
WIDTH_INCHES = 7.2
BAR_HEIGHT_INCHES = 0.42
MIN_HEIGHT_INCHES = 2.2
# 화면에서 한 장이 너무 길어지지 않게 막대를 그리는 ETF 수의 상한
MAX_BARS = 40


@dataclass(frozen=True)
class Bar:
    """ETF 하나의 하루 등락(종가 기준 %)과 장중 최저·최고(%). 장중 흐름이 없으면 범위는 None이다."""

    label: str
    rate: float
    low: float | None = None
    high: float | None = None


def chart_bars(i: DailyInput) -> tuple[Bar, ...]:
    """등락 값이 있는 ETF의 막대. 보유, 관심 순이고 묶음 안에서는 많이 오른 순이다. 관심 ETF는 이름 뒤에 '(관심)'을 붙인다."""
    bars: list[Bar] = []
    for _, members in etf_groups(i.etfs or ()):
        group = [Bar(e.label() + (WATCH_SUFFIX if e.relation == RELATION_WATCH else ""), rate, *_day_range(e, rate))
                 for e in members for rate in [_rate_of(e)] if rate is not None]
        bars.extend(sorted(group, key=lambda b: -b.rate))
    return tuple(bars[:MAX_BARS])


def _day_range(etf: EtfDay, rate: float) -> tuple[float | None, float | None]:
    """장중 흐름의 전일 종가 대비 최저·최고(%). 종가 등락도 범위에 넣는다. 장중 흐름이 없으면 (None, None)."""
    series = etf.intraday_series
    if series is None or not series.present:
        return None, None
    values = [v for p in series.points for v in [_float(p.rate_vs_prev_close)] if v is not None]
    if not values:
        return None, None
    return min(*values, rate), max(*values, rate)


def no_data_names(i: DailyInput) -> tuple[str, ...]:
    """차트에서 빠진(등락 값 없는) ETF 이름."""
    return tuple(e.label() for _, members in etf_groups(i.etfs or ()) for e in members if _rate_of(e) is None)


def _rate_of(etf: EtfDay) -> float | None:
    if not etf.has_day():
        return None
    try:
        value = float(etf.rate)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _font_dir() -> Path:
    override = os.environ.get(FONT_DIR_ENV)
    return Path(override) if override else DEFAULT_FONT_DIR


def load_fonts() -> tuple[Any, Any]:
    """(보통, 굵게) 글꼴. 글꼴 파일이 없으면 경고를 남기고 기본 글꼴을 쓴다. 급변동 차트도 같이 쓴다."""
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import font_manager

    font_dir = _font_dir()
    regular, bold = font_dir / REGULAR_FONT, font_dir / BOLD_FONT
    if regular.is_file() and bold.is_file():
        return font_manager.FontProperties(fname=str(regular)), font_manager.FontProperties(fname=str(bold))
    log.warning("chart font files not found in %s; run scripts/install_font.py (drawing with the default font, "
                "Korean text may not show)", font_dir)
    return font_manager.FontProperties(), font_manager.FontProperties(weight="bold")


def render_day_chart(bars: tuple[Bar, ...]) -> bytes | None:
    """하루 등락 막대 그림을 PNG 바이트로 만든다. 그릴 막대가 없으면 None."""
    if not bars:
        return None
    from matplotlib.figure import Figure

    regular_font, bold_font = load_fonts()

    with warnings.catch_warnings():
        # 글꼴이 없으면 한글 글자마다 경고가 나온다. 글꼴 경고는 위에서 한 번 남겼다.
        warnings.filterwarnings("ignore", message="Glyph .* missing from font")
        height = max(MIN_HEIGHT_INCHES, BAR_HEIGHT_INCHES * len(bars) + 1.0)
        figure = Figure(figsize=(WIDTH_INCHES, height), dpi=DPI)
        axes = figure.subplots()
        positions = range(len(bars))
        for position, bar in zip(positions, bars):
            color = UP_COLOR if bar.rate > 0 else DOWN_COLOR if bar.rate < 0 else FLAT_COLOR
            if bar.low is not None and bar.high is not None:
                axes.plot([bar.low, bar.high], [position, position], color=RANGE_COLOR, linewidth=6,
                          solid_capstyle="round")
            axes.plot([0, bar.rate], [position, position], color=color, linewidth=2)
            axes.plot([bar.rate], [position], "o", color=color, markersize=6)
        axes.set_yticks(list(positions))
        axes.set_yticklabels([b.label for b in bars], fontproperties=regular_font, fontsize=10)
        axes.invert_yaxis()
        axes.axvline(0, color="#616161", linewidth=0.8)
        axes.set_title(CHART_TITLE, fontproperties=bold_font, fontsize=12, loc="left")
        for tick in axes.get_xticklabels():
            tick.set_fontproperties(regular_font)
        for spine in ("top", "right"):
            axes.spines[spine].set_visible(False)
        span = max(abs(v) for b in bars for v in (b.rate, b.low, b.high) if v is not None) or 1.0
        axes.set_xlim(-span * 1.35, span * 1.35)
        for position, bar in zip(positions, bars):
            # 등락 숫자는 범위 막대 바깥쪽 끝 옆에 둔다.
            edge = (bar.high if bar.rate >= 0 else bar.low) if bar.low is not None else bar.rate
            axes.annotate(f"{bar.rate:+.2f}%", (edge, position), xytext=(6 if bar.rate >= 0 else -6, 0),
                          textcoords="offset points", ha="left" if bar.rate >= 0 else "right", va="center",
                          fontproperties=regular_font, fontsize=9)
        figure.tight_layout()
        buffer = io.BytesIO()
        figure.savefig(buffer, format="png")
        return buffer.getvalue()


# ---- 선 그래프: 장중 흐름(아침 판 개장 흐름, 마감 후 판 하루), 기간 흐름(1개월, 6개월) ----

OPEN_FLOW_TITLE = "개장 흐름 (전일 종가 대비 %)"
INTRADAY_TITLE = "오늘 장중 흐름 (전일 종가 대비 %)"
PERIOD_TITLE = "최근 {label} 흐름 (첫날 대비 %)"
OPEN_FLOW_ALT = "ETF 개장 흐름 선 그래프"
INTRADAY_ALT = "ETF 오늘 장중 흐름 선 그래프"
PERIOD_ALT = "ETF 기간 흐름 선 그래프"
# 기간 그래프의 길이(개월)와 제목에 쓰는 말
PERIODS = ((1, "1개월"), (6, "6개월"))
# 선마다 다른 색. 오름·내림 색(빨강·파랑)은 등락률 색과 헷갈려 쓰지 않는다.
LINE_COLORS = ("#2e7d32", "#6a1b9a", "#ef6c00", "#00838f", "#5d4037", "#ad1457", "#558b2f", "#283593",
               "#f9a825", "#4e342e", "#00695c", "#7b1fa2")
LINE_HEIGHT_INCHES = 3.6
# 선 끝 이름표가 겹치지 않게 벌리는 최소 간격(축 높이에 대한 비율)
LABEL_GAP = 0.06
# 기간 그래프를 그리려면 자료의 첫 일자가 기간 첫날에서 이만큼 안에 있어야 한다. 추석과 한글날이 이어지면 첫 거래일이
# 일주일 넘게 늦을 수 있어 넉넉히 둔다. 자료가 정말 짧은 ETF는 한 달 넘게 모자라므로 구분에는 영향이 없다.
PERIOD_START_SLACK = timedelta(days=10)
# 한 그림에 그리는 선 수의 상한. 넘으면 보유 먼저, 그다음 관심 순으로 자른다.
MAX_LINES = 12


@dataclass(frozen=True)
class Line:
    """선 하나: ETF 이름, 보유 여부(보유는 진하게, 관심은 옅게), (x, y) 점들. x는 시각이나 날짜다."""

    label: str
    held: bool
    points: tuple[tuple[Any, float], ...]


def _float(text: str | None) -> float | None:
    try:
        value = float(text) if text is not None else None
    except ValueError:
        return None
    return value if value is not None and math.isfinite(value) else None


def _ordered(lines: list[Line]) -> tuple[Line, ...]:
    """보유 먼저, 그다음 관심. 상한을 넘으면 뒤를 자른다."""
    return tuple(sorted(lines, key=lambda line: not line.held)[:MAX_LINES])


def intraday_lines(i: DailyInput) -> tuple[Line, ...]:
    """ETF마다 장중 흐름(전일 종가 대비 %). 점이 둘보다 적은 ETF는 선이 되지 않아 뺀다."""
    lines = []
    for e in i.etfs or ():
        series = e.intraday_series
        if series is None or not series.present:
            continue
        points = tuple((p.at.astimezone(KST), value) for p in series.points
                       for value in [_float(p.rate_vs_prev_close)] if value is not None)
        if len(points) >= 2:
            lines.append(Line(e.label(), e.relation != RELATION_WATCH, points))
    return _ordered(lines)


def period_lines(i: DailyInput, months: int) -> tuple[Line, ...]:
    """ETF마다 최근 months개월 종가를 첫날 종가 대비 %로 바꾼 선. 기간은 마지막 일자에서 거슬러 센다."""
    lines = []
    for e in i.etfs or ():
        closes = e.daily_closes
        if closes is None or not closes.present or not closes.points:
            continue
        last = _day(closes.points[-1].date)
        start = _months_before(last, months)
        # 자료가 그 기간보다 짧은 ETF는 그리지 않는다. 기간 첫날이 휴장일일 수 있어 며칠은 늦게 시작해도 된다.
        if _day(closes.points[0].date) > start + PERIOD_START_SLACK:
            continue
        values = [(day, value) for c in closes.points for day in [_day(c.date)] for value in [_float(c.close)]
                  if day >= start and value is not None and value > 0]
        if len(values) < 2:
            continue
        base = values[0][1]
        lines.append(Line(e.label(), e.relation != RELATION_WATCH,
                          tuple((day, (value / base - 1) * 100) for day, value in values)))
    return _ordered(lines)


def _day(text: str) -> date:
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _months_before(day: date, months: int) -> date:
    month = day.month - months
    year = day.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    # 없는 날(예: 3월 31일의 한 달 전)은 그달 마지막 날로 맞춘다.
    for d in (day.day, 30, 29, 28):
        try:
            return date(year, month, d)
        except ValueError:
            continue
    return date(year, month, 28)


@dataclass(frozen=True)
class LineChart:
    """메일에 붙일 선 그래프 하나: 그림 자리 이름(content id), 대체 글, PNG."""

    content_id: str
    alt: str
    png: bytes


def line_charts(i: DailyInput, post_close: bool) -> tuple[LineChart, ...]:
    """판의 선 그래프. 아침 판은 개장 흐름, 마감 후 판은 오늘 장중 흐름과 1개월·6개월 흐름이다. 그릴 선이 없는 그래프는 뺀다."""
    if not post_close:
        wanted = [("chart-open", OPEN_FLOW_ALT, intraday_lines(i), OPEN_FLOW_TITLE, True)]
    else:
        wanted = [("chart-intraday", INTRADAY_ALT, intraday_lines(i), INTRADAY_TITLE, True)]
        wanted += [(f"chart-{months}m", PERIOD_ALT, period_lines(i, months), PERIOD_TITLE.format(label=label), False)
                   for months, label in PERIODS]
    charts = []
    for content_id, alt, lines, title, by_time in wanted:
        png = render_line_chart(lines, title, by_time)
        if png is not None:
            charts.append(LineChart(content_id, alt, png))
    return tuple(charts)


def render_line_chart(lines: tuple[Line, ...], title: str, by_time: bool) -> bytes | None:
    """선 그래프를 PNG 바이트로 만든다. 선은 보유가 진하고 관심이 옅으며, 범례 대신 선 끝에 이름과 마지막 값을 쓴다.
    by_time이면 x축이 시각(HH:MM), 아니면 날짜(MM-DD)다. 그릴 선이 없으면 None."""
    if not lines:
        return None
    from matplotlib import dates as mdates
    from matplotlib.figure import Figure

    regular_font, bold_font = load_fonts()
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Glyph .* missing from font")
        figure = Figure(figsize=(WIDTH_INCHES, LINE_HEIGHT_INCHES), dpi=DPI)
        axes = figure.subplots()
        ends = []
        for n, line in enumerate(lines):
            color = LINE_COLORS[n % len(LINE_COLORS)]
            xs, ys = zip(*line.points)
            axes.plot(xs, ys, color=color, linewidth=2.2 if line.held else 1.2, alpha=1.0 if line.held else 0.55,
                      linestyle="-" if line.held else "--")
            label = line.label + ("" if line.held else WATCH_SUFFIX)
            ends.append((ys[-1], xs[-1], f"{label} {ys[-1]:+.2f}%", color))
        axes.axhline(0, color="#616161", linewidth=0.8)
        axes.set_title(title, fontproperties=bold_font, fontsize=12, loc="left")
        axes.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M" if by_time else "%m-%d", tz=KST if by_time else None))
        for tick in (*axes.get_xticklabels(), *axes.get_yticklabels()):
            tick.set_fontproperties(regular_font)
        for spine in ("top", "right"):
            axes.spines[spine].set_visible(False)
        figure.subplots_adjust(left=0.08, right=0.72, top=0.88, bottom=0.12)
        low, high = axes.get_ylim()
        gap = (high - low) * LABEL_GAP
        # 축 높이(포인트)로 값 차이를 이름표 위치 차이(포인트)로 바꾼다.
        points_per_value = LINE_HEIGHT_INCHES * 72 * (0.88 - 0.12) / (high - low)
        last = None
        # 선 끝 이름표는 끝 값이 낮은 것부터 놓으며 바로 아래 이름표와 겹치면 위로 민다.
        for y, x, text, color in sorted(ends, key=lambda e: e[0]):
            at = y if last is None else max(y, last + gap)
            last = at
            axes.annotate(text, (x, y), xytext=(6, (at - y) * points_per_value), textcoords="offset points",
                          ha="left", va="center", color=color, fontproperties=regular_font, fontsize=8,
                          annotation_clip=False)
        buffer = io.BytesIO()
        figure.savefig(buffer, format="png")
        return buffer.getvalue()

