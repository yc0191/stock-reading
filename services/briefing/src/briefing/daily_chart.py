"""마감 후 판의 하루 등락 차트 그림(PNG). Kafka를 모르며 모은 자료(DailyInput)를 인자로 받는다.

보유·관심 ETF의 하루 등락률을 가로 막대로 그린다. 자료 없는 ETF는 막대에서 빼고 이름은 호출한 쪽이 그림 아래에 적는다.
한글 글꼴(Noto Sans KR)은 scripts/install_font.py 로 내려받는다. 글꼴 파일이 없으면 경고를 남기고 기본 글꼴로 그린다
(한글이 네모로 보일 수 있다).
"""
from __future__ import annotations

import io
import logging
import math
import os
import warnings
from dataclasses import dataclass
from pathlib import Path

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
CHART_TITLE = "하루 등락률 (%)"
# 보유/관심 구분이 없는 응답에서도 같이 쓰므로 alt에는 구분 말을 넣지 않는다.
CHART_ALT = "ETF 하루 등락률 막대 그래프"

DPI = 100
WIDTH_INCHES = 7.2
BAR_HEIGHT_INCHES = 0.42
MIN_HEIGHT_INCHES = 2.2
# 화면에서 한 장이 너무 길어지지 않게 막대를 그리는 ETF 수의 상한
MAX_BARS = 40


@dataclass(frozen=True)
class Bar:
    label: str
    rate: float


def chart_bars(i: DailyInput) -> tuple[Bar, ...]:
    """등락 값이 있는 ETF의 막대. 보유, 관심 순이고 묶음 안에서는 많이 오른 순이다. 관심 ETF는 이름 뒤에 '(관심)'을 붙인다."""
    bars: list[Bar] = []
    for _, members in etf_groups(i.etfs or ()):
        group = [Bar(e.label() + (WATCH_SUFFIX if e.relation == RELATION_WATCH else ""), rate)
                 for e in members for rate in [_rate_of(e)] if rate is not None]
        bars.extend(sorted(group, key=lambda b: -b.rate))
    return tuple(bars[:MAX_BARS])


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


def render_day_chart(bars: tuple[Bar, ...]) -> bytes | None:
    """하루 등락 막대 그림을 PNG 바이트로 만든다. 그릴 막대가 없으면 None."""
    if not bars:
        return None
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import font_manager
    from matplotlib.figure import Figure

    font_dir = _font_dir()
    regular, bold = font_dir / REGULAR_FONT, font_dir / BOLD_FONT
    if regular.is_file() and bold.is_file():
        regular_font = font_manager.FontProperties(fname=str(regular))
        bold_font = font_manager.FontProperties(fname=str(bold))
    else:
        log.warning("chart font files not found in %s; run scripts/install_font.py (drawing with the default font, "
                    "Korean text may not show)", font_dir)
        regular_font = font_manager.FontProperties()
        bold_font = font_manager.FontProperties(weight="bold")

    with warnings.catch_warnings():
        # 글꼴이 없으면 한글 글자마다 경고가 나온다. 글꼴 경고는 위에서 한 번 남겼다.
        warnings.filterwarnings("ignore", message="Glyph .* missing from font")
        height = max(MIN_HEIGHT_INCHES, BAR_HEIGHT_INCHES * len(bars) + 1.0)
        figure = Figure(figsize=(WIDTH_INCHES, height), dpi=DPI)
        axes = figure.subplots()
        positions = range(len(bars))
        colors = [UP_COLOR if b.rate > 0 else DOWN_COLOR if b.rate < 0 else FLAT_COLOR for b in bars]
        axes.barh(positions, [b.rate for b in bars], color=colors, height=0.6)
        axes.set_yticks(list(positions))
        axes.set_yticklabels([b.label for b in bars], fontproperties=regular_font, fontsize=10)
        axes.invert_yaxis()
        axes.axvline(0, color="#616161", linewidth=0.8)
        axes.set_title(CHART_TITLE, fontproperties=bold_font, fontsize=12, loc="left")
        for tick in axes.get_xticklabels():
            tick.set_fontproperties(regular_font)
        for spine in ("top", "right"):
            axes.spines[spine].set_visible(False)
        span = max(abs(b.rate) for b in bars) or 1.0
        axes.set_xlim(-span * 1.35, span * 1.35)
        for position, bar in zip(positions, bars):
            axes.annotate(f"{bar.rate:+.2f}%", (bar.rate, position), xytext=(4 if bar.rate >= 0 else -4, 0),
                          textcoords="offset points", ha="left" if bar.rate >= 0 else "right", va="center",
                          fontproperties=regular_font, fontsize=9)
        figure.tight_layout()
        buffer = io.BytesIO()
        figure.savefig(buffer, format="png")
        return buffer.getvalue()
