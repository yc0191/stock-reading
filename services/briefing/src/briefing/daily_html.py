"""데일리 판 HTML 본문(인라인 스타일만). Kafka를 모르며 글자판과 같은 값을 인자로 받는다.

순서는 글자판과 같다: 제목 → 결론(마감 후 판) → 차트(마감 후 판) → 항목별 숫자 → 요약 → 면책 → 서버 상태(마감 후 판,
작게, '주의'가 있을 때만 그 줄이 주황) → 근거 목록(작게). 발송기는 html 칸을 받은 그대로 보내므로 이벤트 값
(ETF 이름, LLM 문장, 근거 설명, 서버 상태 줄)은 모두 여기서 이스케이프한다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from html import escape

from briefing.daily_chart import CHART_ALT
from briefing.daily_report import (
    DAILY_DISCLAIMER,
    DAILY_INSUFFICIENT,
    ITEM_RESEARCH,
    MINUS,
    DailyInput,
    DailyOutput,
    Section,
    conclusion,
    headline,
    section_title,
    summary_view,
)
from briefing.events import EDITION_POST_CLOSE
from briefing.llm_output import WEAK_NOTE_PREFIX

# 메일 앱은 <style>을 지우는 곳이 있어 모든 스타일을 요소에 인라인으로 둔다.
FONT_STACK = "'Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic','맑은 고딕',sans-serif"
UP_COLOR = "#d32f2f"
DOWN_COLOR = "#1565c0"
FLAT_COLOR = "#757575"
TEXT_COLOR = "#212121"
MUTED_COLOR = "#9e9e9e"
ATTENTION_COLOR = "#e65100"

NO_CHART_TOO_LARGE = "차트 없음(크기 초과)"
NO_STATUS_ATTENTION = "없음"
ATTENTION_MARK = "주의:"

BODY_STYLE = (f"margin:0;padding:16px;background:#ffffff;color:{TEXT_COLOR};font-family:{FONT_STACK};"
              "font-size:15px;line-height:1.6")
WRAP_STYLE = "max-width:640px;margin:0 auto"
TITLE_STYLE = "margin:0 0 4px;font-size:20px;font-weight:700;line-height:1.4"
RANGE_STYLE = f"margin:0 0 14px;font-size:13px;color:{FLAT_COLOR}"
CONCLUSION_STYLE = "margin:0 0 14px;font-size:16px;font-weight:700"
HEADING_STYLE = "margin:20px 0 6px;font-size:16px;font-weight:700"
GROUP_STYLE = "margin:10px 0 2px;font-size:14px;font-weight:700"
LIST_STYLE = "margin:0;padding-left:20px"
ITEM_STYLE = "margin:2px 0"
NOTE_STYLE = f"margin:2px 0 6px;font-size:13px;color:{FLAT_COLOR}"
SMALL_STYLE = f"margin:2px 0;font-size:12px;color:{MUTED_COLOR}"
DISCLAIMER_STYLE = f"margin:18px 0 6px;font-size:12px;color:{MUTED_COLOR}"
STATUS_BOX_STYLE = "margin:6px 0 0"
IMAGE_STYLE = "display:block;max-width:100%;height:auto;margin:0 0 6px;border:0"

RATE = re.compile(r"(?<![\w.])([+\-−]?\d+(?:\.\d+)?)%")


@dataclass(frozen=True)
class ChartSlot:
    """마감 후 판의 차트 자리. content_id가 있으면 그림, oversized면 크기 초과로 뺀 자리다. no_data는 그림에 없는 ETF."""

    content_id: str | None = None
    oversized: bool = False
    no_data: tuple[str, ...] = ()


def _rate_spans(escaped: str) -> str:
    """이스케이프한 글 안의 등락률(+2.1%, -1.5%, 2.04%)에 오름·내림 색을 칠한다."""

    def paint(match: re.Match[str]) -> str:
        sign = -1 if match.group(1)[0] in "-" + MINUS else 1
        value = sign * float(match.group(1).lstrip("+-" + MINUS))
        color = UP_COLOR if value > 0 else DOWN_COLOR if value < 0 else FLAT_COLOR
        return f'<span style="color:{color};font-weight:700">{match.group(0)}</span>'

    return RATE.sub(paint, escaped)


def _p(style: str, inner: str) -> str:
    return f'<p style="{style}">{inner}</p>'


def _list(lines: tuple[str, ...]) -> str:
    items = "".join(f'<li style="{ITEM_STYLE}">{_rate_spans(escape(line))}</li>' for line in lines)
    return f'<ul style="{LIST_STYLE}">{items}</ul>'


def _section(s: Section) -> str:
    parts = [_p(HEADING_STYLE, f"■ {escape(section_title(s))}")]
    if s.groups:
        grouped = {line for _, members in s.groups for line in members}
        for title, members in s.groups:
            parts.append(_p(GROUP_STYLE, escape(title)))
            parts.append(_list(members))
        extra = tuple(line for line in s.lines if line not in grouped)
        if extra:
            parts.append(_list(extra))
    else:
        parts.append(_list(s.lines))
    return "".join(parts)


def _chart(slot: ChartSlot) -> str:
    parts = []
    if slot.content_id is not None:
        parts.append(f'<img src="cid:{escape(slot.content_id)}" alt="{escape(CHART_ALT)}" style="{IMAGE_STYLE}">')
    elif slot.oversized:
        parts.append(_p(NOTE_STYLE, NO_CHART_TOO_LARGE))
    if slot.no_data and (slot.content_id is not None or slot.oversized):
        parts.append(_p(SMALL_STYLE, "등락 자료 없음(차트에서 뺌): " + escape(", ".join(slot.no_data))))
    return "".join(parts)


def _summary(sections: tuple[Section, ...], output: DailyOutput) -> tuple[str, str]:
    """(요약 HTML, 근거 목록 HTML)"""
    view = summary_view(sections, output)
    parts = [_p(HEADING_STYLE, "요약")]
    if view.insufficient:
        parts.append(_p(ITEM_STYLE, escape(DAILY_INSUFFICIENT)))
        return "".join(parts), ""
    items = []
    for statement, numbers, shortfall in view.points:
        cite = escape(f"[근거 {', '.join(str(n) for n in numbers)}]")
        note = _p(NOTE_STYLE, escape(f"{WEAK_NOTE_PREFIX}{shortfall}")) if shortfall else ""
        items.append(f'<li style="{ITEM_STYLE}">{escape(statement)} {cite}{note}</li>')
    parts.append(f'<ul style="{LIST_STYLE}">{"".join(items)}</ul>')
    evidence = ""
    if view.evidence:
        rows = "".join(f'<li style="{SMALL_STYLE}">{escape(describe)}</li>' for _, describe in view.evidence)
        evidence = _p(SMALL_STYLE, "근거:") + f'<ol style="{LIST_STYLE};font-size:12px;color:{MUTED_COLOR}">{rows}</ol>'
    return "".join(parts), evidence


def _status(status_text: str) -> str:
    lines = []
    for raw in status_text.split("\n"):
        line = raw.removeprefix("- ").strip()
        if not line:
            continue
        color = MUTED_COLOR
        # '주의'가 '없음'이 아닐 때만 그 줄을 주황으로 칠한다.
        if line.startswith(ATTENTION_MARK) and line.removeprefix(ATTENTION_MARK).strip() != NO_STATUS_ATTENTION:
            color = ATTENTION_COLOR
        lines.append(_p(f"margin:2px 0;font-size:12px;color:{color}", escape(line)))
    return f'<div style="{STATUS_BOX_STYLE}">{"".join(lines)}</div>'


def render_html(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None,
                status_text: str | None = None, chart: ChartSlot | None = None) -> str:
    title, scope = headline(i)
    parts = [f'<h1 style="{TITLE_STYLE}">{escape(title)}</h1>', _p(RANGE_STYLE, escape(scope))]
    if i.edition == EDITION_POST_CLOSE:
        parts.append(_p(CONCLUSION_STYLE, _rate_spans(escape(conclusion(i)))))
        if chart is not None:
            parts.append(_chart(chart))
    parts.extend(_section(s) for s in sections if s.name != ITEM_RESEARCH and not s.hidden)
    evidence = ""
    if output is not None:
        summary, evidence = _summary(sections, output)
        parts.append(summary)
    parts.append(_p(DISCLAIMER_STYLE, escape(DAILY_DISCLAIMER)))
    if status_text:
        parts.append(_status(status_text))
    parts.append(evidence)
    return (f'<html lang="ko"><body style="{BODY_STYLE}"><div style="{WRAP_STYLE}">'
            f'{"".join(parts)}</div></body></html>')
