"""레벨 상 메일의 HTML 본문과 이벤트에 담을 html·images를 만든다. Kafka를 모르며 리포트(Report)와 리포트 입력을 인자로 받는다.

글자판(report.text)과 같은 내용(MoveView)을 인라인 스타일만으로 꾸미고, 장중 차트(PNG)는 cid로 가리키는 인라인 그림으로 싣는다.
발송기는 html 칸을 받은 그대로 보내므로 이벤트 값(LLM 문장, 근거 제목)은 모두 여기서 이스케이프한다.
차트는 덤이라 그리지 못하거나 상한을 넘으면 그림만 빼고 글자판+HTML로 보낸다.
"""
from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from html import escape
from typing import TYPE_CHECKING, Callable

from briefing.config import CoreSettings
from briefing.daily_html import (
    BODY_STYLE,
    DISCLAIMER_STYLE,
    HEADING_STYLE,
    IMAGE_STYLE,
    ITEM_STYLE,
    LIST_STYLE,
    MUTED_COLOR,
    SMALL_STYLE,
    TITLE_STYLE,
    WRAP_STYLE,
    _link,
    _p,
    _rate_spans,
)
from briefing.events import OutboundEvent
from briefing.move_chart import CHART_ALT, CHART_CONTENT_ID, chart_input, render_move_chart
from briefing.move_flow import FLOW_TITLE
from briefing.move_view import CONFLICT_LABEL, MoveView, cite

if TYPE_CHECKING:
    from briefing.jobs import ReportInput

log = logging.getLogger("briefing.move_mail")

# 작업 기록(missing-items)에 남기는 차트 관련 줄. 메일 본문에는 싣지 않는다.
CHART_MISSING_ITEM = "장중 차트: 시세 이력이 없거나 그리지 못해 그림 없음"
CHART_OVERSIZED_ITEM = "장중 차트: 크기 초과로 그림 뺌"


def render_move_html(v: MoveView, chart_cid: str | None) -> str:
    parts = [f'<h1 style="{TITLE_STYLE}">{_rate_spans(escape(v.headline))}</h1>']
    if chart_cid is not None:
        parts.append(f'<img src="cid:{escape(chart_cid)}" alt="{escape(CHART_ALT)}" style="{IMAGE_STYLE}">')
    if v.flow:
        parts.append(_p(HEADING_STYLE, escape(FLOW_TITLE)))
        items = "".join(f'<li style="{ITEM_STYLE}">{_rate_spans(escape(line))}</li>' for line in v.flow)
        parts.append(f'<ul style="{LIST_STYLE}">{items}</ul>')
    for line in v.notes:
        parts.append(_p(ITEM_STYLE, _rate_spans(escape(line))))
    for index, c in enumerate(v.causes, start=1):
        parts.append(_p(HEADING_STYLE, escape(f"{index}. {c.title}")))
        items = [f"{_rate_spans(escape(c.detail))} {escape(cite(c.detail_numbers))}",
                 f"{_rate_spans(escape(c.meaning))} {escape(cite(c.meaning_numbers))}"]
        if c.conflict:
            items.append(f"{escape(CONFLICT_LABEL)}: {_rate_spans(escape(c.conflict))} {escape(cite(c.conflict_numbers))}")
        parts.append(f'<ul style="{LIST_STYLE}">{"".join(f"<li style=\"{ITEM_STYLE}\">{x}</li>" for x in items)}</ul>')
    if v.evidence:
        rows = "".join(f'<li style="{SMALL_STYLE}">{escape(e.text)}{_link(e.url)}</li>' for e in v.evidence)
        parts.append(_p(SMALL_STYLE, escape(v.evidence_title)))
        parts.append(f'<ol style="{LIST_STYLE};font-size:12px;color:{MUTED_COLOR}">{rows}</ol>')
    if v.closing:
        parts.append(_p(DISCLAIMER_STYLE, escape(v.closing)))
    return f'<html lang="ko"><body style="{BODY_STYLE}"><div style="{WRAP_STYLE}">{"".join(parts)}</div></body></html>'


@dataclass(frozen=True)
class MoveMail:
    """이벤트에 담을 html과 그림, 작업 기록에만 남길 줄(차트 빠짐 등)."""

    html: str
    images: tuple[dict[str, str], ...]
    notes: tuple[str, ...] = ()


def build_move_mail(i: ReportInput, view: MoveView, settings: CoreSettings,
                    event_for: Callable[[str, list[dict[str, str]] | None], OutboundEvent]) -> MoveMail:
    """차트를 그려 html·images를 만든다. 그림이 한 장 상한이나 이벤트 상한을 넘으면 그림만 뺀다.

    event_for(html, images)는 같은 내용으로 이벤트를 만들어 크기를 재는 데 쓴다."""
    png, note = None, ()
    chart = chart_input(i)
    if chart is None:
        note = (CHART_MISSING_ITEM,)
    else:
        try:
            png = render_move_chart(chart)
        except Exception as error:  # noqa: BLE001 - 차트는 덤이라 실패해도 메일은 나간다
            log.warning("move chart failed for job %s: %s", i.job_id, type(error).__name__)
            note = (CHART_MISSING_ITEM,)
    if png is not None and len(png) > settings.max_mail_image_bytes:
        png, note = None, (CHART_OVERSIZED_ITEM,)

    def pack(image: bytes | None) -> tuple[str, list[dict[str, str]] | None]:
        html = render_move_html(view, CHART_CONTENT_ID if image is not None else None)
        images = [{"contentId": CHART_CONTENT_ID, "contentType": "image/png",
                   "data": base64.b64encode(image).decode("ascii")}] if image is not None else None
        return html, images

    html, images = pack(png)
    if png is not None and len(json.dumps(event_for(html, images).value, ensure_ascii=False).encode("utf-8")) \
            > settings.max_mail_event_bytes:
        html, images = pack(None)
        note = (CHART_OVERSIZED_ITEM,)
    return MoveMail(html, tuple(images or ()), note)
