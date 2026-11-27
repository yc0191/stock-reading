"""데일리 판 메일 본문 이벤트 만들기: 하루 등락 차트와 흐름 그래프를 붙이고, 이벤트가 상한을 넘으면 그림을 뺀다.
마감 후 판 끝의 서버 상태 절도 여기서 만든다. 작업 흐름(조회, 작성, 멤버별 판 짜기, 작업 기록)은 daily_jobs.py에 있다.
"""
from __future__ import annotations

import base64
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from briefing.config import DailySettings
from briefing.daily_chart import LineChart, chart_bars, line_charts, no_data_names, render_day_chart
from briefing.daily_html import ChartSlot, render_html
from briefing.daily_report import GENERATED_BY_RULE
from briefing.daily_sections import DailyInput, Section
from briefing.events import EDITION_POST_CLOSE, OutboundEvent, daily_briefing_event
from briefing.status import StatusPort, StatusReport, build_status_report

if TYPE_CHECKING:
    from briefing.daily_jobs import _DailyJob, _Draft

# daily_jobs.py에서 나눈 코드라 운영 로그와 로그를 읽는 테스트가 보는 로거 이름을 그대로 둔다.
log = logging.getLogger("briefing.daily_jobs")

# 차트를 싣지 못했을 때 자료 없음 목록(missingItems)에 남기는 줄.
CHART_OVERSIZED_ITEM = "하루 등락 차트: 크기 초과"
CHART_FAILED_ITEM = "하루 등락 차트: 그리지 못함"
LINE_CHARTS_OVERSIZED_ITEM = "흐름 그래프: 크기 초과"
LINE_CHARTS_FAILED_ITEM = "흐름 그래프: 그리지 못함"
# 그림이 있는 메일의 글자판 끝에 붙인다. 글자판에는 그림을 넣지 못한다.
CHARTS_IN_HTML_NOTE = "(그래프는 HTML 메일에 있습니다)"
# 서버 상태 절을 만들다 실패했을 때 본문 끝에 대신 붙이는 절.
NO_STATUS_TEXT = "──────── 서버 상태 ────────\n- 자료 없음"


def status_report(job: _DailyJob, draft: _Draft, research_count: int, status: StatusPort | None,
                  settings: DailySettings) -> StatusReport | None:
    """마감 후 판 끝에 붙일 서버 상태 절. 만들지 못해도 판은 나간다."""
    if job.edition != EDITION_POST_CLOSE or status is None:
        return None
    try:
        return build_status_report(
            range_start=job.range_start, as_of=job.scheduled_for, current_job_id=job.job_id,
            current_rule_based=draft.generated_by == GENERATED_BY_RULE, operations=job.operations,
            collections=status.collections_between(job.range_start, job.scheduled_for),
            runs=status.job_runs_between(job.range_start, job.scheduled_for),
            stuck_after=settings.stuck_after, current_research_count=research_count)
    except Exception as error:  # noqa: BLE001 - 서버 상태는 덤이라 실패해도 본문을 막지 않는다
        log.warning("server status section failed for job %s: %s", job.job_id, type(error).__name__)
        return StatusReport(NO_STATUS_TEXT, ("서버 상태: 전체(만들지 못함)",))


def chart_image(daily_input: DailyInput, settings: DailySettings) -> tuple[bytes | None, bool, str | None]:
    """마감 후 판의 하루 등락 차트. (그림, 크기 초과 여부, 자료 없음 목록에 더할 줄). 그릴 막대가 없으면 그림이 없다."""
    bars = chart_bars(daily_input)
    if not bars:
        return None, False, None
    try:
        png = render_day_chart(bars)
    except Exception as error:  # noqa: BLE001 - 차트는 덤이라 실패해도 메일은 나간다
        log.warning("daily chart failed for job %s: %s", daily_input.job_id, type(error).__name__)
        return None, False, CHART_FAILED_ITEM
    # 그림은 한 장씩 만들지만 상한(장 수)도 지켜서, 여러 장이 되는 날에도 같은 방식으로 뺀다.
    image_count = 1
    if png is not None and (image_count > settings.max_chart_images
                            or len(png) > settings.max_chart_image_bytes):
        return None, True, CHART_OVERSIZED_ITEM
    return png, False, None


def flow_charts(daily_input: DailyInput, post_close: bool, room: int,
                settings: DailySettings) -> tuple[tuple[LineChart, ...], str | None]:
    """선 그래프(아침 판 개장 흐름, 마감 후 판 장중·기간 흐름). room은 남은 그림 장 수다. (그림들, 자료 없음 목록에 더할 줄)."""
    try:
        charts = line_charts(daily_input, post_close)
    except Exception as error:  # noqa: BLE001 - 그래프는 덤이라 실패해도 메일은 나간다
        log.warning("daily line charts failed for job %s: %s", daily_input.job_id, type(error).__name__)
        return (), LINE_CHARTS_FAILED_ITEM
    kept = tuple(c for c in charts if len(c.png) <= settings.max_chart_image_bytes)[:max(room, 0)]
    return kept, LINE_CHARTS_OVERSIZED_ITEM if len(kept) < len(charts) else None


def daily_event(job: _DailyJob, now: datetime, member_id: int, daily_input: DailyInput,
                sections: tuple[Section, ...], draft: _Draft, text: str, status_text: str | None,
                missing: list[str], settings: DailySettings) -> tuple[OutboundEvent, list[str]]:
    """멤버 한 명의 글자판·HTML·차트를 담은 메일 본문 이벤트. 이벤트가 상한을 넘으면 그림만 빼고 다시 만든다."""
    post_close = job.edition == EDITION_POST_CLOSE
    png, oversized, chart_missing = chart_image(daily_input, settings) if post_close else (None, False, None)
    room = settings.max_chart_images - (1 if png is not None else 0)
    lines, lines_missing = flow_charts(daily_input, post_close, room, settings)

    def build(image: bytes | None, too_large: bool, charts: tuple[LineChart, ...], items: list[str]) -> OutboundEvent:
        slot = None
        if post_close:
            slot = ChartSlot("chart-day" if image is not None else None, too_large, no_data_names(daily_input))
        slots = tuple(ChartSlot(c.content_id, alt=c.alt) for c in charts)
        html = render_html(daily_input, sections, draft.output, status_text, slot, slots)
        # 본문에 자리가 없는 그림(예: 예상 절이 없는 아침 판의 개장 흐름)은 붙이지 않는다. 붙이면 본문에 안 보이는 첨부만 남는다.
        charts = tuple(c for c in charts if f"cid:{c.content_id}" in html)
        images = ([{"contentId": "chart-day", "contentType": "image/png",
                    "data": base64.b64encode(image).decode("ascii")}] if image is not None else []) \
            + [{"contentId": c.content_id, "contentType": "image/png", "data": base64.b64encode(c.png).decode("ascii")}
               for c in charts]
        body = f"{text}\n\n{CHARTS_IN_HTML_NOTE}" if images else text
        return daily_briefing_event(job.job_id, job.correlation_id, job.scheduled_for, now, job.edition,
                                    job.range_start, job.scheduled_for, body, items, draft.generated_by,
                                    member_id, html, images or None)

    def too_big(event: OutboundEvent) -> bool:
        return len(json.dumps(event.value, ensure_ascii=False).encode("utf-8")) > settings.max_event_bytes

    items = [*missing, *(m for m in (chart_missing, lines_missing) if m)]
    event = build(png, oversized, lines, items)
    # 이벤트가 상한을 넘으면 흐름 그래프부터 빼고, 그래도 넘으면 하루 등락 그림까지 뺀다.
    if lines and too_big(event):
        items = [*missing, *(m for m in (chart_missing, LINE_CHARTS_OVERSIZED_ITEM) if m)]
        event = build(png, oversized, (), items)
    if png is not None and too_big(event):
        items = [*missing, CHART_OVERSIZED_ITEM, *([LINE_CHARTS_OVERSIZED_ITEM] if lines else [])]
        event = build(None, True, (), items)
    return event, items
