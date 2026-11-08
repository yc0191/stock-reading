"""브리핑 코어가 내보내고 받는 이벤트의 모양. Kafka는 모르고, 통신은 main.py가 맡는다."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

HISTORY_REQUESTED_TOPIC = "market.history.requested"
ARTICLES_REQUESTED_TOPIC = "news.articles.requested"
CONTEXT_REQUESTED_TOPIC = "market.context.requested"
FILINGS_REQUESTED_TOPIC = "news.filings.requested"
RESEARCH_REQUESTED_TOPIC = "news.research.requested"
BRIEFING_GENERATED_TOPIC = "briefing.generated"
DAILY_BRIEFING_GENERATED_TOPIC = "briefing.daily.generated"

# 데일리 브리핑의 판 종류(briefing.daily.generated 의 edition)와 그 판을 시작시키는 schedule.tick 의 jobType.
EDITION_MORNING = "morning"
EDITION_POST_CLOSE = "post-close"
JOB_TYPE_BY_EDITION = {
    EDITION_MORNING: "daily-briefing-morning",
    EDITION_POST_CLOSE: "daily-briefing-post-close",
}

# 재검색을 부른 작업의 종류(news.research.requested 의 jobKind). 데일리 판은 판 종류 값을 그대로 쓴다.
JOB_KIND_MOVE = "move"
RESEARCH_JOB_KINDS = (JOB_KIND_MOVE, EDITION_MORNING, EDITION_POST_CLOSE)


@dataclass(frozen=True)
class OutboundEvent:
    topic: str
    key: str
    value: dict[str, Any]


def parse_timestamp(text: str) -> datetime:
    """'Z'와 '+09:00' 표기를 모두 읽는다. 오프셋이 없는 시각은 거부한다."""
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp without offset: {text}")
    return parsed


def format_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def request_event(topic: str, kind: str, correlation_id: str, now: datetime, payload: dict[str, Any]) -> OutboundEvent:
    """요청 봉투를 만든다. eventId는 (작업, 종류, seq)에서 정해져 같은 요청이 다시 나가도 같다."""
    event_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{kind}:{payload['jobId']}:{payload['seq']}")
    return OutboundEvent(
        topic=topic,
        key=payload["symbol"],
        value={
            "eventId": str(event_id),
            "eventType": kind,
            "schemaVersion": 1,
            "occurredAt": format_timestamp(now),
            "producedAt": format_timestamp(now),
            "correlationId": correlation_id,
            "payload": payload,
        },
    )


def research_request_event(job_id: str, seq: int, job_kind: str, symbol: str, queries: list[str],
                           correlation_id: str, now: datetime) -> OutboundEvent:
    """재검색 요청 봉투를 만든다. 응답 이벤트는 없고, 수집기는 기사와 완료 알림에 job_id를 researchJobId로 싣는다."""
    if job_kind not in RESEARCH_JOB_KINDS:
        raise ValueError(f"unknown job kind: {job_kind}")
    return request_event(RESEARCH_REQUESTED_TOPIC, RESEARCH_REQUESTED_TOPIC, correlation_id, now, {
        "jobId": job_id,
        "seq": seq,
        "jobKind": job_kind,
        "symbol": symbol,
        "queries": list(queries),
    })


def daily_briefing_event(tick_event_id: str, correlation_id: str, scheduled_for: datetime, now: datetime,
                         edition: str, range_start: datetime, range_end: datetime, text: str,
                         missing_items: list[str], generated_by: str, html: str | None = None,
                         images: list[dict[str, str]] | None = None) -> OutboundEvent:
    """데일리 브리핑 결과 봉투를 만든다. html·images는 선택 칸이라 있을 때만 싣는다.
    eventId는 (tick, 판)에서 정해져 같은 tick을 다시 처리해도 같다."""
    if edition not in JOB_TYPE_BY_EDITION:
        raise ValueError(f"unknown edition: {edition}")
    event_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{DAILY_BRIEFING_GENERATED_TOPIC}:{tick_event_id}:{edition}")
    payload: dict[str, Any] = {
        "edition": edition,
        "rangeStart": format_timestamp(range_start),
        "rangeEnd": format_timestamp(range_end),
        "text": text,
        "missingItems": list(missing_items),
        "generatedBy": generated_by,
    }
    if html:
        payload["html"] = html
        if images:
            payload["images"] = list(images)
    return OutboundEvent(
        topic=DAILY_BRIEFING_GENERATED_TOPIC,
        key=edition,
        value={
            "eventId": str(event_id),
            "eventType": DAILY_BRIEFING_GENERATED_TOPIC,
            "schemaVersion": 1,
            "occurredAt": format_timestamp(scheduled_for),
            "producedAt": format_timestamp(now),
            "correlationId": correlation_id,
            "payload": payload,
        },
    )
