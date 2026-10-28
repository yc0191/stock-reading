"""브리핑 코어가 내보내고 받는 이벤트의 모양. Kafka는 모르고, 통신은 main.py가 맡는다."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

HISTORY_REQUESTED_TOPIC = "market.history.requested"
ARTICLES_REQUESTED_TOPIC = "news.articles.requested"


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

