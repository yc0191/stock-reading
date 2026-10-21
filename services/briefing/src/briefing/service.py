"""market.move -> briefing.generated 메시지 처리. Kafka와 분리되어 있어 직접 테스트할 수 있다."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime

from briefing.compose import compose
from briefing.evidence import NewsItem, select_evidence

GENERATED_BY = "rule-based"


@dataclass(frozen=True)
class BriefingResult:
    key: str
    value: str
    text: str


def handle_move(message: str | bytes, news: list[NewsItem], now: datetime) -> BriefingResult:
    """market.move 이벤트 하나로 브리핑을 만든다. 형식이 잘못되면 ValueError를 던진다."""
    try:
        move = json.loads(message)
        if move["eventType"] != "market.move":
            raise ValueError(f"unexpected eventType: {move['eventType']}")
        payload = move["payload"]
        symbol = payload["symbol"]
        as_of = datetime.fromisoformat(payload["asOf"])
        change = float(payload["changePercent"])
        previous_price, price = int(payload["previousPrice"]), int(payload["price"])
        move_event_id, correlation_id = move["eventId"], move["correlationId"]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"malformed market.move event: {error!r}") from error

    evidence = select_evidence(news, symbol, as_of)
    text = compose(symbol, previous_price, price, change, as_of, evidence)

    event = {
        # 결정적 ID: 같은 변동을 다시 받아도 같은 브리핑 ID가 나와 소비자가 중복을 걸러낼 수 있다.
        "eventId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"briefing:{move_event_id}")),
        "eventType": "briefing.generated",
        "schemaVersion": 1,
        "occurredAt": as_of.isoformat(timespec="seconds"),
        "producedAt": now.isoformat(timespec="seconds"),
        "correlationId": correlation_id,
        "payload": {
            "symbol": symbol,
            "changePercent": change,
            "asOf": as_of.isoformat(timespec="seconds"),
            "text": text,
            "generatedBy": GENERATED_BY,
            "evidence": [
                {
                    "title": item.title,
                    "source": item.source,
                    "publishedAt": item.published_at.isoformat(timespec="seconds"),
                }
                for item in evidence
            ],
        },
    }
    return BriefingResult(key=symbol, value=json.dumps(event, ensure_ascii=False), text=text)
