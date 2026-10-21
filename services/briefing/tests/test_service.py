import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from briefing.evidence import load_sample_news
from briefing.service import handle_move
from contracts import sample_text, validate

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 10, 20, 16, 0, 5, tzinfo=KST)
SAMPLE_FILE = Path(__file__).resolve().parents[1] / "data" / "sample_news.json"


@pytest.fixture
def news():
    # 샘플 뉴스를 커밋된 market.move 샘플의 변동 시각에 맞춘다.
    return load_sample_news(SAMPLE_FILE, anchor=datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST))


def move_json() -> str:
    return sample_text("market.move.sample.json")


def test_briefing_event_matches_the_contract(news):
    result = handle_move(move_json(), news, NOW)

    assert validate("briefing.generated.v1.schema.json", json.loads(result.value)) == []


def test_briefing_is_keyed_by_symbol_and_carries_the_correlation_id_and_move_time(news):
    result = handle_move(move_json(), news, NOW)
    event = json.loads(result.value)

    assert result.key == "TEST01"
    assert event["correlationId"] == "collect-2026-10-20T16:00:30+09:00"
    assert event["occurredAt"] == "2026-10-20T16:00:31+09:00"
    assert event["payload"]["generatedBy"] == "rule-based"


def test_redelivering_the_same_move_yields_the_same_briefing_event_id(news):
    first = json.loads(handle_move(move_json(), news, NOW).value)
    second = json.loads(handle_move(move_json(), news, NOW + timedelta(minutes=5)).value)

    assert first["eventId"] == second["eventId"]


def test_evidence_never_includes_news_published_after_the_move(news):
    event = json.loads(handle_move(move_json(), news, NOW).value)
    titles = [item["title"] for item in event["payload"]["evidence"]]

    assert titles, "sample data should produce at least one evidence item"
    assert not any("FUTURE" in title for title in titles)
    assert not any("OLD" in title for title in titles)
    for item in event["payload"]["evidence"]:
        assert item["publishedAt"] <= event["payload"]["asOf"]


def test_move_without_any_evidence_still_produces_a_valid_honest_briefing():
    result = handle_move(move_json(), [], NOW)

    assert "찾지 못했습니다" in result.text
    event = json.loads(result.value)
    assert event["payload"]["evidence"] == []
    assert validate("briefing.generated.v1.schema.json", event) == []


@pytest.mark.parametrize("message", ["not json", "{}", '{"eventType": "market.snapshot", "payload": {}}', b"\xff\xfe"])
def test_malformed_input_raises_value_error(message, news):
    with pytest.raises(ValueError):
        handle_move(message, news, NOW)
