"""daily_e2e.py 의 계산: 가짜 수집 자료·시작 신호의 모양, 가짜 LLM 응답. 실제 Kafka·DB·LLM 은 부르지 않는다."""
import json
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import daily_e2e as de

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 11, 5, 8, 45, tzinfo=KST)


def test_every_seeded_event_passes_its_contract():
    events = de.seed_events(NOW, "RUN1")
    assert [topic for topic, _, _ in events].count(de.INSIGHT_TOPIC) == 8
    assert [topic for topic, _, _ in events].count(de.FILING_TOPIC) == 1
    for topic, _, event in events:
        assert de.schema_errors(topic, event) == [], topic


def test_seeded_data_is_received_after_the_previous_weekday_and_before_the_tick():
    previous_weekday = NOW - timedelta(days=1)
    for topic, _, event in de.seed_events(NOW, "RUN1"):
        payload = event["payload"]
        at = datetime.fromisoformat(payload.get("fetchedAt") or payload["acceptedAt"].replace("Z", "+00:00"))
        assert previous_weekday <= at < NOW, topic


def test_values_are_fake():
    for _, key, event in de.seed_events(NOW, "RUN1"):
        target = event["payload"].get("target")
        if target is not None:
            assert target.startswith(("TEST", "FAKE")) or len(target) == 1
    assert de.holdings_setting() == "TEST01=TESTA,FAKEB"


def test_holdings_file_matches_the_collector_setting():
    items = json.loads(de.holdings_file_json())
    from_file = {item["symbol"]: [m["symbol"] for m in item["members"] if m["market"] == "US"] for item in items}
    from_setting = dict(part.split("=") for part in de.holdings_setting().split(";"))

    # 미국 종목이 있는 ETF 의 티커 목록이 ETF_US_HOLDINGS 와 같아야 시세처리기가 시작한다.
    assert {etf: ",".join(t) for etf, t in from_file.items() if t} == from_setting
    assert all(item["kind"] == "ETF" and item["market"] == "KR" for item in items)


def test_us_close_is_the_latest_new_york_close_before_the_time():
    ny = ZoneInfo("America/New_York")
    # 뉴욕 목요일 15:59 에는 아직 수요일 장이 마지막 마감이다.
    assert de.last_ny_close(datetime(2026, 11, 5, 15, 59, tzinfo=ny)) == date(2026, 11, 4)
    assert de.last_ny_close(datetime(2026, 11, 5, 16, 0, tzinfo=ny)) == date(2026, 11, 5)
    # 월요일 아침이면 금요일 장이다.
    assert de.last_ny_close(datetime(2026, 11, 9, 9, 0, tzinfo=ny)) == date(2026, 11, 6)


def test_ticks_are_one_per_edition_with_the_same_scheduled_time():
    ticks = de.tick_events(NOW)
    assert [e["payload"]["jobType"] for _, _, e in ticks] == ["daily-briefing-morning", "daily-briefing-post-close"]
    assert {e["payload"]["scheduledFor"] for _, _, e in ticks} == {"2026-11-05T08:45:00+09:00"}
    for topic, _, event in ticks:
        assert de.schema_errors(topic, event) == []


def test_fake_llm_cites_the_first_ref_given():
    user = json.dumps({"items": [{"name": "a", "entries": []}, {"name": "b", "entries": [{"ref": "B1"}, {"ref": "E1"}]}]})
    output = json.loads(de.fake_llm_output(user))
    assert output == {"points": [{"statement": de.FAKE_STATEMENT, "refs": ["B1"]}], "insufficientEvidence": False}


def test_fake_llm_says_evidence_is_insufficient_without_refs():
    assert json.loads(de.fake_llm_output(json.dumps({"items": []}))) == {"points": [], "insufficientEvidence": True}
    assert json.loads(de.fake_llm_output("not json"))["insufficientEvidence"] is True


def test_fake_message_has_the_fields_the_sdk_reads():
    message = de.fake_message({"model": "m", "messages": [{"role": "user", "content": json.dumps({"items": []})}]})
    assert message["type"] == "message" and message["stop_reason"] == "end_turn"
    assert message["content"][0]["type"] == "text"
    assert json.loads(message["content"][0]["text"])["insufficientEvidence"] is True
