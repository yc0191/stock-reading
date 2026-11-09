"""daily_e2e.py 의 계산: 가짜 수집 자료·시작 신호의 모양, 가짜 LLM 응답. 실제 Kafka·DB·LLM 은 부르지 않는다."""
import json
import smtplib
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
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


def test_fake_llm_writes_the_post_close_shape_with_one_reason_per_etf_that_has_a_daily_move():
    user = json.dumps({"edition": "마감 후 판", "items": [
        {"name": "그날 한국 증시", "entries": [{"ref": "B1"}]},
        {"name": "보유 ETF별 하루 등락", "entries": [{"ref": "E1", "symbol": "TEST01"}, {"ref": "E2", "symbol": "TEST02"}]}]})
    output = json.loads(de.fake_llm_output(user))
    assert output["summary"] == [de.FAKE_STATEMENT, de.FAKE_STATEMENT] and output["notables"] == []
    assert output["etfs"] == [{"symbol": "TEST01", "reason": de.FAKE_STATEMENT, "refs": ["E1"], "weak": False,
                               "shortfall": ""},
                              {"symbol": "TEST02", "reason": de.FAKE_STATEMENT, "refs": ["E2"], "weak": False,
                               "shortfall": ""}]
    assert json.loads(de.fake_llm_output(json.dumps({"edition": "마감 후 판", "items": []}))) == {
        "summary": [], "etfs": [], "notables": [], "insufficientEvidence": True}


def test_fake_message_has_the_fields_the_sdk_reads():
    message = de.fake_message({"model": "m", "messages": [{"role": "user", "content": json.dumps({"items": []})}]})
    assert message["type"] == "message" and message["stop_reason"] == "end_turn"
    assert message["content"][0]["type"] == "text"
    assert json.loads(message["content"][0]["text"])["insufficientEvidence"] is True


def test_smtp_sink_saves_a_received_mail_and_sends_it_nowhere(tmp_path):
    port_file = tmp_path / "port"
    threading.Thread(target=de.serve_smtp_sink, args=(str(port_file), str(tmp_path / "mail")), daemon=True).start()
    for _ in range(100):
        if port_file.exists() and port_file.read_text():
            break
        threading.Event().wait(0.05)
    with smtplib.SMTP("127.0.0.1", int(port_file.read_text()), timeout=5) as smtp:
        smtp.sendmail("a@example.com", ["b@example.com"], "Subject: t\r\n\r\n.점으로 시작하는 줄\r\n".encode())
    saved = (tmp_path / "mail" / "mail-01.eml").read_bytes().decode()
    assert "점으로 시작하는 줄" in saved


def test_smtp_json_points_the_notifier_only_at_loopback_with_no_account():
    mail = json.loads(de.smtp_sink_json(2525))["spring"]["mail"]
    assert (mail["host"], mail["port"], mail["username"], mail["password"]) == ("127.0.0.1", 2525, "", "")
    assert mail["properties"]["mail.smtp.auth"] is False and mail["properties"]["mail.smtp.starttls.enable"] is False


def test_smtp_json_command_prints_the_same_value(capsys):
    de.main(["daily_e2e.py", "smtp-json", "2525"])
    assert capsys.readouterr().out.strip() == de.smtp_sink_json(2525)


def test_the_sink_mode_of_the_script_drops_leftover_spring_mail_settings_and_uses_the_json():
    script = (Path(__file__).resolve().parents[1] / "manual-daily-briefing.sh").read_text(encoding="utf-8")
    sink = script[script.index('if [ "$SMTP_SINK" = 1 ]; then\n    # 이 인스턴스에만'):]
    sink = sink[:sink.index("else")]
    unset = sink.index("unset SPRING_MAIL_HOST SPRING_MAIL_PORT SPRING_MAIL_USERNAME SPRING_MAIL_PASSWORD")
    assert unset < sink.index("daily_e2e.py\" smtp-json") < sink.index("export SPRING_APPLICATION_JSON")
    assert "SMTP_HOST=" not in sink
