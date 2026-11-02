"""live_briefing.py 의 계산: 가짜 변동·수집 완료·기사의 모양, 로그 세기, 리포트 파일. 실제 DB·Kafka 는 부르지 않는다."""
import json
import stat
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import live_briefing as lb

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts" / "events"
KST = timezone(timedelta(hours=9))
SNAPSHOT = lb.Snapshot("TEST01", 10150, datetime(2026, 10, 20, 7, 0, 31, tzinfo=timezone.utc))
NOW = datetime(2026, 10, 20, 16, 2, tzinfo=KST)


def schema_errors(name: str, instance: dict) -> list:
    schema = json.loads((CONTRACTS / f"{name}.v1.schema.json").read_text(encoding="utf-8"))
    return [e.message for e in Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(instance)]


def test_move_uses_the_snapshot_price_and_a_lower_previous_price():
    move = lb.move_event(SNAPSHOT, "RUN1", NOW)
    payload = move["payload"]
    assert payload["price"] == 10150
    assert payload["previousPrice"] == 10000
    assert payload["changePercent"] == 1.5
    assert payload["changePercent"] >= payload["thresholdPercent"]
    assert payload["asOf"] == "2026-10-20T16:00:31+09:00"
    assert move["correlationId"] == "test-all-RUN1"
    assert schema_errors("market.move", move) == []


def test_change_percent_is_computed_from_the_rounded_previous_price():
    payload = lb.move_event(lb.Snapshot("TEST01", 12345, SNAPSHOT.as_of), "RUN1", NOW)["payload"]
    expected = round((12345 - payload["previousPrice"]) / payload["previousPrice"] * 100, 2)
    assert payload["changePercent"] == expected
    assert 1.0 <= payload["changePercent"] <= 2.0


@pytest.mark.parametrize("price", [0, 1, 2])
def test_move_refuses_a_price_too_small_to_move(price):
    with pytest.raises(ValueError):
        lb.move_event(lb.Snapshot("TEST01", price, SNAPSHOT.as_of), "RUN1", NOW)


def test_completion_starts_after_move_time_plus_delta_so_briefing_can_use_it():
    # 브리핑 설정의 Δ 와 비교한다. Δ 가 늘어 가짜 완료를 못 쓰게 되면 여기서 알 수 있다.
    sys.path.insert(0, str(ROOT / "services" / "briefing" / "src"))
    try:
        from briefing.config import DEFAULT_SETTINGS
    finally:
        sys.path.pop(0)
    completion = lb.completion_event(SNAPSHOT, "RUN1", NOW)
    started = datetime.fromisoformat(completion["payload"]["startedAt"])
    assert started >= SNAPSHOT.as_of + DEFAULT_SETTINGS.delta
    assert completion["payload"]["jobType"] == "collect-news"
    assert completion["payload"]["incompleteSymbols"] == []
    assert completion["occurredAt"] == completion["payload"]["finishedAt"]
    assert schema_errors("collection.completed", completion) == []


def test_fake_article_is_inside_the_evidence_window_and_clearly_fake():
    article = lb.fake_article(SNAPSHOT, "RUN1")
    assert SNAPSHOT.as_of - timedelta(hours=24) <= article.published_at <= SNAPSHOT.as_of
    assert article.title.startswith("[테스트]")
    assert article.url.startswith("https://test-all.invalid/")
    assert len(article.article_key) == 64
    assert lb.fake_article(SNAPSHOT, "RUN2").article_key != article.article_key


def test_llm_calls_are_retry_warnings_plus_the_successful_call():
    warning = "WARNING briefing.report: LLM report attempt 1/3 failed for job x: output is not valid JSON\n"
    assert lb.count_llm_calls("", published=True) == 1
    assert lb.count_llm_calls(warning, published=True) == 2
    assert lb.count_llm_calls(warning * 3, published=False) == 3
    assert lb.count_llm_calls("", published=False) == 0


def test_mail_counts_real_sends_and_dry_runs_separately():
    log = ('INFO SmtpMailSender : email sent to=a***@example.com subject="[테스트] [TEST01] 상승 +1.50% 브리핑"\n'
           'INFO LoggingMailSender : DRY-RUN (not sent) to=a***@example.com subject="x"\n')
    assert lb.count_mail(log) == (1, 1)
    assert lb.count_mail("") == (0, 0)


def test_run_briefings_are_picked_by_the_run_correlation_id():
    messages = [{"correlationId": "test-all-RUN1"}, {"correlationId": "collect-x"}]
    mine, others = lb.run_briefings(messages, "RUN1")
    assert mine == [messages[0]]
    assert others == 1


def test_report_file_is_private_and_has_the_text_and_evidence(tmp_path):
    event = json.loads((CONTRACTS / "samples" / "v1" / "briefing.generated.sample.json").read_text(encoding="utf-8"))
    path = tmp_path / "build" / "test-all-live" / "briefing-RUN1.txt"
    lb.write_private(path, lb.report_text(event))
    text = path.read_text(encoding="utf-8")
    assert event["payload"]["text"] in text
    assert f"생성 방식: {event['payload']['generatedBy']}" in text
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700


def test_report_directory_is_ignored_by_git():
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "build/" in gitignore


def test_main_refuses_unknown_commands(capsys):
    assert lb.main([]) == 64
    assert lb.main(["send"]) == 64


def test_seed_without_settings_fails_without_touching_anything(monkeypatch, capsys):
    for name in ("LIVE_BRIEFING_RUN_ID", "LIVE_BRIEFING_KAFKA", "LIVE_BRIEFING_DB_URL"):
        monkeypatch.delenv(name, raising=False)
    assert lb.main(["seed"]) == 1
    assert "LIVE_BRIEFING_RUN_ID" in capsys.readouterr().err
