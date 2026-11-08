"""데일리 판이 글자판과 함께 HTML·차트 그림을 메일 본문 이벤트에 싣는 흐름을 가짜 요청 출구·가짜 LLM으로 돌려 확인한다."""
import base64
import json
import logging

import pytest

import briefing.daily_jobs as daily_jobs
from briefing.config import DailySettings
from briefing.daily_jobs import CHART_FAILED_ITEM, CHART_OVERSIZED_ITEM, DailyCore
from briefing.daily_report import DailyLlmWriter
from contracts import validate
from test_daily_jobs import (
    MORNING_AT,
    MORNING_JOB,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    etf_v2,
    etf_v2_missing,
    good_output,
    tick,
    POST_CLOSE_MARKET,
)

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
FETCHED = "2026-10-20T16:30:02+09:00"


def etfs(relation=True):
    items = [etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="-1.50"),
             etf_v2("TEST02", "TEST 둘", fetched=FETCHED, rate="2.10"),
             etf_v2("TEST03", "TEST 셋", fetched=FETCHED, rate="0.40"),
             etf_v2_missing("TEST04", name="TEST 넷")]
    if relation:
        for item, value in zip(items, ("HELD", "HELD", "WATCH", "WATCH")):
            item["relation"] = value
    return items


def run(edition="post-close", settings=None, llm=True, answer_etfs=None, ref=None, whole_event=False):
    requests, details = Requests(), Details()
    settings = settings or DailySettings()
    writer = DailyLlmWriter(FakeLlm(good_output(ref or ("E1" if edition == "post-close" else "B1"))),
                            settings.llm_max_attempts) if llm else None
    core = DailyCore(requests, writer, details, settings)
    job, at = (POST_CLOSE_JOB, POST_CLOSE_AT) if edition == "post-close" else (MORNING_JOB, MORNING_AT)
    core.on_schedule_tick(tick(job, at), at, edition)
    background = POST_CLOSE_MARKET if edition == "post-close" else None
    core.on_daily_data_responded(daily_response(etfs=answer_etfs or etfs(), background=background), at)
    if edition == "morning":
        from test_daily_jobs import filings_response
        core.on_filings_responded(filings_response(), at)
    [event] = requests.topic("briefing.daily.generated")
    assert validate("briefing.daily.generated.v1.schema.json", event.value) == []
    return (event.value if whole_event else event.value["payload"]), details


def test_post_close_event_carries_text_html_and_one_chart_the_html_points_at():
    payload, _ = run()
    [image] = payload["images"]
    assert image["contentType"] == "image/png" and base64.b64decode(image["data"]).startswith(PNG_MAGIC)
    assert f'src="cid:{image["contentId"]}"' in payload["html"]
    assert payload["text"].splitlines()[3].startswith("결론: 보유 2개 중 1개 상승")
    assert "결론: 보유 2개 중 1개 상승" in payload["html"]
    assert payload["generatedBy"] == "llm" and "미국 지수가 오른 가운데" in payload["html"]
    # 그림에서 빠진 자료 없음 ETF는 자료 없음 목록에 그대로 있다.
    assert payload["missingItems"] == ["보유 ETF별 하루 등락: TEST 넷, 범위 안에 받은 일봉이 없음"]


def test_the_stored_final_text_is_the_text_sent_in_the_event():
    payload, details = run()
    [result] = details.results()
    assert result.final_text == payload["text"]


def test_rule_based_post_close_also_gets_html_and_a_chart():
    payload, _ = run(llm=False)
    assert payload["generatedBy"] == "rule-based" and payload["html"] and len(payload["images"]) == 1
    assert "요약" not in payload["html"]


def test_morning_event_has_html_without_a_chart_or_conclusion():
    payload, _ = run("morning")
    assert payload["html"] and "images" not in payload
    assert "결론" not in payload["html"] and "cid:" not in payload["html"]


def test_without_relation_the_conclusion_is_one_group_and_no_watch_suffix():
    payload, _ = run(answer_etfs=etfs(relation=False))
    assert "ETF 4개 중 2개 상승(자료 없음 1)" in payload["text"] and "관심" not in payload["html"]


def test_an_unknown_relation_value_is_treated_as_no_relation():
    items = etfs()
    for item in items:
        item["relation"] = "NONE"
    payload, _ = run(answer_etfs=items)
    assert "ETF 4개 중 2개 상승" in payload["text"]


def test_no_chart_is_drawn_when_no_etf_has_a_value():
    payload, _ = run(answer_etfs=[etf_v2_missing("TEST04", name="TEST 넷")], ref="B1")
    assert "images" not in payload and "결론: 등락 자료 없음" in payload["text"] and "cid:" not in payload["html"]


def test_a_chart_over_the_image_limit_is_dropped_and_said_so(monkeypatch):
    monkeypatch.setattr(daily_jobs, "render_day_chart", lambda bars: PNG_MAGIC + b"\0" * (300 * 1024 + 1))
    payload, _ = run()
    assert "images" not in payload and "cid:" not in payload["html"]
    assert "차트 없음(크기 초과)" in payload["html"] and CHART_OVERSIZED_ITEM in payload["missingItems"]
    # 메일은 늘 나가고 글자판은 그대로다.
    assert payload["text"].startswith("[마감 후 판]") and "결론:" in payload["text"]


def test_a_chart_exactly_at_the_image_limit_is_kept(monkeypatch):
    monkeypatch.setattr(daily_jobs, "render_day_chart", lambda bars: PNG_MAGIC + b"\0" * (300 * 1024 - len(PNG_MAGIC)))
    payload, _ = run()
    assert len(payload["images"]) == 1 and CHART_OVERSIZED_ITEM not in payload["missingItems"]


def test_more_charts_than_the_image_count_limit_are_dropped_like_an_oversized_one():
    payload, _ = run(settings=DailySettings(max_chart_images=0))
    assert "images" not in payload and "cid:" not in payload["html"]
    assert "차트 없음(크기 초과)" in payload["html"] and CHART_OVERSIZED_ITEM in payload["missingItems"]


def test_the_event_size_is_measured_the_way_it_is_published():
    # 발행은 ensure_ascii=False 로 직렬화하므로, 기본 직렬화로 재면 한글이 많은 이벤트를 크게 잘못 잰다.
    event, _ = run(whole_event=True)
    published = len(json.dumps(event, ensure_ascii=False).encode("utf-8"))
    escaped = len(json.dumps(event).encode("utf-8"))
    assert escaped > published
    # 상한이 두 크기 사이면 발행 크기로 재는 코드에서만 그림이 남는다.
    payload, _ = run(settings=DailySettings(max_event_bytes=(published + escaped) // 2))
    assert len(payload["images"]) == 1 and CHART_OVERSIZED_ITEM not in payload["missingItems"]


def test_one_chart_is_kept_at_an_image_count_limit_of_one():
    payload, _ = run(settings=DailySettings(max_chart_images=1))
    assert len(payload["images"]) == 1 and CHART_OVERSIZED_ITEM not in payload["missingItems"]


def test_an_event_over_the_event_limit_loses_only_the_chart():
    payload, _ = run(settings=DailySettings(max_event_bytes=6000))
    assert "images" not in payload and "차트 없음(크기 초과)" in payload["html"]
    assert CHART_OVERSIZED_ITEM in payload["missingItems"]
    assert len(json.dumps(payload).encode("utf-8")) < 6000


def test_a_chart_failure_still_sends_the_mail_and_logs_no_detail(monkeypatch, caplog):
    def boom(bars):
        raise RuntimeError("secret detail TEST01")
    monkeypatch.setattr(daily_jobs, "render_day_chart", boom)
    with caplog.at_level(logging.WARNING):
        payload, _ = run()
    assert "images" not in payload and CHART_FAILED_ITEM in payload["missingItems"] and payload["html"]
    assert "daily chart failed" in caplog.text and "RuntimeError" in caplog.text
    assert "secret detail" not in caplog.text and "TEST01" not in caplog.text
