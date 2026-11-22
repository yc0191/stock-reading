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
from fake_members import FakeMembers, member
from fake_news import answer_news
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
    post_close_output,
    tick,
    POST_CLOSE_BACKGROUND,
)

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
FETCHED = "2026-10-20T16:30:02+09:00"


def etfs():
    return [etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="-1.50"),
            etf_v2("TEST02", "TEST 둘", fetched=FETCHED, rate="2.10"),
            etf_v2("TEST03", "TEST 셋", fetched=FETCHED, rate="0.40"),
            etf_v2_missing("TEST04", name="TEST 넷")]


# 멤버 한 명의 포트폴리오: TEST01·TEST02는 보유, TEST03·TEST04는 관심
ONE_MEMBER = (member(1, held=("TEST01", "TEST02"), watched=("TEST03", "TEST04")),)


def run(edition="post-close", settings=None, llm=True, answer_etfs=None, ref=None, whole_event=False, output=None,
        members=ONE_MEMBER):
    requests, details = Requests(), Details()
    settings = settings or DailySettings()
    output = output or (post_close_output() if edition == "post-close" else good_output(ref or "B1", symbols=(
        "TEST04",)))  # 아침 판 범위 밖에 받은 ETF 셋은 버려지고 일봉이 빈 TEST04만 남는다
    writer = DailyLlmWriter(FakeLlm(output), settings.llm_max_attempts) if llm else None
    core = DailyCore(requests, requests.api, writer, details, settings, members=FakeMembers(members))
    job, at = (POST_CLOSE_JOB, POST_CLOSE_AT) if edition == "post-close" else (MORNING_JOB, MORNING_AT)
    core.on_collection_ready(tick(job, at), at)
    background = POST_CLOSE_BACKGROUND if edition == "post-close" else None
    core.api.reply(daily_response(etfs=answer_etfs or etfs(), background=background), at)
    answer_news(core, requests, at)
    [event] = requests.topic("briefing.daily.generated")
    assert validate("briefing.daily.generated.v2.schema.json", event.value) == []
    return (event.value if whole_event else event.value["payload"]), details


def test_post_close_event_carries_text_html_and_one_chart_the_html_points_at():
    payload, _ = run()
    [image] = payload["images"]
    assert image["contentType"] == "image/png" and base64.b64decode(image["data"]).startswith(PNG_MAGIC)
    assert f'src="cid:{image["contentId"]}"' in payload["html"]
    assert payload["text"].splitlines()[3].startswith("결론: 보유 2개 중 1개 상승")
    assert "결론: 보유 2개 중 1개 상승" in payload["html"]
    assert payload["generatedBy"] == "llm" and "시장 흐름과 함께 움직였을 수 있습니다." in payload["html"]
    # 그림에서 빠진 자료 없음 ETF는 자료 없음 목록에 그대로 있다.
    assert payload["missingItems"] == ["보유 ETF별 하루 등락: TEST 넷, 범위 안에 받은 일봉이 없음"]


def test_the_stored_final_text_is_the_job_wide_text_while_the_mail_is_split_by_the_members_portfolio():
    payload, details = run()
    [result] = details.results()
    # 작업 기록에는 멤버를 가르지 않은 판 전체 글이 남고, 메일 글은 그 멤버의 보유/관심으로 나뉜다.
    assert "결론: ETF 4개 중 2개 상승(자료 없음 1)" in result.final_text and "[보유]" not in result.final_text
    assert "결론: 보유 2개 중 1개 상승 · 관심 2개 중 1개 상승(자료 없음 1)" in payload["text"]


def test_rule_based_post_close_also_gets_html_and_a_chart():
    payload, _ = run(llm=False)
    assert payload["generatedBy"] == "rule-based" and payload["html"] and len(payload["images"]) == 1
    assert "요약" not in payload["html"]


def test_morning_event_has_html_without_a_chart_or_conclusion():
    payload, _ = run("morning")
    assert payload["html"] and "images" not in payload
    assert "결론" not in payload["html"] and "cid:" not in payload["html"]


def test_no_chart_is_drawn_when_no_etf_has_a_value():
    payload, _ = run(answer_etfs=[etf_v2_missing("TEST04", name="TEST 넷")], output=post_close_output(symbols=(), reasons={}))
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
    payload, _ = run(settings=DailySettings(max_event_bytes=8000))
    assert "images" not in payload and "차트 없음(크기 초과)" in payload["html"]
    assert CHART_OVERSIZED_ITEM in payload["missingItems"]
    # 발행과 같은 방식(ensure_ascii=False)으로 잰 크기다.
    assert len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) < 8000


def test_a_chart_failure_still_sends_the_mail_and_logs_no_detail(monkeypatch, caplog):
    def boom(bars):
        raise RuntimeError("secret detail TEST01")
    monkeypatch.setattr(daily_jobs, "render_day_chart", boom)
    with caplog.at_level(logging.WARNING):
        payload, _ = run()
    assert "images" not in payload and CHART_FAILED_ITEM in payload["missingItems"] and payload["html"]
    assert "daily chart failed" in caplog.text and "RuntimeError" in caplog.text
    assert "secret detail" not in caplog.text and "TEST01" not in caplog.text


# ---- 흐름 그래프(장중, 1개월, 6개월) ----

def charted_etfs():
    """TEST01에 장중 흐름(09:00부터 1분마다)과 약 6개월치 기간 종가를 붙인 응답."""
    from datetime import date, timedelta
    items = etfs()
    points = [{"at": f"2026-10-20T{9 + m // 60:02d}:{m % 60:02d}:58+09:00", "price": "10000",
               "rateVsPrevClose": f"{(m % 7 - 3) / 10:.2f}"} for m in range(0, 390, 30)]
    days, day = [], date(2026, 10, 20)
    while len(days) < 130:
        if day.weekday() < 5:
            days.append({"date": day.strftime("%Y%m%d"), "close": str(10000 + len(days)), "rate": None})
        day -= timedelta(days=1)
    items[0] = {**items[0], "intradaySeries": {"status": "present", "prevClose": "10000", "truncated": False,
                                               "points": points},
                "dailyCloses": {"status": "present", "fetchedAt": FETCHED, "truncated": False,
                                "points": list(reversed(days))}}
    return items


def test_the_post_close_mail_carries_the_day_intraday_and_period_charts_in_order_before_the_etf_reasons():
    payload, _ = run(answer_etfs=charted_etfs())
    assert [i["contentId"] for i in payload["images"]] == ["chart-day", "chart-intraday", "chart-1m", "chart-6m"]
    html = payload["html"]
    spots = [html.index(f"cid:{cid}") for cid in ("chart-day", "chart-intraday", "chart-1m", "chart-6m")]
    assert spots == sorted(spots) and spots[-1] < html.index("이유:")
    assert payload["text"].endswith("(그래프는 HTML 메일에 있습니다)")


def test_the_line_charts_are_dropped_before_the_day_chart_when_the_event_is_too_big():
    full, _ = run(answer_etfs=charted_etfs(), whole_event=True)
    day_only, _ = run(whole_event=True)
    size = lambda event: len(json.dumps(event, ensure_ascii=False).encode("utf-8"))
    payload, _ = run(answer_etfs=charted_etfs(), settings=DailySettings(max_event_bytes=(size(full) + size(day_only)) // 2))
    assert [i["contentId"] for i in payload["images"]] == ["chart-day"]
    assert "흐름 그래프: 크기 초과" in payload["missingItems"] and "cid:chart-intraday" not in payload["html"]


def test_a_morning_without_forecasts_attaches_no_open_flow_chart_and_no_note():
    from test_daily_jobs import OPEN_AT
    requests, details = Requests(), Details()
    # Claude 없이(규칙 기반) 나가는 아침 판에는 예상 절이 없어 개장 흐름 그림의 자리가 없다.
    core = DailyCore(requests, requests.api, None, details, DailySettings(), members=FakeMembers(ONE_MEMBER))
    core.on_collection_ready(tick(MORNING_JOB, OPEN_AT), OPEN_AT)
    points = [{"at": f"2026-10-20T09:0{m}:58+09:00", "price": "10000", "rateVsPrevClose": f"0.{m}0"} for m in range(5)]
    item = {**etf_v2("TEST01", "TEST 하나"), "intradaySeries": {"status": "present", "prevClose": "10000",
                                                              "truncated": False, "points": points}}
    core.api.reply(daily_response(etfs=[item]), OPEN_AT)
    answer_news(core, requests, OPEN_AT)
    [event] = requests.topic("briefing.daily.generated")
    payload = event.value["payload"]
    assert "images" not in payload and "cid:" not in payload["html"]
    assert "(그래프는 HTML 메일에 있습니다)" not in payload["text"]

