"""아침 판의 전 거래일 한국 증시 칸: 조회 구간, 마감 값 판정, 입력·출력 검사, 글자판·HTML, 조회 실패. 외부 의존은 모두 가짜다."""
import json
from datetime import datetime, timedelta

import pytest

from briefing.config import DEFAULT_SETTINGS, DailySettings
from briefing.daily_html import render_html
from briefing.daily_inputs import is_prev_close, prev_close_range
from briefing.daily_jobs import PREV_MARKET, STAGE_PREV_MARKET, DailyCore
from briefing.daily_morning import KR_PREV_TITLE, OVERNIGHT_TITLE
from briefing.daily_report import GENERATED_BY_LLM, GENERATED_BY_RULE, DailyLlmWriter
from briefing.daily_sections import ITEM_KR_PREV, build_sections
from briefing.daily_output import InvalidDailyOutput, parse_morning_output
from briefing.job_log import EntryType
from briefing.ports import FailureReason
from briefing.queries import QueryKind
from briefing.sources import Background, parse_background
from contracts import validate_payload
from fake_api import prev_market_background
from fake_news import answer_news
from test_daily_jobs import (
    KST,
    MORNING_AT,
    TICK_ID,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    morning_output,
    published,
    tick,
)

# 화요일 아침 판(MORNING_AT)의 전 거래일 조회 구간 끝(판 날짜 0시)과 그 전날 마감 뒤 받은 시각
UNTIL = datetime(2026, 10, 20, 0, 0, tzinfo=KST)
CLOSE_FETCHED = "2026-10-19T16:30:00+09:00"
# 판 자료의 배경 B1~B3(세계지수, 환율종합, 오늘 시장종합) 다음이 전 거래일 한국 증시다.
KR_REF = "B4"
KR_LINE = "코스피 −1.23% · 코스닥 +0.45% (10-19(월) 마감)"
KR_REASON = "외국인 매도가 이어지며 코스피가 내렸을 수 있습니다."


def make(llm=None):
    requests, details = Requests(), Details()
    settings = DailySettings()
    writer = DailyLlmWriter(llm, settings.llm_max_attempts) if llm is not None else None
    return DailyCore(requests, requests.api, writer, details, settings), requests, details


def kr_issue(statement=KR_REASON, refs=(KR_REF,), weak=False):
    return {"statement": statement, "refs": list(refs), "weak": weak,
            "shortfall": "전 거래일 장 기사가 더 있어야 합니다." if weak else ""}


def run_morning(llm=None, prev=None, auto=True):
    """아침 판 하나를 끝까지 돌린다. prev는 전 거래일 조회에 답할 시장종합 배경이다(None이면 가짜 기본값)."""
    c, requests, details = make(llm)
    c.api.auto_prev_market = auto
    c.api.prev_market = prev
    c.on_collection_ready(tick(), MORNING_AT)
    c.api.reply(daily_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    return c, requests, details


# ---- 조회 구간과 마감 값 판정 ----

def test_the_range_ends_at_midnight_of_the_edition_day_and_spans_the_background_window():
    since, until = prev_close_range(MORNING_AT, DEFAULT_SETTINGS.background_window)
    assert until == UNTIL and since == UNTIL - timedelta(days=4)
    # 월요일 판은 금요일 16:30 마감 값이 구간 안에 든다(약 65시간 전).
    monday = datetime(2026, 10, 26, 9, 10, tzinfo=KST)
    since, until = prev_close_range(monday, DEFAULT_SETTINGS.background_window)
    friday_close = datetime(2026, 10, 23, 16, 30, tzinfo=KST)
    assert since <= friday_close < until


def summary_at(at, status="ok"):
    return Background("market-summary", None, status, "{}", at)


@pytest.mark.parametrize("at, usable", [
    (datetime(2026, 10, 19, 16, 0, tzinfo=KST), True),         # 기준 시각과 같으면 마감 값
    (datetime(2026, 10, 19, 15, 59, 59, tzinfo=KST), False),   # 그 직전은 아니다
    (datetime(2026, 10, 19, 15, 30, 2, tzinfo=KST), False),    # 장중 수집 마지막 회차(15:30)는 마감 값으로 보지 않는다
    (datetime(2026, 10, 19, 16, 48, 5, tzinfo=KST), True),     # 마감 후 수집 마지막 재시도
    (datetime(2026, 10, 19, 23, 59, 59, tzinfo=KST), True),
    (UNTIL, False),                                            # 판 날짜 0시(구간 끝)는 빼므로 그날 값이 아니다
    (UNTIL - timedelta(days=4), False),                        # 구간 시작(포함)이지만 0시라 장 마감 전
    (UNTIL - timedelta(days=4, hours=-16), True),              # 구간 첫날 16:00
    (UNTIL - timedelta(days=4, seconds=1), False),             # 구간 시작 직전
])
def test_only_a_market_summary_received_after_the_close_and_before_the_edition_day_counts(at, usable):
    since, until = prev_close_range(MORNING_AT, DEFAULT_SETTINGS.background_window)
    assert is_prev_close(summary_at(at), since, until) is usable


def test_a_summary_that_is_missing_or_too_large_does_not_count():
    since, until = prev_close_range(MORNING_AT, DEFAULT_SETTINGS.background_window)
    at = datetime(2026, 10, 19, 16, 30, tzinfo=KST)
    assert not is_prev_close(summary_at(at, "too-large"), since, until)
    assert not is_prev_close(Background("market-summary", None, "missing"), since, until)


# ---- 조회 ----

def test_the_morning_edition_asks_the_same_daily_query_again_for_midnight_of_its_day():
    c, requests, _ = make()
    c.api.auto_prev_market = False
    c.on_collection_ready(tick(), MORNING_AT)
    main, prev = requests.api.of(QueryKind.DAILY)
    assert main.payload["asOf"] == "2026-10-20T08:45:00+09:00"
    assert prev.stage == STAGE_PREV_MARKET
    assert prev.payload["asOf"] == "2026-10-20T00:00:00+09:00" and prev.payload["since"] == "2026-10-16T00:00:00+09:00"
    assert prev.payload["maxEtfs"] == 1 and "openFlowSince" not in prev.payload
    assert validate_payload("market.daily.requested.v1.schema.json", prev.payload) == []


def test_the_post_close_edition_does_not_ask_for_the_previous_session():
    c, requests, _ = make()
    c.api.auto_prev_market = False
    c.on_collection_ready(tick("post-close", datetime(2026, 10, 20, 17, 0, tzinfo=KST)),
                          datetime(2026, 10, 20, 17, 0, tzinfo=KST))
    assert [q.stage for q in requests.api.of(QueryKind.DAILY)] == ["daily-data-request"]


def test_the_edition_waits_for_the_previous_session_answer_before_it_goes_out():
    c, requests, _ = make()
    c.api.auto_prev_market = False
    c.on_collection_ready(tick(), MORNING_AT)
    c.api.reply(daily_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    assert published(requests) == []
    c.api.reply({"jobId": TICK_ID, "seq": 1, "etfs": [], "closeRates": [], "truncated": False,
                 "background": [prev_market_background(UNTIL.isoformat())]}, MORNING_AT, kind=QueryKind.DAILY)
    [event] = published(requests)
    assert KR_LINE in event.value["payload"]["text"]


def test_a_previous_session_query_that_finally_fails_still_sends_the_edition_without_the_figures():
    c, requests, details = make()
    c.api.auto_prev_market = False
    c.on_collection_ready(tick(), MORNING_AT)
    c.api.reply(daily_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    c.api.fail(QueryKind.DAILY, MORNING_AT + timedelta(minutes=1), failure_kind="timeout")
    [event] = published(requests)
    assert ITEM_KR_PREV in event.value["payload"]["missingItems"]
    [final] = details.by_reason(FailureReason.CONTEXT_NO_RESPONSE.value)
    assert final.data["failed"] == {"query": PREV_MARKET, "seq": 1, "failureKind": "timeout"}
    assert final.data["notReceived"] == [PREV_MARKET]


# ---- 입력 ----

def kr_section(prev):
    _, requests, _ = run_morning(prev=prev)
    [event] = published(requests)
    return event.value["payload"]


def test_a_value_received_before_the_close_is_no_data_and_not_counted_as_out_of_range():
    # 마감 후 수집이 실패한 날: 그날 마지막으로 받은 값이 장중 수집 마지막 회차다.
    payload = kr_section(prev_market_background(UNTIL.isoformat(), fetched_at="2026-10-19T15:30:02+09:00"))
    assert ITEM_KR_PREV in payload["missingItems"]
    assert "자료 없음 (전 거래일 장 마감 뒤 받은 값이 없음)" in payload["text"]


def test_a_summary_without_the_index_rows_is_no_data():
    prev = {**prev_market_background(UNTIL.isoformat()), "response": json.dumps({"dataBody": {"out2": []}})}
    payload = kr_section(prev)
    assert ITEM_KR_PREV in payload["missingItems"]
    assert "시장종합 원문에서 지수를 찾지 못함" in payload["text"]


def test_the_llm_gets_the_previous_close_figures_apart_from_this_mornings_summary():
    llm = FakeLlm(morning_output(krPrevIssue=kr_issue()))
    run_morning(llm)
    _, user, schema = llm.calls[0]
    items = {item["name"]: item for item in json.loads(user)["items"]}
    [entry] = items[ITEM_KR_PREV]["entries"]
    assert entry["ref"] == KR_REF and entry["closeDate"] == "2026-10-19"
    assert entry["receivedAt"] == CLOSE_FETCHED and entry["minutesBeforeAsOf"] == 975
    assert entry["kospi"] == {"value": "2500.00", "change": "-30.00", "ratePercent": "-1.23"}
    assert entry["kosdaq"]["ratePercent"] == "+0.45"
    # 원문은 다시 넣지 않는다. 그날 아침 시장종합은 제 항목에 원문 그대로 있다.
    assert "response" not in entry and items["시장종합"]["entries"][0]["response"] == "{\"kr\": \"TEST\"}"
    assert "krPrevIssue" in schema["required"]


# ---- 출력 검사 ----

def parse(**extra):
    text = json.loads(morning_output())
    return parse_morning_output(json.dumps({**text, **extra}, ensure_ascii=False), frozenset({"B1", KR_REF}),
                                frozenset(), frozenset({"TEST01"}), frozenset({"TEST 하나"}))


def test_the_previous_session_cell_is_an_issue_and_may_be_weak_without_refs():
    out = parse(krPrevIssue=kr_issue())
    assert [p.kind for p in out.issues] == ["krPrev", "fx", "rate"]
    assert KR_REF in out.cited_refs()
    weak = parse(krPrevIssue=kr_issue("받은 자료에서 전 거래일 한국 증시가 움직인 이유를 찾지 못했습니다.", (), True))
    assert weak.issues[0].refs == () and "전 거래일 장 기사가 더 있어야 합니다." in weak.weak_shortfalls()


@pytest.mark.parametrize("bad, reason", [
    (kr_issue(refs=("Z9",)), "were not given"),                 # 목록 밖 근거 번호
    (kr_issue(refs=()), "not marked weak"),                     # 근거 없는데 약함 표시 없음
    (kr_issue("TEST 하나가 영향을 줬을 수 있습니다."), "etf name"),  # 모두에게 같은 글에 ETF 이름
])
def test_a_bad_previous_session_cell_is_rejected(bad, reason):
    with pytest.raises(InvalidDailyOutput, match=reason):
        parse(krPrevIssue=bad)


# ---- 글자판·HTML ----

def test_the_fake_morning_edition_shows_the_previous_close_and_its_reason_first():
    llm = FakeLlm(morning_output(krPrevIssue=kr_issue()))
    _, requests, _ = run_morning(llm)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_LLM and ITEM_KR_PREV not in payload["missingItems"]
    lines = payload["text"].splitlines()
    head = lines.index(f"■ {KR_PREV_TITLE}")
    # 숫자 줄은 코드가, 이유 문장은 Claude가 쓴다. 이 칸이 맨 먼저라 근거 번호 1번이다.
    assert lines[head + 1:head + 3] == [f"  {KR_LINE}", f"  {KR_REASON} [근거 1]"]
    assert head < lines.index(f"■ {OVERNIGHT_TITLE}")
    assert "  1. 시장종합(전 거래일 마감) " + KR_LINE in payload["text"]
    assert "(10-19(월) 마감)" in payload["html"] and KR_REASON in payload["html"]
    assert payload["html"].index(KR_PREV_TITLE) < payload["html"].index(OVERNIGHT_TITLE)


def test_the_figures_are_shown_even_when_claude_did_not_write_the_cell():
    _, requests, _ = run_morning(FakeLlm(morning_output()))
    [event] = published(requests)
    lines = event.value["payload"]["text"].splitlines()
    head = lines.index(f"■ {KR_PREV_TITLE}")
    assert lines[head + 1:head + 3] == [f"  {KR_LINE}", ""]


def test_the_rule_based_morning_edition_lists_the_previous_close_as_an_item():
    _, requests, _ = run_morning()
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_RULE
    assert f"■ {ITEM_KR_PREV}\n  - {KR_LINE}" in payload["text"]
    # HTML은 등락률에 색을 칠하므로 줄의 나머지 글자만 본다.
    assert f"■ {ITEM_KR_PREV}" in payload["html"] and "(10-19(월) 마감)" in payload["html"]


def test_a_weak_previous_session_cell_is_marked_and_listed_with_the_other_weak_sentences():
    llm = FakeLlm(morning_output(krPrevIssue=kr_issue("받은 자료에서 이유를 찾지 못했습니다.", (), True)))
    _, requests, details = run_morning(llm)
    [event] = published(requests)
    text = event.value["payload"]["text"]
    assert "  받은 자료에서 이유를 찾지 못했습니다. (근거 약함)" in text
    assert f"  - {KR_PREV_TITLE}: 전 거래일 장 기사가 더 있어야 합니다." in text
    assert details.of_type(EntryType.REPORT_WRITTEN)
