"""아침 판의 개장 흐름: 읽을 시세 범위, 요청과 응답, 범위 재검사, 본문·근거 번호. 값은 모두 가짜다."""
import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta

from briefing.config import CoreSettings
from briefing.daily_members import MemberPortfolio, member_input, member_sections
from briefing.daily_report import ITEM_OPEN_FLOW, build_llm_input, build_sections, missing_items
from briefing.open_flow import OpenFlow, in_range, open_flow_start, parse_open_flow, signed
from briefing.queries import QueryKind
from contracts import validate_payload
from fake_news import answer_news
from test_daily_jobs import (  # noqa: F401  core는 테스트가 쓰는 fixture
    KST,
    MORNING_AT,
    OPEN_AT,
    OPEN_FLOW,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    FakeLlm,
    core,
    daily_response,
    etf,
    filings_response,
    good_output,
    published,
    tick,
)
from test_daily_report import daily_input, etf as etf_day


def at(hour, minute, second=0):
    return datetime(2026, 10, 20, hour, minute, second, tzinfo=KST)


def start_job(c, now=OPEN_AT, response=None):
    c.on_collection_ready(tick(at=now), now)
    c.api.reply(response or daily_response(etfs=[etf(open_flow=OPEN_FLOW)]), now)
    c.api.reply(filings_response(), now)


# ---- 범위 ----

def test_the_range_starts_at_the_market_open_of_the_edition_date():
    assert open_flow_start(OPEN_AT) == at(9, 0)
    assert open_flow_start(at(9, 0)) == at(9, 0)


def test_the_range_never_reaches_back_further_than_the_window_when_the_edition_starts_late():
    assert open_flow_start(at(9, 45)) == at(9, 15)
    assert open_flow_start(at(9, 45), CoreSettings(open_flow_max_window=timedelta(minutes=60))) == at(9, 0)


def test_there_is_no_range_before_the_market_opens():
    assert open_flow_start(MORNING_AT) is None
    assert open_flow_start(at(8, 59, 59)) is None


def test_the_range_is_decided_in_korean_time():
    assert open_flow_start(OPEN_AT.astimezone(__import__("datetime").timezone.utc)) == at(9, 0)


def test_both_range_ends_are_inside_and_one_second_outside_is_not():
    start, end = at(9, 0), at(9, 10)

    def flow(a, b):
        return OpenFlow(True, start_at=a, end_at=b)

    assert in_range(flow(start, end), start, end)
    assert not in_range(flow(start - timedelta(seconds=1), end), start, end)
    assert not in_range(flow(start, end + timedelta(seconds=1)), start, end)
    assert not in_range(OpenFlow(True), start, end)
    # 범위 안 시세가 없다는 응답은 버릴 값이 없다.
    assert in_range(OpenFlow(False), start, end)


def test_parse_reads_the_cell_and_keeps_missing_values_as_none():
    assert parse_open_flow(None) is None
    flow = parse_open_flow(OPEN_FLOW)
    assert (flow.present, flow.open, flow.open_rate, flow.flow_rate) == (True, "10200", "2.00", "1.98")
    assert flow.end_at == at(9, 9, 58)
    none = parse_open_flow({"status": "no-snapshot", "prevClose": None, "open": None, "openSource": None,
                            "openRate": None, "startAt": None, "startPrice": None, "endAt": None, "endPrice": None,
                            "flowRate": None, "rateVsPrevClose": None})
    assert none.present is False and none.start_at is None


def test_signed_rates():
    assert [signed(r) for r in ("2.00", "-0.50", "0.00", None)] == ["+2.00%", "-0.50%", "0.00%", "자료 없음"]


# ---- 요청과 응답 ----

def test_the_morning_request_asks_for_the_open_flow_from_the_market_open(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)

    [request] = requests.api.of(QueryKind.DAILY)
    assert request.payload["openFlowSince"] == "2026-10-20T09:00:00+09:00"
    assert validate_payload("market.daily.requested.v1.schema.json", request.payload) == []


def test_no_open_flow_is_requested_before_the_market_opens_or_by_the_post_close_edition(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, "9d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68"), POST_CLOSE_AT)

    assert all("openFlowSince" not in q.payload for q in requests.api.of(QueryKind.DAILY))


def test_the_open_flow_goes_into_the_body_the_evidence_list_and_the_llm_input(core):
    llm = FakeLlm(good_output("O1"))
    c, requests, _, details = core(llm)
    start_job(c)
    answer_news(c, requests, OPEN_AT)

    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["missingItems"] == []
    assert "■ ETF별 개장 흐름" in payload["text"]
    assert "시가 +2.00%" in payload["text"] and "09:00~09:09 +1.98%" in payload["text"]
    assert "현재가 전일 종가 대비 +3.00%" in payload["text"]
    items = {i["name"]: i for i in json.loads(llm.calls[0][1])["items"]}
    [entry] = items[ITEM_OPEN_FLOW]["entries"]
    assert entry["ref"] == "O1" and entry["openRatePercent"] == "2.00" and entry["minutesBeforeAsOf"] == 0
    # O1은 목록에 있는 번호라 인용을 받아들이고 출처에 개장 시세로 남는다.
    assert "TEST01 개장 흐름 시가 +2.00%" in payload["text"]


def test_a_quote_outside_the_range_is_dropped_with_a_warning_and_shown_as_no_data(core, caplog):
    c, requests, _, _ = core()
    late = {**OPEN_FLOW, "endAt": "2026-10-20T09:10:01+09:00"}
    with caplog.at_level(logging.WARNING):
        start_job(c, response=daily_response(etfs=[etf(open_flow=late)]))

    assert "dropped 1 openFlow" in caplog.text
    answer_news(c, requests, OPEN_AT)
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [f"{ITEM_OPEN_FLOW}: TEST01, 범위 안 시세 없음"] \
        or ITEM_OPEN_FLOW in event.value["payload"]["missingItems"][0]
    assert "시가 +2.00%" not in event.value["payload"]["text"]


def test_a_quote_before_the_market_open_is_dropped(core):
    c, requests, _, _ = core()
    early = {**OPEN_FLOW, "startAt": "2026-10-20T08:59:59+09:00"}
    start_job(c, response=daily_response(etfs=[etf(open_flow=early)]))
    answer_news(c, requests, OPEN_AT)

    [event] = published(requests)
    assert "시가 +2.00%" not in event.value["payload"]["text"]


def test_a_response_without_the_cell_leaves_the_etf_as_no_data_with_a_reason(core):
    c, requests, _, _ = core()
    start_job(c, response=daily_response(etfs=[etf()]))
    answer_news(c, requests, OPEN_AT)

    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [f"{ITEM_OPEN_FLOW}: TEST01, 응답에 개장 흐름 칸이 없음"]


def test_an_etf_without_any_quote_in_the_range_is_listed_as_a_gap(core):
    none = {"status": "no-snapshot", "prevClose": None, "open": None, "openSource": None, "openRate": None,
            "startAt": None, "startPrice": None, "endAt": None, "endPrice": None, "flowRate": None,
            "rateVsPrevClose": None}
    c, requests, _, _ = core()
    start_job(c, response=daily_response(etfs=[etf("TEST01", open_flow=OPEN_FLOW), etf("TEST02", open_flow=none)]))
    answer_news(c, requests, OPEN_AT)

    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [f"{ITEM_OPEN_FLOW}: TEST02, 범위 안 시세 없음"]
    assert "시가 +2.00%" in event.value["payload"]["text"]


def test_the_post_close_edition_drops_an_open_flow_it_did_not_ask_for(core, caplog):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    with caplog.at_level(logging.WARNING):
        c.api.reply(daily_response(etfs=[etf(fetched="2026-10-20T16:30:00+09:00", open_flow=OPEN_FLOW)]), POST_CLOSE_AT)

    assert "dropped 1 openFlow" in caplog.text


# ---- 항목 ----

def flowed(i, flow):
    return replace(i, open_flow_start=at(9, 0), etfs=tuple(replace(e, open_flow=flow) for e in i.etfs))


def test_the_section_marks_how_the_open_price_was_found():
    from_raw = parse_open_flow(OPEN_FLOW)
    fallback = parse_open_flow({**OPEN_FLOW, "openSource": "first-snapshot"})

    [raw_section] = [s for s in build_sections(flowed(daily_input(), from_raw)) if s.name == ITEM_OPEN_FLOW]
    [fallback_section] = [s for s in build_sections(flowed(daily_input(), fallback)) if s.name == ITEM_OPEN_FLOW]

    assert "시세 원문의 시가" in raw_section.lines[0]
    assert "시가 칸 없음" in fallback_section.lines[0]
    assert fallback_section.entries[0].llm["openSource"] == "first-snapshot"


def test_the_section_is_marked_missing_with_a_reason_before_the_market_opens():
    i = replace(daily_input(), open_flow_start=None)

    [section] = [s for s in build_sections(i) if s.name == ITEM_OPEN_FLOW]

    assert section.missing and "장 시작 전" in section.lines[0]
    assert missing_items([section]) == [ITEM_OPEN_FLOW]


def test_the_llm_input_lists_the_open_flow_only_for_the_edition_that_has_it():
    i = flowed(daily_input(), parse_open_flow(OPEN_FLOW))

    names = [item["name"] for item in build_llm_input(i, build_sections(i))["items"]]
    post_close = replace(i, edition="post-close")
    post_names = [item["name"] for item in build_llm_input(post_close, build_sections(post_close))["items"]]

    assert ITEM_OPEN_FLOW in names and ITEM_OPEN_FLOW not in post_names


def test_a_members_open_flow_keeps_the_whole_editions_evidence_number():
    two = replace(daily_input(), etfs=(etf_day("TEST01"), etf_day("TEST02")))
    full = flowed(two, parse_open_flow(OPEN_FLOW))
    full_sections = build_sections(full)
    mine = member_input(full, MemberPortfolio(1, (("TEST02", "HELD"),)))

    [section] = [s for s in member_sections(mine, full_sections) if s.name == ITEM_OPEN_FLOW]

    # TEST02는 판 전체에서 O2였고, 멤버의 판에서도 O2다(멤버 목록 안의 첫 번째라고 O1로 밀리지 않는다).
    assert [e.ref for e in section.entries] == ["O2"]
    assert section.entries[0].llm["symbol"] == "TEST02"


# ---- 지시문 ----

def test_the_morning_prompt_reads_the_open_flow_and_uses_the_grading_band_of_the_post_close_edition():
    from briefing.config import DailySettings
    from briefing.daily_prompts import SYSTEM_PROMPT

    assert ITEM_OPEN_FLOW in SYSTEM_PROMPT and "openRatePercent" in SYSTEM_PROMPT and "first-snapshot" in SYSTEM_PROMPT
    band = f"{DailySettings().flat_band_percent:g}"
    assert f"±{band}% 안일 것 같으면 \"보합\"" in SYSTEM_PROMPT and "__FLAT_BAND__" not in SYSTEM_PROMPT
    assert "장이 막 열린 뒤" in SYSTEM_PROMPT
