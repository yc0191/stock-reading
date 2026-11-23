"""데일리 판 본문(항목, 자료 없음 표시, LLM 입력·출력 검증, 작성기 선택)을 확인한다. 실제 LLM은 부르지 않는다."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from fake_anthropic import as_reply
from briefing.config import DailySettings
from briefing.daily_report import (
    DAILY_DISCLAIMER,
    ITEM_ETFS,
    ITEM_ETF_ARTICLES,
    ITEM_MEMBER_ISSUES,
    ITEM_EXCHANGE,
    ITEM_FILINGS,
    ITEM_KR_PREV,
    ITEM_MARKET_SUMMARY,
    ITEM_OPEN_FLOW,
    ITEM_US_INDEX,
    ITEM_US_STOCKS,
    NO_DATA,
    OUTPUT_SCHEMA,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_POST_CLOSE,
    DailyInput,
    DailyLlmWriter,
    EtfDay,
    InvalidDailyOutput,
    build_daily_writer,
    build_llm_input,
    build_sections,
    missing_items,
    parse_output,
    rule_based_text,
)
from briefing.sources import Background, CloseRate, EtfInfo, EtfMember, Filing, IndexRate

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 8, 45, tzinfo=KST)
START = datetime(2026, 10, 19, 8, 45, tzinfo=KST)
FETCHED = datetime(2026, 10, 20, 8, 30, tzinfo=KST)


def etf(symbol="TEST01", tickers=("TESTA",), rate="-1.50", truncated=False):
    return EtfDay(symbol, "20261019", "9850", "10000", rate, FETCHED, tickers, truncated)


def world(rates=("2.04",)):
    return Background("world-index", "C", "ok", "{\"raw\": 1}", FETCHED, None,
                      tuple(IndexRate(f"TESTIX{n}", f"TEST INDEX {n}", "20261019", "100.00",
                                      "20261016" if r else None, "98.00" if r else None, r)
                            for n, r in enumerate(rates)))


def daily_input(edition="morning", etfs=(etf(),), close_rates=None, background=None, filings=(),
                etfs_truncated=False, filings_truncated=False):
    return DailyInput(
        job_id="job", edition=edition, range_start=START, as_of=AT, etfs=etfs, etfs_truncated=etfs_truncated,
        close_rates=(CloseRate("TESTA", "20261019", AT - timedelta(hours=3, minutes=45), "-1.25", FETCHED),)
        if close_rates is None else close_rates,
        background=(world(), Background("exchange-rate-summary", None, "ok", "{\"fx\": 1}", FETCHED),
                    Background("market-summary", None, "ok", "{\"kr\": 1}", FETCHED - timedelta(hours=16)))
        if background is None else background,
        filings=filings, filings_truncated=filings_truncated,
    )


class FakeLlm:
    def __init__(self, *outputs):
        self.outputs = list(outputs)
        self.calls = 0

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
        self.calls += 1
        return self.outputs.pop(0)


def morning_reply(points, symbols=("TEST01",)):
    """아침 판 Claude 출력: 요약 문장들, ETF마다 방향 예상, 환율·금리 이슈 칸."""
    etfs = [{"symbol": s, "direction": "상승", "reason": "미국 지수가 영향을 줄 수 있습니다.", "refs": ["B1"],
             "weak": False, "shortfall": ""} for s in symbols]
    issue = {"statement": "환율이 영향을 줄 수 있습니다.", "refs": ["B2"], "weak": False, "shortfall": ""}
    return json.dumps({"points": points, "etfs": etfs, "fxIssue": issue,
                       "rateIssue": {**issue, "statement": "금리 기사가 없습니다.", "refs": [], "weak": True,
                                     "shortfall": "금리 기사가 필요합니다."},
                       "insufficientEvidence": False}, ensure_ascii=False)


def test_morning_and_post_close_choose_their_own_items():
    assert [s.name for s in build_sections(daily_input())] == \
        [ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_US_STOCKS, ITEM_FILINGS, ITEM_MARKET_SUMMARY, ITEM_KR_PREV, ITEM_OPEN_FLOW,
         ITEM_ETF_ARTICLES]
    assert [s.name for s in build_sections(daily_input("post-close"))] == \
        ["그날 한국 증시", ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_ETFS, ITEM_ETF_ARTICLES, ITEM_FILINGS, ITEM_MEMBER_ISSUES]


def test_each_missing_item_is_marked_no_data():
    i = daily_input(etfs=None, close_rates=None, background=None, filings=None)
    i = DailyInput(**{**i.__dict__, "close_rates": None, "background": None})
    sections = build_sections(i)
    assert missing_items(sections) == [ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_US_STOCKS, ITEM_FILINGS, ITEM_MARKET_SUMMARY,
                                       ITEM_KR_PREV, ITEM_OPEN_FLOW, ITEM_ETF_ARTICLES]
    text = rule_based_text(i, sections)
    # 항목 7개와, 규칙 기반 본문이 환율 이슈·금리 이슈 칸에 쓰는 대체 줄 2개
    assert text.count(f"- {NO_DATA}") == 9
    assert text.endswith(DAILY_DISCLAIMER)


def test_too_large_background_counts_as_missing():
    big = Background("exchange-rate-summary", None, "too-large", None, FETCHED, 40000)
    sections = build_sections(daily_input(background=(world(), big)))
    assert ITEM_EXCHANGE in missing_items(sections)
    assert ITEM_MARKET_SUMMARY in missing_items(sections)
    assert "너무 커서" in rule_based_text(daily_input(background=(world(), big)), sections)


def test_index_without_a_previous_close_shows_no_data_for_that_index_only():
    sections = build_sections(daily_input(background=(world(("2.04", None)),)))
    us = sections[0]
    assert us.status == "있음"
    assert any("TEST INDEX 0" in line and "2.04%" in line for line in us.lines)
    assert any("TEST INDEX 1" in line and NO_DATA in line for line in us.lines)
    assert ITEM_US_INDEX in missing_items(build_sections(daily_input(background=(world((None,)),))))


def test_us_stock_moves_are_counted_by_code_without_tickers_in_the_body():
    rates = tuple(CloseRate(t, "20261019", AT - timedelta(hours=4), r, FETCHED)
                  for t, r in (("TESTA", "-1.25"), ("FAKEB", "2.00"), ("TESTC", "0.00")))
    i = daily_input(etfs=(etf(tickers=("TESTA", "FAKEB", "TESTC")),), close_rates=rates)
    section = build_sections(i)[2]
    assert section.lines[0] == "미국 종목 3개(전체 3개 중): 오름 1, 내림 1, 보합 1, 평균 +0.25%"
    assert section.lines[1] == "가장 큰 상승 +2.00%, 가장 큰 하락 -1.25%"
    text = rule_based_text(i, build_sections(i))
    assert "TESTA" not in text and "FAKEB" not in text
    # LLM 입력에는 티커가 들어간다.
    assert "TESTA" in json.dumps(build_llm_input(i, build_sections(i)))


def test_us_stocks_and_filings_are_not_applicable_without_us_holdings():
    i = daily_input(etfs=(etf(tickers=()),), close_rates=(), filings=None)
    sections = build_sections(i)
    assert sections[2].status == "해당 없음" and sections[3].status == "해당 없음"
    # 기사 응답과 전 거래일 시장종합이 없는 입력이라 그 둘과 개장 흐름만 빠진 항목이다.
    assert missing_items(sections) == [ITEM_KR_PREV, ITEM_OPEN_FLOW, ITEM_ETF_ARTICLES]


def test_filings_and_etf_lists_mark_truncation():
    filing = Filing("0000000001-26-000001", ("TESTA",), "8-K", ("2.02",), AT - timedelta(hours=3), "https://x.test")
    sections = build_sections(daily_input(filings=(filing,), filings_truncated=True))
    assert sections[3].status == "잘림"
    assert sections[3].entries[0].ref == "0000000001-26-000001"
    post = build_sections(daily_input("post-close", etfs=(etf(), etf("TEST02")), etfs_truncated=True))
    etfs = next(s for s in post if s.name == ITEM_ETFS)
    assert etfs.status == "잘림" and [e.ref for e in etfs.entries] == ["E1", "E2"]


def test_a_filing_shows_the_company_name_and_what_its_items_mean():
    filing = Filing("0000000001-26-000002", ("TESTA",), "8-K", ("2.02", "9.01", "9.99"), AT - timedelta(hours=3),
                    "https://x.test")
    named = EtfDay("TEST01", "20261019", "9850", "10000", "-1.50", FETCHED, ("TESTA",), False,
                   EtfInfo((EtfMember("US", "TESTA", 1, "TEST CORP A"),), False, "TEST ETF ONE"))
    [entry] = next(s for s in build_sections(daily_input(etfs=(named,), filings=(filing,)))
                   if s.name == ITEM_FILINGS).entries
    # 첨부 서류(9.01)와 모르는 번호는 뜻을 붙이지 않는다. 원래 번호는 그대로 Claude에 준다.
    assert entry.llm["companyNames"] == ["TEST CORP A"] and entry.llm["itemNames"] == ["실적 발표"]
    assert entry.llm["items"] == ["2.02", "9.01", "9.99"]
    assert entry.describe.startswith("TEST CORP A 8-K 공시(실적 발표) — ")
    # 이름을 모르는 종목은 '미국 종목'으로 쓰고 이름 칸을 싣지 않는다.
    [unnamed] = next(s for s in build_sections(daily_input(filings=(filing,))) if s.name == ITEM_FILINGS).entries
    assert "companyNames" not in unnamed.llm and unnamed.describe.startswith("미국 종목 8-K 공시(실적 발표)")


def test_the_morning_market_summary_gives_claude_overseas_quotes_and_investor_flows():
    raw = json.dumps({"dataBody": {
        "out4": [{"symbl_nm": "TEST 원유", "now_prc": "11.11", "up_dwn_r_p2": "-2.22", "dt_tm": "20261019"}],
        "out5": [{"invstr_clsf_nm": "TEST외국인", "kspi_nt_b": "7777", "ksdq_nt_b": "-888"}]}}, ensure_ascii=False)
    i = daily_input(background=(world(), Background("market-summary", None, "ok", raw, FETCHED)))
    section = next(s for s in build_sections(i) if s.name == ITEM_MARKET_SUMMARY)
    [entry] = section.entries
    assert entry.llm["overseas"] == [{"name": "TEST 원유", "price": "11.11", "ratePercent": "-2.22", "at": "20261019"}]
    assert entry.llm["investorNetBuy"] == [{"investor": "TEST외국인", "kospi": "7777", "kosdaq": "-888"}]
    assert entry.llm["response"] == raw and "해외 시세·투자자별 순매수" in entry.describe
    # 두 블록이 없는 원문은 입력 모양을 바꾸지 않는다.
    [plain] = next(s for s in build_sections(daily_input()) if s.name == ITEM_MARKET_SUMMARY).entries
    assert "overseas" not in plain.llm and "investorNetBuy" not in plain.llm


def test_llm_input_shows_status_and_time_gaps_and_raw_background():
    i = daily_input()
    data = build_llm_input(i, build_sections(i))
    assert data["edition"] == "아침 판" and data["asOf"] == "2026-10-20T08:45:00+09:00"
    us_index = data["items"][0]
    assert us_index["status"] == "있음"
    entry = us_index["entries"][0]
    assert entry["category"] == "배경" and entry["minutesBeforeAsOf"] == 15 and entry["response"] == "{\"raw\": 1}"
    assert entry["indexRates"][0]["rate"] == "2.04"
    assert data["items"][2]["entries"][0]["hoursBeforeAsOf"] == 3.8


def test_system_prompt_forbids_assertions_advice_and_reading_zero_rates_as_no_change():
    assert "단정하지 않습니다" in SYSTEM_PROMPT and "투자 조언" in SYSTEM_PROMPT
    assert "0.00" in SYSTEM_PROMPT and "변동 없음" in SYSTEM_PROMPT
    assert OUTPUT_SCHEMA["required"] == ["points", "insufficientEvidence"]


@pytest.mark.parametrize("text", [
    "not json",
    json.dumps({"points": []}),
    json.dumps({"points": [{"statement": "", "refs": ["B1"]}], "insufficientEvidence": False}),
    json.dumps({"points": [{"statement": "문장", "refs": []}], "insufficientEvidence": False}),
    json.dumps({"points": [{"statement": "문장", "refs": ["Z1"]}], "insufficientEvidence": False}),
    json.dumps({"points": [], "insufficientEvidence": False}),
])
def test_invalid_outputs_and_refs_outside_the_list_are_rejected(text):
    with pytest.raises(InvalidDailyOutput):
        parse_output(text, frozenset({"B1"}))


def test_valid_output_with_insufficient_evidence_is_accepted():
    output = parse_output(json.dumps({"points": [], "insufficientEvidence": True}), frozenset())
    assert output.insufficient_evidence and output.points == ()


def test_llm_body_numbers_the_cited_refs_and_leaves_the_raw_figures_to_the_evidence_list():
    i = daily_input()
    sections = build_sections(i)
    llm = FakeLlm(morning_reply([{"statement": "미국 지수가 올랐을 수 있습니다.", "refs": ["B1", "U1"]}]))
    result = DailyLlmWriter(llm).write(i, sections)
    assert result.rejected == ()
    text = result.body.text
    # 지수 종가·직전 종가 같은 숫자 목록은 본문에 싣지 않고, 인용한 자료만 근거 목록에 나온다.
    assert "TEST INDEX 0(TESTIX0) 2.04%" not in text
    assert "  1. 미국 지수가 올랐을 수 있습니다. [근거 1, 2]" in text
    assert "1. 세계지수 — 시세 자료(배경)" in text and "2. 미국 종목 직전 정규장 등락률 -1.25%" in text
    # 면책 문구가 밤사이 이슈 뒤에, 근거 목록이 맨 끝에 온다.
    assert text.index("■ 밤사이 이슈") < text.index(DAILY_DISCLAIMER) < text.index("근거:")


def test_writer_gives_up_after_the_attempt_limit():
    llm = FakeLlm("a", "b", "c")
    i = daily_input()
    result = DailyLlmWriter(llm, max_attempts=3).write(i, build_sections(i))
    assert result.body is None and [a.attempt for a in result.rejected] == [1, 2, 3]
    assert llm.calls == 3


def test_writer_is_none_without_a_key_and_the_key_is_not_kept_in_its_repr():
    assert build_daily_writer({}) is None
    assert build_daily_writer({"ANTHROPIC_API_KEY": "change-me"}) is None
    made = {}

    def factory(key, model):
        made["key"], made["model"] = key, model
        return FakeLlm()

    writer = build_daily_writer({"ANTHROPIC_API_KEY": "sk-test-value"}, factory, DailySettings(llm_max_attempts=2))
    assert made == {"key": "sk-test-value", "model": "claude-sonnet-5-5"}
    assert writer.max_attempts == 2 and "sk-test-value" not in repr(writer)


def test_the_morning_prompt_asks_for_company_names_and_keeps_etf_names_out_of_shared_sentences():
    # 아침 판은 미국 종목을 회사 이름으로 쓰고, 멤버 모두에게 같은 글인 칸에는 ETF 이름을 쓰지 않는다.
    assert "티커 대신 회사 이름" in SYSTEM_PROMPT
    assert "ETF 이름이나 ETF 종목코드를 쓰지 않습니다" in SYSTEM_PROMPT
    assert '"미국 종목"이라고 씁니다' in SYSTEM_PROMPT_POST_CLOSE


@pytest.mark.parametrize("statement", [
    "TESTA가 내렸을 수 있습니다.",
    "미국 종목(TESTA) 하락이 영향을 줬을 수 있습니다.",
    "TEST.B 하락이 영향을 줬을 수 있습니다.",
])
def test_statement_with_a_given_us_ticker_is_rejected(statement):
    text = json.dumps({"points": [{"statement": statement, "refs": ["U1"]}], "insufficientEvidence": False},
                      ensure_ascii=False)
    with pytest.raises(InvalidDailyOutput) as error:
        parse_output(text, frozenset({"U1"}), frozenset({"TESTA", "TEST-B"}))
    # 사유는 실패 기록·로그로 가므로 티커 값을 담지 않는다.
    assert "TESTA" not in str(error.value) and "TEST" not in str(error.value)


def test_ticker_inside_a_longer_word_or_without_a_ban_list_is_not_rejected():
    text = json.dumps({"points": [{"statement": "TESTAB와 미국 종목이 내렸을 수 있습니다.", "refs": ["U1"]}],
                       "insufficientEvidence": False}, ensure_ascii=False)
    assert parse_output(text, frozenset({"U1"}), frozenset({"TESTA"})).points
    assert parse_output(json.dumps({"points": [{"statement": "TESTA 하락", "refs": ["U1"]}],
                                    "insufficientEvidence": False}), frozenset({"U1"})).points


def test_the_morning_writer_accepts_a_us_company_ticker_in_a_sentence():
    i = daily_input()
    answer = morning_reply([{"statement": "TESTA가 내렸습니다.", "refs": ["U1"]}])
    result = DailyLlmWriter(FakeLlm(answer)).write(i, build_sections(i))
    assert result.rejected == () and "TESTA가 내렸습니다." in result.body.text


@pytest.mark.parametrize("statement", ["TEST01이 내릴 수 있습니다.", "TEST ETF ONE이 내릴 수 있습니다."])
def test_the_morning_writer_rejects_an_etf_name_in_a_shared_sentence_and_asks_again(statement):
    i = daily_input(etfs=(EtfDay("TEST01", "20261019", "9850", "10000", "-1.50", FETCHED, ("TESTA",), False,
                                 EtfInfo((), False, "TEST ETF ONE")),))
    sections = build_sections(i)
    bad = morning_reply([{"statement": statement, "refs": ["U1"]}])
    good = morning_reply([{"statement": "미국 기술주가 내렸습니다.", "refs": ["U1"]}])
    llm = FakeLlm(bad, good)
    result = DailyLlmWriter(llm).write(i, sections)
    [rejected] = result.rejected
    assert rejected.attempt == 1 and rejected.reason == "point mentions an etf name or code"
    assert "미국 기술주가 내렸습니다." in result.body.text


def test_writer_gives_up_when_every_answer_names_an_etf_in_a_shared_sentence():
    i = daily_input()
    bad = morning_reply([{"statement": "TEST01 하락", "refs": ["U1"]}])
    llm = FakeLlm(bad, bad, bad)
    result = DailyLlmWriter(llm, max_attempts=3).write(i, build_sections(i))
    assert result.body is None and len(result.rejected) == 3
