"""아침 판의 ETF별 방향 예상: 기사 요청, 출력 검사, 글자판·HTML, 작업 기록. 외부 의존은 모두 가짜다."""
import json
from types import SimpleNamespace

import pytest

from briefing.daily_jobs import DailyCore
from briefing.daily_html import render_html
from briefing.daily_morning import NO_OVERNIGHT, OUTLOOK_NOTE, morning_text, outlook_records
from briefing.daily_prompts import OUTPUT_SCHEMA_MORNING, SYSTEM_PROMPT
from briefing.daily_members import member_output
from briefing.config import DailySettings
from briefing.daily_jobs import DailyDraft, DailySentences, _Draft
from briefing.daily_sections import DailyInput, EtfDay, Entry, Section, build_sections
from briefing.daily_report import DailyLlmWriter
from briefing.daily_output import DailyOutput, Point, InvalidDailyOutput, parse_morning_output
from briefing.job_log import EntryType
from briefing.queries import QueryKind
from briefing.sources import Background
from fake_members import FakeMembers, member
from fake_news import answer_news, article
from test_daily_jobs import (
    MORNING_AT,
    MORNING_JOB,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    etf,
    morning_output,
    published,
    tick,
)

ALLOWED = frozenset({"B1", "B2"})
SYMBOLS = frozenset({"TEST01", "TEST02"})
ISSUE = {"statement": "환율이 영향을 줄 수 있습니다.", "refs": ["B1"], "weak": False, "shortfall": ""}


def etf_item(symbol, direction="상승", refs=("B1",), weak=False, **extra):
    return {"symbol": symbol, "direction": direction, "reason": "미국 지수가 영향을 줄 수 있습니다.",
            "refs": list(refs), "weak": weak, "shortfall": "근거가 더 필요합니다." if weak else "", **extra}


def reply(etfs, **extra):
    return json.dumps({"points": [{"statement": "시장 흐름입니다.", "refs": ["B1"], "weak": False, "shortfall": ""}],
                       "etfs": etfs, "fxIssue": ISSUE, "rateIssue": ISSUE, "insufficientEvidence": False, **extra},
                      ensure_ascii=False)


def parse(text, tickers=frozenset()):
    return parse_morning_output(text, ALLOWED, tickers, SYMBOLS)


# ---- 출력 검사 ----

def test_every_etf_gets_one_direction_and_the_issue_cells_are_kept():
    out = parse(reply([etf_item("TEST01"), etf_item("TEST02", "하락", ("B2",))]))
    assert [(p.symbol, p.direction, p.refs) for p in out.outlooks] == [("TEST01", "상승", ("B1",)),
                                                                       ("TEST02", "하락", ("B2",))]
    assert [p.kind for p in out.issues] == ["fx", "rate"]
    assert out.cited_refs() == ("B1", "B2")


def test_an_unsure_etf_may_have_no_refs_only_when_it_is_marked_weak():
    out = parse(reply([etf_item("TEST01"), etf_item("TEST02", "예상 어려움", (), weak=True)]))
    unsure = out.outlooks[1]
    assert unsure.refs == () and unsure.shortfall == "근거가 더 필요합니다."
    assert out.weak_shortfalls() == ("근거가 더 필요합니다.",)
    with pytest.raises(InvalidDailyOutput, match="not marked weak"):
        parse(reply([etf_item("TEST01"), etf_item("TEST02", "예상 어려움", ())]))
    with pytest.raises(InvalidDailyOutput, match="unsure but is not marked weak"):
        parse(reply([etf_item("TEST01"), etf_item("TEST02", "예상 어려움", ("B2",))]))


@pytest.mark.parametrize("etfs, reason", [
    ([etf_item("TEST01")], "no outlook"),                                           # 빠진 ETF
    ([etf_item("TEST01"), etf_item("TEST01")], "more than once"),                   # 겹침
    ([etf_item("TEST01"), etf_item("TEST09")], "not one that was given"),           # 모르는 ETF
    ([etf_item("TEST01"), etf_item("TEST02", "급등")], "allowed values"),            # 목록 밖 방향
    ([etf_item("TEST01"), etf_item("TEST02", refs=("Z9",))], "were not given"),     # 목록 밖 근거 번호
    ([etf_item("TEST01"), etf_item("TEST02", "하락", (), weak=True)], "cites no refs"),  # 방향을 정하고 근거 없음
])
def test_a_bad_etf_outlook_is_rejected(etfs, reason):
    with pytest.raises(InvalidDailyOutput, match=reason):
        parse(reply(etfs))


def test_a_ticker_in_an_outlook_reason_is_rejected():
    bad = etf_item("TEST02")
    bad["reason"] = "TESTA가 올랐습니다."
    with pytest.raises(InvalidDailyOutput, match="US ticker"):
        parse(reply([etf_item("TEST01"), bad]), frozenset({"TESTA"}))


def other(statement="해외 원유가 크게 내렸습니다.", refs=("B2",)):
    return {"statement": statement, "refs": list(refs), "weak": False, "shortfall": ""}


def test_other_issues_are_kept_in_order_up_to_the_limit_and_may_be_missing():
    out = parse(reply([etf_item("TEST01"), etf_item("TEST02")],
                      otherIssues=[other(f"그 밖의 이슈 {n}입니다.") for n in range(1, 5)]))
    # 상한(3)을 넘는 문장은 거부하지 않고 앞에서부터 상한까지만 쓴다.
    assert [p.statement for p in out.others] == ["그 밖의 이슈 1입니다.", "그 밖의 이슈 2입니다.", "그 밖의 이슈 3입니다."]
    assert {p.kind for p in out.others} == {"other"} and "B2" in out.cited_refs()
    assert parse(reply([etf_item("TEST01"), etf_item("TEST02")])).others == ()


@pytest.mark.parametrize("bad, reason", [
    (other(refs=()), "otherIssues"),           # 근거 번호 없음
    (other(refs=("Z9",)), "were not given"),   # 목록 밖 근거 번호
])
def test_an_other_issue_needs_a_ref_from_the_list(bad, reason):
    with pytest.raises(InvalidDailyOutput, match=reason):
        parse(reply([etf_item("TEST01"), etf_item("TEST02")], otherIssues=[bad]))


@pytest.mark.parametrize("field", ["points", "fxIssue", "otherIssues"])
def test_an_etf_name_in_a_sentence_every_member_gets_is_rejected_but_not_in_an_etf_reason(field):
    named = {"statement": "TEST ETF ONE이 오를 수 있습니다.", "refs": ["B1"], "weak": False, "shortfall": ""}
    extra = {"points": [named]} if field == "points" else {field: [named] if field == "otherIssues" else named}
    with pytest.raises(InvalidDailyOutput, match="etf name or code"):
        parse_morning_output(reply([etf_item("TEST01"), etf_item("TEST02")], **extra), ALLOWED, frozenset(), SYMBOLS,
                             frozenset({"TEST01", "TEST ETF ONE"}))
    reason = etf_item("TEST01")
    reason["reason"] = "TEST ETF ONE은 밤사이 지수 상승을 따라 오를 수 있습니다."
    assert parse_morning_output(reply([reason, etf_item("TEST02")]), ALLOWED, frozenset(), SYMBOLS,
                                frozenset({"TEST01", "TEST ETF ONE"})).outlooks


def test_an_etf_name_in_the_shortfall_of_a_shared_sentence_is_rejected():
    # 부족한 점은 근거 약함 모음으로 멤버 모두에게 나간다.
    named = {"statement": "환율이 영향을 줄 수 있습니다.", "refs": ["B1"], "weak": True,
             "shortfall": "TEST ETF ONE에 담긴 자산 기사가 더 필요합니다."}
    with pytest.raises(InvalidDailyOutput, match="etf name or code"):
        parse_morning_output(reply([etf_item("TEST01"), etf_item("TEST02")], fxIssue=named), ALLOWED, frozenset(),
                             SYMBOLS, frozenset({"TEST01", "TEST ETF ONE"}))


def test_the_weak_count_has_every_weak_sentence_the_mail_shows():
    weak = {"statement": "문장입니다.", "refs": ["B1"], "weak": True, "shortfall": "자료가 더 필요합니다."}
    out = parse(reply([etf_item("TEST01"), etf_item("TEST02", "예상 어려움", (), weak=True)], rateIssue=weak,
                      otherIssues=[weak]))
    assert out.weak_shortfalls() == ("자료가 더 필요합니다.", "자료가 더 필요합니다.", "근거가 더 필요합니다.")


def test_a_member_left_without_overnight_issues_gets_a_line_saying_so_in_text_and_html():
    i = DailyInput("job", "morning", MORNING_AT, MORNING_AT, (), False, (), (), (), False)
    out = DailyOutput((), False, (), (Point("환율이 영향을 줄 수 있습니다.", (), "기사가 더 필요합니다.", "fx"),))
    assert f"■ 밤사이 이슈\n  {NO_OVERNIGHT}" in morning_text(i, (), out)
    assert NO_OVERNIGHT in render_html(i, (), out)


def test_a_member_does_not_get_an_other_issue_that_cites_evidence_outside_their_items():
    out = parse(reply([etf_item("TEST01"), etf_item("TEST02")],
                      otherIssues=[other("모두가 받는 이슈입니다.", ("B1",)), other("다른 멤버 공시입니다.", ("B2",))]))
    mine = DailyInput("job", "morning", MORNING_AT, MORNING_AT, (), False, (), (), (), False)
    sections = (Section("TEST", "있음", (), (Entry("B1", {}, "자료 하나"),)),)
    assert [p.statement for p in member_output(out, mine, sections).others] == ["모두가 받는 이슈입니다."]


def test_insufficient_evidence_skips_the_outlook_and_the_issue_cells():
    out = parse(json.dumps({"points": [], "etfs": [], "insufficientEvidence": True}))
    assert out.insufficient_evidence and out.outlooks == () and outlook_records(out) == []


# ---- 지시문 ----

def test_the_morning_prompt_allows_a_direction_for_etfs_only_and_still_forbids_advice():
    for word in ("상승", "하락", "보합", "예상 어려움", "매수·매도·보유 권유"):
        assert word in SYSTEM_PROMPT
    assert "투자 조언(매수·매도·보유 권유, 가격 전망)을 하지 않습니다." not in SYSTEM_PROMPT
    assert OUTPUT_SCHEMA_MORNING["properties"]["etfs"]["items"]["properties"]["direction"]["enum"] == \
        ["상승", "하락", "보합", "예상 어려움"]


# ---- 흐름: 기사 요청, 걸러내기, 본문, 작업 기록 ----

ETF_INFO = {"name": "TEST ETF ONE", "tracks": "TEST INDEX ONE", "sector": "TEST SECTOR",
            "members": [{"market": "US", "symbol": "TESTA", "name": "TEST CORP A", "weight": "12.5", "rank": 1}],
            "membersTruncated": False}
IN_RANGE = "2026-10-20T07:00:00+09:00"


def run_morning(output, rows=None, articles=None, llm=True):
    requests, details = Requests(), Details()
    writer = DailyLlmWriter(FakeLlm(output), 3) if llm else None
    core = DailyCore(requests, requests.api, writer, details)
    core.on_collection_ready(tick(MORNING_JOB, MORNING_AT), MORNING_AT)
    core.api.reply(daily_response(etfs=rows or [{**etf("TEST01"), "etf": ETF_INFO}, etf("TEST02", ())]),
                                 MORNING_AT)
    answer_news(core, requests, MORNING_AT, articles=articles)
    [event] = published(requests)
    return event.value["payload"], details, requests


def test_watch_etfs_are_asked_too_and_unrelated_articles_never_reach_claude():
    llm = FakeLlm(morning_output(symbols=("TEST01", "TEST02")))
    requests, details = Requests(), Details()
    core = DailyCore(requests, requests.api, DailyLlmWriter(llm, 3), details,
                     members=FakeMembers([member(1, held=("TEST01",), watched=("TEST02",))]))
    core.on_collection_ready(tick(MORNING_JOB, MORNING_AT), MORNING_AT)
    core.api.reply(daily_response(etfs=[{**etf("TEST01"), "etf": ETF_INFO}, etf("TEST02", ())]),
                                 MORNING_AT)
    answer_news(core, requests, MORNING_AT, articles={"TEST01": [
        article("a1", "TEST CORP A 신제품 공개", IN_RANGE), article("a2", "FAKE 지역 축제 일정 안내", IN_RANGE)]})
    published(requests)
    asked = [q.payload["symbol"] for q in requests.api.of(QueryKind.ARTICLES)]
    assert asked == ["TEST01", "TEST02"]
    user = json.loads(llm.calls[0][1])
    titles = [e["title"] for i in user["items"] if i["name"] == "ETF별 기사" for e in i["entries"]]
    assert titles == ["TEST CORP A 신제품 공개"]
    # 보유·관심은 멤버마다 달라 Claude에는 주지 않는다. 메일 본문에서만 그 멤버 기준으로 나뉜다.
    assert user["outlookEtfs"] == [{"symbol": "TEST01", "name": "TEST ETF ONE"}, {"symbol": "TEST02", "name": "TEST02"}]
    mail = published(requests)[0].value["payload"]["text"]
    assert "[보유]" in mail and "[관심]" in mail
    assert "notableCandidates" not in user
    [irrelevant] = [v for e in details.entries for k, v in e.data.get("excluded", {}).items() if k == "irrelevantArticles"][:1]
    assert irrelevant == 1


def test_the_mail_shows_each_direction_the_issue_cells_and_the_notes_in_text_and_html():
    output = morning_output(symbols=("TEST01", "TEST02"), directions={"TEST01": "상승", "TEST02": "예상 어려움"})
    data = json.loads(output)
    data["etfs"][1].update(refs=[], weak=True, shortfall="기사가 더 있어야 합니다.")
    payload, _, _ = run_morning(json.dumps(data, ensure_ascii=False))
    text, html = payload["text"], payload["html"]
    line = next(ln for ln in text.splitlines() if "TEST01" in ln and "예상" in ln)
    assert "상승 예상" in line
    assert "예상 어려움(근거 부족)" in next(ln for ln in text.splitlines() if "TEST02" in ln and "예상" in ln)
    for heading in ("밤사이 이슈", "환율·금리", "내 ETF: 개장 흐름과 예상"):
        assert heading in text and heading in html
    assert "  - 환율: " in text and "  - 금리: " in text
    assert "투자 조언이 아니" in text and OUTLOOK_NOTE in text and OUTLOOK_NOTE in html
    assert text.index("■ 밤사이 이슈") < text.index("■ 환율·금리") < text.index("■ 내 ETF")
    # 방향을 정하지 못한 ETF는 줄 끝에 근거 약함 표시만 하고, 부족한 점은 아래 한곳에 모은다.
    assert "TEST02" in text and "  - TEST02: 기사가 더 있어야 합니다." in text


def test_the_mail_leads_with_overnight_issues_and_leaves_the_raw_figures_to_the_evidence_list():
    output = morning_output(symbols=("TEST01", "TEST02"),
                            otherIssues=[{"statement": "개장 직후 외국인이 순매수했습니다.", "refs": ["B1"],
                                          "weak": False, "shortfall": ""}])
    payload, _, _ = run_morning(output)
    text, html = payload["text"], payload["html"]
    # 항목별 숫자 목록(미국 지수 종가, '받은 값 있음')은 본문에서 빠지고 인용한 자료만 근거 목록에 남는다.
    for gone in ("■ 미국 지수", "받은 값 있음", "■ 범위 안 미국 공시", "요약:"):
        assert gone not in text and gone not in html
    assert text.index("■ 밤사이 이슈") < text.index("■ 환율·금리") < text.index("■ 그 밖의 이슈") < text.index("■ 내 ETF")
    assert "  - 개장 직후 외국인이 순매수했습니다. [근거 1]" in text and "개장 직후 외국인이 순매수했습니다." in html
    # 근거 약함은 줄 끝 표시와 면책 위 모음으로만 보인다.
    assert "  - 금리: 받은 자료에 금리 관련 기사가 없습니다. (근거 약함)" in text
    assert "※ 근거 약함 1건" in text and "  - 금리: 금리 기사가 더 있어야 합니다." in text
    assert "※ 근거 약함 1건" in html and text.index("※ 근거 약함") < text.index("※ 투자 조언")
    # 개장 전 판이라 개장 흐름이 없는 ETF는 그렇다고 쓴다.
    assert "개장 흐름 자료 없음  상승 예상" in text


def test_the_outlook_is_left_in_the_job_log_once_in_a_readable_form():
    payload, details, _ = run_morning(morning_output(symbols=("TEST01", "TEST02"),
                                                     directions={"TEST01": "상승", "TEST02": "하락"}))
    [entry] = details.of_type(EntryType.ETF_OUTLOOK)
    assert entry.data["asOf"] == "2026-10-20T08:45:00+09:00"
    assert [(o["symbol"], o["direction"], o["weak"]) for o in entry.data["outlooks"]] == \
        [("TEST01", "상승", False), ("TEST02", "하락", False)]
    assert entry.data["outlooks"][0]["refs"] == ["B1"] and entry.data["outlooks"][0]["reason"]
    assert payload["generatedBy"] == "llm"


def test_a_rule_based_morning_leaves_no_outlook_and_still_shows_the_issue_cells():
    payload, details, _ = run_morning("x", llm=False)
    assert details.of_type(EntryType.ETF_OUTLOOK) == []
    assert "환율 이슈:" in payload["text"] and "금리 이슈:" in payload["text"]
    assert "■ ETF별 예상" not in payload["text"]


# ---- 문장 단위 재검색: 대상과 차례, ETF별 예상·환율·금리 다시 쓰기 ----

def research_draft(members):
    """밤사이 이슈 1(약함), 환율 1(약함), ETF별 예상 2(TEST01·TEST02 모두 약함)인 아침 판 초안."""
    i = DailyInput("job", "morning", MORNING_AT, MORNING_AT,
                   tuple(EtfDay(s, None, None, None, None, None, (), False) for s in ("TEST01", "TEST02")),
                   False, (), (), (), False)
    output = DailyOutput(
        (Point("밤사이 이슈입니다.", ("B1",), "부족 이슈"),), False,
        issues=(Point("환율 문장입니다.", ("B1",), "부족 환율", "fx"), Point("금리 문장입니다.", ("B1",), None, "rate")),
        outlooks=(Point("관심 예상입니다.", (), "부족 관심", "outlook", "TEST01", "예상 어려움"),
                  Point("보유 예상입니다.", (), "부족 보유", "outlook", "TEST02", "예상 어려움")))
    core = SimpleNamespace(settings=DailySettings(), members=FakeMembers(members))
    job = SimpleNamespace(job_id="job", edition="morning")
    return DailySentences(core), DailyDraft(job, i, (), _Draft("", "llm", (), False, (), output))


def test_weak_sentences_are_searched_held_etfs_first_then_fx_rate_then_overnight_then_watched_etfs():
    sentences, draft = research_draft([member(1, held=("TEST02",), watched=("TEST01",))])
    # 문장 번호: 밤사이 이슈 1, 환율 2, 금리 3, ETF 예상 TEST01 4, TEST02 5.
    assert [(w.no, w.text) for w in sentences.weak_sentences(draft)] == [
        (5, "보유 예상입니다."), (2, "환율 문장입니다."), (1, "밤사이 이슈입니다."), (4, "관심 예상입니다.")]
    assert sentences.sentence_text(draft, 2) == "환율 문장입니다."
    assert sentences.query_context(draft, sentences.weak_sentences(draft)[0])["etf"]["symbol"] == "TEST02"


def test_an_etf_held_by_any_member_counts_as_held_and_unknown_members_make_every_etf_held():
    sentences, draft = research_draft([member(1, watched=("TEST01", "TEST02")), member(2, held=("TEST01",))])
    assert [w.no for w in sentences.weak_sentences(draft)] == [4, 2, 1, 5]
    sentences, draft = research_draft(None)
    assert [w.no for w in sentences.weak_sentences(draft)] == [4, 5, 2, 1]


def test_a_rewritten_outlook_may_take_a_new_direction_and_keeps_its_etf():
    i, sections = morning_input()
    current = Point("근거가 부족합니다.", (), "기사가 더 필요합니다.", "outlook", "TEST01", "예상 어려움")
    answer = json.dumps({"points": [{"direction": "상승", "statement": "새 기사로 오를 수 있습니다.", "refs": ["B1"],
                                     "weak": False, "shortfall": ""}], "insufficientEvidence": False}, ensure_ascii=False)
    llm = FakeLlm(answer)
    result = DailyLlmWriter(llm, 3).rewrite_point(i, sections, current, 4, ())
    assert (result.point.kind, result.point.symbol, result.point.direction) == ("outlook", "TEST01", "상승")
    system, user, schema = llm.calls[0]
    assert json.loads(user)["rewrite"]["direction"] == "예상 어려움" and "direction" in schema["properties"]["points"]["items"]["properties"]


@pytest.mark.parametrize("item, reason", [
    ({"direction": "급등", "refs": ["B1"], "weak": False, "shortfall": ""}, "allowed values"),
    ({"direction": "상승", "refs": [], "weak": True, "shortfall": "부족"}, "cites no refs"),
    ({"direction": "예상 어려움", "refs": ["B1"], "weak": False, "shortfall": ""}, "unsure but is not marked weak"),
])
def test_a_rewritten_outlook_follows_the_first_write_rules(item, reason):
    i, sections = morning_input()
    current = Point("근거가 부족합니다.", (), "기사가 더 필요합니다.", "outlook", "TEST01", "예상 어려움")
    bad = json.dumps({"points": [{"statement": "다시 쓴 이유입니다.", **item}], "insufficientEvidence": False},
                     ensure_ascii=False)
    result = DailyLlmWriter(FakeLlm(bad, bad, bad), 3).rewrite_point(i, sections, current, 4, ())
    assert result.point is None and reason in result.rejected[0].reason


def test_a_rewritten_rate_cell_may_still_have_no_refs_when_weak_but_not_name_an_etf():
    i, sections = morning_input()
    current = Point("받은 자료에 금리 관련 기사가 없습니다.", (), "금리 기사가 필요합니다.", "rate")
    still = json.dumps({"points": [{"statement": "받은 자료에 금리 관련 기사가 없습니다.", "refs": [], "weak": True,
                                    "shortfall": "금리 기사가 여전히 필요합니다."}], "insufficientEvidence": False},
                       ensure_ascii=False)
    named = json.dumps({"points": [{"statement": "TEST01에 금리가 부담입니다.", "refs": ["B1"], "weak": False,
                                    "shortfall": ""}], "insufficientEvidence": False}, ensure_ascii=False)
    llm = FakeLlm(named, still)
    result = DailyLlmWriter(llm, 3).rewrite_point(i, sections, current, 3, ())
    assert result.point.kind == "rate" and result.point.refs == ()
    assert result.rejected[0].reason == "point mentions an etf name or code"
    assert json.loads(llm.calls[0][1])["rewrite"]["kind"] == "rate"


def morning_input():
    i = DailyInput("job", "morning", MORNING_AT, MORNING_AT,
                   (EtfDay("TEST01", None, None, None, None, None, (), False),), False, (),
                   (Background("exchange-rate-summary", None, "ok", "{}", MORNING_AT),), (), False)
    return i, build_sections(i)


def test_the_open_flow_chart_sits_right_under_the_etf_heading_in_html():
    from briefing.daily_html import ChartSlot
    i, sections = morning_input()
    out = DailyOutput((Point("밤사이 이슈입니다.", ("B1",)),), False,
                      outlooks=(Point("오를 수 있습니다.", ("B1",), None, "outlook", "TEST01", "상승"),))
    html = render_html(i, sections, out, charts=(ChartSlot("chart-open", alt="개장 흐름"),))
    heading = html.index("내 ETF: 개장 흐름과 예상")
    assert heading < html.index("cid:chart-open") < html.index("오를 수 있습니다.")
    assert 'alt="개장 흐름"' in html

