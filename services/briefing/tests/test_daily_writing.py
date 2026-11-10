"""마감 후 판 Claude 출력(요약 2줄, ETF마다 이유, 눈에 띄는 점)의 검사, 재검색 대상, 본문 배치를 확인한다. 외부 의존 없음."""
import json
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from briefing.daily_html import ChartSlot, render_html
from briefing.daily_inputs import EtfArticles, MemberIssue, NotableCandidate, KIND_OPPOSITE, moves_with_market
from briefing.daily_jobs import DailyDraft, DailySentences, _Draft
from briefing.config import DEFAULT_DAILY_SETTINGS
from briefing.daily_post_close import post_close_view
from briefing.daily_report import (
    DAILY_DISCLAIMER,
    DAILY_INSUFFICIENT,
    OUTPUT_SCHEMA,
    OUTPUT_SCHEMA_POST_CLOSE,
    POINT_ETF,
    POINT_NOTABLE,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_POST_CLOSE,
    DailyInput,
    DailyLlmWriter,
    DailyOutput,
    EtfDay,
    ITEM_EXCHANGE,
    ITEM_MEMBER_ISSUES,
    ITEM_US_INDEX,
    InvalidDailyOutput,
    Point,
    build_llm_input,
    build_sections,
    conclusion,
    daily_text,
    member_line,
    parse_post_close_output,
    repeats_member_line,
)
from briefing.sources import Article, Background, EtfInfo, EtfMember, Filing, IndexRate

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
FETCHED = datetime(2026, 10, 20, 16, 30, tzinfo=KST)
ALLOWED = frozenset({"E1", "E2", "a-1", "M1", "0000000001-26-000001"})
TICKERS = frozenset({"TESTA"})
REQUIRED = frozenset({"TEST01", "TEST02"})
ISSUE = MemberIssue("TEST01", 2, "TESTA", "테스트전자", "3.50", ())


def etf_row(symbol, rate, name=None, members=(), relation="HELD", sector="TEST 업종"):
    info = EtfInfo(tuple(members), False, name or f"TEST {symbol}", None, sector)
    if rate is None:
        return EtfDay(symbol, None, None, None, None, None, (), False, info, "no-daily-price-in-range", relation)
    return EtfDay(symbol, "20261020", "9850", "10000", rate, FETCHED, (), False, info, None, relation)


def day_input(etfs, issues=(), articles=None, filings=(), background=()):
    return DailyInput("job", "post-close", AT - timedelta(days=1), AT, tuple(etfs), False, (), tuple(background),
                      tuple(filings), False, etf_articles=articles, member_issues=tuple(issues))


def llm_output(etfs=None, summary=("첫째 줄입니다.", "둘째 줄입니다."), notables=(), insufficient=False):
    etfs = [{"symbol": "TEST01", "reason": "업종 흐름이 영향을 줬을 수 있습니다.", "refs": ["E1"], "weak": False,
             "shortfall": ""},
            {"symbol": "TEST02", "reason": "시장과 함께 움직였을 수 있습니다.", "refs": ["E2"], "weak": False,
             "shortfall": ""}] if etfs is None else etfs
    return json.dumps({"summary": list(summary), "etfs": etfs, "notables": list(notables),
                       "insufficientEvidence": insufficient}, ensure_ascii=False)


def parse(text, issues=None, required=REQUIRED):
    return parse_post_close_output(text, ALLOWED, TICKERS, required, issues or {"TEST01": (ISSUE,)})


# ---- 출력 검사 ----

def test_a_valid_output_keeps_the_summary_and_puts_the_etf_reasons_before_the_notables():
    notable = {"statement": "두 ETF가 반대로 움직였습니다.", "refs": ["E1", "E2"], "weak": True, "shortfall": "업종 기사 필요"}
    out = parse(llm_output(notables=[notable]))
    assert out.summary == ("첫째 줄입니다.", "둘째 줄입니다.") and not out.insufficient_evidence
    assert [(p.kind, p.symbol) for p in out.points] == [(POINT_ETF, "TEST01"), (POINT_ETF, "TEST02"),
                                                         (POINT_NOTABLE, None)]
    assert out.points[2].shortfall == "업종 기사 필요" and out.points[0].shortfall is None


def test_notables_written_on_a_day_without_candidates_are_dropped_and_the_rest_is_kept():
    notable = {"statement": "묶어 말합니다.", "refs": ["E1"], "weak": False, "shortfall": ""}
    dropped = parse_post_close_output(llm_output(notables=[notable]), ALLOWED, TICKERS, REQUIRED, {}, False)
    assert [(p.kind, p.symbol) for p in dropped.points] == [(POINT_ETF, "TEST01"), (POINT_ETF, "TEST02")]
    assert dropped.summary == ("첫째 줄입니다.", "둘째 줄입니다.")
    out = parse_post_close_output(llm_output(), ALLOWED, TICKERS, REQUIRED, {}, False)
    assert all(p.kind == POINT_ETF for p in out.points)


def test_an_etf_with_a_daily_move_but_no_reason_is_rejected():
    only_first = [{"symbol": "TEST01", "reason": "이유입니다.", "refs": ["E1"], "weak": False, "shortfall": ""}]
    with pytest.raises(InvalidDailyOutput, match="1 etf.*no reason"):
        parse(llm_output(only_first))


def test_an_etf_without_a_daily_move_or_not_given_is_rejected_so_no_reason_is_made_for_it():
    # 자료 없음 ETF는 이 목록에 없으므로(코드가 자료 없음 줄로 쓴다) 이유를 쓰면 거부한다.
    extra = [{"symbol": s, "reason": "이유입니다.", "refs": ["E1"], "weak": False, "shortfall": ""}
             for s in ("TEST01", "TEST02", "TEST09")]
    with pytest.raises(InvalidDailyOutput, match="not one that was given"):
        parse(llm_output(extra))


def test_the_same_etf_twice_is_rejected():
    twice = [{"symbol": "TEST01", "reason": "이유입니다.", "refs": ["E1"], "weak": False, "shortfall": ""}] * 2
    with pytest.raises(InvalidDailyOutput, match="more than once"):
        parse(llm_output(twice))


def test_a_ref_outside_the_given_list_is_rejected_for_reasons_and_notables_and_the_value_is_not_in_the_reason():
    bad = [{"symbol": "TEST01", "reason": "이유입니다.", "refs": ["E9"], "weak": False, "shortfall": ""},
           {"symbol": "TEST02", "reason": "이유입니다.", "refs": ["E2"], "weak": False, "shortfall": ""}]
    with pytest.raises(InvalidDailyOutput, match="1 ref") as error:
        parse(llm_output(bad))
    assert "E9" not in str(error.value)
    with pytest.raises(InvalidDailyOutput, match="notable cites 1 ref"):
        parse(llm_output(notables=[{"statement": "점입니다.", "refs": ["E9"], "weak": False, "shortfall": ""}]))


def test_a_reason_without_refs_is_rejected_unless_it_is_marked_weak():
    none = [{"symbol": s, "reason": "이유입니다.", "refs": [], "weak": False, "shortfall": ""} for s in REQUIRED]
    with pytest.raises(InvalidDailyOutput, match="cites no refs but is not marked weak"):
        parse(llm_output(none))
    with pytest.raises(InvalidDailyOutput, match="notable cites no refs"):
        parse(llm_output(notables=[{"statement": "점입니다.", "refs": [], "weak": True, "shortfall": "기사 필요"}]))


def test_a_reason_that_found_nothing_has_no_source_number_in_the_body():
    # 이유를 못 찾은 ETF가 자기 일봉을 인용하면 근거가 있는 것처럼 보이므로 근거 번호 없이 둔다.
    i = day_input([etf_row("TEST01", "1.00"), etf_row("TEST02", "0.20")])
    found_nothing = {"symbol": "TEST01", "reason": "받은 자료에서 이유를 찾지 못했습니다.", "refs": [], "weak": True,
                     "shortfall": "이 ETF를 다룬 기사가 더 있어야 합니다."}
    other = {"symbol": "TEST02", "reason": "시장과 함께 움직였을 수 있습니다.", "refs": ["E2"], "weak": False,
             "shortfall": ""}
    result = DailyLlmWriter(Sequence(llm_output([found_nothing, other]))).write(i, build_sections(i))
    assert result.rejected == () and result.body.cited == ("E2",)
    text = result.body.text
    assert "    이유: 받은 자료에서 이유를 찾지 못했습니다.\n" in text + "\n"
    assert "이유: 시장과 함께 움직였을 수 있습니다. [출처 1]" in text
    assert "1. 일봉 · TEST02" in text and "TEST01 하루 등락" not in text
    html = render_html(i, build_sections(i), result.body.output)
    assert "이유: 받은 자료에서 이유를 찾지 못했습니다.</p>" in html


@pytest.mark.parametrize("where", ["summary", "reason", "shortfall", "notable"])
def test_a_us_ticker_in_any_sentence_is_rejected_without_naming_it(where):
    def etfs(reason="이유입니다.", shortfall=None):
        return [{"symbol": "TEST01", "reason": reason, "refs": ["E1"], "weak": shortfall is not None,
                 "shortfall": shortfall or ""},
                {"symbol": "TEST02", "reason": "이유입니다.", "refs": ["E2"], "weak": False, "shortfall": ""}]
    cases = {
        "summary": llm_output(summary=("TESTA가 올랐습니다.", "둘째 줄입니다.")),
        "reason": llm_output(etfs("TESTA 실적이 영향을 줬을 수 있습니다.")),
        "shortfall": llm_output(etfs(shortfall="TESTA 공시가 더 필요합니다.")),
        "notable": llm_output(notables=[{"statement": "TESTA 이야기입니다.", "refs": ["E1"], "weak": False,
                                         "shortfall": ""}]),
    }
    with pytest.raises(InvalidDailyOutput, match="US ticker") as error:
        parse(cases[where])
    assert "TESTA" not in str(error.value)


@pytest.mark.parametrize("lines", [(), ("한 줄입니다.",), ("하나", "둘", "셋")])
def test_the_summary_must_be_exactly_two_lines(lines):
    with pytest.raises(InvalidDailyOutput, match="summary must have 2 lines"):
        parse(llm_output(summary=lines))


def test_an_empty_summary_line_is_rejected():
    with pytest.raises(InvalidDailyOutput, match="no summary"):
        parse(llm_output(summary=("첫째 줄입니다.", "  ")))


@pytest.mark.parametrize("reason", [
    "테스트전자가 영향을 줬을 수 있습니다.",   # 종목 이름
    "담긴 종목이 3.50% 올라 영향을 줬을 수 있습니다.",   # 등락률 그대로
    "담긴 종목이 3.5% 올라 영향을 줬을 수 있습니다.",   # 끝 0을 뗀 같은 값
    "담긴 종목이 +3.5 % 올라 영향을 줬을 수 있습니다.",   # 부호와 공백
])
def test_a_reason_that_repeats_the_member_line_is_rejected_and_other_numbers_are_fine(reason):
    etfs = [{"symbol": "TEST01", "reason": reason, "refs": ["E1"], "weak": False, "shortfall": ""},
            {"symbol": "TEST02", "reason": "시장과 함께 움직였을 수 있습니다.", "refs": ["E2"], "weak": False,
             "shortfall": ""}]
    with pytest.raises(InvalidDailyOutput, match="repeats the member issue line"):
        parse(llm_output(etfs))


def test_a_different_number_or_another_etfs_issue_is_not_a_repeat():
    assert not repeats_member_line("담긴 종목이 13.5% 올랐습니다.", (ISSUE,))
    assert not repeats_member_line("담긴 종목이 3.51% 올랐습니다.", (ISSUE,))
    assert not repeats_member_line("3.5 배 늘었습니다.", (ISSUE,))
    # 이슈 목록이 없는 ETF의 이유는 검사하지 않는다.
    etfs = [{"symbol": s, "reason": "테스트전자가 영향을 줬을 수 있습니다.", "refs": ["E1"], "weak": False,
             "shortfall": ""} for s in ("TEST01", "TEST02")]
    parse(llm_output(etfs), issues={"TEST01": ()})
    # 한 글자 이름은 아무 글에나 걸리므로 이름 비교에서 뺀다.
    assert not repeats_member_line("가 오르내렸습니다.", (MemberIssue("TEST01", 1, "TESTB", "가", None, ()),))


def test_insufficient_evidence_drops_everything_else_and_needs_no_etfs():
    out = parse(llm_output(etfs=[], summary=(), insufficient=True))
    assert out == DailyOutput((), True)
    assert parse(llm_output(summary=("a", "b", "c"), insufficient=True)).insufficient_evidence


def test_an_output_in_the_old_points_shape_is_rejected():
    with pytest.raises(InvalidDailyOutput, match="does not have etfs"):
        parse(json.dumps({"points": [], "insufficientEvidence": False}))


def test_no_required_etf_still_needs_the_two_summary_lines():
    assert parse(llm_output(etfs=[]), required=frozenset()).points == ()
    with pytest.raises(InvalidDailyOutput, match="summary must have 2"):
        parse(llm_output(etfs=[], summary=()), required=frozenset())


# ---- 지시문과 형식 ----

def test_the_post_close_prompt_forbids_the_unsafe_things_and_names_the_output_fields():
    for word in ("단정", "투자 조언", "티커", "정확히 2개", "되풀이", "weak", "↳"):
        assert word in SYSTEM_PROMPT_POST_CLOSE
    for key in ('"summary"', '"etfs"', '"notables"', '"reason"'):
        assert key in SYSTEM_PROMPT_POST_CLOSE
    assert set(OUTPUT_SCHEMA_POST_CLOSE["required"]) == {"summary", "etfs", "notables", "fxIssue", "rateIssue",
                                                         "insufficientEvidence"}


def test_each_edition_asks_with_its_own_prompt_and_schema():
    class Recorder:
        def __init__(self, reply):
            self.reply, self.calls = reply, []

        def complete(self, system, user, schema):
            self.calls.append((system, schema))
            return self.reply

    post = Recorder(llm_output())
    i = day_input([etf_row("TEST01", "1.00"), etf_row("TEST02", "2.00")])
    DailyLlmWriter(post).write(i, build_sections(i))
    assert post.calls == [(SYSTEM_PROMPT_POST_CLOSE, OUTPUT_SCHEMA_POST_CLOSE)]
    morning = Recorder(json.dumps({"points": [], "insufficientEvidence": True}))
    m = DailyInput("job", "morning", AT - timedelta(days=1), AT, (etf_row("TEST01", "1.00"),), False, (), (), (),
                   False)
    DailyLlmWriter(morning).write(m, build_sections(m))
    assert morning.calls == [(SYSTEM_PROMPT, OUTPUT_SCHEMA)]


# ---- 거부하고 다시 쓰게 함 ----

class Sequence:
    def __init__(self, *replies):
        self.replies, self.calls = list(replies), []

    def complete(self, system, user, schema):
        self.calls.append(user)
        return self.replies.pop(0)


def issue_input():
    members = (EtfMember("US", "TESTA", 2, "테스트전자", "5.0"),)
    return day_input([etf_row("TEST01", "-3.50", members=members), etf_row("TEST02", "0.20")], issues=[ISSUE])


def test_a_rejected_output_is_asked_again_up_to_the_attempt_limit_and_then_the_body_is_rule_based():
    i = issue_input()
    repeat = llm_output([{"symbol": "TEST01", "reason": "테스트전자가 영향을 줬을 수 있습니다.", "refs": ["E1"],
                          "weak": False, "shortfall": ""},
                         {"symbol": "TEST02", "reason": "이유입니다.", "refs": ["E2"], "weak": False, "shortfall": ""}])
    llm = Sequence(repeat, llm_output())
    result = DailyLlmWriter(llm, max_attempts=3).write(i, build_sections(i))
    assert [a.attempt for a in result.rejected] == [1] and "repeats the member issue line" in result.rejected[0].reason
    assert result.body is not None and len(llm.calls) == 2
    bad = Sequence(repeat, repeat, repeat)
    failed = DailyLlmWriter(bad, max_attempts=3).write(i, build_sections(i))
    assert failed.body is None and len(failed.rejected) == 3 and len(bad.calls) == 3


def test_the_member_name_is_shown_in_the_body_but_the_ticker_reaches_claude_only_in_the_input():
    i = issue_input()
    sections = build_sections(i)
    result = DailyLlmWriter(Sequence(llm_output())).write(i, sections)
    text, html = result.body.text, render_html(i, sections, result.body.output)
    for shown in (text, html):
        assert "TESTA" not in shown
    assert "↳ 비중 2위 테스트전자 직전 장 +3.50%" in text and "↳ 비중 2위 테스트전자 직전 장 <span" in html


def test_a_member_without_a_usable_name_is_shown_as_a_us_stock():
    # 이름이 없거나 이름에 티커가 들어 있으면 티커가 본문에 나오지 않게 '미국 종목'으로 쓴다.
    assert member_line(MemberIssue("TEST01", 1, "TESTA", "  ", "4.00", ())) == "↳ 비중 1위 미국 종목 직전 장 +4.00%"
    assert member_line(MemberIssue("TEST01", 1, "TESTA", "TESTA", "4.00", ())) == "↳ 비중 1위 미국 종목 직전 장 +4.00%"
    assert member_line(MemberIssue("TEST01", 1, "TESTA", "테스트 TESTA 홀딩스", "4.00", ())) == \
        "↳ 비중 1위 미국 종목 직전 장 +4.00%"
    # 티커가 다른 낱말의 일부일 뿐이면 이름을 그대로 쓴다.
    assert member_line(MemberIssue("TEST01", 1, "TESTA", "TESTAB 테스트", "4.00", ())) == \
        "↳ 비중 1위 TESTAB 테스트 직전 장 +4.00%"


def test_the_member_source_line_names_the_stock_once_without_the_ticker():
    i = issue_input()
    [entry] = next(s for s in build_sections(i) if s.name == ITEM_MEMBER_ISSUES).entries
    assert entry.describe == "비중 2위 테스트전자 직전 장 등락률 +3.50% — 해외 시세 자료"
    assert entry.llm["ticker"] == "TESTA" and entry.llm["name"] == "테스트전자"


def test_a_rewritten_reason_keeps_its_kind_and_etf_and_is_checked_for_repeats_too():
    i = issue_input()
    sections = build_sections(i)
    current = Point("이유입니다.", ("E1",), "더 필요한 자료", POINT_ETF, "TEST01")
    ok = json.dumps({"points": [{"statement": "다시 쓴 이유입니다.", "refs": ["E1"], "weak": False, "shortfall": ""}],
                     "insufficientEvidence": False})
    result = DailyLlmWriter(Sequence(ok)).rewrite_point(i, sections, current, 1, ())
    assert (result.point.kind, result.point.symbol, result.point.statement) == (POINT_ETF, "TEST01", "다시 쓴 이유입니다.")
    repeat = json.dumps({"points": [{"statement": "담긴 종목이 3.5% 올랐습니다.", "refs": ["E1"], "weak": False,
                                     "shortfall": ""}], "insufficientEvidence": False})
    failed = DailyLlmWriter(Sequence(repeat, repeat, repeat)).rewrite_point(i, sections, current, 1, ())
    assert failed.point is None and "repeats the member issue line" in failed.rejected[0].reason
    # 눈에 띄는 점은 이름·숫자를 그대로 말해도 되므로 되풀이 검사를 하지 않는다.
    notable = Point("점입니다.", ("E1",), "부족", POINT_NOTABLE)
    assert DailyLlmWriter(Sequence(repeat)).rewrite_point(i, sections, notable, 2, ()).point.kind == POINT_NOTABLE



def test_a_rewritten_etf_reason_may_still_find_nothing_but_only_when_it_stays_weak():
    i = issue_input()
    sections = build_sections(i)
    current = Point("받은 자료에서 이유를 찾지 못했습니다.", (), "이유를 찾지 못함", POINT_ETF, "TEST01")
    nothing = json.dumps({"points": [{"statement": "받은 자료에서 이유를 찾지 못했습니다.", "refs": [], "weak": True,
                                      "shortfall": "새 기사에도 이유가 없음"}], "insufficientEvidence": False})
    result = DailyLlmWriter(Sequence(nothing)).rewrite_point(i, sections, current, 1, ())
    assert result.point is not None and result.point.refs == () and result.rejected == ()
    not_weak = json.dumps({"points": [{"statement": "이유입니다.", "refs": [], "weak": False, "shortfall": ""}],
                           "insufficientEvidence": False})
    failed = DailyLlmWriter(Sequence(not_weak, not_weak, not_weak)).rewrite_point(i, sections, current, 1, ())
    assert failed.point is None and "not marked weak" in failed.rejected[0].reason
    # 눈에 띄는 점은 다시 쓸 때도 근거 번호가 있어야 한다.
    notable = Point("점입니다.", ("E1",), "부족", POINT_NOTABLE)
    rejected = DailyLlmWriter(Sequence(nothing, nothing, nothing)).rewrite_point(i, sections, notable, 2, ())
    assert rejected.point is None and "cites no refs" in rejected.rejected[0].reason


# ---- 재검색 대상 ----

def market(rate, name="KOSPI종합"):
    row = {"indx_nm": name, "up_dwn_r_p2": rate, "bdy_cmpr_p2": "10.0", "bdy_cmpr_ccd": "2", "now_indx_p2": "2500"}
    return Background("market-summary", None, "ok", json.dumps({"dataBody": {"out2": [row]}}), FETCHED)


@pytest.mark.parametrize("etf_rate, kospi, same", [
    ("1.20", "0.50", True), ("-1.20", "-0.50", True), ("1.20", "-0.50", False), ("-1.20", "0.50", False),
    ("0.00", "0.50", False), ("1.20", "0.00", False), (None, "0.50", False), ("abc", "0.50", False),
    # 차이가 1%p 이내일 때만 같은 방향이다(경계 포함).
    ("1.30", "0.30", True), ("1.31", "0.30", False), ("-1.30", "-0.30", True), ("-1.31", "-0.30", False),
    ("6.00", "0.30", False),
])
def test_an_etf_moves_with_the_market_only_when_both_go_the_same_way_within_the_gap(etf_rate, kospi, same):
    assert moves_with_market(etf_rate, [market(kospi)]) is same


def test_the_gap_is_a_setting():
    wide = replace(DEFAULT_DAILY_SETTINGS, market_same_direction_gap_percent=6.0)
    assert moves_with_market("6.00", [market("0.30")], wide) is True
    assert moves_with_market("6.00", [market("0.30")]) is False


def test_an_unknown_market_is_never_the_same_direction():
    assert moves_with_market("1.20", []) is False
    assert moves_with_market("1.20", [market("0.50", name="다른지수")]) is False
    assert moves_with_market("1.20", [Background("market-summary", None, "missing")]) is False


def weak_draft(kospi):
    background = (market(kospi),) if kospi is not None else ()
    i = day_input([etf_row("TEST01", "1.20"), etf_row("TEST02", "-0.80"), etf_row("TEST03", "0.30")],
                  background=background)
    points = (Point("첫째 이유", ("E1",), "부족 하나", POINT_ETF, "TEST01"),
              Point("둘째 이유", ("E2",), "부족 둘", POINT_ETF, "TEST02"),
              Point("셋째 이유", ("E3",), None, POINT_ETF, "TEST03"),
              Point("눈에 띄는 점", ("E1",), "부족 점", POINT_NOTABLE))
    output = DailyOutput(points, False, ("a", "b"))
    return DailyDraft(None, i, build_sections(i), _Draft("", "llm", (), False, (), output))


def test_only_weak_etf_reasons_that_did_not_move_with_the_market_are_searched():
    sentences = DailySentences(type("Core", (), {"settings": DEFAULT_DAILY_SETTINGS})())
    # 코스피 +0.5%: 오른 TEST01은 시장과 같은 방향이라 찾지 않고, 내린 TEST02만 찾는다. 눈에 띄는 점은 찾지 않는다.
    assert [(w.no, w.text) for w in sentences.weak_sentences(weak_draft("0.50"))] == [(2, "둘째 이유")]
    # 코스피 -0.5%: 반대로 TEST02가 같은 방향이다.
    assert [w.no for w in sentences.weak_sentences(weak_draft("-0.50"))] == [1]
    # 시장 등락을 모르면 같은 방향이라고 보지 않아 약한 ETF 이유를 모두 찾는다.
    assert [w.no for w in sentences.weak_sentences(weak_draft(None))] == [1, 2]


def test_the_search_context_names_the_etf_of_the_sentence_but_not_for_a_notable():
    sentences = DailySentences(type("Core", (), {"settings": DEFAULT_DAILY_SETTINGS})())
    draft = weak_draft("0.50")
    [weak] = sentences.weak_sentences(draft)
    assert sentences.query_context(draft, weak)["etf"] == {"symbol": "TEST02", "name": "TEST TEST02"}
    from briefing.sentence_research import WeakSentence
    assert "etf" not in sentences.query_context(draft, WeakSentence(4, "눈에 띄는 점", "부족 점"))


# ---- 본문 배치 ----

ARTICLE = Article("a-1", "TEST 기사 제목", "요약", AT - timedelta(hours=2), "news.example.com",
                  "https://news.example.com/a-1")
FILING = Filing("0000000001-26-000001", ("TESTA",), "8-K", ("2.02",), AT - timedelta(hours=9),
                "https://filings.example.com/8k.htm")


def layout_input():
    members = (EtfMember("US", "TESTA", 2, "테스트전자", "5.0"),)
    first = replace(etf_row("TEST01", "-3.50", members=members), tickers=("TESTA", "TESTB"))
    return day_input([first, etf_row("TEST02", "0.20", relation="WATCH"),
                      etf_row("TEST03", None, relation="WATCH")],
                     issues=[ISSUE, MemberIssue("TEST01", 5, "TESTB", None, None, ("0000000001-26-000001",))],
                     articles=(EtfArticles("TEST01", (ARTICLE,), True),), filings=(FILING,))


def layout_output():
    return DailyOutput((
        Point("기사가 영향을 줬을 수 있습니다.", ("a-1", "E1"), "업종 기사 필요", POINT_ETF, "TEST01"),
        Point("시장과 함께 움직였을 수 있습니다.", ("E2",), None, POINT_ETF, "TEST02"),
        Point("구성 종목 이슈가 있는 ETF입니다.", ("M1", "0000000001-26-000001"), None, POINT_NOTABLE)),
        False, ("첫째 요약입니다.", "둘째 요약입니다."))


def test_the_text_follows_the_plan_order_and_ties_each_reason_to_its_member_lines():
    i = layout_input()
    text = daily_text(i, build_sections(i), layout_output(), "──── 서버 상태 ────\n- 주의: 없음")
    marks = ["[마감 후 판]", "결론:", "요약:", "눈에 띄는 점:", "■ 보유·관심 ETF별 하루 등락", "이유: 기사가", DAILY_DISCLAIMER,
             "서버 상태", "출처:"]
    positions = [text.index(m) for m in marks]
    assert positions == sorted(positions)
    lines = text.splitlines()
    first = next(n for n, line in enumerate(lines) if "TEST TEST01(TEST01) -3.50%" in line)
    # 줄, 이유, 근거 약함, ↳ 줄 순서
    assert lines[first + 1].startswith("    이유: 기사가 영향을 줬을 수 있습니다.")
    assert lines[first + 2] == "      근거 약함: 업종 기사 필요"
    assert lines[first + 3] == "    ↳ 비중 2위 테스트전자 직전 장 +3.50%"
    assert lines[first + 4] == "    ↳ 비중 5위 미국 종목 범위 안 공시"
    # 이유를 쓰지 않는 자료 없음 ETF는 숫자 없이 이유와 함께 한 줄만 나온다.
    gap = next(n for n, line in enumerate(lines) if line.startswith("  - 자료 없음: TEST TEST03"))
    assert "범위 안에 받은 일봉이 없음" in lines[gap] and not lines[gap + 1].startswith("    ")
    assert "TESTA" not in text and "TESTB" not in text


def test_sources_are_numbered_in_the_order_they_appear_with_kind_and_links_for_articles_and_filings():
    i = layout_input()
    text = daily_text(i, build_sections(i), layout_output())
    tail = text.split("출처:\n")[1].splitlines()
    # 눈에 띄는 점(M1, 공시)이 먼저, 이어서 ETF 줄의 기사와 일봉 순이다.
    assert [line.split(" · ")[0].strip() for line in tail] == ["1. 구성 종목 이슈", "2. 공시", "3. 기사", "4. 일봉", "5. 일봉"]
    assert tail[1].endswith("https://filings.example.com/8k.htm")
    assert tail[2].endswith("https://news.example.com/a-1") and "news.example.com, 10-20(화) 15:00 발행" in tail[2]
    assert "이유: 기사가 영향을 줬을 수 있습니다. [출처 3, 4]" in text
    assert "- 구성 종목 이슈가 있는 ETF입니다. [출처 1, 2]" in text


def test_the_html_has_the_same_order_and_links_only_http_sources_and_paints_the_member_rate():
    i = layout_input()
    html = render_html(i, build_sections(i), layout_output(), "- 주의: 없음", ChartSlot("chart-day", False, ("TEST TEST03",)))
    marks = ["결론:", "요약", "눈에 띄는 점", 'src="cid:chart-day"', "■ 보유·관심 ETF별 하루 등락", "이유: 기사가", "↳ 비중 2위",
             DAILY_DISCLAIMER, "주의: 없음", "출처:"]
    positions = [html.index(m) for m in marks]
    assert positions == sorted(positions)
    assert 'href="https://news.example.com/a-1"' in html and 'href="https://filings.example.com/8k.htm"' in html
    assert '<span style="color:#d32f2f;font-weight:700">+3.50%</span>' in html
    assert "TESTA" not in html and "<script" not in html


def test_a_source_link_that_is_not_http_is_shown_as_text_only_and_values_are_escaped():
    bad = Article("a-1", "<b>제목</b>", "", AT - timedelta(hours=2), "pub<>", "javascript:alert(1)")
    i = day_input([etf_row("TEST01", "1.00")], articles=(EtfArticles("TEST01", (bad,), True),))
    out = DailyOutput((Point("이유입니다.", ("a-1",), None, POINT_ETF, "TEST01"),), False, ("가", "나"))
    html = render_html(i, build_sections(i), out)
    assert "href=" not in html and "&lt;b&gt;제목&lt;/b&gt;" in html and "<b>제목" not in html
    assert "javascript:" not in html  # 링크도 글자도 아니다(글자판에는 받은 주소가 그대로 남는다)


def test_the_insufficient_notice_replaces_the_summary_and_the_etf_lines_have_no_reasons():
    i = layout_input()
    out = DailyOutput((), True)
    text = daily_text(i, build_sections(i), out)
    assert DAILY_INSUFFICIENT in text and "이유:" not in text and "출처:" not in text and "눈에 띄는 점" not in text
    assert "↳ 비중 2위 테스트전자 직전 장 +3.50%" in text
    assert DAILY_INSUFFICIENT in render_html(i, build_sections(i), out)


def test_the_rule_based_post_close_body_has_no_summary_or_reasons_but_keeps_the_member_lines():
    i = layout_input()
    text = daily_text(i, build_sections(i), None)
    assert "요약:" not in text and "이유:" not in text and "출처:" not in text
    assert text.index("결론:") < text.index("■ 보유·관심 ETF별 하루 등락") < text.index(DAILY_DISCLAIMER)
    assert "↳ 비중 2위 테스트전자 직전 장 +3.50%" in text
    view = post_close_view(i, build_sections(i), None)
    assert not view.has_output and view.summary == () and view.sources == ()


def test_a_positive_member_rate_gets_a_plus_sign_and_a_rate_less_issue_shows_only_the_filing():
    assert member_line(MemberIssue("TEST01", 1, "TESTA", None, "4.00", ())) == "↳ 비중 1위 미국 종목 직전 장 +4.00%"
    assert member_line(MemberIssue("TEST01", 3, "TESTA", None, "+4.00", ("x",))) == \
        "↳ 비중 3위 미국 종목 직전 장 +4.00% · 범위 안 공시"
    assert member_line(MemberIssue("TEST01", 4, "TESTA", None, None, ("x",))) == "↳ 비중 4위 미국 종목 범위 안 공시"


# ---- 점검 후 고침: 자료 없음 ETF의 ↳ 줄, 거부 사유, 그날 한국 증시 줄, 재검색 간격 ----

def no_day_input():
    members = (EtfMember("US", "TESTA", 2, "테스트전자", "5.0"),)
    gap = replace(etf_row("TEST03", None, members=members), tickers=("TESTA",))
    issue = MemberIssue("TEST03", 2, "TESTA", "테스트전자", "3.50", ())
    return day_input([etf_row("TEST01", "1.00"), gap], issues=[issue])


def test_an_etf_without_a_daily_move_gets_no_member_line_even_when_it_has_an_issue():
    i = no_day_input()
    out = DailyOutput((Point("이유입니다.", ("E1",), None, POINT_ETF, "TEST01"),), False, ("가", "나"))
    text = daily_text(i, build_sections(i), out)
    assert "자료 없음: TEST TEST03" in text and "↳" not in text
    assert "↳" not in render_html(i, build_sections(i), out)
    assert "↳" not in daily_text(i, build_sections(i), None)


def both_indices(kospi, kosdaq, status="ok"):
    rows = [{"indx_nm": "KOSPI종합", "up_dwn_r_p2": kospi, "bdy_cmpr_p2": "1", "bdy_cmpr_ccd": "2", "now_indx_p2": "2500"},
            {"indx_nm": "KOSDAQ종합", "up_dwn_r_p2": kosdaq, "bdy_cmpr_p2": "1", "bdy_cmpr_ccd": "5", "now_indx_p2": "800"}]
    return Background("market-summary", None, status, json.dumps({"dataBody": {"out2": rows}}), FETCHED)


def market_text(background):
    i = day_input([etf_row("TEST01", "1.00")], background=background)
    return daily_text(i, build_sections(i), None), render_html(i, build_sections(i), None)


def test_the_korean_market_line_sits_right_under_the_conclusion_in_both_forms():
    text, html = market_text((both_indices("0.30", "-0.50"),))
    assert "그날 한국 증시: 코스피 +0.30% · 코스닥 −0.50%" in text
    lines = text.splitlines()
    assert lines[next(n for n, line in enumerate(lines) if line.startswith("결론:")) + 1].startswith("그날 한국 증시:")
    assert html.index("결론:") < html.index("그날 한국 증시:") < html.index("■ ")


@pytest.mark.parametrize("background, expected", [
    ((), "코스피 자료 없음 · 코스닥 자료 없음"),
    ((Background("market-summary", None, "missing"),), "코스피 자료 없음 · 코스닥 자료 없음"),
    ((both_indices("0.00", "abc"),), "코스피 +0.00% · 코스닥 자료 없음"),
    ((market("0.30"),), "코스피 +0.30% · 코스닥 자료 없음"),
])
def test_an_index_that_cannot_be_read_is_shown_as_no_data(background, expected):
    text, html = market_text(background)
    assert f"그날 한국 증시: {expected}" in text and f"그날 한국 증시: {expected}" in re.sub(r"<[^>]+>", "", html)


def test_the_market_line_is_not_in_the_morning_edition():
    i = replace(day_input([etf_row("TEST01", "1.00")], background=(both_indices("0.30", "0.10"),)), edition="morning",
                etfs=None)
    assert "그날 한국 증시" not in daily_text(i, build_sections(i), None)


def test_a_rejected_repeat_says_which_number_but_not_the_name_or_ticker():
    reason = "담긴 종목이 3.5% 올라 영향을 줬을 수 있습니다."
    with pytest.raises(InvalidDailyOutput, match=r"member issue line \(등락률 숫자 3\.5%\)") as number:
        parse(llm_output([{"symbol": "TEST01", "reason": reason, "refs": ["E1"], "weak": False, "shortfall": ""},
                          {"symbol": "TEST02", "reason": "이유입니다.", "refs": ["E2"], "weak": False,
                           "shortfall": ""}]))
    assert "TESTA" not in str(number.value)
    with pytest.raises(InvalidDailyOutput, match=r"\(종목 이름\)") as name:
        parse(llm_output([{"symbol": "TEST01", "reason": "테스트전자 때문입니다.", "refs": ["E1"], "weak": False,
                           "shortfall": ""},
                          {"symbol": "TEST02", "reason": "이유입니다.", "refs": ["E2"], "weak": False, "shortfall": ""}]))
    assert "테스트전자" not in str(name.value)


def test_an_etf_that_moved_far_more_than_the_market_is_searched_again():
    sentences = DailySentences(type("Core", (), {"settings": DEFAULT_DAILY_SETTINGS})())
    i = day_input([etf_row("TEST01", "6.00")], background=(market("0.30"),))
    point = Point("이유", ("E1",), "부족", POINT_ETF, "TEST01")
    draft = DailyDraft(None, i, build_sections(i), _Draft("", "llm", (), False, (), DailyOutput((point,), False, ("a", "b"))))
    assert [w.no for w in sentences.weak_sentences(draft)] == [1]


# ---- 눈에 띄는 점 절과 결론 줄의 이유 없음 수 ----

def test_the_writer_drops_notables_when_no_candidate_was_given_and_keeps_them_when_one_was():
    notable = {"statement": "두 ETF가 반대로 움직였습니다.", "refs": ["E1", "E2"], "weak": False, "shortfall": ""}
    i = day_input([etf_row("TEST01", "1.00"), etf_row("TEST02", "-1.00")])
    result = DailyLlmWriter(Sequence(llm_output(notables=[notable])), max_attempts=3).write(i, build_sections(i))
    assert result.rejected == () and result.body is not None
    assert all(p.kind == POINT_ETF for p in result.body.output.points)
    given = replace(i, notables=(NotableCandidate(KIND_OPPOSITE, ("TEST01", "TEST02")),))
    ok = DailyLlmWriter(Sequence(llm_output(notables=[notable]))).write(given, build_sections(given))
    assert ok.rejected == () and ok.body is not None


def test_without_notables_the_section_is_left_out_of_the_text_and_the_html():
    i = day_input([etf_row("TEST01", "1.00"), etf_row("TEST02", "2.00")])
    out = DailyOutput((Point("이유입니다.", ("E1",), None, POINT_ETF, "TEST01"),
                       Point("이유입니다.", ("E2",), None, POINT_ETF, "TEST02")), False, ("가", "나"))
    assert "눈에 띄는 점" not in daily_text(i, build_sections(i), out)
    assert "눈에 띄는 점" not in render_html(i, build_sections(i), out)


def test_the_conclusion_ends_with_the_count_of_etfs_without_a_reason_and_leaves_it_out_at_zero():
    i = day_input([etf_row("TEST01", "1.00"), etf_row("TEST02", "2.00")],
                  articles=(EtfArticles("TEST01", (), True), EtfArticles("TEST02", (ARTICLE,), True)))
    assert conclusion(i).endswith(" · 이유를 찾지 못한 ETF 1개")
    none = day_input([etf_row("TEST01", "1.00")], articles=(EtfArticles("TEST01", (ARTICLE,), True),))
    assert "이유를 찾지 못한" not in conclusion(none)
    # 기사 응답을 못 받은 ETF와 기사를 요청하지 않은 판은 세지 않는다.
    unknown = day_input([etf_row("TEST01", "1.00")], articles=(EtfArticles("TEST01", (), False),))
    assert "이유를 찾지 못한" not in conclusion(unknown)
    assert "이유를 찾지 못한" not in conclusion(day_input([etf_row("TEST01", "1.00")]))


# ---- 환율·미국 지수 ----

def fx_and_index_background():
    fx_rows = [{"crncy_cd_nm": "TEST통화", "cls_prc_p4": " 9.0000", "bdy_cmpr_p4": " 0.1000", "bdy_cmpr_r_p2": " 1.00"},
               {"crncy_cd_nm": "원/달러", "cls_prc_p4": "  1111.1100", "bdy_cmpr_p4": "  -2.2200",
                "bdy_cmpr_r_p2": "  -0.20", "fnl_rcp_tm": "153000"}]
    world = Background("world-index", "C", "ok", "{\"raw\": \"TEST\"}", AT - timedelta(hours=9), index_rates=(
        IndexRate("TESTIX", "TEST INDEX", "20261019", "100.00", "20261016", "98.00", "2.04"),))
    fx = Background("exchange-rate-summary", None, "ok", json.dumps({"dataBody": {"out2": fx_rows}}, ensure_ascii=False),
                    FETCHED)
    return world, fx


def test_post_close_gives_claude_the_us_index_and_the_usd_krw_rate_and_keeps_them_out_of_the_body():
    i = day_input([etf_row("TEST01", "1.00")], background=fx_and_index_background())
    sections = build_sections(i)
    llm = {item["name"]: item for item in build_llm_input(i, sections)["items"]}
    [index] = llm[ITEM_US_INDEX]["entries"]
    assert index["ref"] == "B1" and index["indexRates"][0]["rate"] == "2.04"
    [fx] = llm[ITEM_EXCHANGE]["entries"]
    # 원문은 그대로 두고 꺼낸 원/달러 값을 덧붙인다.
    assert fx["ref"] == "B2" and fx["usdKrw"] == {"value": "1111.1100", "change": "-2.2200", "ratePercent": "-0.20"}
    assert "TEST통화" in fx["response"]
    text = daily_text(i, sections, None)
    # 두 항목은 절로 나열하지 않는다. 환율은 환율 이슈 칸에서만 코드가 받은 값을 옮겨 보인다.
    assert f"■ {ITEM_US_INDEX}" not in text and f"■ {ITEM_EXCHANGE}" not in text
    assert "환율 이슈:\n  - 원/달러 1111.1100(전일대비 -2.2200, -0.20%)" in text


def test_a_reason_can_cite_the_exchange_rate_and_its_source_line_shows_the_usd_krw_numbers():
    i = day_input([etf_row("TEST01", "1.00")], background=fx_and_index_background())
    sections = build_sections(i)
    out = json.dumps({"summary": ["가입니다.", "나입니다."], "notables": [], "insufficientEvidence": False,
                      "etfs": [{"symbol": "TEST01", "reason": "미국 지수가 오르고 환율이 내린 흐름이 영향을 줬을 수 있습니다.",
                                "refs": ["B1", "B2"], "weak": False, "shortfall": ""}]}, ensure_ascii=False)
    result = DailyLlmWriter(Sequence(out)).write(i, sections)
    assert result.rejected == ()
    tail = result.body.text.split("출처:\n")[1].splitlines()
    assert tail[0].startswith("  1. 시장 자료 · 세계지수")
    assert tail[1].startswith("  2. 시장 자료 · 환율종합 원/달러 1111.1100(전일대비 -2.2200, -0.20%)")


def test_the_morning_exchange_input_is_unchanged_and_an_unreadable_rate_keeps_the_plain_source_line():
    world, fx = fx_and_index_background()
    morning = replace(day_input([etf_row("TEST01", "1.00")], background=(world, fx)), edition="morning")
    [entry] = next(s for s in build_sections(morning) if s.name == ITEM_EXCHANGE).entries
    assert "usdKrw" not in entry.llm and entry.describe.startswith("환율종합 — ")
    plain = Background("exchange-rate-summary", None, "ok", "{\"fx\": \"TEST\"}", FETCHED)
    post = day_input([etf_row("TEST01", "1.00")], background=(world, plain))
    [entry] = next(s for s in build_sections(post) if s.name == ITEM_EXCHANGE).entries
    assert "usdKrw" not in entry.llm and entry.describe.startswith("환율종합 — ")


def test_the_post_close_prompt_reads_us_assets_with_the_exchange_rate_and_ends_sentences_politely():
    assert "달러-원 환율 변화를 함께 봅니다" in SYSTEM_PROMPT_POST_CLOSE
    assert "기사·공시·구성 종목 이슈·미국 지수·환율 번호" in SYSTEM_PROMPT_POST_CLOSE
    assert "관련 없는 기사는 인용하지 않습니다" in SYSTEM_PROMPT_POST_CLOSE
    assert '"~습니다"로 끝맺습니다' in SYSTEM_PROMPT_POST_CLOSE
    assert "refs는 빈 목록" in SYSTEM_PROMPT_POST_CLOSE and "찾지 못했다" not in SYSTEM_PROMPT_POST_CLOSE
