"""마감 후 판 바로잡기: 장 시작 전 판, 0.00% ETF 이유, Claude 기준 이유 없음 수, 관계없는 기사 거르기, 환율·금리 이슈 칸."""
from datetime import datetime, timedelta, timezone

import pytest

from briefing.daily_inputs import PRE_OPEN_NOTICE, is_before_open
from briefing.daily_jobs import DailyCore
from briefing.daily_prompts import NO_REASON_STATEMENT, SYSTEM_PROMPT_POST_CLOSE
from briefing.llm import LlmCallError
from briefing.daily_report import DailyInput, DailyLlmWriter, EtfDay
from fake_news import answer_news
from test_daily_jobs import (
    POST_CLOSE_JOB,
    Details,
    FakeLlm,
    POST_CLOSE_BACKGROUND,
    Requests,
    daily_response,
    etf,
    post_close_output,
    published,
    tick,
)

KST = timezone(timedelta(hours=9))
PRE_OPEN_AT = datetime(2026, 10, 20, 8, 40, tzinfo=KST)
TODAY = "20261020"


def day_row(symbol, rate, date=TODAY):
    """일봉 한 줄. 장 시작 전에는 그날 일자 일봉이 종가=전일 종가, 등락 0.00으로 온다."""
    return {**etf(symbol, fetched="2026-10-20T08:35:00+09:00", rate=rate), "date": date, "close": "10000",
            "prevClose": "10000"}


def etf_day(rate, date=TODAY, symbol="TEST01"):
    return EtfDay(symbol, date, "10000", "10000", rate, PRE_OPEN_AT, (), False)


# ---- 장 시작 전 판정 ----

def test_before_open_needs_every_etf_with_a_day_to_be_todays_bar_with_a_zero_rate():
    assert is_before_open((etf_day("0.00"), etf_day("0.00", symbol="TEST02")), PRE_OPEN_AT)
    # 하나라도 등락이 있으면 정식 판이다.
    assert not is_before_open((etf_day("0.00"), etf_day("0.30", symbol="TEST02")), PRE_OPEN_AT)
    # 일자가 판을 만든 날이 아니면 장이 끝난 뒤에도 등락이 없던 것이다.
    assert not is_before_open((etf_day("0.00", date="20261019"),), PRE_OPEN_AT)
    assert not is_before_open((etf_day("0.00"), etf_day("0.00", date="20261019", symbol="TEST02")), PRE_OPEN_AT)


def test_before_open_ignores_etfs_without_a_day_but_needs_at_least_one_with_a_day():
    no_day = EtfDay("TEST09", None, None, None, None, None, (), False, None, "no-daily-price-in-range")
    assert is_before_open((etf_day("0.00"), no_day), PRE_OPEN_AT)
    assert not is_before_open((no_day,), PRE_OPEN_AT) and not is_before_open((), PRE_OPEN_AT)


def test_before_open_reads_the_date_in_korean_time():
    # 17:00 KST는 UTC로 08:00이라 UTC 날짜로 보면 같은 날이지만, 자정 직후 판은 UTC 날짜가 하루 전이다.
    just_after_midnight = datetime(2026, 10, 20, 0, 30, tzinfo=KST)
    assert is_before_open((etf_day("0.00"),), just_after_midnight)


def test_a_rate_that_is_not_a_number_is_not_before_open():
    assert not is_before_open((etf_day("abc"),), PRE_OPEN_AT)


# ---- 장 시작 전 판 ----

def run_post_close(etfs, llm, at=PRE_OPEN_AT, background=None, settings=None):
    requests = Requests()
    core = DailyCore(requests, DailyLlmWriter(llm), Details(), **({"settings": settings} if settings else {}))
    core.on_schedule_tick(tick(POST_CLOSE_JOB, at), at, "post-close")
    core.on_daily_data_responded(daily_response(etfs=etfs, background=background), at)
    answer_news(core, requests, at)
    [event] = published(requests)
    return event.value["payload"]


def test_a_post_close_edition_made_before_the_market_opens_says_so_and_asks_claude_nothing():
    llm = FakeLlm()
    payload = run_post_close([day_row("TEST01", "0.00"), day_row("TEST02", "0.00")], llm)
    assert llm.calls == []
    assert payload["generatedBy"] == "rule-based"
    for body in (payload["text"], payload["html"]):
        assert PRE_OPEN_NOTICE in body
        assert "이유:" not in body
    assert f"결론: {PRE_OPEN_NOTICE}" in payload["text"]


def test_one_etf_with_a_move_makes_it_a_regular_edition():
    llm = FakeLlm(post_close_output(("TEST01", "TEST02")))
    payload = run_post_close([day_row("TEST01", "0.00"), day_row("TEST02", "0.30")], llm)
    assert len(llm.calls) == 1
    assert PRE_OPEN_NOTICE not in payload["text"] and payload["generatedBy"] == "llm"


def test_a_zero_rate_edition_at_five_pm_is_not_treated_as_before_the_open():
    at = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
    llm = FakeLlm(post_close_output(("TEST01",)))
    payload = run_post_close([day_row("TEST01", "0.00", date="20261019")], llm, at)
    assert len(llm.calls) == 1 and PRE_OPEN_NOTICE not in payload["text"]


def test_the_morning_edition_is_never_before_open():
    i = DailyInput("job", "morning", PRE_OPEN_AT - timedelta(days=1), PRE_OPEN_AT, (etf_day("0.00"),), False, (), (),
                   (), False)
    assert not i.before_open()


# ---- 0.00% ETF 이유와 지시문 ----

REGULAR_AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)


def regular_rows():
    return [etf(symbol, fetched="2026-10-20T16:30:02+09:00", rate=rate)
            for symbol, rate in (("TEST01", "1.20"), ("TEST02", "0.00"), ("TEST03", "-0.50"))]


def test_the_prompt_tells_claude_to_explain_a_zero_percent_etf_with_the_numbers_it_was_given():
    zero_rule = [line for line in SYSTEM_PROMPT_POST_CLOSE.splitlines() if "0.00" in line and "이유" in line
                 and any(word in line for word in ("환율", "금리", "지수"))]
    assert zero_rule
    assert "금리" in SYSTEM_PROMPT_POST_CLOSE and "금리 이슈" in SYSTEM_PROMPT_POST_CLOSE
    assert "환율 이슈" in SYSTEM_PROMPT_POST_CLOSE


def test_the_not_found_sentence_in_the_prompt_is_the_one_the_code_counts():
    assert NO_REASON_STATEMENT in SYSTEM_PROMPT_POST_CLOSE


def test_a_zero_percent_etf_in_a_regular_edition_gets_the_reason_claude_wrote():
    reasons = {"TEST02": "금리는 올랐고 환율은 내려 크게 움직이지 않았을 수 있습니다."}
    llm = FakeLlm(post_close_output(reasons=reasons))
    payload = run_post_close(regular_rows(), llm, REGULAR_AT, POST_CLOSE_BACKGROUND)
    assert reasons["TEST02"] in payload["text"] and reasons["TEST02"] in payload["html"]
    # 요청한 ETF 모두(0.00% 포함)에 이유를 묻는다.
    assert '"TEST02"' in llm.calls[0][1]


# ---- 이유를 찾지 못한 ETF 수 ----

def not_found(*symbols):
    return post_close_output(reasons={s: NO_REASON_STATEMENT for s in symbols}, refs={s: [] for s in symbols},
                             weak=symbols)


def test_the_no_reason_count_is_the_number_of_etfs_claude_wrote_the_not_found_sentence_for():
    # 기사가 하나도 없어 코드 기준으로는 3개지만, Claude가 이유를 못 찾았다고 쓴 것은 2개다.
    payload = run_post_close(regular_rows(), FakeLlm(not_found("TEST01", "TEST02")), REGULAR_AT, POST_CLOSE_BACKGROUND)
    assert "이유를 찾지 못한 ETF 2개" in payload["text"] and "이유를 찾지 못한 ETF 2개" in payload["html"]


def test_the_no_reason_count_is_left_out_when_claude_found_a_reason_for_every_etf():
    payload = run_post_close(regular_rows(), FakeLlm(post_close_output()), REGULAR_AT, POST_CLOSE_BACKGROUND)
    assert "이유를 찾지 못한 ETF" not in payload["text"]


def test_the_count_ignores_a_missing_final_period_and_other_sentences_that_only_mention_it():
    from briefing.daily_report import DailyOutput, Point, POINT_ETF, claude_no_reason_count
    output = DailyOutput((
        Point("받은 자료에서 이유를 찾지 못했습니다", (), "부족", POINT_ETF, "TEST01"),
        Point("받은 자료에서 이유를 찾지 못했습니다. 다만 환율이 영향을 줬을 수 있습니다.", ("B1",), None, POINT_ETF, "TEST02"),
        Point(NO_REASON_STATEMENT, (), "부족", "notable")), False)
    assert claude_no_reason_count(output) == 1


def test_a_rule_based_edition_keeps_counting_by_the_data():
    llm = FakeLlm(*[LlmCallError("down")] * 3)
    payload = run_post_close(regular_rows(), llm, REGULAR_AT, POST_CLOSE_BACKGROUND)
    assert payload["generatedBy"] == "rule-based"
    assert "이유를 찾지 못한 ETF 3개" in payload["text"]


# ---- 관계없는 기사 거르기 ----

from briefing.daily_relevance import MIN_TERM_LENGTH, etf_terms, split_related  # noqa: E402
from briefing.sources import Article, EtfInfo, EtfMember  # noqa: E402

IN_RANGE = "2026-10-20T10:00:00+09:00"
INFO = EtfInfo((EtfMember("US", "TESTA", 1, "TEST CORP A", "12.5"), EtfMember("KR", "TEST900", 2, "TEST 국내 B", "5.0")),
               False, "TEST ETF ONE", "TEST INDEX ONE", "TEST SECTOR")


def news(title, summary="FAKE 요약입니다."):
    return Article("a-" + title[:3], title, summary, PRE_OPEN_AT, "TEST 신문", "https://example.test/a")


def test_an_article_naming_the_etf_its_index_or_a_member_is_kept_in_title_or_summary():
    articles = (news("TEST ETF ONE 거래 동향"), news("FAKE 흐름", "TEST INDEX ONE 관련 흐름입니다."),
                news("TEST CORP A 신제품 공개"), news("FAKE 둘째", "오늘 TEST 국내 B 가 올랐습니다."))
    kept, dropped = split_related(INFO, articles)
    assert kept == articles and dropped == ()


def test_an_article_naming_none_of_them_is_dropped_and_counted_and_the_order_stays():
    first, stray, last = news("TEST ETF ONE 소식"), news("FAKE 지역 축제 일정 안내"), news("TEST CORP A 소식")
    kept, dropped = split_related(INFO, (first, stray, last))
    assert kept == (first, last) and dropped == (stray,)


def test_names_match_without_regard_to_case_or_spacing():
    kept, dropped = split_related(INFO, (news("test etf  one 거래 동향"), news("TESTCORPA 신제품")))
    assert len(kept) == 2 and dropped == ()


def test_an_etf_with_no_names_to_compare_keeps_every_article():
    articles = (news("FAKE 지역 축제 일정 안내"),)
    assert split_related(None, articles) == (articles, ())
    assert split_related(EtfInfo((), False), articles) == (articles, ())
    assert split_related(EtfInfo((EtfMember("KR", "TEST1", 1, None, "1.0"),), False, "  "), articles) == (articles, ())


def test_a_one_character_name_is_not_compared_because_it_would_match_anything():
    assert MIN_TERM_LENGTH == 2
    info = EtfInfo((EtfMember("KR", "TEST1", 1, "A", "1.0"),), False, "TEST ETF ONE")
    assert etf_terms(info) == ("testetfone", "etfone")
    kept, dropped = split_related(info, (news("A 로 시작하는 FAKE 기사"),))
    assert kept == () and len(dropped) == 1


def test_the_etf_name_is_also_compared_without_its_brand_prefix_and_the_index_ignores_spacing():
    info = EtfInfo((), False, "ACMEX Alpha Fund", "S&P 500")
    assert etf_terms(info) == ("acmexalphafund", "alphafund", "s&p500")
    articles = (news("Alpha Fund 거래 동향"), news("FAKE 지수", "S&P500 이 올랐습니다."), news("FAKE 다른 기사"))
    kept, dropped = split_related(info, articles)
    assert kept == articles[:2] and dropped == articles[2:]


def test_only_an_all_capital_ascii_first_word_is_taken_as_a_brand_prefix():
    # 첫 낱말이 한글이거나 소문자가 섞였거나 기호가 든 이름은 그대로만 비교한다.
    for name in ("알파 펀드", "Acme Alpha Fund", "S&P Alpha Fund", "ACMEXAlphaFund"):
        assert etf_terms(EtfInfo((), False, name)) == (name.replace(" ", "").casefold(),)


def test_a_name_that_is_only_digits_after_the_brand_is_not_compared_on_its_own():
    # 접두어를 떼면 '200'뿐이라 '1200원' 같은 기사에 걸리므로, 전체 이름과 다른 이름으로만 비교한다.
    info = EtfInfo((EtfMember("KR", "TEST1", 1, "TEST 국내 B", "1.0"),), False, "TEST 200", "TEST INDEX ONE")
    assert etf_terms(info) == ("test200", "testindexone", "test국내b")
    stray, named = news("FAKE 환율", "달러가 1200원 선을 넘었습니다."), news("TEST 200 거래 동향")
    kept, dropped = split_related(info, (stray, named))
    assert kept == (named,) and dropped == (stray,)


def info_row(symbol):
    """이름·추종 대상·구성 종목 정보가 든 일봉 한 줄."""
    return {**etf(symbol, fetched="2026-10-20T16:30:02+09:00", rate="1.20", tickers=()),
            "etf": {"name": f"TEST ETF {symbol}", "tracks": f"TEST INDEX {symbol}", "sector": "TEST SECTOR",
                    "membersTruncated": False,
                    "members": [{"market": "KR", "symbol": "TEST900", "name": f"TEST CORP {symbol}", "rank": 1,
                                 "weight": "5.0"}]}}


def raw_article(article_id, title, summary="FAKE 요약입니다."):
    return {"articleId": article_id, "title": title, "summary": summary, "publishedAt": IN_RANGE,
            "publisher": "TEST 신문", "url": f"https://example.test/{article_id}"}


def run_with_articles(rows, articles, details=None):
    llm = FakeLlm(post_close_output(tuple(r["symbol"] for r in rows)))
    requests, details = Requests(), details or Details()
    core = DailyCore(requests, DailyLlmWriter(llm), details)
    core.on_schedule_tick(tick(POST_CLOSE_JOB, REGULAR_AT), REGULAR_AT, "post-close")
    core.on_daily_data_responded(daily_response(etfs=rows, background=POST_CLOSE_BACKGROUND), REGULAR_AT)
    answer_news(core, requests, REGULAR_AT, articles=articles)
    return llm, details, published(requests)[0].value["payload"]


def test_unrelated_articles_never_reach_claude_and_the_count_is_in_the_job_log():
    import json
    rows = [info_row("TEST01"), info_row("TEST02")]
    llm, details, _ = run_with_articles(rows, {
        "TEST01": [raw_article("a-1", "TEST ETF TEST01 거래 동향"), raw_article("a-2", "FAKE 지역 축제 일정 안내")],
        "TEST02": [raw_article("a-3", "FAKE 오늘의 시장 흐름", "TEST INDEX TEST02 관련 흐름입니다."),
                   raw_article("a-4", "FAKE 다른 행사 안내")]})
    sent = llm.calls[0][1]
    for kept in ("TEST ETF TEST01 거래 동향", "FAKE 오늘의 시장 흐름"):
        assert kept in sent
    for dropped in ("FAKE 지역 축제 일정 안내", "FAKE 다른 행사 안내"):
        assert dropped not in sent
    records = [e.data["excluded"] for e in details.entries if "excluded" in e.data]
    assert records and all(r["irrelevantArticles"] == 2 for r in records)
    assert json.dumps(records)  # 작업 기록에 쓸 수 있는 값이어야 한다.
    # 뺀 기사의 제목은 작업 기록에서 확인할 수 있다.
    titles = [e.data["irrelevantArticleTitles"] for e in details.entries if "irrelevantArticleTitles" in e.data]
    assert titles and {t["title"] for t in titles[-1]} == {"FAKE 지역 축제 일정 안내", "FAKE 다른 행사 안내"}


def test_unrelated_articles_do_not_take_one_of_the_five_places():
    rows = [info_row("TEST01")]
    articles = [raw_article(f"x-{n}", f"FAKE 관계없는 기사 {n}") for n in range(8)]
    articles.append(raw_article("a-1", "TEST ETF TEST01 거래 동향"))
    llm, _, _ = run_with_articles(rows, {"TEST01": articles})
    sent = llm.calls[0][1]
    assert "TEST ETF TEST01 거래 동향" in sent and "FAKE 관계없는" not in sent


def test_an_etf_without_info_keeps_its_articles():
    row = {**etf("TEST01", fetched="2026-10-20T16:30:02+09:00", rate="1.20", tickers=())}
    llm, _, _ = run_with_articles([row], {"TEST01": [raw_article("a-1", "FAKE 정보 없는 ETF 기사")]})
    assert "FAKE 정보 없는 ETF 기사" in llm.calls[0][1]


# ---- 환율 이슈·금리 이슈 칸 ----

import json  # noqa: E402

from briefing.daily_report import InvalidDailyOutput, parse_post_close_output  # noqa: E402

FX_SENTENCE = "원/달러 환율이 내려 미국 자산에 투자하는 ETF의 원화 가격에 영향을 줬을 수 있습니다."
RATE_SENTENCE = "받은 자료에 금리 관련 기사가 없습니다."


def with_issues(output, fx=None, rate=None):
    """post_close_output에 환율 이슈·금리 이슈를 더한 JSON."""
    data = json.loads(output)
    if fx is not None:
        data["fxIssue"] = fx
    if rate is not None:
        data["rateIssue"] = rate
    return json.dumps(data, ensure_ascii=False)


def issue(statement, refs, weak=False, shortfall=""):
    return {"statement": statement, "refs": refs, "weak": weak, "shortfall": shortfall}


def test_the_issue_cells_sit_between_the_notables_and_the_etf_lines_in_both_forms():
    # 근거 번호는 항목 순서(그날 한국 증시 B1, 미국 지수 B2, 환율 B3)로 붙는다.
    out = with_issues(post_close_output(), issue(FX_SENTENCE, ["B3"]),
                      issue(RATE_SENTENCE, [], True, "금리를 다룬 기사가 더 있어야 합니다."))
    payload = run_post_close(regular_rows(), FakeLlm(out), REGULAR_AT, POST_CLOSE_BACKGROUND)
    text, html = payload["text"], payload["html"]
    # 환율 이슈 근거(B3)는 ETF 이유의 근거보다 먼저 본문에 나오므로 출처 1번이다.
    assert f"환율 이슈:\n  - {FX_SENTENCE} [출처 1]" in text
    assert f"금리 이슈:\n  - {RATE_SENTENCE}\n    근거 약함: 금리를 다룬 기사가 더 있어야 합니다." in text
    assert text.index("환율 이슈:") < text.index("금리 이슈:") < text.index("■ ") < text.index("출처:")
    assert "환율종합" in text.split("출처:\n")[1].splitlines()[0]
    for body in (html,):
        assert body.index("환율 이슈") < body.index("금리 이슈") < body.index("■ ")
        assert FX_SENTENCE in body and RATE_SENTENCE in body and "[출처 1]" in body


def test_an_issue_cell_is_written_even_without_claude_text_and_the_rate_cell_says_it_has_no_data():
    payload = run_post_close(regular_rows(), FakeLlm(post_close_output()), REGULAR_AT, POST_CLOSE_BACKGROUND)
    # Claude가 칸을 내지 않은 판도 두 칸은 늘 있다.
    for body in (payload["text"], payload["html"]):
        assert "환율 이슈" in body and "금리 이슈" in body
    assert "금리 이슈:\n  - 자료 없음" in payload["text"]


def test_the_issue_cells_are_in_a_rule_based_edition_and_the_one_made_before_the_open():
    rule = run_post_close(regular_rows(), FakeLlm(*[LlmCallError("down")] * 3), REGULAR_AT, POST_CLOSE_BACKGROUND)
    pre = run_post_close([day_row("TEST01", "0.00")], FakeLlm())
    for payload in (rule, pre):
        for body in (payload["text"], payload["html"]):
            assert "환율 이슈" in body and "금리 이슈" in body


def parse(raw, allowed=frozenset({"E1", "E2", "B2"}), tickers=frozenset()):
    return parse_post_close_output(raw, allowed, tickers, frozenset({"TEST01", "TEST02"}), {})


def two_etf_output(**issues):
    return with_issues(post_close_output(("TEST01", "TEST02")), **issues)


def test_issue_sentences_are_parsed_with_their_kind_and_refs():
    output = parse(two_etf_output(fx=issue(FX_SENTENCE, ["B2"]), rate=issue(RATE_SENTENCE, [], True, "부족")))
    assert [(p.kind, p.refs, p.shortfall) for p in output.issues] == [("fx", ("B2",), None), ("rate", (), "부족")]
    # 이슈 문장은 다시 쓰기 대상인 points에 섞이지 않는다.
    assert all(p.kind == "etf" for p in output.points)


def test_an_output_without_the_issue_cells_is_still_accepted():
    assert parse(two_etf_output()).issues == ()


@pytest.mark.parametrize("bad", [
    issue(FX_SENTENCE, ["B9"]),           # 준 적 없는 근거 번호
    issue(FX_SENTENCE, []),               # 근거 번호도 없고 근거 약함도 아님
    issue(FX_SENTENCE, ["B2"], True, ""),  # 약하다면서 부족한 점이 없음
    issue("   ", ["B2"]),                 # 빈 문장
    issue("TESTA 종목이 영향을 줬을 수 있습니다.", ["B2"]),  # 미국 티커
    "문장",                                # 객체가 아님
])
def test_a_broken_issue_cell_is_rejected(bad):
    with pytest.raises(InvalidDailyOutput):
        parse(two_etf_output(fx=bad), tickers=frozenset({"TESTA"}))
    with pytest.raises(InvalidDailyOutput):
        parse(two_etf_output(rate=bad), tickers=frozenset({"TESTA"}))


def test_the_issue_refs_count_as_cited_and_an_insufficient_output_drops_the_cells():
    out = two_etf_output(fx=issue(FX_SENTENCE, ["B2"]), rate=issue(RATE_SENTENCE, [], True, "부족"))
    assert parse(out).issues
    data = json.loads(out)
    data.update({"insufficientEvidence": True, "etfs": [], "summary": []})
    insufficient = parse(json.dumps(data, ensure_ascii=False))
    assert insufficient.insufficient_evidence and insufficient.issues == ()


def test_the_prompt_and_schema_ask_for_both_cells_and_point_the_etf_reasons_at_them():
    from briefing.daily_prompts import OUTPUT_SCHEMA_POST_CLOSE
    for key in ("fxIssue", "rateIssue"):
        assert key in SYSTEM_PROMPT_POST_CLOSE and key in OUTPUT_SCHEMA_POST_CLOSE["properties"]
    assert "(환율 이슈 참고)" in SYSTEM_PROMPT_POST_CLOSE and "(금리 이슈 참고)" in SYSTEM_PROMPT_POST_CLOSE
    assert "이슈 칸 참고" not in SYSTEM_PROMPT_POST_CLOSE
    # 금리는 기사에 적힌 숫자만 쓴다.
    assert "금리 숫자는 그 기사에 적힌 것만" in SYSTEM_PROMPT_POST_CLOSE


def test_an_insufficient_output_is_accepted_even_when_the_issue_cells_are_empty_or_broken():
    for cells in ({}, {"fxIssue": {}, "rateIssue": {}}, {"fxIssue": "문장", "rateIssue": issue(FX_SENTENCE, ["B9"])}):
        data = {**json.loads(two_etf_output()), **cells, "insufficientEvidence": True, "etfs": [], "summary": []}
        output = parse(json.dumps(data, ensure_ascii=False))
        assert output.insufficient_evidence and output.points == () and output.issues == ()


# ---- 장 시작 시각 ----

def test_before_open_also_needs_the_edition_time_to_be_before_the_market_open():
    from briefing.config import DailySettings
    from datetime import time
    # 09:00 전에는 장 전, 이후에는 한 ETF만 0.00%여도 정식 판이다.
    assert is_before_open((etf_day("0.00"),), datetime(2026, 10, 20, 8, 59, tzinfo=KST))
    assert not is_before_open((etf_day("0.00"),), datetime(2026, 10, 20, 9, 0, tzinfo=KST))
    assert not is_before_open((etf_day("0.00"),), REGULAR_AT)
    # 장 시작 시각은 설정값이다.
    assert is_before_open((etf_day("0.00"),), REGULAR_AT, DailySettings(market_open=time(18, 0)))


def test_the_market_open_setting_decides_whether_the_real_edition_is_before_the_open():
    from datetime import time
    from briefing.config import DailySettings
    rows = [day_row("TEST01", "0.00"), day_row("TEST02", "0.00")]
    # 기본 설정(09:00)으로는 17:00 판이 정식 판이다.
    llm = FakeLlm(post_close_output(("TEST01", "TEST02")))
    default = run_post_close(rows, llm, REGULAR_AT, POST_CLOSE_BACKGROUND)
    assert len(llm.calls) == 1 and PRE_OPEN_NOTICE not in default["text"]
    # 장 시작 시각을 18:00으로 바꾸면 같은 17:00 판이 장 전 판이 되어 Claude를 부르지 않고 안내 문구가 나간다.
    llm = FakeLlm()
    later = run_post_close(rows, llm, REGULAR_AT, POST_CLOSE_BACKGROUND, DailySettings(market_open=time(18, 0)))
    assert llm.calls == [] and later["generatedBy"] == "rule-based"
    for body in (later["text"], later["html"]):
        assert PRE_OPEN_NOTICE in body and "이유:" not in body
    assert f"결론: {PRE_OPEN_NOTICE}" in later["text"]


def test_a_single_zero_percent_etf_with_a_bar_in_the_five_pm_edition_is_a_regular_edition():
    llm = FakeLlm(post_close_output(("TEST01",)))
    payload = run_post_close([day_row("TEST01", "0.00")], llm, REGULAR_AT, POST_CLOSE_BACKGROUND)
    assert len(llm.calls) == 1 and PRE_OPEN_NOTICE not in payload["text"]


# ---- 다시 쓴 뒤의 근거 번호 ----

def test_the_issue_refs_stay_in_the_published_evidence_after_a_sentence_is_rewritten():
    from test_weak_research import DailyFlow, DailyLlm, SHORTFALL, QUERIES, daily_output, in_range, point, queries

    flow = DailyFlow("post-close", [], [])
    original = DailyLlm._post_close

    def with_fx_issue(data, user):
        # 처음 쓰기 응답에 환율 이슈 칸을 더한다. 근거는 ETF 이유가 인용하지 않는 시장 자료(B1)다.
        out = json.loads(original(data, user))
        out["fxIssue"] = issue(FX_SENTENCE, ["B1"])
        return json.dumps(out, ensure_ascii=False)

    flow.report_llm._post_close = with_fx_issue
    rewritten = []
    real_rewrite = flow.core.rewrite_point
    flow.core.rewrite_point = lambda *a, **k: rewritten.append(real_rewrite(*a, **k)) or rewritten[-1]
    flow.report_llm.outputs = [
        daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref),
                     point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1")),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(2))
    flow.stable([in_range(flow)], flow.rid(2))
    assert flow.published()
    (draft,) = rewritten
    assert "B1" in draft.draft.cited and "r1" in draft.draft.cited


# ---- 재작업: 비교어 넓히기, 미국 시장 단어, JSON 조각 거부 ----
from briefing.llm_output import InvalidLlmOutput, output_sentence, output_weak_shortfall  # noqa: E402


def test_country_prefix_and_suffix_are_trimmed_from_terms():
    info = EtfInfo((), False, "TEST 미국나스닥100", "미국 나스닥100 지수")
    terms = etf_terms(info)
    assert "나스닥100" in terms and "미국나스닥100지수" in terms
    kept, dropped = split_related(info, (news("나스닥100 혼합 ETF 출시"), news("전혀 다른 기사")))
    assert [a.title for a in kept] == ["나스닥100 혼합 ETF 출시"] and len(dropped) == 1


def test_trimmed_term_without_letters_or_too_short_is_not_used():
    # 뗀 결과가 숫자뿐이거나 한 글자이면 비교어로 쓰지 않는다.
    assert etf_terms(EtfInfo((), False, None, "미국 500 지수")) == ("미국500지수",)
    assert etf_terms(EtfInfo((), False, None, "미국 가 지수")) == ("미국가지수",)


def test_us_asset_etf_keeps_articles_with_market_words():
    us = EtfInfo((EtfMember("US", "TESTUS", 1, "테스트코퍼A", "1.0"),), False, "TEST 가짜ETF", "가짜 지수")
    kr = EtfInfo((EtfMember("KR", "TEST1", 1, "테스트코퍼B", "1.0"),), False, "TEST 가짜ETF", "가짜 지수")
    articles = (news("[뉴욕증시 금주전망] AI주 급락"), news("엉뚱한 기사"))
    assert [a.title for a in split_related(us, articles)[0]] == ["[뉴욕증시 금주전망] AI주 급락"]
    assert split_related(kr, articles)[0] == ()


def test_sentence_with_json_residue_is_rejected():
    class Bad(InvalidLlmOutput):
        pass

    for text in ("자료가 더 있어야 합니다.}],", "끝이 닫힘 기호입니다]", "열림 기호로 끝납니다{", "중간 }], 조각 입니다.",
                 "쉼표가 붙습니다.},", "닫힘만 있습니다.}]"):
        with pytest.raises(Bad, match="JSON residue"):
            output_sentence({"statement": text}, "statement", "point", Bad)
        with pytest.raises(Bad, match="JSON residue"):
            output_weak_shortfall({"weak": True, "shortfall": text}, "point", Bad)
    assert output_sentence({"statement": "정상 문장입니다."}, "statement", "point", Bad) == "정상 문장입니다."


def test_normal_sentences_with_quotes_and_bracket_titles_pass():
    class Bad(InvalidLlmOutput):
        pass

    for text in ('기사는 "금리 인하 기대가 커졌다."', "제목은 [속보]", "[단독] 가짜전자 공급 계약",
                 "[뉴욕증시 금주전망], 반도체 약세를 짚었습니다.", '그는 "더 오를 수 있다"'):
        assert output_sentence({"statement": text}, "statement", "point", Bad) == text
        assert output_weak_shortfall({"weak": True, "shortfall": text}, "point", Bad).endswith(text)


def test_json_residue_makes_incident_report_output_invalid():
    from briefing.report import InvalidOutput, parse_output

    cause = {"title": "제목입니다", "detail": "자료가 더 있어야 합니다.}],", "refs": ["B1"],
             "meaning": "뜻입니다", "meaningRefs": ["B1"], "weak": False, "shortfall": ""}
    text = json.dumps({"causes": [cause], "insufficientEvidence": False}, ensure_ascii=False)
    with pytest.raises(InvalidOutput, match="JSON residue"):
        parse_output(text, frozenset({"B1"}))


def test_dow_term_does_not_match_korean_company_name():
    from briefing.config import DailySettings

    terms = DailySettings().relevance_us_market_terms
    assert "다우" not in terms and {"다우존스", "다우지수"} <= set(terms)
    assert not any(t in "다우기술 실적 발표" for t in terms)
