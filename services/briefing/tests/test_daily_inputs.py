"""마감 후 판의 미국 구성 종목 이슈와 눈에 띄는 점 후보와 이유를 찾지 못한 ETF 수 계산(daily_inputs)과 Claude 입력에 담기는 모양을 확인한다. 가짜 자료만 쓴다."""
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import DailySettings
from briefing.daily_inputs import (
    KIND_CLUSTER,
    KIND_OPPOSITE,
    EtfArticles,
    member_issues,
    no_reason_count,
    notable_candidates,
)
from briefing.daily_report import (
    ITEM_ETF_ARTICLES,
    ITEM_FILINGS,
    ITEM_MEMBER_ISSUES,
    DailyInput,
    EtfDay,
    build_llm_input,
    build_sections,
    daily_text,
    missing_items,
)
from briefing.sources import Article, CloseRate, EtfInfo, EtfMember, Filing

KST = timezone(timedelta(hours=9))
AS_OF = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
RANGE_START = datetime(2026, 10, 19, 17, 0, tzinfo=KST)
S = DailySettings()


def member(symbol, rank, market="US", name=None):
    return EtfMember(market, symbol, rank, name or f"TEST 종목 {symbol}")


def etf_day(symbol="TEST01", rate="1.00", sector="TEST 업종", members=(), tickers=()):
    info = EtfInfo(tuple(members), False, f"TEST {symbol}", None, sector)
    return EtfDay(symbol, "20261020", "100", "99", rate, AS_OF, tuple(tickers), False, info)


def close(ticker, rate, closed=datetime(2026, 10, 20, 5, 0, tzinfo=KST)):
    return CloseRate(ticker, "20261019", closed, rate, AS_OF)


def filing(accession, *tickers):
    return Filing(accession, tuple(tickers), "8-K", (), AS_OF - timedelta(hours=3), "https://example.test/f")


def article(article_id, title="TEST 기사"):
    return Article(article_id, title, "TEST 요약", AS_OF - timedelta(hours=2), "TEST 신문", "https://example.test/a")


# ---- 미국 구성 종목 이슈 ----

@pytest.mark.parametrize("rate,expected", [("3.00", True), ("-3.00", True), ("2.99", False), ("-2.99", False),
                                           ("0.00", False)])
def test_a_member_is_an_issue_when_its_move_is_at_least_the_limit_in_either_direction(rate, expected):
    etf = etf_day(members=[member("TESTA", 1)])
    issues, cut = member_issues([etf], [close("TESTA", rate)], [])
    assert [i.ticker for i in issues] == (["TESTA"] if expected else [])
    assert cut == 0


def test_only_the_top_ranked_members_are_looked_at_and_the_eleventh_is_not():
    members = [member(f"TST{n:02d}", n) for n in range(1, 12)]
    rates = [close(m.symbol, "5.00") for m in members]
    issues, cut = member_issues([etf_day(members=members)], rates, [], DailySettings(member_issues_per_etf=20))
    assert [i.rank for i in issues] == list(range(1, 11))
    assert cut == 0


def test_the_ranking_comes_from_rank_not_from_the_order_received():
    members = [member("TESTB", 2), member("TESTA", 1)]
    issues, _ = member_issues([etf_day(members=members)], [close("TESTA", "4.00"), close("TESTB", "4.00")], [])
    assert [i.ticker for i in issues] == ["TESTA", "TESTB"]


def test_at_most_three_members_are_kept_per_etf_and_the_rest_are_counted_as_cut():
    members = [member(f"TST{n:02d}", n) for n in range(1, 6)]
    rates = [close(m.symbol, "-4.00") for m in members]
    issues, cut = member_issues([etf_day(members=members), etf_day("TEST02", members=members[:1])], rates, [])
    assert [(i.etf, i.rank) for i in issues] == [("TEST01", 1), ("TEST01", 2), ("TEST01", 3), ("TEST02", 1)]
    assert cut == 2


def test_a_filing_in_the_range_makes_a_member_an_issue_even_with_a_small_move():
    etf = etf_day(members=[member("TESTA", 1), member("TESTB", 2)])
    issues, _ = member_issues([etf], [close("TESTA", "0.10")], [filing("0000000001-26-000002", "TESTA"),
                                                                filing("0000000001-26-000001", "TESTA")])
    [issue] = issues
    assert issue.ticker == "TESTA" and issue.rate == "0.10"
    assert issue.filing_refs == ("0000000001-26-000001", "0000000001-26-000002")


def test_a_member_without_a_rate_or_filing_and_non_us_members_are_not_issues():
    members = [member("TESTA", 1), member("TEST01", 2, market="KR")]
    issues, _ = member_issues([etf_day(members=members)], [close("TEST01", "9.00")], [])
    assert issues == ()


def test_an_issue_without_a_rate_says_so_and_the_latest_close_wins_when_a_ticker_repeats():
    etf = etf_day(members=[member("TESTA", 1)])
    [no_rate], _ = member_issues([etf], [], [filing("0000000001-26-000001", "TESTA")])
    assert no_rate.rate is None
    old = close("TESTA", "9.00", datetime(2026, 10, 19, 5, 0, tzinfo=KST))
    new = close("TESTA", "1.00")
    assert member_issues([etf], [old, new], [])[0] == ()


def test_an_etf_without_info_has_no_member_issues():
    bare = EtfDay("TEST01", "20261020", "100", "99", "1.00", AS_OF, ("TESTA",), False)
    assert member_issues([bare], [close("TESTA", "9.00")], []) == ((), 0)


def test_thresholds_come_from_the_settings():
    etf = etf_day(members=[member("TESTA", 1)])
    rates = [close("TESTA", "4.00")]
    assert len(member_issues([etf], rates, [], DailySettings(member_move_percent=5.0))[0]) == 0
    assert len(member_issues([etf], rates, [], DailySettings(member_move_percent=4.0))[0]) == 1
    assert len(member_issues([etf_day(members=[member("TESTA", 1)])], rates, [],
                              DailySettings(member_rank_limit=0))[0]) == 0


# ---- 눈에 띄는 점 후보 ----

def kinds(etfs, articles=(), settings=S):
    # articles는 예전 호출 모양을 남긴 것으로, 후보 계산은 기사에 기대지 않는다.
    return [(c.kind, c.symbols) for c in notable_candidates(etfs, settings)]


def with_reason(*symbols):
    return [EtfArticles(s, (article(f"a-{s}"),), True) for s in symbols]


@pytest.mark.parametrize("a,b,expected", [("0.50", "-0.50", True), ("0.49", "-0.50", False),
                                          ("0.50", "-0.49", False), ("1.00", "2.00", False),
                                          ("-1.00", "-2.00", False)])
def test_opposite_moves_need_both_etfs_at_least_half_a_percent_in_opposite_directions(a, b, expected):
    etfs = [etf_day("TEST01", a), etf_day("TEST02", b)]
    assert (kinds(etfs, articles=with_reason("TEST01", "TEST02")) == [(KIND_OPPOSITE, ("TEST01", "TEST02"))]) is expected


def test_opposite_moves_need_the_same_sector():
    etfs = [etf_day("TEST01", "1.00", sector="가 업종"), etf_day("TEST02", "-1.00", sector="나 업종")]
    assert kinds(etfs, articles=with_reason("TEST01", "TEST02")) == []
    no_sector = [etf_day("TEST01", "1.00", sector=None), etf_day("TEST02", "-1.00", sector=None)]
    assert kinds(no_sector, articles=with_reason("TEST01", "TEST02")) == []


def test_a_cluster_needs_three_etfs_moving_two_percent_in_the_same_direction():
    arts = with_reason("TEST01", "TEST02", "TEST03", "TEST04")
    sectors = ("가", "나", "다", "라")
    up = [etf_day(f"TEST0{n}", "2.00", sector=sectors[n - 1]) for n in (1, 2, 3)]
    assert kinds(up, articles=arts) == [(KIND_CLUSTER, ("TEST01", "TEST02", "TEST03"))]
    down = [etf_day(f"TEST0{n}", "-2.00", sector=sectors[n - 1]) for n in (1, 2, 3)]
    assert kinds(down, articles=arts) == [(KIND_CLUSTER, ("TEST01", "TEST02", "TEST03"))]
    two_only = [etf_day("TEST01", "2.00", sector="가"), etf_day("TEST02", "2.50", sector="나")]
    assert kinds(two_only, articles=arts) == []
    short = [etf_day(f"TEST0{n}", "1.99", sector=sectors[n - 1]) for n in (1, 2, 3)]
    assert kinds(short, articles=arts) == []
    mixed = [etf_day("TEST01", "2.00", sector="가"), etf_day("TEST02", "2.00", sector="나"),
             etf_day("TEST03", "-2.00", sector="다")]
    assert kinds(mixed, articles=arts) == []


def test_one_etf_alone_is_never_a_candidate_even_with_a_member_issue_or_no_reason():
    # 구성 종목 이슈는 ETF 줄의 '↳'로, 이유 없음은 ETF 줄 이유로만 보인다.
    etf = etf_day("TEST01", "0.10", members=[member("TESTA", 1)], tickers=("TESTA",))
    assert member_issues([etf], [close("TESTA", "5.00")], [])[0] != ()
    assert kinds([etf]) == []


def test_no_reason_count_needs_a_daily_move_an_answered_request_and_no_article_filing_or_member_issue():
    etfs = [etf_day("TEST01", "0.10", tickers=("TESTA",)), etf_day("TEST02", "0.10", tickers=("TESTB",)),
            etf_day("TEST03", "0.10", tickers=("TESTC",)), etf_day("TEST04", "0.10", tickers=("TESTD",)),
            etf_day("TEST05", "0.10", tickers=("TESTE",), members=[member("TESTE", 1)])]
    articles = [EtfArticles("TEST01", (article("a1"),), True), EtfArticles("TEST02", (), True),
                EtfArticles("TEST03", (), True), EtfArticles("TEST04", (), True), EtfArticles("TEST05", (), True)]
    issues, _ = member_issues(etfs, [close("TESTE", "5.00")], [])
    assert no_reason_count(etfs, issues, articles, [filing("0000000001-26-000001", "TESTB")]) == 2


def test_no_reason_count_does_not_count_an_unanswered_or_dayless_etf():
    etfs = [etf_day("TEST01", "0.10"), etf_day("TEST02", "0.10"),
            EtfDay("TEST03", None, None, None, None, None, (), False, None, "no-daily-price-in-range")]
    articles = [EtfArticles("TEST01", (), False), EtfArticles("TEST02", (), True), EtfArticles("TEST03", (), True)]
    assert no_reason_count(etfs, (), articles, ()) == 1
    assert no_reason_count(etfs, (), (), ()) == 0


def test_an_etf_without_a_daily_price_is_never_a_candidate():
    etfs = [EtfDay("TEST01", None, None, None, None, None, (), False, None, "no-daily-price-in-range")]
    assert kinds(etfs) == []


def test_candidate_thresholds_come_from_the_settings():
    etfs = [etf_day("TEST01", "1.00"), etf_day("TEST02", "-1.00")]
    arts = ()
    assert kinds(etfs, articles=arts, settings=DailySettings(opposite_move_percent=1.5)) == []
    three = [etf_day(f"TEST0{n}", "1.00", sector=str(n)) for n in (1, 2)]
    assert kinds(three, articles=arts, settings=DailySettings(cluster_move_percent=1.0, cluster_min_size=2)) == \
        [(KIND_CLUSTER, ("TEST01", "TEST02"))]


# ---- Claude 입력 ----

def post_close_input(etfs, **kwargs):
    base = dict(job_id="j", edition="post-close", range_start=RANGE_START, as_of=AS_OF, etfs=tuple(etfs),
                etfs_truncated=False, close_rates=(), background=(), filings=(), filings_truncated=False)
    return DailyInput(**{**base, **kwargs})


def test_articles_filings_and_member_issues_are_in_the_claude_input_with_refs():
    etf = etf_day("TEST01", "-2.50", members=[member("TESTA", 1)], tickers=("TESTA",))
    f = filing("0000000001-26-000001", "TESTA")
    issues, _ = member_issues([etf], [close("TESTA", "-4.00")], [f])
    i = post_close_input([etf], filings=(f,), member_issues=issues,
                         etf_articles=(EtfArticles("TEST01", (article("art-1", "TEST 제목"),), True),),
                         notables=notable_candidates([etf]))
    llm = build_llm_input(i, build_sections(i))
    items = {item["name"]: item for item in llm["items"]}
    assert [e["ref"] for e in items[ITEM_ETF_ARTICLES]["entries"]] == ["art-1"]
    assert items[ITEM_ETF_ARTICLES]["entries"][0]["etf"] == ["TEST01"]
    assert [e["ref"] for e in items[ITEM_FILINGS]["entries"]] == ["0000000001-26-000001"]
    [issue] = items[ITEM_MEMBER_ISSUES]["entries"]
    assert issue["ref"] == "M1" and issue["etf"] == "TEST01" and issue["ticker"] == "TESTA"
    assert issue["rate"] == "-4.00" and issue["filingRefs"] == ["0000000001-26-000001"]
    # 한 ETF의 구성 종목 이슈는 후보가 아니다. 후보가 없는 날의 입력은 빈 목록이다.
    assert llm["notableCandidates"] == []


def test_the_morning_input_has_no_candidates_and_post_close_items_stay_out_of_the_body():
    etf = etf_day("TEST01", "-2.50", members=[member("TESTA", 1)])
    issues, _ = member_issues([etf], [close("TESTA", "-4.00")], [])
    i = post_close_input([etf], member_issues=issues,
                         etf_articles=(EtfArticles("TEST01", (article("art-1", "TEST 제목"),), True),))
    sections = build_sections(i)
    text = daily_text(i, sections, None)
    assert "TEST 제목" not in text and "TESTA" not in text and ITEM_MEMBER_ISSUES not in text
    morning = post_close_input([etf], edition="morning")
    assert "notableCandidates" not in build_llm_input(morning, build_sections(morning))


def test_every_etf_whose_articles_never_came_is_listed_as_missing_and_an_empty_answer_is_not():
    etfs = [etf_day("TEST01"), etf_day("TEST02")]

    def article_items(i):
        return [m for m in missing_items(build_sections(i)) if m.startswith(ITEM_ETF_ARTICLES)]

    i = post_close_input(etfs, etf_articles=(EtfArticles("TEST01", (), True), EtfArticles("TEST02", (), False)))
    assert article_items(i) == [f"{ITEM_ETF_ARTICLES}: TEST TEST02, 기사 응답을 받지 못함"]
    all_in = post_close_input(etfs, etf_articles=(EtfArticles("TEST01", (), True), EtfArticles("TEST02", (), True)))
    assert article_items(all_in) == []


def test_a_member_issue_from_a_filing_names_the_filing_even_when_it_also_has_a_rate():
    etf = etf_day("TEST01", "1.00", members=[member("TESTA", 1), member("TESTB", 2)])
    f = filing("0000000001-26-000001", "TESTA")
    issues, _ = member_issues([etf], [close("TESTA", "0.50"), close("TESTB", "-4.00")], [f])
    i = post_close_input([etf], filings=(f,), member_issues=issues)
    why = [e.describe for e in next(s for s in build_sections(i) if s.name == ITEM_MEMBER_ISSUES).entries]
    assert "직전 장 등락률 +0.50%" in why[0] and "범위 안 공시" in why[0]
    assert "직전 장 등락률 -4.00%" in why[1] and "범위 안 공시" not in why[1]


def test_the_same_article_from_several_etfs_enters_the_claude_input_once_with_all_its_etfs():
    etfs = [etf_day("TEST01"), etf_day("TEST02", sector="다른 업종")]
    shared, own = article("market-1", "TEST 시장 기사"), article("own-2", "TEST 개별 기사")
    i = post_close_input(etfs, etf_articles=(EtfArticles("TEST01", (shared, own), True),
                                             EtfArticles("TEST02", (shared,), True)))
    entries = next(s for s in build_sections(i) if s.name == ITEM_ETF_ARTICLES).entries
    assert [(e.ref, e.llm["etf"]) for e in entries] == [("market-1", ["TEST01", "TEST02"]), ("own-2", ["TEST01"])]
