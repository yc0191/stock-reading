"""마감 후 판이 ETF마다 기사를, 범위 안 공시를 받아 Claude 입력에 담는 흐름을 가짜 요청 출구·가짜 LLM으로 돌린다."""
import json
import logging
from datetime import timedelta

import pytest

from briefing.config import DailySettings
from briefing.daily_jobs import STAGE_ETF_ARTICLES, DailyCore
from briefing.daily_prompts import NO_REASON_STATEMENT
from briefing.daily_report import DailyLlmWriter
from briefing.job_log import EntryType
from briefing.ports import FailureReason
from briefing.queries import QueryKind, QueryResult
from contracts import validate_payload
from fake_news import answer_news, article as plain_article
from test_daily_jobs import (
    MORNING_AT,
    MORNING_JOB,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    etf,
    etf_v2,
    filings_response,
    post_close_output,
    published,
    tick,
    POST_CLOSE_BACKGROUND,
)

FETCHED = "2026-10-20T16:30:02+09:00"
IN_RANGE = "2026-10-20T10:00:00+09:00"
BEFORE_RANGE = "2026-10-19T16:59:59+09:00"
AFTER_SCHEDULE = "2026-10-20T17:00:01+09:00"
FILING = {"accessionNumber": "0000000001-26-000001", "tickers": ["TESTA"], "form": "8-K", "items": ["2.02"],
          "acceptedAt": "2026-10-20T05:30:28+09:00", "url": "https://example.test/8k.htm"}
OUT_OF_RANGE_FILING = {**FILING, "accessionNumber": "0000000001-26-000002",
                       "acceptedAt": "2026-10-19T16:59:00+09:00"}


def article(article_id, title, published_at):
    """세 ETF의 이름이 요약에 든 기사. 관계없는 기사로 걸러지지 않는다."""
    return {**plain_article(article_id, title, published_at), "summary": "TEST TEST01 TEST TEST02 TEST TEST03 관련 요약"}


def etf_with_members(symbol, rate, tickers, members, sector="TEST 업종"):
    item = etf_v2(symbol, f"TEST {symbol}", fetched=FETCHED, rate=rate, tickers=tickers)
    item["etf"] = {"name": f"TEST {symbol}", "sector": sector, "membersTruncated": False,
                   "members": [{"market": "US", "symbol": t, "rank": n, "name": f"TEST 종목 {t}", "weight": "5.0"}
                               for n, t in enumerate(members, start=1)]}
    return item


def three_etfs():
    return [etf_with_members("TEST01", "-3.50", ("TESTA", "FAKEB"), ("TESTA", "FAKEB")),
            etf_with_members("TEST02", "0.20", ("TESTC",), ("TESTC",), sector="TEST 다른 업종"),
            etf_with_members("TEST03", "1.00", (), (), sector="TEST 또 다른 업종")]


def close_rates():
    return [{"ticker": t, "date": "20261019", "closeAt": "2026-10-20T05:00:00+09:00", "rate": r,
             "fetchedAt": "2026-10-20T08:30:02+09:00"} for t, r in (("TESTA", "-3.50"), ("FAKEB", "1.00"),
                                                                    ("TESTC", "0.20"))]


@pytest.fixture
def start():
    def build(settings=DailySettings(), llm=None, etfs=None):
        requests, details = Requests(), Details()
        llm = llm or FakeLlm(post_close_output())
        core = DailyCore(requests, requests.api, DailyLlmWriter(llm, settings.llm_max_attempts, settings), details, settings)
        core.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
        core.api.reply(daily_response(etfs=three_etfs() if etfs is None else etfs,
                                                    close_rates=close_rates(), background=POST_CLOSE_BACKGROUND),
                                     POST_CLOSE_AT)
        return core, requests, details, llm
    return build


def articles_requested(requests):
    return [q.payload for q in requests.api.of(QueryKind.ARTICLES)]


def llm_input(llm):
    return json.loads(llm.calls[0][1])


def item(llm, name):
    return next(i for i in llm_input(llm)["items"] if i["name"] == name)


# ---- 요청 ----

def test_one_articles_request_goes_out_per_etf_with_five_articles_and_the_edition_range(start):
    _, requests, _, _ = start()
    payloads = articles_requested(requests)
    assert [p["symbol"] for p in payloads] == ["TEST01", "TEST02", "TEST03"]
    assert {p["maxArticles"] for p in payloads} == {5}
    assert {(p["since"], p["until"]) for p in payloads} == {("2026-10-19T17:00:00+09:00", "2026-10-20T17:00:00+09:00")}
    # 같은 작업의 조회는 번호가 겹치지 않아 뉴스처리기 로그에서 서로 구별된다.
    assert len({p["seq"] for p in payloads}) == 3
    assert all("researchJobId" not in p for p in payloads)
    for query in requests.api.of(QueryKind.ARTICLES):
        assert validate_payload("news.articles.requested.v1.schema.json", query.payload) == []
        assert query.stage == STAGE_ETF_ARTICLES


def test_the_article_count_comes_from_the_settings(start):
    _, requests, _, _ = start(DailySettings(etf_max_articles=2))
    assert {p["maxArticles"] for p in articles_requested(requests)} == {2}


def test_the_edition_waits_for_every_answer_then_goes_out_once(start):
    c, requests, _, _ = start()
    assert published(requests) == []
    answer_news(c, requests, POST_CLOSE_AT)
    assert len(published(requests)) == 1
    answer_news(c, requests, POST_CLOSE_AT)
    assert len(published(requests)) == 1


def test_the_morning_edition_also_requests_articles_per_etf_and_waits_for_them():
    requests = Requests()
    c = DailyCore(requests, requests.api, None, Details())
    c.on_collection_ready(tick(MORNING_JOB, MORNING_AT), MORNING_AT)
    c.api.reply(daily_response(etfs=[etf("TEST01"), etf("TEST02")]), MORNING_AT)
    c.api.reply(filings_response(), MORNING_AT)
    assert [p["symbol"] for p in articles_requested(requests)] == ["TEST01", "TEST02"]
    assert published(requests) == []
    answer_news(c, requests, MORNING_AT)
    assert len(published(requests)) == 1


# ---- 범위와 건수 ----

def test_articles_enter_the_claude_input_per_etf_with_their_refs_and_out_of_range_ones_are_dropped(start, caplog):
    c, requests, _, llm = start()
    with caplog.at_level(logging.INFO, logger="briefing.daily_jobs"):
        answer_news(c, requests, POST_CLOSE_AT, articles={
            "TEST01": [article("a-1", "TEST 안 기사", IN_RANGE), article("a-2", "TEST 앞 기사", BEFORE_RANGE),
                       article("a-3", "TEST 뒤 기사", AFTER_SCHEDULE)],
            "TEST02": [article("a-1", "TEST 안 기사", IN_RANGE)]})
    entries = item(llm, "ETF별 기사")["entries"]
    assert [(e["etf"], e["ref"]) for e in entries] == [(["TEST01", "TEST02"], "a-1")]
    assert entries[0]["title"] == "TEST 안 기사" and "minutesBeforeAsOf" in entries[0]
    assert "TEST 앞 기사" not in llm.calls[0][1] and "TEST 뒤 기사" not in llm.calls[0][1]
    assert "dropped 2 articles outside the range" in caplog.text
    assert "TEST01" not in caplog.text and "TESTA" not in caplog.text


def test_articles_exactly_at_both_ends_of_the_range_are_kept(start):
    c, requests, _, llm = start()
    answer_news(c, requests, POST_CLOSE_AT, articles={"TEST01": [
        article("a-1", "TEST 시작 기사", "2026-10-19T17:00:00+09:00"),
        article("a-2", "TEST 끝 기사", "2026-10-20T17:00:00+09:00")]})
    assert {e["ref"] for e in item(llm, "ETF별 기사")["entries"]} == {"a-1", "a-2"}


def test_only_the_newest_five_articles_per_etf_are_kept_even_if_the_answer_has_more(start, caplog):
    c, requests, _, llm = start()
    # 제목이 서로 다른 사건이라 같은 사건으로 묶이지 않고 기사마다 한 칸씩 들어간다.
    many = [article(f"a-{n}", f"TEST{n} 기사", f"2026-10-20T{9 + n:02d}:00:00+09:00") for n in range(1, 8)]
    with caplog.at_level(logging.INFO, logger="briefing.daily_jobs"):
        answer_news(c, requests, POST_CLOSE_AT, articles={"TEST01": many})
    assert [e["ref"] for e in item(llm, "ETF별 기사")["entries"]] == ["a-7", "a-6", "a-5", "a-4", "a-3"]
    assert "articles=5" in caplog.text and "articlesCut=2" in caplog.text and "articlesTruncated=1" in caplog.text


def test_in_range_filings_enter_the_claude_input_and_out_of_range_ones_are_dropped(start):
    c, requests, _, llm = start()
    answer_news(c, requests, POST_CLOSE_AT, filings=[FILING, OUT_OF_RANGE_FILING])
    refs = [e["ref"] for e in item(llm, "범위 안 미국 공시")["entries"]]
    assert refs == ["0000000001-26-000001"]
    assert "0000000001-26-000002" not in llm.calls[0][1]


# ---- 응답이 없을 때 ----

def test_an_etf_whose_articles_finally_fail_goes_on_without_articles(start):
    c, requests, details, llm = start()
    answer_news(c, requests, POST_CLOSE_AT, articles={"TEST01": [article("a-1", "TEST 안 기사", IN_RANGE)]},
                silent=("TEST02",))
    # TEST02만 답이 없다. 다시 부르는 것은 조회 구현이 하므로 판은 그 결과를 기다린다.
    assert published(requests) == []
    c.api.fail(QueryKind.ARTICLES, POST_CLOSE_AT + timedelta(minutes=1), symbol="TEST02", attempts=4)
    assert len([p for p in articles_requested(requests) if p["symbol"] == "TEST02"]) == 1
    [event] = published(requests)
    # 답한 ETF의 기사는 그대로 있고, 못 받은 ETF는 자료 없음 목록에 오른다.
    assert [e["ref"] for e in item(llm, "ETF별 기사")["entries"]] == ["a-1"]
    assert event.value["payload"]["missingItems"] == ["ETF별 기사: TEST TEST02, 기사 응답을 받지 못함"]
    [failure] = [d for d in details.entries if d.entry_type == EntryType.STEP_FAILED]
    assert failure.stage == STAGE_ETF_ARTICLES and failure.reason == FailureReason.NEWS_NO_RESPONSE.value
    assert failure.attempts == 4
    assert [d for d in details.entries if d.entry_type == EntryType.RESENT] == []


def test_a_late_result_after_the_edition_went_out_is_ignored(start):
    c, requests, _, llm = start()
    answer_news(c, requests, POST_CLOSE_AT, silent=("TEST02",))
    [(query, on_result)] = c.api.pending
    c.api.fail(QueryKind.ARTICLES, POST_CLOSE_AT + timedelta(minutes=1))
    assert len(published(requests)) == 1
    late = {"jobId": query.payload["jobId"], "seq": query.payload["seq"], "symbol": "TEST02",
            "articles": [article("a-1", "TEST 늦은 기사", IN_RANGE)], "truncated": False}
    on_result(QueryResult(query, late, 1), POST_CLOSE_AT + timedelta(minutes=2))
    assert len(published(requests)) == 1
    assert "TEST 늦은 기사" not in llm.calls[0][1]


def test_a_response_for_another_job_is_ignored_without_an_error(start):
    c, requests, _, _ = start()
    other = json.dumps({"eventId": "00000000-0000-4000-8000-000000000001", "eventType": "news.articles.responded",
                        "schemaVersion": 1, "occurredAt": FETCHED, "producedAt": FETCHED, "correlationId": "c",
                        "payload": {"jobId": "00000000-0000-4000-8000-0000000000ff", "seq": 1, "symbol": "TEST01",
                                    "articles": [], "truncated": False}})
    assert c.api.reply(other, POST_CLOSE_AT) is False
    assert published(requests) == []


def test_a_malformed_articles_response_counts_as_not_received(start):
    c, requests, details, _ = start()
    p = articles_requested(requests)[0]
    c.api.reply({"jobId": p["jobId"], "seq": p["seq"], "symbol": p["symbol"], "truncated": False}, POST_CLOSE_AT,
                kind=QueryKind.ARTICLES)
    [failure] = [d for d in details.entries if d.entry_type == EntryType.STEP_FAILED]
    assert (failure.stage, failure.error_type) == (STAGE_ETF_ARTICLES, "MalformedResponse")


# ---- 구성 종목 이슈와 눈에 띄는 점 후보 ----

def test_member_issues_and_candidates_reach_the_claude_input_without_tickers_in_the_body(start, caplog):
    # 결론 줄의 이유를 찾지 못한 ETF 수는 Claude가 그렇게 쓴 ETF 수다.
    llm = FakeLlm(post_close_output(reasons={"TEST03": NO_REASON_STATEMENT}, refs={"TEST03": []}, weak=("TEST03",)))
    c, requests, _, llm = start(llm=llm)
    with caplog.at_level(logging.INFO, logger="briefing.daily_jobs"):
        answer_news(c, requests, POST_CLOSE_AT, filings=[FILING],
                    articles={"TEST02": [article("a-2", "TEST 기사", IN_RANGE)]})
    [issue] = item(llm, "미국 구성 종목 이슈")["entries"]
    assert issue["etf"] == "TEST01" and issue["ticker"] == "TESTA" and issue["rate"] == "-3.50"
    assert issue["filingRefs"] == ["0000000001-26-000001"] and issue["ref"].startswith("M")
    candidates = llm_input(llm)["notableCandidates"]
    # 한 ETF의 구성 종목 이슈와 이유 없음은 후보가 아니라 ETF 줄과 결론 줄로 보인다.
    assert candidates == []
    [event] = published(requests)
    body = event.value["payload"]["text"] + event.value["payload"]["html"]
    assert "TESTA" not in body and "FAKEB" not in body and "TESTC" not in body
    assert "이유를 찾지 못한 ETF 1개" in body and "눈에 띄는 점" not in body
    assert "memberIssues=1" in caplog.text and "notables=0" in caplog.text and "filings=1" in caplog.text
    assert "TESTA" not in caplog.text and "TEST01" not in caplog.text


def test_the_member_issue_ref_is_accepted_and_a_ref_outside_the_list_is_still_rejected(start):
    llm = FakeLlm(post_close_output(refs={"TEST01": ["M1"]}))
    c, requests, _, _ = start(llm=llm)
    answer_news(c, requests, POST_CLOSE_AT, filings=[FILING])
    assert published(requests)[0].value["payload"]["generatedBy"] == "llm"

    wrong = post_close_output(refs={"TEST01": ["M9"]})
    bad = FakeLlm(wrong, wrong, wrong)
    c, requests, _, _ = start(llm=bad)
    answer_news(c, requests, POST_CLOSE_AT, filings=[FILING])
    assert len(bad.calls) == 3
    assert published(requests)[0].value["payload"]["generatedBy"] == "rule-based"


def test_the_thresholds_for_the_inputs_come_from_the_settings(start):
    c, requests, _, llm = start(DailySettings(member_move_percent=4.0))
    answer_news(c, requests, POST_CLOSE_AT)
    assert item(llm, "미국 구성 종목 이슈")["entries"] == []
