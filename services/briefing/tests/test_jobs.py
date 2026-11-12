import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings
from briefing.job_log import EntryType, JobKind, RunStatus
from briefing.jobs import BriefingCore
from briefing.ports import FailureReason
from contracts import sample, validate
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
S = CoreSettings()
DELTA = S.delta
SEC = timedelta(seconds=1)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"


class Recorder:
    def __init__(self):
        self.sent, self.readied = [], []
        self.log = FakeJobLog()

    @property
    def failed(self):
        """끝내 실패한 작업의 줄. 줄마다 사유와 대상(종목)이 담겨 있다."""
        return self.log.failed

    def send(self, event):
        self.sent.append(event)

    def on_ready(self, report_input):
        self.readied.append(report_input)

    def of(self, topic):
        return [e for e in self.sent if e.topic == topic]


@pytest.fixture
def rec():
    return Recorder()


@pytest.fixture
def core(rec):
    return BriefingCore(rec, rec, job_log=rec.log)


def move(event_id=JOB_ID, symbol="TEST01", as_of=T, legacy=False):
    event = sample("market.move.sample.json")
    event["eventId"] = event_id
    event["payload"]["symbol"] = symbol
    event["payload"]["asOf"] = as_of.isoformat()
    if legacy:
        del event["payload"]["detectionType"], event["payload"]["moveStartedAt"]
    return json.dumps(event)


def completed(started_at=T + DELTA, incomplete=(), job_type="collect-news"):
    event = sample("collection.completed.sample.json")
    event["payload"].update(jobType=job_type, startedAt=started_at.isoformat(), incompleteSymbols=list(incomplete))
    return json.dumps(event)


def history_response(seq, job_id=JOB_ID, points=()):
    return json.dumps({"eventType": "market.history.responded", "payload": {
        "jobId": job_id, "seq": seq, "symbol": "TEST01", "truncated": False,
        "points": [{"asOf": T.isoformat(), "price": p} for p in points]}})


def article(article_id, published_at=None):
    return {"articleId": article_id, "title": f"t {article_id}", "summary": "", "publisher": "p",
            "publishedAt": (published_at or T - timedelta(hours=1)).isoformat(), "url": "https://example.com/a"}


def news_response(seq, ids=(), job_id=JOB_ID, articles=None):
    return json.dumps({"eventType": "news.articles.responded", "payload": {
        "jobId": job_id, "seq": seq, "symbol": "TEST01", "truncated": False,
        "articles": articles if articles is not None else [article(i) for i in ids]}})


def context_response(seq=1, job_id=JOB_ID, tickers=(), close_rates=(), background=None, truncated=False):
    return json.dumps({"eventType": "market.context.responded", "payload": {
        "jobId": job_id, "seq": seq, "symbol": "TEST01", "tickers": list(tickers), "truncated": truncated,
        "closeRates": list(close_rates),
        "background": background if background is not None else [
            {"apiType": "exchange-rate-summary", "target": None, "status": "missing"}]}})


def start(core, now=T):
    core.on_move(move(), now)
    core.on_collection_completed(completed(), now)


def test_requests_match_the_contract_and_cover_the_required_ranges(core, rec):
    start(core)

    (history,) = rec.of("market.history.requested")
    (news,) = rec.of("news.articles.requested")
    assert validate("market.history.requested.v1.schema.json", history.value) == []
    assert validate("news.articles.requested.v1.schema.json", news.value) == []
    assert history.key == news.key == "TEST01"
    h, n = history.value["payload"], news.value["payload"]
    assert (h["jobId"], h["seq"], h["maxPoints"], h["intervalSeconds"]) == (JOB_ID, 1, 61, 10)
    assert datetime.fromisoformat(h["since"]) == T - timedelta(minutes=5)
    assert datetime.fromisoformat(h["until"]) == T + DELTA
    assert (n["seq"], n["maxArticles"]) == (1, 50)
    assert datetime.fromisoformat(n["since"]) == T - timedelta(hours=24)
    assert datetime.fromisoformat(n["until"]) == T + DELTA


def test_nothing_is_requested_until_a_usable_completion_arrives(core, rec):
    core.on_move(move(), T)

    assert rec.sent == []


def test_completion_that_arrived_before_the_move_is_used(core, rec):
    core.on_collection_completed(completed(), T)
    core.on_move(move(), T + SEC)

    assert len(rec.sent) == 3


def test_only_the_ten_most_recent_completions_are_remembered(core, rec):
    core.on_collection_completed(completed(), T)
    for _ in range(10):
        core.on_collection_completed(completed(started_at=T, incomplete=["TEST01"]), T)
    core.on_move(move(), T)

    assert rec.sent == []


def test_completion_started_inside_the_move_window_is_rejected_then_next_one_is_used(core, rec):
    core.on_move(move(), T)
    core.on_collection_completed(completed(started_at=T + DELTA - SEC), T)
    assert rec.sent == []

    core.on_collection_completed(completed(started_at=T + DELTA), T)
    assert len(rec.sent) == 3


@pytest.mark.parametrize("incomplete", [["TEST01"], ["*"]])
def test_completion_with_the_symbol_or_market_wide_incomplete_is_rejected(core, rec, incomplete):
    core.on_move(move(), T)
    core.on_collection_completed(completed(incomplete=incomplete), T)

    assert rec.sent == []


def test_completion_missing_only_other_symbols_is_accepted(core, rec):
    core.on_move(move(), T)
    core.on_collection_completed(completed(incomplete=["TEST02"]), T)

    assert len(rec.sent) == 3


def test_completion_of_another_job_type_is_rejected(core, rec):
    core.on_move(move(), T)
    core.on_collection_completed(completed(job_type="collect-other"), T)

    assert rec.sent == []


def test_legacy_move_without_detection_type_uses_as_of_the_same_way(core, rec):
    core.on_move(move(legacy=True), T)
    core.on_collection_completed(completed(), T)

    assert len(rec.sent) == 3


def test_same_event_id_twice_is_processed_once(core, rec):
    core.on_move(move(), T)
    core.on_move(move(), T + SEC)
    core.on_collection_completed(completed(), T)

    assert len(rec.of("market.history.requested")) == 1


def test_same_symbol_different_events_make_separate_jobs(core, rec):
    core.on_move(move(), T)
    core.on_move(move(event_id="6d3f9b52-8e40-4c7b-b2da-1f503c9e7b24"), T)
    core.on_collection_completed(completed(), T)

    assert len(rec.of("market.history.requested")) == 2


@pytest.mark.parametrize("message", ["not json", "{}", b"\xff", '{"eventType": "market.snapshot", "payload": {}}'])
def test_malformed_move_raises_value_error(core, message):
    with pytest.raises(ValueError):
        core.on_move(message, T)


def test_stable_news_and_market_response_complete_the_report_input(core, rec):
    start(core)
    core.on_context_responded(context_response(), T + SEC)
    core.on_history_responded(history_response(1, points=[100, 101]), T + SEC)
    assert rec.readied == []  # 시세와 배경 자료만으로는 진행하지 않는다
    core.on_articles_responded(news_response(1, ["a", "b"]), T + SEC)
    assert rec.readied == []  # 기사는 한 라운드로는 안정이 아니다

    core.on_tick(T + SEC + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["b", "a"]), T + 12 * SEC)  # 순서 무시

    (ready,) = rec.readied
    assert ready.job_id == JOB_ID and ready.symbol == "TEST01" and ready.as_of == T
    assert [p.price for p in ready.price_points] == [100, 101]
    assert sorted(a.article_id for a in ready.articles) == ["a", "b"]
    assert rec.failed == []


def test_news_is_requested_again_only_after_the_recheck_interval_with_a_new_seq(core, rec):
    start(core)
    core.on_articles_responded(news_response(1, ["a"]), T)

    core.on_tick(T + S.recheck_interval - SEC)
    assert len(rec.of("news.articles.requested")) == 1
    core.on_tick(T + S.recheck_interval)
    assert [e.value["payload"]["seq"] for e in rec.of("news.articles.requested")] == [1, 2]


def test_zero_article_responses_count_as_received_and_can_be_stable(core, rec):
    start(core)
    core.on_context_responded(context_response(), T)
    core.on_history_responded(history_response(1), T)
    core.on_articles_responded(news_response(1, []), T)
    core.on_tick(T + S.recheck_interval)
    core.on_articles_responded(news_response(2, []), T + S.recheck_interval)

    (ready,) = rec.readied
    assert ready.articles == () and ready.price_points == ()
    assert rec.failed == []


def test_late_response_with_an_old_seq_is_ignored_and_logged(core, rec, caplog):
    start(core)
    core.on_context_responded(context_response(), T)
    core.on_tick(T + S.response_timeout)  # 기사·시세 모두 seq 2로 재요청
    with caplog.at_level("INFO", logger="briefing.jobs"):
        core.on_history_responded(history_response(1, points=[1]), T + 11 * SEC)
        core.on_articles_responded(news_response(1, ["a"]), T + 11 * SEC)

    assert "stale" in caplog.text
    core.on_history_responded(history_response(2, points=[2]), T + 12 * SEC)
    core.on_articles_responded(news_response(2, ["a"]), T + 12 * SEC)
    core.on_tick(T + 22 * SEC)
    core.on_articles_responded(news_response(3, ["a"]), T + 22 * SEC)
    assert [p.price for p in rec.readied[0].price_points] == [2]


def test_stale_response_does_not_overwrite_a_newer_round(core, rec):
    start(core)
    core.on_articles_responded(news_response(1, ["a"]), T)
    core.on_tick(T + S.recheck_interval)
    core.on_articles_responded(news_response(1, ["a"]), T + S.recheck_interval)  # 옛 seq 재도착

    assert rec.readied == []
    assert rec.failed == []


def test_unanswered_requests_are_retried_with_new_seq_then_fail_after_three_attempts(core, rec):
    start(core)
    for step in (1, 2, 3):
        core.on_tick(T + step * S.response_timeout)

    seqs = [e.value["payload"]["seq"] for e in rec.of("market.history.requested")]
    assert seqs == [1, 2, 3]
    reasons = {f.reason: f for f in rec.failed}
    assert set(reasons) == {FailureReason.MARKET_NO_RESPONSE}
    assert reasons[FailureReason.MARKET_NO_RESPONSE].attempts == 3
    assert len(rec.of("market.history.requested")) == 3  # 실패 뒤 더 보내지 않는다


def test_news_no_response_after_three_attempts(core, rec):
    start(core)
    core.on_history_responded(history_response(1), T)
    for step in (1, 2, 3):
        core.on_tick(T + step * S.response_timeout)

    (failure,) = rec.failed
    assert (failure.reason, failure.attempts, failure.job_id) == (FailureReason.NEWS_NO_RESPONSE, 3, JOB_ID)
    assert rec.log.starts[0].target["symbol"] == "TEST01"
    assert [e.value["payload"]["seq"] for e in rec.of("news.articles.requested")] == [1, 2, 3]
    assert rec.readied == []


def test_news_that_never_stabilizes_fails_after_five_rounds(core, rec):
    start(core)
    core.on_context_responded(context_response(), T)
    core.on_history_responded(history_response(1), T)
    now = T
    for round_number in range(1, 6):
        core.on_articles_responded(news_response(round_number, [f"r{round_number}"]), now)
        if round_number < 5:
            now += S.recheck_interval
            core.on_tick(now)

    (failure,) = rec.failed
    assert failure.reason == FailureReason.NEWS_UNSTABLE
    assert len(rec.of("news.articles.requested")) == 5
    assert rec.readied == []


def test_news_stabilizing_on_the_fifth_round_still_succeeds(core, rec):
    start(core)
    core.on_context_responded(context_response(), T)
    core.on_history_responded(history_response(1), T)
    now = T
    for round_number, ids in enumerate([["a"], ["b"], ["c"], ["d"], ["d"]], start=1):
        core.on_articles_responded(news_response(round_number, ids), now)
        now += S.recheck_interval
        core.on_tick(now)

    assert len(rec.readied) == 1 and rec.failed == []


def test_collection_not_covered_after_the_wait_limit(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.collection_wait_limit - SEC)
    assert rec.failed == []

    core.on_tick(T + S.collection_wait_limit)

    (failure,) = rec.failed
    assert failure.reason == FailureReason.COLLECTION_NOT_COVERED
    core.on_collection_completed(completed(), T + S.collection_wait_limit + SEC)
    # 실패한 작업은 되살아나지 않는다. 나간 것은 T+5분에 ETF 이름을 알려고 보낸 배경 자료 요청뿐이다.
    assert [e.topic for e in rec.sent] == ["market.context.requested"]


def test_a_failed_job_leaves_one_failure_line_and_one_result_line_under_the_same_job_id(core, rec):
    start(core)
    for step in (1, 2, 3):
        core.on_tick(T + step * S.response_timeout)

    (failure,) = rec.log.failures()
    (result,) = rec.log.results()
    assert failure.job_id == result.job_id == JOB_ID
    # 작업 한 줄(job_run)은 시작 때 move로 만들고 끝날 때 실패 사유와 함께 고친다.
    (run,) = rec.log.starts
    assert run.job_kind == JobKind.MOVE and run.job_id == JOB_ID and run.target["symbol"] == "TEST01"
    assert result.status == RunStatus.FAILED and result.failure_reason == FailureReason.MARKET_NO_RESPONSE.value
    assert len([e for e in rec.log.of_type(EntryType.RESENT) if e.stage == "market.history"]) == 2  # 재요청 두 번


def test_failure_reasons_are_exactly_the_seven_defined(core):
    assert {r.value for r in FailureReason} == {
        "collection_not_covered", "market_no_response", "news_no_response", "news_unstable", "llm_error",
        "context_no_response", "filings_no_response"}


def test_articles_outside_the_range_are_dropped_from_the_report_input(core, rec, caplog):
    start(core)
    core.on_context_responded(context_response(), T)
    core.on_history_responded(history_response(1), T)
    mixed = [article("ok"), article("old", T - timedelta(hours=25)), article("late", T + DELTA + SEC),
             article("edge", T + DELTA)]
    core.on_articles_responded(news_response(1, articles=mixed), T)
    core.on_tick(T + S.recheck_interval)
    with caplog.at_level("WARNING", logger="briefing.jobs"):
        core.on_articles_responded(news_response(2, articles=mixed), T + S.recheck_interval)

    assert sorted(a.article_id for a in rec.readied[0].articles) == ["edge", "ok"]
    assert "old" in caplog.text and "late" in caplog.text


def test_next_deadline_reports_the_earliest_timer(core):
    assert core.next_deadline() is None
    core.on_move(move(), T)
    assert core.next_deadline() == T + S.early_research_delay  # 정기 수집을 기다리는 동안 T+5분에 먼저 재검색한다
    core.on_tick(T + S.early_research_delay)
    assert core.next_deadline() == T + S.early_research_delay + S.response_timeout
    core.on_tick(T + S.early_research_delay + S.response_timeout)  # 배경 자료가 안 와서 포기
    assert core.next_deadline() == T + S.collection_wait_limit
    core.on_collection_completed(completed(), T)
    assert core.next_deadline() == T + S.response_timeout


def test_core_does_not_import_kafka():
    import briefing.config, briefing.events, briefing.jobs, briefing.llm, briefing.ports, briefing.sources  # noqa: E401

    for module in (briefing.config, briefing.events, briefing.jobs, briefing.llm, briefing.ports, briefing.sources):
        source = open(module.__file__, encoding="utf-8").read()
        assert "confluent_kafka" not in source


def test_default_history_window_fits_in_the_point_cap_so_it_is_not_always_truncated():
    # 양끝을 포함한 구간이라 칸 수는 (구간 길이 / 간격) + 1이다. 상한이 이보다 작으면 마지막 칸이 늘 잘린다.
    s = CoreSettings()
    buckets = int((s.market_lookback + s.delta) / s.history_interval) + 1
    assert buckets <= s.max_history_points


# ---- 변동 T+5분 재검색(정기 수집을 기다리지 않고 진행) ----

RESEARCH_ID = "00000000-0000-4000-8000-0000000000aa"
ETF = {"name": "가짜 반도체 ETF", "membersTruncated": False, "members": [
    {"market": "US", "symbol": "TESTA", "rank": 2, "name": "둘째회사"},
    {"market": "KR", "symbol": "TESTB", "rank": 1, "name": "첫째회사"},
    {"market": "KR", "symbol": "TESTC", "rank": 3, "name": "셋째회사"}]}


def etf_context(seq=1, etf=ETF, job_id=JOB_ID):
    event = json.loads(context_response(seq, job_id))
    if etf is not None:
        event["payload"]["etf"] = etf
    return json.dumps(event)


def research_completed(job_id=JOB_ID, incomplete=(), started_at=T + DELTA, rid=None):
    from briefing.jobs import early_research_id
    event = sample("collection.completed.sample.json")
    event["payload"].update(jobType="research-news", startedAt=started_at.isoformat(),
                            incompleteSymbols=list(incomplete), researchJobId=rid or early_research_id(job_id))
    return json.dumps(event)


def early_started(core, rec, etf=ETF):
    """T+5분에 배경 자료를 먼저 요청하고 그 응답으로 재검색 요청까지 보낸다."""
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    core.on_context_responded(etf_context(1, etf), T + S.early_research_delay + SEC)


def test_the_context_name_reaches_the_report_input_only_when_present(core, rec):
    start(core)
    event = json.loads(context_response())
    event["payload"]["name"] = " TEST 주식 "
    core.on_context_responded(json.dumps(event), T + SEC)
    core.on_history_responded(history_response(1, points=[100, 101]), T + SEC)
    core.on_articles_responded(news_response(1, ["a"]), T + SEC)
    core.on_tick(T + SEC + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["a"]), T + 12 * SEC)

    (ready,) = rec.readied
    assert ready.name == "TEST 주식"


def test_a_response_without_a_name_leaves_the_report_input_name_empty(core, rec):
    start(core)
    core.on_context_responded(context_response(), T + SEC)
    core.on_history_responded(history_response(1, points=[100, 101]), T + SEC)
    core.on_articles_responded(news_response(1, ["a"]), T + SEC)
    core.on_tick(T + SEC + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["a"]), T + 12 * SEC)

    (ready,) = rec.readied
    assert ready.name is None


@pytest.mark.parametrize("bad_name", [5, "", "   ", ["x"]])
def test_a_context_name_that_is_not_usable_text_is_treated_as_no_name(core, rec, bad_name):
    start(core)
    event = json.loads(context_response())
    event["payload"]["name"] = bad_name
    core.on_context_responded(json.dumps(event), T + SEC)
    core.on_history_responded(history_response(1, points=[100, 101]), T + SEC)
    core.on_articles_responded(news_response(1, ["a"]), T + SEC)
    core.on_tick(T + SEC + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["a"]), T + 12 * SEC)

    (ready,) = rec.readied
    assert ready.name is None


def test_a_move_asks_for_the_context_at_t_plus_five_minutes_and_nothing_before(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay - SEC)
    assert rec.sent == []

    core.on_tick(T + S.early_research_delay)

    (context,) = rec.of("market.context.requested")
    assert context.value["payload"]["seq"] == 1 and len(rec.sent) == 1


def test_a_move_noticed_after_t_plus_five_minutes_asks_at_once(core, rec):
    core.on_move(move(), T + S.early_research_delay + SEC)

    assert len(rec.of("market.context.requested")) == 1


def test_research_is_requested_with_the_etf_name_and_top_member_names_by_rank(core, rec):
    early_started(core, rec)

    (request,) = rec.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    payload = request.value["payload"]
    # 비중 순위(rank)가 높은 종목 순서이고, 검색어 상한(3개)을 넘기지 않는다.
    assert payload["queries"] == ["가짜 반도체 ETF", "첫째회사", "둘째회사"]
    assert (payload["jobKind"], payload["symbol"], payload["seq"]) == ("move", "TEST01", 1)
    (entry,) = [e for e in rec.log.entries if e.entry_type == EntryType.RESEARCH_REQUESTED]
    assert entry.data["queries"] == payload["queries"] and entry.data["researchJobId"] == payload["jobId"]


@pytest.mark.parametrize("etf", [None, {**ETF, "name": "  "}, {**{k: v for k, v in ETF.items() if k != "name"}}])
def test_without_an_etf_name_no_research_is_requested_and_the_regular_collection_is_awaited(core, rec, etf):
    early_started(core, rec, etf)

    assert rec.of("news.research.requested") == []
    core.on_collection_completed(completed(), T + S.early_research_delay + 2 * SEC)
    # 이미 받은 배경 자료는 다시 받지 않는다.
    assert len(rec.of("market.context.requested")) == 1
    assert len(rec.of("market.history.requested")) == 1 and len(rec.of("news.articles.requested")) == 1


def test_a_stock_move_researches_the_stock_name_alone_at_t_plus_five_minutes(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    event = json.loads(context_response(1))
    event["payload"]["name"] = " TEST 주식 "
    core.on_context_responded(json.dumps(event), T + S.early_research_delay + SEC)

    (request,) = rec.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    assert request.value["payload"]["queries"] == ["TEST 주식"]


def test_a_stock_whose_name_is_oversized_is_not_researched(core, rec):
    from briefing.research import MAX_QUERY_LENGTH
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    event = json.loads(context_response(1))
    event["payload"]["name"] = "가" * (MAX_QUERY_LENGTH + 1)
    core.on_context_responded(json.dumps(event), T + S.early_research_delay + SEC)

    assert rec.of("news.research.requested") == []


def test_an_etf_move_ignores_the_context_name_for_research_queries(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    event = json.loads(etf_context(1))
    event["payload"]["name"] = "다른 이름"
    core.on_context_responded(json.dumps(event), T + S.early_research_delay + SEC)

    (request,) = rec.of("news.research.requested")
    assert request.value["payload"]["queries"] == ["가짜 반도체 ETF", "첫째회사", "둘째회사"]


def test_early_research_completion_starts_the_evidence_fetch_without_the_regular_collection(core, rec):
    early_started(core, rec)
    done_at = T + S.early_research_delay + 20 * SEC

    assert core.on_research_completed(research_completed(), done_at) is True

    assert len(rec.of("market.history.requested")) == 1 and len(rec.of("news.articles.requested")) == 1
    # 배경 자료는 이미 받았으므로 다시 요청하지 않고 곧바로 진행한다.
    assert len(rec.of("market.context.requested")) == 1
    core.on_history_responded(history_response(1, points=[100]), done_at)
    core.on_articles_responded(news_response(1, ["a"]), done_at)
    core.on_tick(done_at + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["a"]), done_at + S.recheck_interval)
    (ready,) = rec.readied
    assert [a.article_id for a in ready.articles] == ["a"]
    assert rec.failed == []


def test_early_research_completion_of_a_us_etf_still_requests_filings(core, rec):
    event = json.loads(etf_context(1))
    event["payload"]["tickers"] = ["TESTA"]
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    core.on_context_responded(json.dumps(event), T + S.early_research_delay + SEC)
    core.on_research_completed(research_completed(), T + S.early_research_delay + 2 * SEC)

    assert [e.value["payload"]["tickers"] for e in rec.of("news.filings.requested")] == [["TESTA"]]


def test_whichever_completion_comes_first_wins_and_the_later_one_changes_nothing(core, rec):
    early_started(core, rec)
    core.on_collection_completed(completed(started_at=T + S.early_research_delay + 5 * SEC),
                                 T + S.early_research_delay + 6 * SEC)
    assert len(rec.of("market.history.requested")) == 1

    assert core.on_research_completed(research_completed(), T + S.early_research_delay + 20 * SEC) is True

    assert len(rec.of("market.history.requested")) == 1 and len(rec.of("news.articles.requested")) == 1


def test_early_research_completion_missing_this_symbol_keeps_waiting_for_the_regular_collection(core, rec):
    early_started(core, rec)

    assert core.on_research_completed(research_completed(incomplete=["TEST01"]), T + 330 * SEC) is True
    assert rec.of("market.history.requested") == []
    (entry,) = [e for e in rec.log.entries if e.reason == "collection_incomplete"]
    assert entry.entry_type == EntryType.STEP_FAILED

    core.on_collection_completed(completed(), T + 340 * SEC)
    assert len(rec.of("market.history.requested")) == 1


def test_completions_of_other_research_jobs_and_regular_collections_are_not_taken(core, rec):
    early_started(core, rec)

    assert core.on_research_completed(research_completed(rid=RESEARCH_ID), T + 330 * SEC) is False
    assert core.on_research_completed(completed(), T + 330 * SEC) is False
    assert rec.of("market.history.requested") == []


def test_context_that_never_arrives_gives_up_early_research_and_late_reply_is_ignored(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    core.on_tick(T + S.early_research_delay + S.response_timeout)

    core.on_context_responded(etf_context(1), T + S.early_research_delay + S.response_timeout + SEC)
    assert rec.of("news.research.requested") == []
    at = T + S.early_research_delay + S.response_timeout + 2 * SEC
    core.on_collection_completed(completed(), at)
    # 정기 수집이 와도 답을 못 받은 요청은 그대로 두고, 다시 보내는 것은 제한 시간 규칙이 한다.
    assert [e.value["payload"]["seq"] for e in rec.of("market.context.requested")] == [1]
    core.on_tick(at + S.response_timeout)
    assert [e.value["payload"]["seq"] for e in rec.of("market.context.requested")] == [1, 2]


def test_a_job_that_is_already_fetching_does_not_start_early_research(core, rec):
    start(core)
    core.on_context_responded(context_response(), T + SEC)
    core.on_tick(T + S.early_research_delay)

    assert len(rec.of("market.context.requested")) == 1  # 근거 조회가 보낸 하나뿐
    assert rec.of("news.research.requested") == []


def test_early_queries_are_capped_deduplicated_and_skip_nameless_or_oversized_members():
    from briefing.jobs import early_queries
    from briefing.research import MAX_QUERY_LENGTH
    from briefing.sources import EtfInfo, EtfMember
    members = (EtfMember("KR", "A", 1, "가"), EtfMember("KR", "B", 2, None), EtfMember("KR", "C", 3, "가"),
               EtfMember("KR", "D", 4, "나" * (MAX_QUERY_LENGTH + 1)), EtfMember("KR", "E", 5, "다"))

    assert early_queries(EtfInfo(members, False, name="이름"), 3) == ("이름", "가", "다")
    assert early_queries(EtfInfo(members, False, name="이름"), 1) == ("이름",)
    assert early_queries(EtfInfo(members, False, name=None), 3) == ()
    assert early_queries(None, 3) == ()
    assert early_queries(None, 3, " 주식 ") == ("주식",)
    assert early_queries(None, 3, "  ") == ()


def test_an_early_context_request_still_waiting_when_the_regular_completion_arrives_is_reused(core, rec):
    core.on_move(move(), T + S.early_research_delay)
    core.on_collection_completed(completed(), T + S.early_research_delay + SEC)

    # 이미 나간 요청의 응답이 낡은 응답이 되지 않도록 새 순번으로 다시 보내지 않는다.
    assert len(rec.of("market.context.requested")) == 1
    core.on_context_responded(context_response(1), T + S.early_research_delay + 2 * SEC)
    core.on_history_responded(history_response(1, points=[100]), T + S.early_research_delay + 2 * SEC)
    core.on_articles_responded(news_response(1, ["a"]), T + S.early_research_delay + 2 * SEC)
    core.on_tick(T + S.early_research_delay + 2 * SEC + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["a"]), T + S.early_research_delay + 2 * SEC + S.recheck_interval)
    assert len(rec.readied) == 1 and rec.failed == []
