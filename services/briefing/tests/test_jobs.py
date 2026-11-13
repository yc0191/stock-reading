import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings
from briefing.job_log import EntryType, JobKind, RunStatus
from briefing.jobs import BriefingCore
from briefing.ports import FailureReason
from briefing.queries import QueryKind
from contracts import sample, validate, validate_payload
from fake_api import FakeApi
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
S = CoreSettings()
DELTA = S.delta
SEC = timedelta(seconds=1)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"


HISTORY, CONTEXT, ARTICLES, FILINGS = QueryKind.HISTORY, QueryKind.CONTEXT, QueryKind.ARTICLES, QueryKind.FILINGS


class Recorder:
    """발행한 이벤트(sent)와 맡긴 조회(api)와 완성된 리포트 입력(readied)을 모은다."""

    def __init__(self):
        self.sent, self.readied = [], []
        self.log = FakeJobLog()
        self.api = FakeApi()

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
    return BriefingCore(rec, rec.api, rec, job_log=rec.log)


def move(event_id=JOB_ID, symbol="TEST01", as_of=T, legacy=False):
    event = sample("market.move.sample.json")
    event["eventId"] = event_id
    event["payload"]["symbol"] = symbol
    event["payload"]["asOf"] = as_of.isoformat()
    if legacy:
        del event["payload"]["detectionType"], event["payload"]["moveStartedAt"]
    return json.dumps(event)


def completed(started_at=T + DELTA, incomplete=(), job_type="collect-news", unprocessed=()):
    """정기 수집의 저장 끝 알림(news.stored). 근거 조회는 이 알림으로 시작한다."""
    event = sample("news.stored.sample.json")
    event["payload"].update(jobType=job_type, startedAt=started_at.isoformat(), incompleteSymbols=list(incomplete),
                            unprocessedEventIds=list(unprocessed))
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
    core.on_news_stored(completed(), now)


def test_requests_match_the_contract_and_cover_the_required_ranges(core, rec):
    start(core)

    (history,) = rec.api.of(HISTORY)
    (news,) = rec.api.of(ARTICLES)
    # 요청 본문은 옛 요청 이벤트의 payload 계약과 같다.
    assert validate_payload("market.history.requested.v1.schema.json", history.payload) == []
    assert validate_payload("news.articles.requested.v1.schema.json", news.payload) == []
    assert history.payload["symbol"] == news.payload["symbol"] == "TEST01"
    # 추적 ID와 job_step을 남길 작업은 변동 이벤트의 것이다.
    assert history.correlation_id == news.correlation_id == json.loads(move())["correlationId"]
    assert history.job_id == news.job_id == JOB_ID
    assert (history.stage, news.stage) == ("market.history", "news.articles")
    h, n = history.payload, news.payload
    assert (h["jobId"], h["seq"], h["maxPoints"], h["intervalSeconds"]) == (JOB_ID, 1, 61, 10)
    assert datetime.fromisoformat(h["since"]) == T - timedelta(minutes=5)
    assert datetime.fromisoformat(h["until"]) == T + DELTA
    assert (n["seq"], n["maxArticles"]) == (1, 50)
    assert datetime.fromisoformat(n["since"]) == T - timedelta(hours=24)
    assert datetime.fromisoformat(n["until"]) == T + DELTA


def test_nothing_is_requested_until_a_usable_completion_arrives(core, rec):
    core.on_move(move(), T)

    assert rec.api.submitted == []


def test_completion_that_arrived_before_the_move_is_used(core, rec):
    core.on_news_stored(completed(), T)
    core.on_move(move(), T + SEC)

    assert len(rec.api.submitted) == 3


def test_only_the_ten_most_recent_completions_are_remembered(core, rec):
    core.on_news_stored(completed(), T)
    for _ in range(10):
        core.on_news_stored(completed(started_at=T, incomplete=["TEST01"]), T)
    core.on_move(move(), T)

    assert rec.api.submitted == []


def test_completion_started_inside_the_move_window_is_rejected_then_next_one_is_used(core, rec):
    core.on_move(move(), T)
    core.on_news_stored(completed(started_at=T + DELTA - SEC), T)
    assert rec.api.submitted == []

    core.on_news_stored(completed(started_at=T + DELTA), T)
    assert len(rec.api.submitted) == 3


@pytest.mark.parametrize("incomplete", [["TEST01"], ["*"]])
def test_completion_with_the_symbol_or_market_wide_incomplete_is_rejected(core, rec, incomplete):
    core.on_move(move(), T)
    core.on_news_stored(completed(incomplete=incomplete), T)

    assert rec.api.submitted == []


def test_completion_missing_only_other_symbols_is_accepted(core, rec):
    core.on_move(move(), T)
    core.on_news_stored(completed(incomplete=["TEST02"]), T)

    assert len(rec.api.submitted) == 3


def test_completion_of_another_job_type_is_rejected(core, rec):
    core.on_move(move(), T)
    core.on_news_stored(completed(job_type="collect-other"), T)

    assert rec.api.submitted == []


def test_legacy_move_without_detection_type_uses_as_of_the_same_way(core, rec):
    core.on_move(move(legacy=True), T)
    core.on_news_stored(completed(), T)

    assert len(rec.api.submitted) == 3


def test_same_event_id_twice_is_processed_once(core, rec):
    core.on_move(move(), T)
    core.on_move(move(), T + SEC)
    core.on_news_stored(completed(), T)

    assert len(rec.api.of(HISTORY)) == 1


def test_same_symbol_different_events_make_separate_jobs(core, rec):
    core.on_move(move(), T)
    core.on_move(move(event_id="6d3f9b52-8e40-4c7b-b2da-1f503c9e7b24"), T)
    core.on_news_stored(completed(), T)

    assert len(rec.api.of(HISTORY)) == 2


@pytest.mark.parametrize("message", ["not json", "{}", b"\xff", '{"eventType": "market.snapshot", "payload": {}}'])
def test_malformed_move_raises_value_error(core, message):
    with pytest.raises(ValueError):
        core.on_move(message, T)


def test_news_and_market_results_complete_the_report_input(core, rec):
    start(core)
    rec.api.reply(context_response(), T + SEC)
    rec.api.reply(history_response(1, points=[100, 101]), T + SEC)
    assert rec.readied == []  # 시세와 배경 자료만으로는 진행하지 않는다

    # 저장 끝 알림 뒤라 기사는 한 번 받으면 끝이다. 같은 조회를 다시 하지 않는다.
    rec.api.reply(news_response(1, ["b", "a"]), T + 2 * SEC)

    (ready,) = rec.readied
    assert ready.job_id == JOB_ID and ready.symbol == "TEST01" and ready.as_of == T
    assert [p.price for p in ready.price_points] == [100, 101]
    assert sorted(a.article_id for a in ready.articles) == ["a", "b"]
    assert len(rec.api.of(ARTICLES)) == 1
    assert rec.failed == []


def test_articles_are_fetched_once_and_nothing_waits_on_a_timer(core, rec):
    start(core)
    rec.api.reply(news_response(1, ["a"]), T)

    # 기사 다시 조회(안정 확인 라운드)가 없고, 조회 결과는 시각이 아니라 콜백으로 오므로 기다릴 시각도 없다.
    assert core.next_deadline() is None
    core.on_tick(T + timedelta(minutes=1))
    assert len(rec.api.of(ARTICLES)) == 1


def test_zero_article_responses_count_as_received(core, rec):
    start(core)
    rec.api.reply(context_response(), T)
    rec.api.reply(history_response(1), T)
    rec.api.reply(news_response(1, []), T)

    (ready,) = rec.readied
    assert ready.articles == () and ready.price_points == ()
    assert rec.failed == []


@pytest.mark.parametrize("kind, reason", [(HISTORY, FailureReason.MARKET_NO_RESPONSE),
                                          (ARTICLES, FailureReason.NEWS_NO_RESPONSE),
                                          (CONTEXT, FailureReason.CONTEXT_NO_RESPONSE)])
def test_a_query_that_finally_fails_fails_the_job_with_its_reason_and_attempts(core, rec, kind, reason):
    start(core)
    rec.api.fail(kind, T + timedelta(minutes=1), attempts=4, failure_kind="connect-failed")

    (failure,) = rec.failed
    assert (failure.reason, failure.attempts, failure.job_id, failure.error_type) == (
        reason.value, 4, JOB_ID, "connect-failed")
    assert rec.readied == []
    # 실패한 작업은 남은 조회 결과가 와도 되살아나지 않는다.
    for other in (HISTORY, CONTEXT, ARTICLES):
        if other is not kind:
            assert rec.api.reply({"jobId": JOB_ID, "seq": 1, "symbol": "TEST01", "truncated": False, "points": [],
                                  "articles": [], "tickers": [], "closeRates": [], "background": []},
                                 T + timedelta(minutes=2), kind=other) is True
    assert rec.readied == [] and len(rec.failed) == 1
    assert rec.log.starts[0].target["symbol"] == "TEST01"


def test_a_malformed_result_counts_as_not_received(core, rec, caplog):
    start(core)
    with caplog.at_level("ERROR", logger="briefing.jobs"):
        rec.api.reply({"jobId": JOB_ID, "seq": 1, "symbol": "TEST01", "points": [{"asOf": "bad"}]}, T,
                      kind=HISTORY)

    (failure,) = rec.failed
    assert (failure.reason, failure.error_type) == (FailureReason.MARKET_NO_RESPONSE.value, "MalformedResponse")
    assert "malformed market/history" in caplog.text


def test_collection_not_covered_after_the_wait_limit(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.collection_wait_limit - SEC)
    assert rec.failed == []

    core.on_tick(T + S.collection_wait_limit)

    (failure,) = rec.failed
    assert failure.reason == FailureReason.COLLECTION_NOT_COVERED
    assert failure.data["missingItems"] == ["news.stored"]
    core.on_news_stored(completed(), T + S.collection_wait_limit + SEC)
    # 실패한 작업은 되살아나지 않는다. 맡긴 것은 T+5분에 ETF 이름을 알려고 한 배경 자료 조회뿐이다.
    assert [q.kind for q in rec.api.submitted] == [CONTEXT]


def test_a_failed_job_leaves_one_failure_line_and_one_result_line_under_the_same_job_id(core, rec):
    start(core)
    rec.api.fail(HISTORY, T + timedelta(minutes=1))

    (failure,) = rec.log.failures()
    (result,) = rec.log.results()
    assert failure.job_id == result.job_id == JOB_ID
    # 작업 한 줄(job_run)은 시작 때 move로 만들고 끝날 때 실패 사유와 함께 고친다.
    (run,) = rec.log.starts
    assert run.job_kind == JobKind.MOVE and run.job_id == JOB_ID and run.target["symbol"] == "TEST01"
    assert result.status == RunStatus.FAILED and result.failure_reason == FailureReason.MARKET_NO_RESPONSE.value
    # 다시 부른 기록은 job_log가 아니라 조회 구현이 job_step에 남긴다.
    assert rec.log.of_type(EntryType.RESENT) == []
    assert failure.data["requests"]["market.history"][0]["seq"] == 1


def test_failure_reasons_are_exactly_the_six_defined(core):
    assert {r.value for r in FailureReason} == {
        "collection_not_covered", "market_no_response", "news_no_response", "llm_error",
        "context_no_response", "filings_no_response"}


def test_articles_outside_the_range_are_dropped_from_the_report_input(core, rec, caplog):
    start(core)
    rec.api.reply(context_response(), T)
    rec.api.reply(history_response(1), T)
    mixed = [article("ok"), article("old", T - timedelta(hours=25)), article("late", T + DELTA + SEC),
             article("edge", T + DELTA)]
    with caplog.at_level("WARNING", logger="briefing.jobs"):
        rec.api.reply(news_response(1, articles=mixed), T)

    assert sorted(a.article_id for a in rec.readied[0].articles) == ["edge", "ok"]
    assert "old" in caplog.text and "late" in caplog.text


def test_next_deadline_reports_the_earliest_timer(core, rec):
    assert core.next_deadline() is None
    core.on_move(move(), T)
    assert core.next_deadline() == T + S.early_research_delay  # 정기 수집을 기다리는 동안 T+5분에 먼저 재검색한다
    core.on_tick(T + S.early_research_delay)
    # 배경 자료 조회는 시각이 아니라 결과로 끝나므로 남은 시각은 저장 끝을 기다리는 한도뿐이다.
    assert core.next_deadline() == T + S.collection_wait_limit
    rec.api.fail(CONTEXT, T + S.early_research_delay + SEC)  # 배경 자료를 끝내 받지 못해 포기
    assert core.next_deadline() == T + S.collection_wait_limit
    core.on_news_stored(completed(), T)
    assert core.next_deadline() is None


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
    event = sample("news.stored.sample.json")
    event["payload"].update(jobType="research-news", startedAt=started_at.isoformat(),
                            incompleteSymbols=list(incomplete), researchJobId=rid or early_research_id(job_id))
    return json.dumps(event)


def early_started(core, rec, etf=ETF):
    """T+5분에 배경 자료를 먼저 요청하고 그 응답으로 재검색 요청까지 보낸다."""
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    rec.api.reply(etf_context(1, etf), T + S.early_research_delay + SEC)


def test_the_context_name_reaches_the_report_input_only_when_present(core, rec):
    start(core)
    event = json.loads(context_response())
    event["payload"]["name"] = " TEST 주식 "
    rec.api.reply(json.dumps(event), T + SEC)
    rec.api.reply(history_response(1, points=[100, 101]), T + SEC)
    rec.api.reply(news_response(1, ["a"]), T + SEC)

    (ready,) = rec.readied
    assert ready.name == "TEST 주식"


def test_a_response_without_a_name_leaves_the_report_input_name_empty(core, rec):
    start(core)
    rec.api.reply(context_response(), T + SEC)
    rec.api.reply(history_response(1, points=[100, 101]), T + SEC)
    rec.api.reply(news_response(1, ["a"]), T + SEC)

    (ready,) = rec.readied
    assert ready.name is None


@pytest.mark.parametrize("bad_name", [5, "", "   ", ["x"]])
def test_a_context_name_that_is_not_usable_text_is_treated_as_no_name(core, rec, bad_name):
    start(core)
    event = json.loads(context_response())
    event["payload"]["name"] = bad_name
    rec.api.reply(json.dumps(event), T + SEC)
    rec.api.reply(history_response(1, points=[100, 101]), T + SEC)
    rec.api.reply(news_response(1, ["a"]), T + SEC)

    (ready,) = rec.readied
    assert ready.name is None


def test_a_move_asks_for_the_context_at_t_plus_five_minutes_and_nothing_before(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay - SEC)
    assert rec.api.submitted == []

    core.on_tick(T + S.early_research_delay)

    (context,) = rec.api.of(CONTEXT)
    assert context.payload["seq"] == 1 and len(rec.api.submitted) == 1


def test_a_move_noticed_after_t_plus_five_minutes_asks_at_once(core, rec):
    core.on_move(move(), T + S.early_research_delay + SEC)

    assert len(rec.api.of(CONTEXT)) == 1


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
    core.on_news_stored(completed(), T + S.early_research_delay + 2 * SEC)
    # 이미 받은 배경 자료는 다시 받지 않는다.
    assert len(rec.api.of(CONTEXT)) == 1
    assert len(rec.api.of(HISTORY)) == 1 and len(rec.api.of(ARTICLES)) == 1


def test_a_stock_move_researches_the_stock_name_alone_at_t_plus_five_minutes(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    event = json.loads(context_response(1))
    event["payload"]["name"] = " TEST 주식 "
    rec.api.reply(json.dumps(event), T + S.early_research_delay + SEC)

    (request,) = rec.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    assert request.value["payload"]["queries"] == ["TEST 주식"]


def test_a_stock_whose_name_is_oversized_is_not_researched(core, rec):
    from briefing.research import MAX_QUERY_LENGTH
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    event = json.loads(context_response(1))
    event["payload"]["name"] = "가" * (MAX_QUERY_LENGTH + 1)
    rec.api.reply(json.dumps(event), T + S.early_research_delay + SEC)

    assert rec.of("news.research.requested") == []


def test_an_etf_move_ignores_the_context_name_for_research_queries(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    event = json.loads(etf_context(1))
    event["payload"]["name"] = "다른 이름"
    rec.api.reply(json.dumps(event), T + S.early_research_delay + SEC)

    (request,) = rec.of("news.research.requested")
    assert request.value["payload"]["queries"] == ["가짜 반도체 ETF", "첫째회사", "둘째회사"]


def test_early_research_completion_starts_the_evidence_fetch_without_the_regular_collection(core, rec):
    early_started(core, rec)
    done_at = T + S.early_research_delay + 20 * SEC

    assert core.on_research_stored(research_completed(), done_at) is True

    assert len(rec.api.of(HISTORY)) == 1 and len(rec.api.of(ARTICLES)) == 1
    # 배경 자료는 이미 받았으므로 다시 요청하지 않고 곧바로 진행한다.
    assert len(rec.api.of(CONTEXT)) == 1
    rec.api.reply(history_response(1, points=[100]), done_at)
    rec.api.reply(news_response(1, ["a"]), done_at)
    (ready,) = rec.readied
    assert [a.article_id for a in ready.articles] == ["a"]
    assert rec.failed == []


def test_early_research_completion_of_a_us_etf_still_requests_filings(core, rec):
    event = json.loads(etf_context(1))
    event["payload"]["tickers"] = ["TESTA"]
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    rec.api.reply(json.dumps(event), T + S.early_research_delay + SEC)
    core.on_research_stored(research_completed(), T + S.early_research_delay + 2 * SEC)

    assert [q.payload["tickers"] for q in rec.api.of(FILINGS)] == [["TESTA"]]


def test_whichever_completion_comes_first_wins_and_the_later_one_changes_nothing(core, rec):
    early_started(core, rec)
    core.on_news_stored(completed(started_at=T + S.early_research_delay + 5 * SEC),
                                 T + S.early_research_delay + 6 * SEC)
    assert len(rec.api.of(HISTORY)) == 1

    assert core.on_research_stored(research_completed(), T + S.early_research_delay + 20 * SEC) is True

    assert len(rec.api.of(HISTORY)) == 1 and len(rec.api.of(ARTICLES)) == 1


def test_early_research_completion_missing_this_symbol_keeps_waiting_for_the_regular_collection(core, rec):
    early_started(core, rec)

    assert core.on_research_stored(research_completed(incomplete=["TEST01"]), T + 330 * SEC) is True
    assert rec.api.of(HISTORY) == []
    (entry,) = [e for e in rec.log.entries if e.reason == "collection_incomplete"]
    assert entry.entry_type == EntryType.STEP_FAILED

    core.on_news_stored(completed(), T + 340 * SEC)
    assert len(rec.api.of(HISTORY)) == 1


def test_completions_of_other_research_jobs_and_regular_collections_are_not_taken(core, rec):
    early_started(core, rec)

    assert core.on_research_stored(research_completed(rid=RESEARCH_ID), T + 330 * SEC) is False
    assert core.on_research_stored(completed(), T + 330 * SEC) is False
    assert rec.api.of(HISTORY) == []


def test_context_that_finally_fails_gives_up_early_research_and_the_regular_fetch_asks_again(core, rec):
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    rec.api.fail(CONTEXT, T + S.early_research_delay + timedelta(minutes=1))

    assert rec.of("news.research.requested") == [] and rec.failed == []
    core.on_news_stored(completed(), T + S.early_research_delay + timedelta(minutes=2))
    # 이른 조회는 포기했으므로 정기 근거 조회가 배경 자료를 새 순번으로 다시 맡긴다.
    assert [q.payload["seq"] for q in rec.api.of(CONTEXT)] == [1, 2]


def test_a_job_that_is_already_fetching_does_not_start_early_research(core, rec):
    start(core)
    rec.api.reply(context_response(), T + SEC)
    core.on_tick(T + S.early_research_delay)

    assert len(rec.api.of(CONTEXT)) == 1  # 근거 조회가 보낸 하나뿐
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
    core.on_news_stored(completed(), T + S.early_research_delay + SEC)

    # 아직 끝나지 않은 조회를 이어 쓰고 같은 조회를 다시 맡기지 않는다.
    assert len(rec.api.of(CONTEXT)) == 1
    rec.api.reply(context_response(1), T + S.early_research_delay + 2 * SEC)
    rec.api.reply(history_response(1, points=[100]), T + S.early_research_delay + 2 * SEC)
    rec.api.reply(news_response(1, ["a"]), T + S.early_research_delay + 2 * SEC)
    assert len(rec.readied) == 1 and rec.failed == []


def test_a_stored_notice_with_unprocessed_articles_still_starts_the_fetch_and_warns(core, rec, caplog):
    core.on_move(move(), T)
    with caplog.at_level("WARNING", logger="briefing.jobs"):
        core.on_news_stored(completed(unprocessed=["e0000012-0000-4000-8000-000000000012"]), T + DELTA)

    # 저장되지 못한 기사는 조회에 없을 뿐이라 근거 조회는 진행한다(근거가 약하면 재검색이 채운다).
    assert len(rec.api.of(ARTICLES)) == 1
    assert "left 1 article(s) unprocessed" in caplog.text


@pytest.mark.parametrize("message", ["not json", "{}", json.dumps({"eventType": "collection.completed", "payload": {}})])
def test_a_malformed_or_other_stored_notice_raises_value_error(core, message):
    with pytest.raises(ValueError):
        core.on_news_stored(message, T)
