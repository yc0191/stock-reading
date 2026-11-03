import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings
from briefing.jobs import BriefingCore, FailureReason
from contracts import sample, validate

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
S = CoreSettings()
DELTA = S.delta
SEC = timedelta(seconds=1)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"


class Recorder:
    def __init__(self):
        self.sent, self.failed, self.readied = [], [], []

    def send(self, event):
        self.sent.append(event)

    def record(self, failure):
        self.failed.append(failure)

    def on_ready(self, report_input):
        self.readied.append(report_input)

    def of(self, topic):
        return [e for e in self.sent if e.topic == topic]


@pytest.fixture
def rec():
    return Recorder()


@pytest.fixture
def core(rec):
    return BriefingCore(rec, rec, rec)


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
    assert (failure.reason, failure.attempts, failure.job_id, failure.symbol) == (
        FailureReason.NEWS_NO_RESPONSE, 3, JOB_ID, "TEST01")
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
    assert failure.reason is FailureReason.NEWS_UNSTABLE
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
    assert failure.reason is FailureReason.COLLECTION_NOT_COVERED
    core.on_collection_completed(completed(), T + S.collection_wait_limit + SEC)
    assert rec.sent == []  # 실패한 작업은 되살아나지 않는다


def test_llm_error_failure_goes_through_the_same_port(core, rec):
    start(core)
    core.on_context_responded(context_response(), T)
    core.on_history_responded(history_response(1), T)
    core.on_articles_responded(news_response(1, ["a"]), T)
    core.on_tick(T + S.recheck_interval)
    core.on_articles_responded(news_response(2, ["a"]), T + S.recheck_interval)

    core.fail_llm_error(rec.readied[0], "invalid json", 3)

    (failure,) = rec.failed
    assert (failure.reason, failure.attempts) == (FailureReason.LLM_ERROR, 3)


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
    assert core.next_deadline() == T + S.collection_wait_limit
    core.on_collection_completed(completed(), T)
    assert core.next_deadline() == T + S.response_timeout


def test_core_does_not_import_kafka():
    import briefing.config, briefing.events, briefing.jobs  # noqa: E401

    for module in (briefing.config, briefing.events, briefing.jobs):
        source = open(module.__file__, encoding="utf-8").read()
        assert "confluent_kafka" not in source


def test_default_history_window_fits_in_the_point_cap_so_it_is_not_always_truncated():
    # 양끝을 포함한 구간이라 칸 수는 (구간 길이 / 간격) + 1이다. 상한이 이보다 작으면 마지막 칸이 늘 잘린다.
    s = CoreSettings()
    buckets = int((s.market_lookback + s.delta) / s.history_interval) + 1
    assert buckets <= s.max_history_points
