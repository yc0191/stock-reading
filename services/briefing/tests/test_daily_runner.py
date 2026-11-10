"""BriefingRunner의 데일리 판 연결(판마다 따로 둔 tick 소비자, 요청·응답, 발행)을 가짜 Kafka로 확인한다."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing import main
from briefing.daily_report import DailyLlmWriter
from briefing.ports import FailureReason
from contracts import validate
from test_daily_jobs import MARKET, FakeLlm, daily_response, etf_v2, filings_response, good_output, post_close_output, tick
from test_runner import Clock, FakeConsumer, FakeMessage, FakeProducer, FakeStore, sent

KST = timezone(timedelta(hours=9))
MORNING_AT = datetime(2026, 10, 20, 8, 45, tzinfo=KST)


@pytest.fixture
def setup():
    clock = Clock(MORNING_AT)
    store = FakeStore()
    consumers = []

    def make(daily_writer=None):
        def consumer_factory(config):
            consumers.append(FakeConsumer(config))
            return consumers[-1]

        producers = []

        def producer_factory(config):
            producers.append(FakeProducer(config))
            return producers[-1]

        runner = main.BriefingRunner("localhost:9092", None, store, clock=clock, consumer_factory=consumer_factory,
                                     producer_factory=producer_factory, declare_topics=lambda _: None,
                                     daily_writer=daily_writer)
        runner.open()
        return runner, runner.tick_consumers, consumers[-1], producers[-1]

    return make, clock, store


def test_each_edition_has_its_own_tick_consumer_group_reading_only_new_ticks(setup):
    make, _, _ = setup
    _, ticks, main_consumer, _ = make()
    groups = {edition: c.config["group.id"] for edition, c in ticks.items()}
    assert set(groups) == {"morning", "post-close"}
    assert len(set(groups.values())) == 2 and "briefing" not in groups.values()
    assert main_consumer.config["group.id"] == "briefing"
    for consumer in ticks.values():
        assert consumer.subscribed == ["schedule.tick"]
        assert consumer.config["auto.offset.reset"] == "latest"
        assert consumer.config["enable.auto.commit"] is False
    assert main.TOPIC_PARTITIONS["schedule.tick"] == 1


def test_a_morning_tick_flows_to_a_daily_briefing(setup):
    make, clock, store = setup
    runner, ticks, consumer, producer = make(DailyLlmWriter(FakeLlm(good_output("B1"))))
    # 같은 tick 은 두 판의 소비자가 모두 받는다. 아침 판 소비자만 처리한다.
    ticks["morning"].queue.append(FakeMessage("schedule.tick", tick()))
    ticks["post-close"].queue.append(FakeMessage("schedule.tick", tick()))
    runner.run_once()
    assert ticks["morning"].events == [("commit", "schedule.tick")]
    [(key, request)] = sent(producer, "market.daily.requested")
    assert key == request["payload"]["jobId"]
    assert validate("market.daily.requested.v1.schema.json", request) == []

    consumer.queue.append(FakeMessage("market.daily.responded", daily_response()))
    runner.run_once()
    [(_, filings_request)] = sent(producer, "news.filings.requested")
    assert validate("news.filings.requested.v1.schema.json", filings_request) == []

    consumer.queue.append(FakeMessage("news.filings.responded", filings_response()))
    runner.run_once()
    [(key, briefing)] = sent(producer, "briefing.daily.generated")
    assert key == "morning"
    assert validate("briefing.daily.generated.v1.schema.json", briefing) == []
    assert briefing["payload"]["generatedBy"] == "llm"
    assert store.failures == []
    assert sent(producer, "briefing.generated") == []


def test_a_post_close_tick_flows_through_articles_and_filings_to_a_daily_briefing(setup):
    make, clock, store = setup
    runner, ticks, consumer, producer = make(DailyLlmWriter(FakeLlm(post_close_output(symbols=("TEST01",)))))
    ticks["post-close"].queue.append(FakeMessage("schedule.tick", tick("daily-briefing-post-close", MORNING_AT)))
    runner.run_once()
    consumer.queue.append(FakeMessage("market.daily.responded", daily_response(
        etfs=[etf_v2("TEST01", "TEST ETF", fetched="2026-10-19T16:30:02+09:00", rate="0.50")],
        background=[{**MARKET, "fetchedAt": "2026-10-19T16:30:05+09:00"}])))
    runner.run_once()
    [(key, articles_request)] = sent(producer, "news.articles.requested")
    assert key == "TEST01" and validate("news.articles.requested.v1.schema.json", articles_request) == []
    [(_, filings_request)] = sent(producer, "news.filings.requested")
    payload = articles_request["payload"]
    consumer.queue.append(FakeMessage("news.articles.responded", json.dumps({
        "eventId": "00000000-0000-4000-8000-000000000007", "eventType": "news.articles.responded",
        "schemaVersion": 1, "occurredAt": "2026-10-20T08:45:01+09:00", "producedAt": "2026-10-20T08:45:01+09:00",
        "correlationId": "c", "payload": {"jobId": payload["jobId"], "seq": payload["seq"], "symbol": "TEST01",
                                          "articles": [], "truncated": False}})))
    runner.run_once()
    assert sent(producer, "briefing.daily.generated") == []
    consumer.queue.append(FakeMessage("news.filings.responded", filings_response(
        seq=filings_request["payload"]["seq"], filings=[])))
    runner.run_once()
    [(key, briefing)] = sent(producer, "briefing.daily.generated")
    assert key == "post-close" and validate("briefing.daily.generated.v1.schema.json", briefing) == []


def test_daily_no_response_is_resent_by_the_runner_timer_and_still_publishes(setup):
    make, clock, store = setup
    runner, ticks, _, producer = make()
    ticks["post-close"].queue.append(FakeMessage("schedule.tick", tick("daily-briefing-post-close", MORNING_AT)))
    runner.run_once()
    assert runner._poll_timeout() == 1.0
    for _ in range(3):
        clock.advance(seconds=10)
        runner.run_once()
    assert len(sent(producer, "market.daily.requested")) == 3
    [(key, briefing)] = sent(producer, "briefing.daily.generated")
    assert key == "post-close" and briefing["payload"]["generatedBy"] == "rule-based"
    [failure] = store.failures
    assert failure.reason == FailureReason.CONTEXT_NO_RESPONSE
    assert [run.job_kind for run in store.starts] == ["post-close"]


def test_duplicate_tick_is_handled_once_by_the_runner(setup):
    make, _, _ = setup
    runner, ticks, _, producer = make()
    for _ in range(2):
        ticks["morning"].queue.append(FakeMessage("schedule.tick", tick()))
        runner.run_once()
    assert len(sent(producer, "market.daily.requested")) == 1
    assert len(ticks["morning"].commits) == 2


def test_malformed_tick_is_skipped_without_stopping_the_runner(setup):
    make, _, _ = setup
    runner, ticks, _, producer = make()
    ticks["morning"].queue.append(FakeMessage("schedule.tick", "{broken"))
    runner.run_once()
    assert producer.sent == []
    ticks["morning"].queue.append(FakeMessage("schedule.tick", tick()))
    runner.run_once()
    assert len(sent(producer, "market.daily.requested")) == 1


def test_close_closes_the_tick_consumers(setup):
    make, _, _ = setup
    runner, ticks, _, _ = make()
    tick_consumers = list(ticks.values())
    runner.close()
    assert all(c.closed for c in tick_consumers)


# ---- 서버 상태 절의 저장소 연결 ----

class FakeStatusStore:
    def __init__(self):
        self.saved = []

    def save_collection(self, record):
        self.saved.append(record)
        return True

    def collections_between(self, since, until):
        return []

    def job_runs_between(self, since, until):
        return []


def status_runner(setup):
    _, clock, store = setup
    status = FakeStatusStore()
    runner = main.BriefingRunner("localhost:9092", None, store, clock=clock,
                                 consumer_factory=lambda config: FakeConsumer(config),
                                 producer_factory=lambda config: FakeProducer(config),
                                 declare_topics=lambda _: None, status_store=status)
    runner.open()
    return runner, status


def test_every_collection_completed_is_saved_as_received(setup):
    from test_runner import completed_message
    runner, status = status_runner(setup)
    message = completed_message(incomplete=["TEST02"])
    runner._consumer.commit = lambda **_: None
    runner.handle_message(message)
    [record] = status.saved
    assert record.raw == message.value() and record.incomplete_symbols == ("TEST02",)
    assert record.research_job_id is None and not record.is_research()


def test_a_malformed_collection_completed_is_skipped_without_saving(setup):
    from test_runner import FakeMessage
    runner, status = status_runner(setup)
    runner._consumer.commit = lambda **_: None
    runner.handle_message(FakeMessage("collection.completed", "{not json"))
    assert status.saved == []


def test_unreadable_status_fields_do_not_stop_the_evidence_lookup_from_getting_the_notice(setup):
    from test_runner import completed_message
    runner, status = status_runner(setup)
    runner._consumer.commit = lambda **_: None
    received = []
    runner.core.on_collection_completed = lambda message, now: received.append(message)
    runner.sentences.on_collection_completed = lambda message, now: False
    message = completed_message()
    event = json.loads(message.value())
    del event["payload"]["finishedAt"]
    message = type(message)("collection.completed", json.dumps(event))
    runner.handle_message(message)
    assert received == [message.value()] and status.saved == []
