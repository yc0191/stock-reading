"""BriefingRunner의 데일리 판 연결(판마다 따로 둔 tick 소비자, 요청·응답, 발행)을 가짜 Kafka로 확인한다."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing import main
from briefing.daily_report import DailyLlmWriter
from briefing.ports import FailureReason
from contracts import validate
from test_daily_jobs import FakeLlm, daily_response, filings_response, good_output, tick
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
