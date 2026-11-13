"""BriefingRunner의 데일리 판 연결(판 시작 알림, 한도 시각, 내부 조회, 발행)을 가짜 Kafka·가짜 조회 API로 확인한다."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing import main
from briefing.config import DailySettings
from briefing.daily_report import DailyLlmWriter
from briefing.ports import FailureReason
from briefing.queries import QueryKind
from contracts import validate, validate_payload
from fake_api import FakeApi
from fake_members import FakeMembers
from test_daily_jobs import MARKET, FakeLlm, daily_response, etf_v2, filings_response, good_output, post_close_output, tick
from test_runner import Clock, FakeConsumer, FakeMessage, FakeProducer, FakeStore, sent

KST = timezone(timedelta(hours=9))
MORNING_AT = datetime(2026, 10, 20, 8, 45, tzinfo=KST)


@pytest.fixture
def setup():
    clock = Clock(MORNING_AT)
    store = FakeStore()
    consumers = []

    def make(daily_writer=None, daily_settings=DailySettings(), job_log=None):
        def consumer_factory(config):
            consumers.append(FakeConsumer(config))
            return consumers[-1]

        producers = []

        def producer_factory(config):
            producers.append(FakeProducer(config))
            return producers[-1]

        api = FakeApi()
        runner = main.BriefingRunner("localhost:9092", None, job_log or store, clock=clock,
                                     consumer_factory=consumer_factory, producer_factory=producer_factory,
                                     declare_topics=lambda _: None, daily_writer=daily_writer,
                                     daily_settings=daily_settings, member_store=FakeMembers(),
                                     api_factory=lambda: api)
        runner.open()
        return runner, consumers[-1], producers[-1]

    return make, clock, store


def notice(*args, **kwargs):
    return FakeMessage("daily.collection.completed", tick(*args, **kwargs))


def asked(runner, kind):
    """맡긴 조회 전체(요청 본문, 추적 ID)"""
    return runner.api.of(kind)


def answer(runner, body, kind=None):
    """가짜 조회 API의 답을 실행부가 꺼내 갈 결과로 쌓고 한 바퀴 돌린다."""
    runner.api.queue_reply(body, kind)
    runner.run_once()


def give_up(runner, kind):
    """맡긴 조회 하나를 끝내 받지 못한 결과로 쌓고 한 바퀴 돌린다(조회 구현이 재시도를 다 쓴 경우)."""
    for index, (query, on_result) in enumerate(runner.api.pending):
        if query.kind is kind:
            del runner.api.pending[index]
            runner.api.done.append((on_result, main.QueryResult(query, None, 4, "timeout")))
            break
    runner.run_once()


def test_the_start_notice_is_read_by_the_main_consumer_and_no_tick_consumer_is_left(setup):
    make, _, _ = setup
    runner, main_consumer, _ = make()
    assert main_consumer.config["group.id"] == "briefing"
    assert "daily.collection.completed" in main_consumer.subscribed
    assert "schedule.tick" not in main.TOPIC_PARTITIONS
    assert main.TOPIC_PARTITIONS["daily.collection.completed"] == 1
    assert not hasattr(runner, "tick_consumers")


def test_a_morning_notice_flows_to_a_daily_briefing(setup):
    make, clock, store = setup
    runner, consumer, producer = make(DailyLlmWriter(FakeLlm(good_output("B1"))))
    consumer.queue.append(notice())
    runner.run_once()
    assert consumer.events == [("commit", "daily.collection.completed")]
    [request] = asked(runner, QueryKind.DAILY)
    assert validate_payload("market.daily.requested.v1.schema.json", request.payload) == []
    assert request.correlation_id == json.loads(tick())["correlationId"]
    assert producer.sent == []  # 조회는 Kafka로 나가지 않는다

    answer(runner, daily_response())
    [filings_request] = asked(runner, QueryKind.FILINGS)
    assert validate_payload("news.filings.requested.v1.schema.json", filings_request.payload) == []
    # 아침 판도 ETF마다 기사를 받아야 나간다.
    [articles_request] = asked(runner, QueryKind.ARTICLES)
    assert articles_request.payload["symbol"] == "TEST01"
    assert validate_payload("news.articles.requested.v1.schema.json", articles_request.payload) == []

    answer(runner, filings_response())
    assert sent(producer, "briefing.daily.generated") == []
    payload = articles_request.payload
    answer(runner, {"jobId": payload["jobId"], "seq": payload["seq"], "symbol": "TEST01", "articles": [],
                    "truncated": False}, QueryKind.ARTICLES)
    [(key, briefing)] = sent(producer, "briefing.daily.generated")
    assert key == "morning:1" and briefing["payload"]["memberId"] == 1
    assert validate("briefing.daily.generated.v2.schema.json", briefing) == []
    assert briefing["payload"]["generatedBy"] == "llm"
    assert store.failures == []
    assert sent(producer, "briefing.generated") == []


def test_a_post_close_notice_flows_through_articles_and_filings_to_a_daily_briefing(setup):
    make, clock, store = setup
    runner, consumer, producer = make(DailyLlmWriter(FakeLlm(post_close_output(symbols=("TEST01",)))))
    consumer.queue.append(notice("post-close", MORNING_AT))
    runner.run_once()
    answer(runner, daily_response(
        etfs=[etf_v2("TEST01", "TEST ETF", fetched="2026-10-19T16:30:02+09:00", rate="0.50")],
        background=[{**MARKET, "fetchedAt": "2026-10-19T16:30:05+09:00"}]))
    [articles_request] = asked(runner, QueryKind.ARTICLES)
    assert articles_request.payload["symbol"] == "TEST01"
    [filings_request] = asked(runner, QueryKind.FILINGS)
    payload = articles_request.payload
    answer(runner, {"jobId": payload["jobId"], "seq": payload["seq"], "symbol": "TEST01", "articles": [],
                    "truncated": False}, QueryKind.ARTICLES)
    assert sent(producer, "briefing.daily.generated") == []
    answer(runner, filings_response(seq=filings_request.payload["seq"], filings=[]))
    [(key, briefing)] = sent(producer, "briefing.daily.generated")
    assert key == "post-close:1" and validate("briefing.daily.generated.v2.schema.json", briefing) == []


def test_daily_data_that_finally_fails_still_publishes(setup):
    make, clock, store = setup
    runner, consumer, producer = make()
    consumer.queue.append(notice("post-close", MORNING_AT))
    runner.run_once()
    assert runner._poll_timeout() == 1.0
    give_up(runner, QueryKind.DAILY)
    assert len(asked(runner, QueryKind.DAILY)) == 1
    [(key, briefing)] = sent(producer, "briefing.daily.generated")
    assert key == "post-close:1" and briefing["payload"]["generatedBy"] == "rule-based"
    [failure] = store.failures
    assert failure.reason == FailureReason.CONTEXT_NO_RESPONSE
    assert [run.job_kind for run in store.starts] == ["post-close"]


def test_duplicate_notice_is_handled_once_by_the_runner(setup):
    make, _, _ = setup
    runner, consumer, producer = make()
    for _ in range(2):
        consumer.queue.append(notice())
        runner.run_once()
    assert len(asked(runner, QueryKind.DAILY)) == 1
    assert len(consumer.commits) == 2


def test_malformed_notice_is_skipped_without_stopping_the_runner(setup):
    make, _, _ = setup
    runner, consumer, producer = make()
    consumer.queue.append(FakeMessage("daily.collection.completed", "{broken"))
    runner.run_once()
    assert producer.sent == []
    consumer.queue.append(notice())
    runner.run_once()
    assert len(asked(runner, QueryKind.DAILY)) == 1


# ---- 한도 시각 안전장치 ----

ON = DailySettings(deadline_enabled=True)
MORNING_DEADLINE = datetime(2026, 10, 20, 9, 20, tzinfo=KST)


def test_without_a_notice_the_runner_starts_the_morning_edition_at_its_deadline_and_marks_the_data_stale(setup):
    make, clock, store = setup
    clock.now = MORNING_DEADLINE - timedelta(seconds=1)
    runner, _, producer = make(daily_settings=ON)
    runner.run_once()
    assert asked(runner, QueryKind.DAILY) == []

    clock.advance(seconds=1)
    runner.run_once()
    [request] = asked(runner, QueryKind.DAILY)
    assert request.payload["asOf"] == "2026-10-20T09:20:00+09:00"
    assert request.correlation_id == "daily-morning-deadline-2026-10-20T09:20:00+09:00"
    give_up(runner, QueryKind.DAILY)
    [(_, briefing)] = sent(producer, "briefing.daily.generated")
    assert briefing["payload"]["missingItems"][0] == "수집 완료 알림을 받지 못해 자료가 낡았을 수 있음"
    assert validate("briefing.daily.generated.v2.schema.json", briefing) == []
    # 한도 시각이 계속 지나 있어도 같은 날 다시 시작하지 않는다.
    clock.advance(hours=1)
    runner.run_once()
    assert len(asked(runner, QueryKind.DAILY)) == 1
    assert [run.job_kind.value for run in store.starts] == ["morning"]


def test_a_late_notice_after_the_deadline_start_is_ignored(setup):
    make, clock, store = setup
    clock.now = MORNING_DEADLINE
    runner, consumer, producer = make(daily_settings=ON)
    runner.run_once()
    consumer.queue.append(notice(at=MORNING_DEADLINE + timedelta(minutes=3)))
    runner.run_once()
    assert len(asked(runner, QueryKind.DAILY)) == 1
    assert [run.job_kind.value for run in store.starts] == ["morning"]


def test_the_deadline_is_off_when_not_enabled_and_on_weekends(setup):
    make, clock, store = setup
    clock.now = datetime(2026, 10, 20, 18, 0, tzinfo=KST)
    runner, _, producer = make()
    runner.run_once()
    saturday = datetime(2026, 10, 24, 18, 0, tzinfo=KST)
    clock.now = saturday
    weekend_runner, _, weekend_producer = make(daily_settings=ON)
    weekend_runner.run_once()
    assert producer.sent == [] and weekend_producer.sent == [] and store.starts == []


def test_a_market_closed_notice_turns_off_that_editions_deadline_for_the_day(setup):
    make, clock, store = setup
    clock.now = datetime(2026, 10, 20, 9, 10, tzinfo=KST)
    runner, consumer, producer = make(daily_settings=ON)
    consumer.queue.append(notice(at=clock.now, status="market-closed"))
    runner.run_once()
    clock.now = datetime(2026, 10, 20, 17, 30, tzinfo=KST)
    runner.run_once()
    # 아침 판은 휴장 알림으로 꺼졌고, 마감 후 판 휴장 알림은 오지 않았으므로 마감 후 판만 한도 시각에 시작한다.
    assert [run.job_kind.value for run in store.starts] == ["post-close"]
    [request] = asked(runner, QueryKind.DAILY)
    assert request.payload["asOf"] == "2026-10-20T17:30:00+09:00"


def test_after_a_restart_the_same_days_edition_is_not_made_again(setup):
    make, clock, store = setup
    clock.now = datetime(2026, 10, 20, 16, 31, tzinfo=KST)
    first, consumer, producer = make()
    consumer.queue.append(notice("post-close", clock.now))
    first.run_once()
    assert len(asked(first, QueryKind.DAILY)) == 1

    # 재시작: 메모리 기억은 사라지고 작업 기록(job_run)만 남는다. 늦게 온 수동 재수집 알림과 한도 시각 모두 판을 다시 만들지 않는다.
    clock.now = datetime(2026, 10, 20, 17, 40, tzinfo=KST)
    second, consumer, producer = make(daily_settings=ON)
    consumer.queue.append(notice("post-close", clock.now, event_id="5e2a7c1d-3b4f-4d8e-a9c6-0f1b2d3e4a51"))
    second.run_once()
    # 아침 판은 그날 만든 기록이 없어 한도 시각(09:20)이 지났으므로 이때 시작하지만, 마감 후 판은 다시 만들지 않는다.
    assert [run.job_kind.value for run in store.starts] == ["post-close", "morning"]
    [request] = asked(second, QueryKind.DAILY)
    assert request.payload["asOf"] == "2026-10-20T09:20:00+09:00"


def test_when_the_job_log_cannot_be_read_the_edition_is_still_made(setup):
    from fake_job_log import FakeJobLog
    make, clock, _ = setup
    clock.now = datetime(2026, 10, 20, 16, 31, tzinfo=KST)
    unreadable = FakeJobLog(fail_reads=True)
    runner, consumer, producer = make(job_log=unreadable)
    consumer.queue.append(notice("post-close", clock.now))
    runner.run_once()
    assert len(asked(runner, QueryKind.DAILY)) == 1


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
                                 declare_topics=lambda _: None, status_store=status, member_store=FakeMembers(),
                                 api_factory=FakeApi)
    runner.open()
    return runner, status


def collection_completed(incomplete=()):
    return FakeMessage("collection.completed", json.dumps({
        "eventId": "00000000-0000-4000-8000-000000000001", "eventType": "collection.completed", "schemaVersion": 1,
        "occurredAt": "2026-10-20T16:15:42+09:00", "producedAt": "2026-10-20T16:15:42+09:00", "correlationId": "c",
        "payload": {"jobType": "collect-news", "scheduledFor": "2026-10-20T16:15:00+09:00",
                    "startedAt": "2026-10-20T16:15:01+09:00", "finishedAt": "2026-10-20T16:15:42+09:00",
                    "publishedCount": 1, "incompleteSymbols": list(incomplete)}}))


def test_every_collection_completed_is_saved_as_received(setup):
    runner, status = status_runner(setup)
    message = collection_completed(incomplete=["TEST02"])
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


def test_collection_completed_only_feeds_the_server_status_and_starts_no_lookup(setup):
    runner, status = status_runner(setup)
    runner._consumer.commit = lambda **_: None
    runner.core.on_news_stored = lambda message, now: pytest.fail("the lookup starts from news.stored")
    runner.handle_message(collection_completed())
    assert len(status.saved) == 1 and runner.api.submitted == []


def test_the_runner_puts_llm_bodies_through_the_final_check_with_the_writers_own_client(setup):
    from briefing.daily_final_check import DailyFinalCheck
    from briefing.move_final_check import MoveFinalCheck
    from briefing.report import LlmWriter

    make, _, store = setup
    llm = FakeLlm()
    runner, _, _ = make(DailyLlmWriter(llm))
    final = runner.daily.final
    assert isinstance(final.adapter, DailyFinalCheck) and final.client is llm and final.job_log is store
    assert final.max_attempts == runner.daily_writer.max_attempts
    # 급변동 작성기가 규칙 기반이면 LLM 본문이 없으므로 마지막 검사도 없다.
    assert runner.research.final is None

    move_llm = FakeLlm()
    runner = main.BriefingRunner("localhost:9092", LlmWriter(move_llm), store, consumer_factory=FakeConsumer,
                                 producer_factory=FakeProducer, declare_topics=lambda _: None, api_factory=FakeApi)
    runner.open()
    final = runner.research.final
    assert isinstance(final.adapter, MoveFinalCheck) and final.client is move_llm and final.max_attempts == 3
    assert runner.daily.final is None
