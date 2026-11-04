"""BriefingRunner를 가짜 Kafka로 돌려 토픽·키·확정·제한 시간·연결을 확인한다. 실제 브로커와 DB는 쓰지 않는다."""
import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import pytest

from briefing import main
from briefing.ports import FailureReason
from briefing.report import GENERATED_BY_LLM, LlmWriter, Report, ReportError, RuleBasedWriter
from contracts import sample, validate

KST = timezone(timedelta(hours=9))
MOVE = sample("market.move.sample.json")
JOB_ID = MOVE["eventId"]
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)


class FakeMessage:
    def __init__(self, topic, value, offset=0):
        self._topic, self._value, self._offset = topic, value, offset

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, config):
        self.config = config
        self.queue = deque()
        self.subscribed = None
        self.commits = []
        self.events = []  # (종류, 토픽) 순서 기록: 확정이 처리보다 먼저인지 본다
        self.timeouts = []
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        self.timeouts.append(timeout)
        return self.queue.popleft() if self.queue else None

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.commits.append(message)
        self.events.append(("commit", message.topic()))

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.sent = []

    def produce(self, topic, key, value, on_delivery=None):
        self.sent.append((topic, key, json.loads(value)))

    def flush(self, _timeout=None):
        return 0


class FakeStore:
    def __init__(self):
        self.created = 0
        self.failures = []

    def create_table(self):
        self.created += 1

    def record(self, failure):
        self.failures.append(failure)


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, **kwargs):
        self.now += timedelta(**kwargs)


class FakeLlm:
    def __init__(self, *outputs):
        self.outputs = list(outputs)
        self.calls = 0

    def complete(self, system, user, output_schema):
        self.calls += 1
        return self.outputs.pop(0)


def envelope(event_type, payload, event_id="00000000-0000-4000-8000-000000000001"):
    return json.dumps({
        "eventId": event_id, "eventType": event_type, "schemaVersion": 1,
        "occurredAt": "2026-10-20T16:15:42+09:00", "producedAt": "2026-10-20T16:15:42+09:00",
        "correlationId": MOVE["correlationId"], "payload": payload,
    })


def move_message(as_of="2026-10-20T16:00:31+09:00", event_id=JOB_ID):
    move = json.loads(json.dumps(MOVE))
    move["eventId"] = event_id
    move["payload"]["asOf"] = as_of
    return FakeMessage("market.move", json.dumps(move))


def completed_message(started_at="2026-10-20T16:15:01+09:00", incomplete=()):
    return FakeMessage("collection.completed", envelope("collection.completed", {
        "jobType": "collect-news", "scheduledFor": "2026-10-20T16:15:00+09:00",
        "startedAt": started_at, "finishedAt": "2026-10-20T16:15:42+09:00",
        "publishedCount": 1, "incompleteSymbols": list(incomplete)}))


def history_message(seq):
    return FakeMessage("market.history.responded", envelope("market.history.responded", {
        "jobId": JOB_ID, "seq": seq, "symbol": "TEST01",
        "points": [{"asOf": "2026-10-20T15:55:31+09:00", "price": 10000},
                   {"asOf": "2026-10-20T07:00:31Z", "price": 9850}],
        "truncated": False}))


def context_message(seq, tickers=()):
    return FakeMessage("market.context.responded", envelope("market.context.responded", {
        "jobId": JOB_ID, "seq": seq, "symbol": "TEST01", "tickers": list(tickers), "closeRates": [],
        "background": [{"apiType": "market-summary", "target": None, "status": "missing"}],
        "truncated": False}))


def articles_message(seq, published_at="2026-10-20T07:02:00Z"):
    return FakeMessage("news.articles.responded", envelope("news.articles.responded", {
        "jobId": JOB_ID, "seq": seq, "symbol": "TEST01",
        "articles": [{"articleId": "a1", "title": "테스트 기사", "summary": "요약",
                      "publishedAt": published_at, "publisher": "news.example.com",
                      "url": "https://news.example.com/articles/1"}],
        "truncated": False}))


@pytest.fixture
def setup():
    clock = Clock(datetime(2026, 10, 20, 16, 15, 43, tzinfo=KST))
    store = FakeStore()
    created = {}
    declared = []

    def make(writer=None):
        def consumer_factory(config):
            created["consumer"] = FakeConsumer(config)
            return created["consumer"]

        def producer_factory(config):
            created["producer"] = FakeProducer(config)
            return created["producer"]

        runner = main.BriefingRunner("localhost:9092", writer or RuleBasedWriter(), store, clock=clock,
                                     consumer_factory=consumer_factory, producer_factory=producer_factory,
                                     declare_topics=declared.append)
        runner.open()
        return runner, created["consumer"], created["producer"]

    return make, clock, store, declared


def feed(runner, consumer, *messages):
    for message in messages:
        consumer.queue.append(message)
        runner.run_once()


def sent(producer, topic):
    return [(key, value) for t, key, value in producer.sent if t == topic]


def test_open_declares_topics_creates_the_table_and_subscribes_to_the_six_input_topics(setup):
    make, _, store, declared = setup
    _, consumer, producer = make()

    assert declared == ["localhost:9092"]
    assert store.created == 1
    assert consumer.subscribed == ["market.move", "collection.completed",
                                   "market.history.responded", "news.articles.responded",
                                   "market.context.responded", "news.filings.responded",
                                   "market.daily.responded"]
    assert consumer.config["enable.auto.commit"] is False
    assert producer.config["acks"] == "all"


def test_a_move_flows_to_a_briefing_keyed_by_symbol(setup):
    make, clock, store, _ = setup
    runner, consumer, producer = make()

    feed(runner, consumer, move_message(), completed_message())

    history = sent(producer, "market.history.requested")
    articles = sent(producer, "news.articles.requested")
    assert [key for key, _ in history] == ["TEST01"]
    assert [key for key, _ in articles] == ["TEST01"]
    assert validate("market.history.requested.v1.schema.json", history[0][1]) == []
    assert validate("news.articles.requested.v1.schema.json", articles[0][1]) == []

    # 기사 목록이 연속 두 번 같아야 안정으로 보므로, 재확인 간격이 지난 뒤 두 번째 라운드가 나간다.
    feed(runner, consumer, history_message(1), context_message(1), articles_message(1))
    clock.advance(seconds=10)
    runner.run_once()
    assert len(sent(producer, "news.articles.requested")) == 2
    feed(runner, consumer, articles_message(2))

    briefings = sent(producer, "briefing.generated")
    assert len(briefings) == 1
    key, event = briefings[0]
    assert key == "TEST01"
    assert event["correlationId"] == MOVE["correlationId"]
    assert validate("briefing.generated.v1.schema.json", event) == []
    assert store.failures == []


def filings_message(seq):
    return FakeMessage("news.filings.responded", envelope("news.filings.responded", {
        "jobId": JOB_ID, "seq": seq, "symbol": "TEST01",
        "filings": [{"accessionNumber": "0000000001-26-000001", "tickers": ["TESTA"], "form": "8-K",
                     "items": ["2.02"], "acceptedAt": "2026-10-20T05:30:28+09:00",
                     "url": "https://www.sec.gov/Archives/edgar/data/1/test-8k.htm"}],
        "truncated": False}))


def test_a_us_etf_move_sends_filings_after_context_and_flows_to_a_briefing(setup):
    make, clock, store, _ = setup
    llm = FakeLlm(json.dumps({"causes": [{"title": "공시", "detail": "공시가 영향을 줬을 수 있습니다.",
                                          "refs": ["0000000001-26-000001", "a1"],
                                          "meaning": "공시와 기사가 변동 전에 나왔습니다.",
                                          "meaningRefs": ["0000000001-26-000001"]}],
                              "insufficientEvidence": False}))
    runner, consumer, producer = make(LlmWriter(llm))

    feed(runner, consumer, move_message(), completed_message(), history_message(1), articles_message(1))
    assert sent(producer, "news.filings.requested") == []  # 티커를 받기 전에는 보내지 않는다
    context = sent(producer, "market.context.requested")
    assert [key for key, _ in context] == ["TEST01"]
    assert validate("market.context.requested.v1.schema.json", context[0][1]) == []

    feed(runner, consumer, context_message(1, tickers=["TESTA"]))
    filings = sent(producer, "news.filings.requested")
    assert [key for key, _ in filings] == ["TEST01"]
    assert validate("news.filings.requested.v1.schema.json", filings[0][1]) == []
    assert filings[0][1]["payload"]["tickers"] == ["TESTA"]

    feed(runner, consumer, filings_message(1))
    clock.advance(seconds=10)
    runner.run_once()
    feed(runner, consumer, articles_message(2))

    (_, event), = sent(producer, "briefing.generated")
    assert validate("briefing.generated.v1.schema.json", event) == []
    assert [(e["kind"], e["ref"]) for e in event["payload"]["evidence"]] == [
        ("filing", "0000000001-26-000001"), ("article", "a1")]
    assert store.failures == []


def test_every_message_offset_is_committed_before_it_is_handled(setup):
    make, _, _, _ = setup
    runner, consumer, producer = make()
    move = move_message()

    feed(runner, consumer, move, completed_message())

    assert consumer.commits[0] is move
    assert consumer.events == [("commit", "market.move"), ("commit", "collection.completed")]
    # 확정은 처리보다 먼저다: 확정 시점에는 아직 요청이 하나도 나가지 않았다.
    assert len(consumer.commits) == 2 and len(producer.sent) == 3


def test_malformed_message_is_committed_and_skipped_without_stopping_the_loop(setup):
    make, _, _, _ = setup
    runner, consumer, producer = make()
    bad = FakeMessage("market.move", b"not json")

    feed(runner, consumer, bad, move_message(), completed_message())

    assert consumer.commits[0] is bad
    assert len(sent(producer, "market.history.requested")) == 1


def test_timestamps_with_z_and_kst_offset_are_both_read(setup):
    make, _, _, _ = setup
    runner, consumer, producer = make()

    # 07:00:31Z는 16:00:31+09:00과 같은 시각이다. 완료의 startedAt도 Z 표기로 준다.
    feed(runner, consumer, move_message(as_of="2026-10-20T07:00:31Z"),
         completed_message(started_at="2026-10-20T07:15:01Z"))

    request = sent(producer, "market.history.requested")[0][1]["payload"]
    assert datetime.fromisoformat(request["until"]) == T + timedelta(minutes=5)


def test_poll_waits_only_until_the_next_deadline(setup):
    make, clock, _, _ = setup
    runner, consumer, _ = make()
    feed(runner, consumer, move_message(), completed_message())

    clock.advance(seconds=9, milliseconds=600)
    runner.run_once()

    assert consumer.timeouts[0] == main.MAX_POLL_SECONDS
    assert consumer.timeouts[-1] == pytest.approx(0.4)


def test_response_timeout_is_detected_by_the_timer_and_resends_with_a_new_seq(setup):
    make, clock, store, _ = setup
    runner, consumer, producer = make()
    feed(runner, consumer, move_message(), completed_message())

    for _ in range(2):
        clock.advance(seconds=10)
        runner.run_once()
    assert [v["payload"]["seq"] for _, v in sent(producer, "market.history.requested")] == [1, 2, 3]

    clock.advance(seconds=10)
    runner.run_once()
    # 한 작업은 실패를 한 번만 기록한다. 같은 시각에 끝난 시세 쪽이 먼저 판정된다.
    assert [(f.reason, f.attempts) for f in store.failures] == [(FailureReason.MARKET_NO_RESPONSE, 3)]
    assert sent(producer, "briefing.generated") == []


def test_collection_wait_limit_is_detected_by_the_timer(setup):
    make, clock, store, _ = setup
    runner, consumer, producer = make()
    # 변동보다 먼저 시작한 수집은 쓸 수 없다.
    feed(runner, consumer, move_message(), completed_message(started_at="2026-10-20T16:00:00+09:00"))
    assert producer.sent == []

    clock.advance(minutes=29, seconds=59)
    runner.run_once()
    assert store.failures == []
    clock.advance(seconds=1)
    runner.run_once()

    assert [(f.job_id, f.reason) for f in store.failures] == [(JOB_ID, FailureReason.COLLECTION_NOT_COVERED)]


def test_llm_writer_is_connected_and_marks_the_briefing_as_llm(setup):
    make, clock, _, _ = setup
    llm = FakeLlm(json.dumps({"causes": [{"title": "기사", "detail": "기사가 영향을 줬을 수 있습니다.", "refs": ["a1"],
                                          "meaning": "기사가 변동 전에 나왔습니다.", "meaningRefs": ["a1"]}],
                              "insufficientEvidence": False}))
    runner, consumer, producer = make(LlmWriter(llm))

    feed(runner, consumer, move_message(), completed_message(), history_message(1), context_message(1),
         articles_message(1))
    clock.advance(seconds=10)
    runner.run_once()
    feed(runner, consumer, articles_message(2))

    (_, event), = sent(producer, "briefing.generated")
    assert llm.calls == 1
    assert event["payload"]["generatedBy"] == GENERATED_BY_LLM
    assert [e["articleId"] for e in event["payload"]["evidence"]] == ["a1"]


def test_report_failure_is_recorded_through_the_failure_store(setup):
    make, clock, store, _ = setup

    class FailingWriter:
        def write(self, report_input) -> Report:
            raise ReportError("LLM report failed after 3 attempts", 3)

    runner, consumer, producer = make(FailingWriter())
    feed(runner, consumer, move_message(), completed_message(), history_message(1), context_message(1),
         articles_message(1))
    clock.advance(seconds=10)
    runner.run_once()
    feed(runner, consumer, articles_message(2))

    assert [(f.reason, f.attempts) for f in store.failures] == [(FailureReason.LLM_ERROR, 3)]
    assert sent(producer, "briefing.generated") == []


def test_close_closes_the_consumer(setup):
    make, _, _, _ = setup
    runner, consumer, _ = make()

    runner.close()

    assert consumer.closed


def test_start_and_stop_run_the_loop_in_the_background():
    created = {}

    def consumer_factory(config):
        created["consumer"] = FakeConsumer(config)
        return created["consumer"]

    store = FakeStore()
    runner = main.BriefingRunner("localhost:9092", RuleBasedWriter(), store,
                                 consumer_factory=consumer_factory, producer_factory=FakeProducer,
                                 declare_topics=lambda _bootstrap: None)

    runner.start()
    try:
        assert runner.is_running()
        deadline = time.monotonic() + 2
        while not created["consumer"].timeouts and time.monotonic() < deadline:
            time.sleep(0.01)
        assert created["consumer"].timeouts, "loop did not poll"
    finally:
        runner.stop(timeout=5)

    assert not runner.is_running()
    assert created["consumer"].closed
    assert runner.error is None
    assert store.created == 1


def test_build_runner_falls_back_to_rule_based_without_a_key_and_hides_the_db_password():
    runner = main.build_runner({"KAFKA_BOOTSTRAP": "broker:9092", "ANTHROPIC_API_KEY": "change-me",
                                "BRIEFING_DB_PASSWORD": "p w'x"})

    assert runner.bootstrap == "broker:9092"
    assert isinstance(runner.writer, RuleBasedWriter)
    assert "p w'x" not in repr(runner)


def test_db_conninfo_uses_env_example_keys_and_defaults():
    from psycopg.conninfo import conninfo_to_dict

    defaults = conninfo_to_dict(main.db_conninfo({"BRIEFING_DB_PASSWORD": "secret"}))
    custom = conninfo_to_dict(main.db_conninfo({"POSTGRES_DB": "other", "BRIEFING_DB_USER": "u",
                                                "BRIEFING_DB_PASSWORD": "p w'x"}))

    assert defaults == {"host": "localhost", "port": "5432", "dbname": "stock",
                        "user": "briefing_svc", "password": "secret"}
    assert custom["dbname"] == "other" and custom["user"] == "u" and custom["password"] == "p w'x"
