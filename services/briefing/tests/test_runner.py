"""BriefingRunner를 가짜 Kafka로 돌려 토픽·키·확정·제한 시간·연결을 확인한다. 실제 브로커와 DB는 쓰지 않는다."""
import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import pytest

from fake_anthropic import as_reply
from briefing import main
from briefing.config import CoreSettings
from briefing.ports import FailureReason
from fake_job_log import FakeJobLog
from briefing.report import GENERATED_BY_LLM, LlmWriter, Report, ReportError, RuleBasedWriter
from briefing.research import QueryWriter
from briefing.sentence_research import WHOLE_REPORT, research_job_id
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


class FakeStore(FakeJobLog):

    @property
    def failures(self):
        """끝내 실패한 줄(이전의 실패 테이블 한 줄에 해당)"""
        return self.failed


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

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
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

    def make(writer=None, **runner_options):
        def consumer_factory(config):
            created["consumer"] = FakeConsumer(config)
            return created["consumer"]

        def producer_factory(config):
            created["producer"] = FakeProducer(config)
            return created["producer"]

        # 이 파일의 시험은 변동 하나가 바로 작업이 되는 흐름을 본다. 묶는 흐름은 test_move_group.py에서 본다.
        runner_options.setdefault("settings", CoreSettings(move_quiet_period=timedelta(0)))
        runner = main.BriefingRunner("localhost:9092", writer or RuleBasedWriter(), store, clock=clock,
                                     consumer_factory=consumer_factory, producer_factory=producer_factory,
                                     declare_topics=declared.append, **runner_options)
        runner.open()
        return runner, created["consumer"], created["producer"]

    return make, clock, store, declared


def feed(runner, consumer, *messages):
    for message in messages:
        consumer.queue.append(message)
        runner.run_once()


def sent(producer, topic):
    return [(key, value) for t, key, value in producer.sent if t == topic]


def test_open_declares_topics_cleans_up_unfinished_jobs_and_subscribes_to_the_input_topics(setup):
    make, clock, store, declared = setup
    _, consumer, producer = make()

    assert declared == ["localhost:9092"]
    # 테이블은 Flyway가 만든다. 브리핑은 시작할 때 이전 실행의 남은 작업을 정리하러 한 번만 저장소를 부른다.
    assert store.unfinished_calls == [clock.now]
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


def test_the_early_research_completion_is_routed_to_the_move_job_and_starts_the_evidence_fetch(setup):
    from briefing.jobs import early_research_id
    make, _, store, _ = setup
    runner, consumer, producer = make()
    named = context_message(1)
    body = json.loads(named.value())
    body["payload"]["etf"] = {"name": "가짜 반도체 ETF", "membersTruncated": False, "members": []}
    named = FakeMessage("market.context.responded", json.dumps(body))
    done = FakeMessage("collection.completed", envelope("collection.completed", {
        "jobType": "research-news", "scheduledFor": "2026-10-20T16:15:00+09:00",
        "startedAt": "2026-10-20T16:15:44+09:00", "finishedAt": "2026-10-20T16:15:50+09:00",
        "publishedCount": 3, "incompleteSymbols": [], "researchJobId": early_research_id(JOB_ID)}))

    # 시계가 변동 T+5분보다 뒤라 변동을 받자마자 배경 자료를 먼저 요청한다. 정기 수집 완료는 오지 않는다.
    feed(runner, consumer, move_message(), named)
    (_, research), = sent(producer, "news.research.requested")
    assert research["payload"]["queries"] == ["가짜 반도체 ETF"]
    assert sent(producer, "market.history.requested") == []

    feed(runner, consumer, done)

    assert len(sent(producer, "market.history.requested")) == 1 and len(sent(producer, "news.articles.requested")) == 1
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
    # 나간 것은 T+5분이 지나 ETF 이름을 알려고 먼저 보낸 배경 자료 요청뿐이고, 근거 조회 요청은 없다.
    assert [t for t, _, _ in producer.sent] == ["market.context.requested"]

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


def test_report_failure_is_recorded_through_the_job_log(setup):
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


def test_jobs_left_unfinished_by_the_previous_run_are_only_marked_and_nothing_is_published_or_redone(setup):
    make, _, store, _ = setup
    store.unfinished_ids = ["old-job-1", "old-job-2"]

    _, consumer, producer = make()

    # 표시만 하고 리포트를 다시 만들지 않는다: 이벤트도, 새 작업도, 요청도 없다.
    assert len(store.unfinished_calls) == 1
    assert producer.sent == [] and store.starts == [] and store.entries == []


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
    assert len(store.unfinished_calls) == 1


def research_completed_message(incomplete=()):
    return FakeMessage("collection.completed", envelope("collection.completed", {
        "jobType": "research-news", "scheduledFor": "2026-10-20T16:15:43+09:00",
        "startedAt": "2026-10-20T16:15:44+09:00", "finishedAt": "2026-10-20T16:15:50+09:00",
        "publishedCount": 1, "incompleteSymbols": list(incomplete),
        "researchJobId": research_job_id(JOB_ID, WHOLE_REPORT, 1)}))


def research_articles_message(seq):
    return FakeMessage("news.articles.responded", envelope("news.articles.responded", {
        "jobId": research_job_id(JOB_ID, WHOLE_REPORT, 1), "seq": seq, "symbol": "TEST01",
        "articles": [{"articleId": "r1", "title": "다시 찾은 기사", "summary": "",
                      "publishedAt": "2026-10-20T06:00:00Z", "publisher": "news.example.com",
                      "url": "https://news.example.com/articles/r1"}],
        "truncated": False}))


def test_insufficient_evidence_flows_through_research_request_completion_and_rewrite(setup):
    make, clock, store, declared = setup
    report_llm = FakeLlm(json.dumps({"causes": [], "insufficientEvidence": True}),
                         json.dumps({"causes": [{"title": "다시 찾은 기사", "detail": "다시 찾은 기사가 영향을 줬을 수 있습니다.",
                                                 "refs": ["r1"], "meaning": "다시 찾은 기사가 영향을 줬을 수 있습니다.",
                                                 "meaningRefs": ["r1"]}], "insufficientEvidence": False}))
    query_llm = FakeLlm(json.dumps({"queries": ["가짜검색어"]}))
    runner, consumer, producer = make(LlmWriter(report_llm), query_writer=QueryWriter(query_llm))

    feed(runner, consumer, move_message(), completed_message(), history_message(1), context_message(1),
         articles_message(1))
    clock.advance(seconds=10)
    runner.run_once()
    feed(runner, consumer, articles_message(2))

    (key, request), = sent(producer, "news.research.requested")
    assert key == "TEST01"
    assert validate("news.research.requested.v1.schema.json", request) == []
    assert sent(producer, "briefing.generated") == []

    # 재검색 완료는 근거 조회 작업이 아니라 재검색 쪽으로 가서 researchJobId를 단 기사 조회가 나간다.
    feed(runner, consumer, research_completed_message())
    (_, research_request), = [(k, v) for k, v in sent(producer, "news.articles.requested")
                              if "researchJobId" in v["payload"]]
    # 재검색 작업은 자기 id로 따로 돌므로 기사 조회 순번이 1부터 시작한다.
    assert research_request["payload"]["seq"] == 1
    assert research_request["payload"]["researchJobId"] == research_job_id(JOB_ID, WHOLE_REPORT, 1)
    feed(runner, consumer, research_articles_message(1))
    clock.advance(seconds=10)
    runner.run_once()  # 재검색 쪽 제한 시각도 실행부가 깨워 확인 라운드를 보낸다
    feed(runner, consumer, research_articles_message(2))

    (_, event), = sent(producer, "briefing.generated")
    assert [e["articleId"] for e in event["payload"]["evidence"]] == ["r1"]
    assert report_llm.calls == 2 and query_llm.calls == 1
    assert store.failures == []
    assert len(runner.core._completions) == 1  # 재검색 완료는 정기 수집 완료 목록에 쌓이지 않는다


def test_build_runner_falls_back_to_rule_based_without_a_key_and_hides_the_db_password():
    runner = main.build_runner({"KAFKA_BOOTSTRAP": "broker:9092", "ANTHROPIC_API_KEY": "change-me",
                                "BRIEFING_DB_PASSWORD": "p w'x"})

    assert runner.bootstrap == "broker:9092"
    assert isinstance(runner.writer, RuleBasedWriter)
    assert runner.query_writer is None
    assert "p w'x" not in repr(runner)


def test_db_conninfo_uses_env_example_keys_and_defaults():
    from psycopg.conninfo import conninfo_to_dict

    defaults = conninfo_to_dict(main.db_conninfo({"BRIEFING_DB_PASSWORD": "secret"}))
    custom = conninfo_to_dict(main.db_conninfo({"POSTGRES_DB": "other", "BRIEFING_DB_USER": "u",
                                                "BRIEFING_DB_PASSWORD": "p w'x"}))

    assert defaults == {"host": "localhost", "port": "5432", "dbname": "stock",
                        "user": "briefing_svc", "password": "secret"}
    assert custom["dbname"] == "other" and custom["user"] == "u" and custom["password"] == "p w'x"


# ---- 문장 여러 개 동시 재검색: 다시 쓰기 LLM이 느려도 이미 도착한 응답을 잃지 않는다 ----

SLOW_REWRITE = timedelta(seconds=14)
WEAK_SENTENCES = 5


def weak_report():
    return json.dumps({"causes": [
        {"title": f"문장 {n}", "detail": f"문장 {n}입니다.", "refs": ["a1"], "meaning": f"문장 {n}입니다.",
         "meaningRefs": ["a1"], "weak": True, "shortfall": f"문장 {n}에 자료가 더 필요합니다."}
        for n in range(1, WEAK_SENTENCES + 1)], "insufficientEvidence": False}, ensure_ascii=False)


class SlowRewriteLlm:
    """첫 호출은 약한 문장 5개짜리 보고서, 이후 호출은 다시 쓰기이며 한 번에 SLOW_REWRITE만큼 시간이 걸린다."""

    def __init__(self, clock):
        self.clock, self.calls = clock, 0

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
        self.calls += 1
        if self.calls == 1:
            return weak_report()
        self.clock.advance(seconds=SLOW_REWRITE.total_seconds())
        rewrite = json.loads(user)["rewrite"]
        no, ref = rewrite["sentenceNo"], rewrite["newRefs"][0]
        return json.dumps({"causes": [{"title": f"문장 {no}", "detail": f"다시 쓴 문장 {no}입니다.", "refs": [ref],
                                       "meaning": f"다시 쓴 문장 {no}입니다.", "meaningRefs": [ref],
                                       "weak": False, "shortfall": ""}], "insufficientEvidence": False},
                          ensure_ascii=False)


def sentence_research_id(no):
    return research_job_id(JOB_ID, no, 1)


def fake_news_answers(producer, consumer, answered):
    """아직 답하지 않은 재검색 기사 조회마다 즉시 응답을 큐 끝에 넣는다(뉴스 서비스는 요청에 바로 답한다)."""
    for topic, _, value in producer.sent:
        payload = value["payload"]
        if topic != "news.articles.requested" or "researchJobId" not in payload:
            continue
        key = (payload["jobId"], payload["seq"])
        if key in answered:
            continue
        answered.add(key)
        no = next(n for n in range(1, WEAK_SENTENCES + 1) if sentence_research_id(n) == payload["jobId"])
        consumer.queue.append(FakeMessage("news.articles.responded", envelope("news.articles.responded", {
            "jobId": payload["jobId"], "seq": payload["seq"], "symbol": "TEST01",
            "articles": [{"articleId": f"r{no}", "title": f"다시 찾은 기사 {no}", "summary": "",
                          "publishedAt": "2026-10-20T06:00:00Z", "publisher": "news.example.com",
                          "url": f"https://news.example.com/articles/r{no}"}],
            "truncated": False})))


def research_completed_for(no):
    return FakeMessage("collection.completed", envelope("collection.completed", {
        "jobType": "research-news", "scheduledFor": "2026-10-20T16:15:43+09:00",
        "startedAt": "2026-10-20T16:15:44+09:00", "finishedAt": "2026-10-20T16:15:50+09:00",
        "publishedCount": 1, "incompleteSymbols": [], "researchJobId": sentence_research_id(no)}))


def test_slow_rewrites_do_not_make_other_sentences_time_out_with_their_answers_already_waiting(setup):
    make, clock, store, _ = setup
    query_llm = FakeLlm(*[json.dumps({"queries": [f"가짜검색어 {n}"]}) for n in range(1, WEAK_SENTENCES + 1)])
    runner, consumer, producer = make(LlmWriter(SlowRewriteLlm(clock)), query_writer=QueryWriter(query_llm))

    feed(runner, consumer, move_message(), completed_message(), history_message(1), context_message(1),
         articles_message(1))
    clock.advance(seconds=10)
    runner.run_once()
    feed(runner, consumer, articles_message(2))
    assert len(sent(producer, "news.research.requested")) == WEAK_SENTENCES

    feed(runner, consumer, *[research_completed_for(n) for n in range(1, WEAK_SENTENCES + 1)])
    answered = set()
    for _ in range(200):
        fake_news_answers(producer, consumer, answered)
        if not consumer.queue:
            if runner.sentences.next_deadline() is None:
                break
            clock.advance(seconds=10)  # 응답이 더 없으면 시간이 흘러 확인 라운드·재요청 시각이 온다
        runner.run_once()

    (_, briefing), = sent(producer, "briefing.generated")
    text = briefing["payload"]["text"]
    assert all(f"다시 쓴 문장 {n}입니다." in text for n in range(1, WEAK_SENTENCES + 1))
    assert [e for e in store.entries if e.reason == "articles_no_response"] == []
    assert [e for e in store.entries if e.reason == "articles_resent"] == []


def test_reading_queued_messages_stops_at_the_time_cap_and_goes_on_to_the_ticks(setup):
    make, clock, _, _ = setup
    runner, consumer, _ = make()
    handled = []

    def slow_handle(message):
        handled.append(message)
        clock.advance(seconds=main.MAX_READ_SECONDS / 2)

    runner.handle_message = slow_handle
    consumer.queue.extend(FakeMessage("market.move", "{}", offset=n) for n in range(10))
    runner.run_once()

    assert len(handled) == 2  # 두 번째 처리 뒤 상한에 닿아 멈춘다
    assert len(consumer.queue) == 8  # 나머지는 다음 run_once에서 읽는다


def test_reading_queued_messages_stops_at_the_count_cap(setup):
    make, _, _, _ = setup
    runner, consumer, _ = make()
    handled = []
    runner.handle_message = handled.append
    consumer.queue.extend(FakeMessage("market.move", "{}", offset=n) for n in range(main.MAX_MESSAGES_PER_RUN + 5))
    runner.run_once()

    assert len(handled) == main.MAX_MESSAGES_PER_RUN
