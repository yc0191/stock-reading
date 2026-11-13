"""리허설: 가짜 시세처리기·뉴스처리기 HTTP 서버와 가짜 LLM으로 실행부 전체(급변동, 재검색, 데일리 판)를 끝까지 돌린다.

조회는 실제 HttpQueries가 이 기계 안의 가짜 서버를 HTTP로 부르고(재시도·추적 ID 헤더·job_step 포함), Kafka는 가짜 소비자·발행기다.
실제 Kafka·DB·LLM·외부 API는 부르지 않는다. 응답 본문은 계약 샘플(옛 응답 이벤트의 payload)을 고쳐 만든다.
"""
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from fake_anthropic import as_reply
from briefing import main
from briefing.config import CoreSettings, DailySettings, QuerySettings
from briefing.daily_report import DailyLlmWriter
from briefing.http_queries import HttpQueries
from briefing.job_step import StepRecordingLlm, StepResult
from briefing.report import LlmWriter
from briefing.research import QueryWriter
from briefing.sentence_research import research_job_id
from briefing.trace import HTTP_HEADER
from contracts import validate
from fake_members import FakeMembers
from test_daily_jobs import MORNING_AT, daily_response, good_output, tick
from test_runner import (
    JOB_ID,
    MOVE,
    FakeConsumer,
    FakeMessage,
    FakeProducer,
    FakeStore,
    completed_message,
    move_message,
    sent,
    stored_envelope,
)

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 15, 43, tzinfo=KST)
# 시험이 오래 걸리지 않게 재시도 간격만 줄인다. 시도 횟수와 규칙은 운영과 같다.
FAST = QuerySettings(call_timeout=timedelta(seconds=2), retry_intervals=(timedelta(milliseconds=20),) * 3)
WEAK = "업종 소식이 영향을 줬을 수 있습니다."
REWRITTEN = "다시 찾은 기사가 영향을 줬을 수 있습니다."


class FakeServices:
    """시세처리기·뉴스처리기 내부 조회 API를 흉내 내는 HTTP 서버 하나. 받은 요청(경로, 추적 ID 헤더, 본문)을 모은다.

    fail_first는 처음 그 경로 요청에 503으로 답할 횟수, delay는 경로마다 답을 늦출 초다.
    """

    def __init__(self, fail_first=None, delay=None):
        self.requests = []
        self.fail_first = dict(fail_first or {})
        self.delay = dict(delay or {})
        services = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 - http.server가 정한 이름
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                services.requests.append((self.path, self.headers.get(HTTP_HEADER), body))
                if services.fail_first.get(self.path, 0) > 0:
                    services.fail_first[self.path] -= 1
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                time.sleep(services.delay.get(self.path, 0))
                raw = json.dumps(answer(self.path, body), ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()

    def paths(self):
        return [path for path, _, _ in self.requests]

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def answer(path, body):
    """요청 본문의 jobId·seq를 되돌려주는 가짜 응답 본문. 모양은 계약 샘플의 payload와 같다."""
    ids = {"jobId": body["jobId"], "seq": body["seq"]}
    if path == "/internal/market/history":
        return {**ids, "symbol": body["symbol"], "truncated": False,
                "points": [{"asOf": "2026-10-20T15:55:31+09:00", "price": 10000},
                           {"asOf": "2026-10-20T16:00:31+09:00", "price": 9850}]}
    if path == "/internal/market/context":
        return {**ids, "symbol": body["symbol"], "tickers": [], "closeRates": [], "truncated": False,
                "background": [{"apiType": "market-summary", "target": None, "status": "missing"}]}
    if path == "/internal/news/articles":
        article = ({"articleId": "r1", "title": "다시 찾은 기사", "summary": "", "publishedAt": "2026-10-20T06:00:00Z",
                    "publisher": "news.example.com", "url": "https://news.example.com/articles/r1"}
                   if "researchJobId" in body else
                   {"articleId": "a1", "title": "테스트 기사", "summary": "요약", "publishedAt": "2026-10-20T07:02:00Z",
                    "publisher": "news.example.com", "url": "https://news.example.com/articles/1"})
        return {**ids, "symbol": body["symbol"], "articles": [article], "truncated": False}
    if path == "/internal/news/filings":
        return {**ids, "symbol": body["symbol"], "filings": [], "truncated": False}
    if path == "/internal/market/daily":
        return {**json.loads(daily_response())["payload"], **ids}
    raise AssertionError(f"unknown path {path}")


class MoveLlm:
    """급변동 보고서 LLM: 처음에는 둘째 원인이 약한 보고서, 다시 쓰기에는 그 원인만, 마지막 검사의 검토에는 문제 없음."""

    def __init__(self):
        self.calls = []

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        data = json.loads(user)
        self.calls.append("review" if "mail" in data else "rewrite" if "rewrite" in data else "report")
        if "mail" in data:
            return json.dumps({"problems": []})
        if "rewrite" in data:
            return json.dumps({"causes": [{"title": REWRITTEN, "detail": REWRITTEN, "refs": ["r1"], "meaning": REWRITTEN,
                                           "meaningRefs": ["r1"], "weak": False, "shortfall": ""}],
                               "insufficientEvidence": False}, ensure_ascii=False)
        cause = {"title": "기사", "detail": "기사가 영향을 줬을 수 있습니다.", "refs": ["a1"],
                 "meaning": "기사가 변동 전에 나왔습니다.", "meaningRefs": ["a1"], "weak": False, "shortfall": ""}
        weak = {**cause, "title": WEAK, "detail": WEAK, "meaning": WEAK, "weak": True,
                "shortfall": "변동 직전의 업종 기사가 더 필요합니다."}
        return json.dumps({"causes": [cause, weak], "insufficientEvidence": False}, ensure_ascii=False)


class SequenceLlm:
    def __init__(self, *outputs):
        self.outputs = list(outputs)

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        return self.outputs.pop(0)


@pytest.fixture
def rehearsal():
    started = []

    def build(fail_first=None, delay=None):
        services = FakeServices(fail_first, delay)
        store = FakeStore()
        clock = lambda: T  # noqa: E731 - 작업 시각은 고정하고 조회만 실제 시간으로 돈다
        created = {}

        def consumer_factory(config):
            created["consumer"] = FakeConsumer(config)
            return created["consumer"]

        def producer_factory(config):
            created["producer"] = FakeProducer(config)
            return created["producer"]

        move_llm = MoveLlm()
        runner = main.BriefingRunner(
            "localhost:9092", LlmWriter(StepRecordingLlm(move_llm, store, clock)), store, clock=clock,
            settings=CoreSettings(move_quiet_period=timedelta(0)),
            consumer_factory=consumer_factory, producer_factory=producer_factory, declare_topics=lambda _: None,
            query_writer=QueryWriter(StepRecordingLlm(SequenceLlm(json.dumps({"queries": ["업종 소식"]})), store,
                                                      clock)),
            daily_writer=DailyLlmWriter(StepRecordingLlm(SequenceLlm(good_output("B1")), store, clock), 3),
            daily_settings=DailySettings(), member_store=FakeMembers(),
            api_factory=lambda: HttpQueries({"market": services.url, "news": services.url}, store, clock, FAST))
        runner.open()
        started.append((runner, services))
        return runner, created["consumer"], created["producer"], store, services, move_llm

    yield build
    for runner, services in started:
        runner.close()
        services.close()


def run_until(runner, condition, timeout=10.0):
    """조건이 맞을 때까지 수신 루프를 한 바퀴씩 돌린다. 조회 결과는 작업 스레드에서 실제 시간으로 온다."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runner.run_once()
        if condition():
            return
        time.sleep(0.01)
    raise AssertionError("rehearsal did not reach the expected state in time")


def research_stored():
    return FakeMessage("news.stored", stored_envelope("00000000-0000-4000-8000-000000000501", {
        "jobType": "research-news", "scheduledFor": "2026-10-20T16:15:43+09:00",
        "startedAt": "2026-10-20T16:15:44+09:00", "incompleteSymbols": [],
        "researchJobId": research_job_id(JOB_ID, 2, 1)}))


def test_a_move_with_a_weak_sentence_flows_through_http_queries_research_and_rewrite_to_one_mail(rehearsal):
    # 가격 이력 첫 호출은 503으로 실패해 한 번 다시 부른다.
    runner, consumer, producer, store, services, move_llm = rehearsal(fail_first={"/internal/market/history": 1})

    consumer.queue.extend([move_message(), completed_message()])
    run_until(runner, lambda: sent(producer, "news.research.requested"))
    # 근거 조회 세 가지(시세 이력, 배경 자료, 기사)는 Kafka가 아니라 HTTP로 갔고 기사는 한 번만 조회했다.
    assert sorted(services.paths()) == ["/internal/market/context", "/internal/market/history",
                                        "/internal/market/history", "/internal/news/articles"]
    (_, research), = sent(producer, "news.research.requested")
    assert validate("news.research.requested.v1.schema.json", research) == []

    consumer.queue.append(research_stored())
    run_until(runner, lambda: sent(producer, "briefing.generated"))

    (_, briefing), = sent(producer, "briefing.generated")
    assert validate("briefing.generated.v1.schema.json", briefing) == []
    text = briefing["payload"]["text"]
    assert REWRITTEN in text and WEAK not in text
    assert [e["articleId"] for e in briefing["payload"]["evidence"]] == ["a1", "r1"]
    assert move_llm.calls == ["report", "rewrite", "review"]
    # 재검색 기사도 저장 끝을 받은 뒤 한 번만 조회했다.
    research_calls = [body for path, _, body in services.requests if "researchJobId" in body]
    assert len(research_calls) == 1
    # 모든 HTTP 호출에 그 변동의 추적 ID가 헤더로 실렸다.
    assert {header for _, header, _ in services.requests} == {MOVE["correlationId"]}
    # 모든 호출(조회 시도와 LLM)이 그 변동 작업의 job_step으로 남았고, 실패한 시도도 한 줄이다.
    steps = store.steps
    assert {s.job_id for s in steps} == {JOB_ID} and {s.correlation_id for s in steps} == {MOVE["correlationId"]}
    history = [(s.attempt, s.result, s.failure_kind) for s in steps if s.target == "market/history"]
    assert history == [(1, StepResult.FAILED, "http-5xx"), (2, StepResult.SUCCEEDED, None)]
    assert sorted(s.target for s in steps if s.target.startswith("news/")) == ["news/articles", "news/articles"]
    assert [s.step for s in steps if s.target.startswith("llm/")] == ["llm"] * 4  # 보고서·검색어·다시 쓰기·검토
    assert store.failures == []


def test_a_morning_edition_flows_through_http_queries_to_the_members_mail(rehearsal):
    runner, consumer, producer, store, services, _ = rehearsal()

    consumer.queue.append(FakeMessage("daily.collection.completed", tick(at=MORNING_AT)))
    run_until(runner, lambda: sent(producer, "briefing.daily.generated"))

    (_, mail), = sent(producer, "briefing.daily.generated")
    assert validate("briefing.daily.generated.v2.schema.json", mail) == []
    assert mail["payload"]["generatedBy"] == "llm"
    assert services.paths()[0] == "/internal/market/daily"
    assert "/internal/news/articles" in services.paths()
    job_id = json.loads(tick(at=MORNING_AT))["eventId"]
    assert {s.job_id for s in store.steps} == {job_id}
    assert {s.target for s in store.steps} >= {"market/daily", "news/articles"}
    assert all(s.result is StepResult.SUCCEEDED for s in store.steps)


def test_a_slow_daily_query_does_not_hold_up_a_move_received_meanwhile(rehearsal):
    # 데일리 자료 응답이 늦는 동안 받은 급변동이 먼저 끝까지 간다. 한 호출이 기다려도 수신 루프와 다른 작업은 멈추지 않는다.
    runner, consumer, producer, store, services, _ = rehearsal(delay={"/internal/market/daily": 1.5})

    consumer.queue.append(FakeMessage("daily.collection.completed", tick(at=MORNING_AT)))
    runner.run_once()
    consumer.queue.extend([move_message(), completed_message()])
    run_until(runner, lambda: sent(producer, "news.research.requested"), timeout=1.0)

    assert sent(producer, "briefing.daily.generated") == []
    run_until(runner, lambda: sent(producer, "briefing.daily.generated"))


def test_a_service_that_stays_down_leaves_four_failed_attempts_and_the_job_fails_cleanly(rehearsal):
    runner, consumer, producer, store, services, _ = rehearsal(fail_first={"/internal/market/history": 10})

    consumer.queue.extend([move_message(), completed_message()])
    run_until(runner, lambda: store.failures)

    (failure,) = store.failures
    assert (failure.reason, failure.attempts, failure.error_type) == ("market_no_response", 4, "http-5xx")
    history = [s for s in store.steps if s.target == "market/history"]
    assert [(s.attempt, s.result) for s in history] == [(n, StepResult.FAILED) for n in (1, 2, 3, 4)]
    assert sent(producer, "briefing.generated") == []
