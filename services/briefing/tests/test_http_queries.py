"""내부 조회 API 호출(HttpQueries): 재시도 규칙, 추적 ID 헤더, 시도마다의 job_step, 기다리는 동안 수신 루프가 멈추지 않음.

이 기계 안에 띄운 가짜 HTTP 서버(시세처리기·뉴스처리기 대신)를 부른다. 바깥으로는 나가지 않는다.
"""
import json
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from briefing.config import QuerySettings
from briefing.http_queries import (
    FAILURE_BAD_RESPONSE,
    FAILURE_CLIENT,
    FAILURE_CONNECT,
    FAILURE_SERVER,
    FAILURE_TIMEOUT,
    HttpQueries,
    base_urls,
)
from briefing.job_step import StepResult
from briefing.queries import Query, QueryKind
from briefing.trace import HTTP_HEADER
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
CORRELATION_ID = "move-TEST01-20261020T160031"
BODY = {"jobId": JOB_ID, "seq": 1, "symbol": "TEST01", "since": "2026-10-20T15:55:31+09:00",
        "until": "2026-10-20T16:05:31+09:00", "maxPoints": 61}


class FakeService:
    """정해 둔 답(상태 코드, 본문, 늦추는 초)을 차례로 주는 HTTP 서버. 받은 요청(경로, 헤더, 본문)을 모은다."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.requests = []
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 - http.server가 정한 이름
                length = int(self.headers.get("Content-Length", 0))
                service.requests.append((self.path, dict(self.headers), json.loads(self.rfile.read(length))))
                status, body, delay = service.answers.pop(0) if service.answers else (200, {}, 0)
                if delay:
                    time.sleep(delay)
                raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def service():
    started = []

    def start(*answers):
        started.append(FakeService(*answers))
        return started[-1]

    yield start
    for s in started:
        s.close()


class Sleeps:
    """재시도 간격을 실제로 쉬지 않고 모은다."""

    def __init__(self):
        self.calls = []

    def __call__(self, seconds):
        self.calls.append(seconds)


def queries(url, steps, sleep=None, **settings):
    return HttpQueries({"market": url, "news": url}, steps, lambda: T,
                       QuerySettings(**settings) if settings else QuerySettings(), sleep=sleep or Sleeps())


def run(api, kind=QueryKind.HISTORY, body=BODY, timeout=10.0):
    """조회 하나를 맡기고 결과가 쌓일 때까지 기다린다(수신 루프의 drain 대신)."""
    results = []
    api.submit(Query(kind, JOB_ID, "market.history", CORRELATION_ID, body), lambda r, now: results.append(r))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for on_result, result in api.drain():
            on_result(result, T)
        if results:
            return results[0]
        time.sleep(0.01)
    raise AssertionError("no result")


# ---- 설정 ----

def test_the_retry_settings_are_the_agreed_values():
    s = QuerySettings()
    assert s.call_timeout == timedelta(seconds=10)
    assert s.max_attempts == 4
    assert s.retry_intervals == (timedelta(seconds=2), timedelta(seconds=5), timedelta(seconds=15))


def test_service_addresses_come_from_the_environment_with_local_defaults():
    assert base_urls({}) == {"market": "http://127.0.0.1:8091", "news": "http://127.0.0.1:8092"}
    assert base_urls({"MARKET_API_URL": " http://market:8091/ ", "NEWS_API_URL": "http://news:8092"}) == {
        "market": "http://market:8091", "news": "http://news:8092"}


def test_each_kind_calls_its_service_path():
    assert [(k.service, k.path) for k in QueryKind] == [
        ("market", "/internal/market/context"), ("market", "/internal/market/daily"),
        ("market", "/internal/market/history"), ("news", "/internal/news/articles"),
        ("news", "/internal/news/filings")]


# ---- 정상 ----

def test_a_call_posts_the_body_with_the_trace_header_and_records_one_step(service):
    fake = service((200, {"jobId": JOB_ID, "seq": 1, "points": []}, 0))
    steps = FakeJobLog()
    result = run(queries(fake.url, steps))

    assert result.received and result.payload == {"jobId": JOB_ID, "seq": 1, "points": []} and result.attempts == 1
    ((path, headers, body),) = fake.requests
    # 추적 ID는 HTTP 요청 헤더 X-Correlation-Id에 이벤트 본문의 correlationId와 같은 값으로 싣는다.
    assert path == "/internal/market/history" and headers[HTTP_HEADER] == CORRELATION_ID
    assert headers["Content-Type"] == "application/json" and body == BODY
    (step,) = steps.steps
    assert (step.job_id, step.step, step.target, step.result, step.attempt, step.correlation_id, step.failure_kind) == (
        JOB_ID, "market.history", "market/history", StepResult.SUCCEEDED, 1, CORRELATION_ID, None)


# ---- 다시 부르기 ----

def test_server_errors_are_retried_with_the_agreed_intervals_and_every_attempt_is_recorded(service):
    fake = service((500, b"", 0), (503, b"", 0), (502, b"", 0), (200, {"ok": True}, 0))
    steps, sleeps = FakeJobLog(), Sleeps()
    result = run(queries(fake.url, steps, sleeps))

    assert result.received and result.attempts == 4
    assert sleeps.calls == [2.0, 5.0, 15.0]
    assert [(s.attempt, s.result, s.failure_kind) for s in steps.steps] == [
        (1, StepResult.FAILED, FAILURE_SERVER), (2, StepResult.FAILED, FAILURE_SERVER),
        (3, StepResult.FAILED, FAILURE_SERVER), (4, StepResult.SUCCEEDED, None)]
    # 다시 부를 때도 같은 본문과 추적 ID를 싣는다.
    assert all(h[HTTP_HEADER] == CORRELATION_ID and b == BODY for _, h, b in fake.requests)


def test_after_four_failed_attempts_the_result_is_not_received_with_the_last_failure(service):
    fake = service(*[(500, b"", 0)] * 5)
    steps, sleeps = FakeJobLog(), Sleeps()
    result = run(queries(fake.url, steps, sleeps))

    assert (result.received, result.attempts, result.failure_kind) == (False, 4, FAILURE_SERVER)
    assert len(fake.requests) == 4 and sleeps.calls == [2.0, 5.0, 15.0]
    assert [s.attempt for s in steps.steps] == [1, 2, 3, 4]


def test_a_client_error_is_not_retried(service):
    fake = service((400, b"", 0))
    steps, sleeps = FakeJobLog(), Sleeps()
    result = run(queries(fake.url, steps, sleeps))

    assert (result.received, result.attempts, result.failure_kind) == (False, 1, FAILURE_CLIENT)
    assert sleeps.calls == [] and len(steps.steps) == 1


def test_an_answer_that_is_not_a_json_object_is_not_retried(service):
    fake = service((200, b"not json", 0))
    result = run(queries(fake.url, FakeJobLog()))
    assert (result.received, result.attempts, result.failure_kind) == (False, 1, FAILURE_BAD_RESPONSE)
    fake = service((200, [1, 2], 0))
    result = run(queries(fake.url, FakeJobLog()))
    assert (result.received, result.failure_kind) == (False, FAILURE_BAD_RESPONSE)


def test_a_slow_answer_times_out_and_is_retried(service):
    fake = service((200, {"late": True}, 0.5), (200, {"ok": True}, 0))
    steps = FakeJobLog()
    result = run(queries(fake.url, steps, call_timeout=timedelta(seconds=0.1)))

    assert result.received and result.attempts == 2
    assert [s.failure_kind for s in steps.steps] == [FAILURE_TIMEOUT, None]


def test_a_service_that_is_not_listening_is_a_connection_failure_and_is_retried():
    steps, sleeps = FakeJobLog(), Sleeps()
    # 띄웠다 닫은 포트라 연결이 거부된다.
    closed = FakeService()
    closed.close()
    result = run(queries(closed.url, steps, sleeps))

    assert (result.received, result.attempts, result.failure_kind) == (False, 4, FAILURE_CONNECT)
    assert sleeps.calls == [2.0, 5.0, 15.0]


# ---- 기다리는 동안 다른 일이 멈추지 않는다 ----

def test_a_waiting_call_does_not_block_the_caller_or_other_calls(service):
    release = threading.Event()

    def post(url, body, headers, timeout):
        if json.loads(body)["seq"] == 1:
            release.wait(5)
        return {"seq": json.loads(body)["seq"]}

    api = HttpQueries({"market": "http://unused", "news": "http://unused"}, FakeJobLog(), lambda: T, post=post)
    done = []
    started = time.monotonic()
    api.submit(Query(QueryKind.HISTORY, JOB_ID, "market.history", CORRELATION_ID, {**BODY, "seq": 1}),
               lambda r, now: done.append(r.payload["seq"]))
    api.submit(Query(QueryKind.HISTORY, JOB_ID, "market.history", CORRELATION_ID, {**BODY, "seq": 2}),
               lambda r, now: done.append(r.payload["seq"]))
    # 맡기는 쪽(수신 루프)은 기다리지 않고 바로 돌아온다.
    assert time.monotonic() - started < 0.5
    # 첫 호출이 응답을 기다리는 동안 둘째 호출은 끝나 결과가 먼저 나온다.
    deadline = time.monotonic() + 5
    while not done and time.monotonic() < deadline:
        for on_result, result in api.drain():
            on_result(result, T)
        time.sleep(0.01)
    assert done == [2]
    release.set()
    deadline = time.monotonic() + 5
    while len(done) < 2 and time.monotonic() < deadline:
        for on_result, result in api.drain():
            on_result(result, T)
        time.sleep(0.01)
    assert done == [2, 1]
    api.close()


def test_retry_waits_happen_on_the_worker_and_not_on_the_caller():
    waited = threading.Event()

    def post(url, body, headers, timeout):
        from briefing.http_queries import CallFailed
        raise CallFailed(FAILURE_SERVER, True)

    def slow_sleep(seconds):
        waited.wait(5)

    api = HttpQueries({"market": "http://unused", "news": "http://unused"}, FakeJobLog(), lambda: T, post=post,
                      sleep=slow_sleep)
    started = time.monotonic()
    api.submit(Query(QueryKind.ARTICLES, JOB_ID, "news.articles", CORRELATION_ID, BODY), lambda r, now: None)
    assert time.monotonic() - started < 0.5 and api.drain() == []
    waited.set()
    api.close()


def test_an_unexpected_error_still_hands_back_a_result_so_the_job_does_not_wait_forever():
    def post(url, body, headers, timeout):
        raise RuntimeError("boom")

    result = run(HttpQueries({"market": "http://unused", "news": "http://unused"}, FakeJobLog(), lambda: T,
                             post=post))
    assert (result.received, result.attempts, result.failure_kind) == (False, 0, "internal-error")


def test_log_lines_of_a_call_carry_its_trace_id_and_never_the_body(service, caplog):
    from briefing.trace import CORRELATION_ID_KEY, CorrelationIdFilter
    caplog.handler.addFilter(CorrelationIdFilter())
    fake = service((500, b"", 0), (200, {"ok": True}, 0))
    with caplog.at_level("WARNING", logger="briefing.http_queries"):
        run(queries(fake.url, FakeJobLog()))

    (record,) = [r for r in caplog.records if r.name == "briefing.http_queries"]
    assert getattr(record, CORRELATION_ID_KEY) == CORRELATION_ID
    assert "TEST01" not in record.getMessage() and fake.url not in record.getMessage()


def test_closing_wakes_a_call_waiting_between_attempts_and_stops_retrying():
    calls = []

    def post(url, body, headers, timeout):
        from briefing.http_queries import CallFailed
        calls.append(1)
        raise CallFailed(FAILURE_SERVER, True)

    api = HttpQueries({"market": "http://unused", "news": "http://unused"}, FakeJobLog(), lambda: T, post=post,
                      settings=QuerySettings(retry_intervals=(timedelta(seconds=30),) * 3))
    api.submit(Query(QueryKind.HISTORY, JOB_ID, "market.history", CORRELATION_ID, BODY), lambda r, now: None)
    deadline = time.monotonic() + 5
    while not calls and time.monotonic() < deadline:
        time.sleep(0.01)
    started = time.monotonic()
    api.close()
    deadline = time.monotonic() + 5
    while not api.drain() and time.monotonic() < deadline:
        time.sleep(0.01)
    # 30초를 쉬지 않고 바로 끝나며 더 부르지 않는다.
    assert time.monotonic() - started < 2 and calls == [1]


# ---- 응답을 읽는 도중의 끊김과 느린 응답 ----

class RawService:
    """요청을 끝까지 읽은 뒤 정해 둔 바이트를 그대로 보내는 소켓 서버. 요청마다 answer(conn)을 부른다.

    요청 본문을 다 읽고 나서 답하므로 클라이언트가 보내는 중에 끊기는 경쟁이 없어 매번 같은 결과가 나온다.
    """

    def __init__(self, answer):
        self.answer = answer
        self.requests = 0
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        self._stop = threading.Event()
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        with conn:
            data = b""
            while b"\r\n\r\n" not in data:
                data += conn.recv(4096)
            head, rest = data.split(b"\r\n\r\n", 1)
            length = next(int(line.split(b":")[1]) for line in head.split(b"\r\n")
                          if line.lower().startswith(b"content-length"))
            while len(rest) < length:
                rest += conn.recv(4096)
            self.requests += 1
            try:
                self.answer(conn, self._stop)
            except OSError:
                pass

    def close(self):
        self._stop.set()
        self.sock.close()


@pytest.fixture
def raw_service():
    started = []

    def start(answer):
        started.append(RawService(answer))
        return started[-1]

    yield start
    for s in started:
        s.close()


def test_a_connection_cut_while_reading_the_answer_is_a_connection_failure_and_is_retried(raw_service):
    def cut(conn, _stop):
        # 100바이트를 약속하고 20바이트만 보낸 뒤 끊는다(배포·재시작 때 응답 도중 끊기는 경우).
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n"
                     + b'{"points": [1, 2, 3')
        conn.shutdown(socket.SHUT_WR)

    fake = raw_service(cut)
    steps, sleeps = FakeJobLog(), Sleeps()
    result = run(queries(fake.url, steps, sleeps))

    assert (result.received, result.attempts, result.failure_kind) == (False, 4, FAILURE_CONNECT)
    assert fake.requests == 4 and sleeps.calls == [2.0, 5.0, 15.0]
    assert [(s.attempt, s.failure_kind) for s in steps.steps] == [(n, FAILURE_CONNECT) for n in (1, 2, 3, 4)]


def test_a_broken_status_line_is_a_connection_failure_and_is_retried(raw_service):
    def garbage(conn, _stop):
        conn.sendall(b"not http\r\n\r\n")

    fake = raw_service(garbage)
    steps = FakeJobLog()
    result = run(queries(fake.url, steps))

    assert (result.received, result.attempts, result.failure_kind) == (False, 4, FAILURE_CONNECT)
    assert len(steps.steps) == 4


def _drip(header_first):
    def answer(conn, stop):
        # 소켓 연산마다의 제한(call_timeout)보다 짧은 간격으로 한 바이트씩 흘려 끝나지 않는 응답을 흉내 낸다.
        if header_first:
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100000\r\n\r\n")
            chunk = b" "
        else:
            chunk = b"H"
        while not stop.wait(0.02):
            conn.sendall(chunk)
    return answer


@pytest.mark.parametrize("header_first", [True, False], ids=["slow-body", "slow-headers"])
def test_an_answer_that_trickles_in_slowly_times_out_at_the_call_deadline_and_is_retried(raw_service, header_first):
    fake = raw_service(_drip(header_first))
    steps, sleeps = FakeJobLog(), Sleeps()
    started = time.monotonic()
    result = run(queries(fake.url, steps, sleeps, call_timeout=timedelta(seconds=0.3)), timeout=10)

    # 한 바이트씩 계속 와도 시도마다 0.3초 마감에 끊긴다(4번이면 넉넉히 잡아 몇 초 안).
    assert time.monotonic() - started < 5
    assert (result.received, result.attempts, result.failure_kind) == (False, 4, FAILURE_TIMEOUT)
    assert sleeps.calls == [2.0, 5.0, 15.0]
    assert [(s.attempt, s.failure_kind) for s in steps.steps] == [(n, FAILURE_TIMEOUT) for n in (1, 2, 3, 4)]
