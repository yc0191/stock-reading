"""QueryPort의 HTTP 구현: 시세처리기·뉴스처리기 내부 조회 API를 작업 스레드에서 부르고, 결과를 수신 루프가 꺼내 가게 쌓는다.

한 번 호출의 응답 제한·최대 시도·시도 사이 간격은 QuerySettings에서 온다. 연결 실패·시간 초과·5xx만 다시 부르고,
4xx와 해석할 수 없는 응답은 다시 불러도 같으므로 그만둔다. 시도마다 job_step 한 줄을 남긴다. 표준 라이브러리만 쓴다.
"""
from __future__ import annotations

import http.client
import json
import logging
import queue
import socket
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Mapping

from briefing.config import DEFAULT_QUERY_SETTINGS, QuerySettings
from briefing.job_step import JobStep, StepPort, StepResult
from briefing.queries import OnResult, Query, QueryResult
from briefing.trace import HTTP_HEADER, correlation_context

log = logging.getLogger("briefing.http_queries")

# 서비스 주소 환경변수와 로컬 기본값(시세처리기·뉴스처리기 application.yml 의 server.port).
MARKET_URL_ENV = "MARKET_API_URL"
NEWS_URL_ENV = "NEWS_API_URL"
DEFAULT_URLS = {"market": "http://127.0.0.1:8091", "news": "http://127.0.0.1:8092"}

# job_step의 실패 종류
FAILURE_CONNECT = "connect-failed"
FAILURE_TIMEOUT = "timeout"
FAILURE_SERVER = "http-5xx"
FAILURE_CLIENT = "http-4xx"
FAILURE_BAD_RESPONSE = "bad-response"
FAILURE_INTERNAL = "internal-error"


class CallFailed(Exception):
    """호출 한 번의 실패. 내용에는 주소·본문을 담지 않고 종류만 담는다."""

    def __init__(self, kind: str, retryable: bool) -> None:
        super().__init__(kind)
        self.kind = kind
        self.retryable = retryable


Post = Callable[[str, bytes, Mapping[str, str], float], Any]


class _CallDeadline:
    """한 시도의 전체 마감. 넘으면 소켓을 닫아 막혀 있던 읽기를 깨운다.

    소켓 제한(timeout)은 읽기 한 번마다의 제한이라, 서버가 그보다 짧은 간격으로 조금씩 흘려 보내면 시도가 끝나지 않는다.
    """

    def __init__(self, connection: http.client.HTTPConnection, seconds: float) -> None:
        self._connection = connection
        self.expired = False
        self._timer = threading.Timer(seconds, self._cut)
        self._timer.daemon = True

    def __enter__(self) -> "_CallDeadline":
        self._timer.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._timer.cancel()

    def _cut(self) -> None:
        self.expired = True
        sock = self._connection.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def post_json(url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> Any:
    """JSON 본문을 POST하고 응답 JSON을 돌려준다. 실패는 CallFailed로 바꾼다.

    timeout은 연결부터 응답을 다 읽기까지 한 시도 전체의 마감이다.
    """
    parts = urllib.parse.urlsplit(url)
    connection_class = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    connection = connection_class(parts.hostname or "", parts.port, timeout=timeout)
    path = parts.path + (f"?{parts.query}" if parts.query else "")
    deadline = _CallDeadline(connection, timeout)
    try:
        with deadline:
            connection.request("POST", path, body=body, headers={"Content-Type": "application/json", **headers})
            response = connection.getresponse()
            raw = response.read()
            if deadline.expired:
                # 마감에 끊긴 뒤 남은 바이트만 읽고 끝났을 수 있다(길이를 약속하지 않은 응답).
                raise CallFailed(FAILURE_TIMEOUT, True)
    except CallFailed:
        raise
    except (TimeoutError, socket.timeout):
        raise CallFailed(FAILURE_TIMEOUT, True) from None
    except (http.client.HTTPException, OSError):
        # 응답 도중 끊김(IncompleteRead), 잘못된 상태줄 등은 연결 실패로 보고 다시 부른다. 마감에 끊긴 것이면 시간 초과다.
        raise CallFailed(FAILURE_TIMEOUT if deadline.expired else FAILURE_CONNECT, True) from None
    finally:
        connection.close()
    if response.status >= 400:
        raise CallFailed(FAILURE_SERVER if response.status >= 500 else FAILURE_CLIENT, response.status >= 500)
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise CallFailed(FAILURE_BAD_RESPONSE, False) from None


def base_urls(env: Mapping[str, str]) -> dict[str, str]:
    return {"market": env.get(MARKET_URL_ENV, "").strip().rstrip("/") or DEFAULT_URLS["market"],
            "news": env.get(NEWS_URL_ENV, "").strip().rstrip("/") or DEFAULT_URLS["news"]}


class HttpQueries:
    """작업 스레드에서 조회하고 결과를 쌓는다. 수신 루프가 drain()으로 꺼내 콜백을 부른다."""

    def __init__(self, urls: Mapping[str, str], steps: StepPort, clock: Callable[[], datetime],
                 settings: QuerySettings = DEFAULT_QUERY_SETTINGS, post: Post = post_json,
                 sleep: Callable[[float], None] | None = None) -> None:
        self._urls = dict(urls)
        self._steps = steps
        self._clock = clock
        self._settings = settings
        self._post = post
        # 멈출 때 재시도 간격을 쉬던 작업 스레드를 바로 깨운다. 쉬는 중인 스레드가 끝나기를 기다리면 종료가 최대 수십 초 늦어진다.
        self._closing = threading.Event()
        self._sleep = sleep or self._closing.wait
        self._done: queue.SimpleQueue[tuple[OnResult, QueryResult]] = queue.SimpleQueue()
        self._executor = ThreadPoolExecutor(max_workers=settings.max_workers, thread_name_prefix="briefing-query")

    def submit(self, query: Query, on_result: OnResult) -> None:
        self._executor.submit(self._call, query, on_result)

    def drain(self) -> list[tuple[OnResult, QueryResult]]:
        """끝난 호출의 (콜백, 결과)를 모두 꺼낸다. 기다리지 않는다."""
        done = []
        while True:
            try:
                done.append(self._done.get_nowait())
            except queue.Empty:
                return done

    def close(self) -> None:
        # 멈출 때 아직 시작하지 않은 호출은 버리고 재시도 간격을 쉬던 호출은 더 부르지 않는다. 진행 중이던 작업은 다음 시작 때
        # 실패로 표시된다.
        self._closing.set()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _call(self, query: Query, on_result: OnResult) -> None:
        # 작업 스레드의 로그 줄에도 그 작업의 추적 ID를 건다.
        with correlation_context(query.correlation_id):
            try:
                result = self._attempts(query)
            except Exception as error:  # noqa: BLE001 - 어떤 경우에도 결과를 넘겨 작업이 영영 기다리지 않게 한다
                log.error("query %s for job %s stopped by %s", query.kind.value, query.job_id, type(error).__name__)
                result = QueryResult(query, None, 0, FAILURE_INTERNAL)
        self._done.put((on_result, result))

    def _attempts(self, query: Query) -> QueryResult:
        s = self._settings
        url = self._urls[query.kind.service] + query.kind.path
        body = json.dumps(query.payload, ensure_ascii=False).encode("utf-8")
        headers = {HTTP_HEADER: query.correlation_id}
        failure = None
        for attempt in range(1, s.max_attempts + 1):
            started = self._clock()
            try:
                answer = self._post(url, body, headers, s.call_timeout.total_seconds())
                if not isinstance(answer, dict):
                    raise CallFailed(FAILURE_BAD_RESPONSE, False)
            except CallFailed as error:
                failure = error
                self._step(query, attempt, started, StepResult.FAILED, error.kind)
            else:
                self._step(query, attempt, started, StepResult.SUCCEEDED, None)
                return QueryResult(query, answer, attempt)
            if not failure.retryable or attempt == s.max_attempts or self._closing.is_set():
                break
            # 주소·본문(종목코드)은 로그에 쓰지 않는다.
            log.warning("query %s for job %s failed (%s, attempt %d/%d); retrying", query.kind.value, query.job_id,
                        failure.kind, attempt, s.max_attempts)
            self._sleep(s.retry_intervals[min(attempt, len(s.retry_intervals)) - 1].total_seconds())
            if self._closing.is_set():
                break
        log.warning("query %s for job %s gave up after %d attempt(s): %s", query.kind.value, query.job_id, attempt,
                    failure.kind)
        return QueryResult(query, None, attempt, failure.kind)

    def _step(self, query: Query, attempt: int, started: datetime, result: StepResult, kind: str | None) -> None:
        self._steps.record_step(JobStep(query.job_id, query.stage, query.kind.value, started, self._clock(), result,
                                        attempt, query.correlation_id, kind))
