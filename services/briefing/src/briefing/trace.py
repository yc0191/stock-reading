"""추적 ID: 이벤트 본문의 correlationId를 Kafka 헤더, 로그 줄, 서비스 간 HTTP 호출로 옮길 때 쓰는 이름과 로그 문맥.

이름의 원본은 docs/local-development.md의 "추적 ID" 절이며, 5개 서비스가 같은 값을 쓴다. Kafka는 import하지 않는다.
"""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator, Sequence

# Kafka 헤더 키이자 로그 문맥 키. 이벤트 본문 칸 이름과 같게 두어 로그를 같은 말로 찾게 한다.
CORRELATION_ID_KEY = "correlationId"
# 서비스 간 HTTP 호출에 싣는 추적 ID 헤더 이름. 값은 이벤트 본문의 correlationId와 같다.
HTTP_HEADER = "X-Correlation-Id"

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [%(correlationId)s]: %(message)s"

_current: ContextVar[str | None] = ContextVar(CORRELATION_ID_KEY, default=None)


class CorrelationIdFilter(logging.Filter):
    """로그 줄마다 지금 처리 중인 correlationId를 붙인다. 처리 밖의 줄은 빈 값이다.

    로거가 아니라 핸들러에 건다. 로거에 걸면 그 로거에 직접 찍은 줄에만 붙고 하위 로거의 줄에는 붙지 않는다.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not getattr(record, CORRELATION_ID_KEY, None):
            setattr(record, CORRELATION_ID_KEY, _current.get() or "")
        return True


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT)
    for handler in logging.getLogger().handlers:
        handler.addFilter(CorrelationIdFilter())


def current() -> str | None:
    return _current.get()


@contextmanager
def correlation_context(correlation_id: str | None) -> Iterator[None]:
    """이 안에서 찍는 로그 줄에 correlation_id를 붙인다. 값이 없으면 바깥 값도 지워 앞 메시지의 값이 남지 않게 한다."""
    token = _current.set(correlation_id or None)
    try:
        yield
    finally:
        _current.reset(token)


def from_event(value: str | bytes | None) -> str | None:
    """이벤트 본문(JSON) 맨 위의 correlationId. JSON이 아니거나 값이 비었으면 None."""
    if not value:
        return None
    try:
        body = json.loads(value)
    except (ValueError, UnicodeDecodeError):
        return None
    found = body.get(CORRELATION_ID_KEY) if isinstance(body, dict) else None
    return found if isinstance(found, str) and found.strip() else None


def from_headers(headers: Sequence[tuple[str, bytes | None]] | None) -> str | None:
    """Kafka 헤더 목록의 correlationId. 같은 키가 여럿이면 마지막 값을 쓴다."""
    found = None
    for key, value in headers or ():
        if key == CORRELATION_ID_KEY and value:
            found = value.decode("utf-8", errors="replace")
    return found if found and found.strip() else None


def kafka_headers(event_value: dict[str, Any]) -> list[tuple[str, bytes]]:
    """보내는 메시지의 헤더. 본문 correlationId와 같은 값을 싣는다."""
    correlation_id = event_value.get(CORRELATION_ID_KEY)
    if not isinstance(correlation_id, str) or not correlation_id.strip():
        return []
    return [(CORRELATION_ID_KEY, correlation_id.encode("utf-8"))]
