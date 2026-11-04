"""브리핑 작업들이 함께 쓰는 바깥 출구(요청 발행, 실패 기록)와 실패 사유. Kafka와 DB는 모른다."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from briefing.events import OutboundEvent


class FailureReason(str, Enum):
    COLLECTION_NOT_COVERED = "collection_not_covered"
    MARKET_NO_RESPONSE = "market_no_response"
    NEWS_NO_RESPONSE = "news_no_response"
    NEWS_UNSTABLE = "news_unstable"
    CONTEXT_NO_RESPONSE = "context_no_response"
    FILINGS_NO_RESPONSE = "filings_no_response"
    LLM_ERROR = "llm_error"


@dataclass(frozen=True)
class Failure:
    job_id: str
    symbol: str
    reason: FailureReason
    detail: str
    attempts: int


class RequestPort(Protocol):
    def send(self, event: OutboundEvent) -> None: ...


class FailurePort(Protocol):
    def record(self, failure: Failure) -> None: ...
