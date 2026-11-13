"""시세처리기·뉴스처리기 내부 조회 API 호출의 모양. HTTP와 Kafka를 모르며, 호출 구현(http_queries)과 작업 코드가 함께 쓴다.

작업 코드는 조회를 맡기고(submit) 결과는 나중에 콜백으로 받는다. 결과는 수신 루프 스레드에서 전달되므로 작업 상태에
잠금이 필요 없고, 호출이 응답을 기다리거나 재시도 간격을 쉬는 동안에도 다른 작업과 Kafka 받기는 멈추지 않는다.
요청 본문과 응답 본문은 contracts/events/의 옛 요청·응답 이벤트 payload와 같은 JSON이다(docs/local-development.md "내부 조회 API").
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Protocol


class QueryKind(str, Enum):
    """조회 종류. 값은 job_step의 '부른 곳·무엇'(서비스/경로 끝)이다."""

    CONTEXT = "market/context"
    DAILY = "market/daily"
    HISTORY = "market/history"
    ARTICLES = "news/articles"
    FILINGS = "news/filings"

    @property
    def service(self) -> str:
        return self.value.split("/", 1)[0]

    @property
    def path(self) -> str:
        return f"/internal/{self.value}"


@dataclass(frozen=True)
class Query:
    kind: QueryKind
    # job_step을 남길 작업(job_run). 재검색 기사 조회는 재검색을 부른 작업이다.
    job_id: str
    # job_step의 단계 이름(작업 기록 job_log의 단계와 같은 이름)
    stage: str
    correlation_id: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class QueryResult:
    query: Query
    # 응답 본문. 끝내 받지 못했으면 None이고, 그 자료가 빠진 채로 진행한다.
    payload: dict[str, Any] | None
    attempts: int
    # 마지막 실패 종류. 받았으면 None
    failure_kind: str | None = None

    @property
    def received(self) -> bool:
        return self.payload is not None


OnResult = Callable[[QueryResult, datetime], None]


class QueryPort(Protocol):
    def submit(self, query: Query, on_result: OnResult) -> None:
        """조회를 맡기고 바로 돌아온다. on_result는 받았든 끝내 실패했든 꼭 한 번, 수신 루프 스레드에서 불린다."""
