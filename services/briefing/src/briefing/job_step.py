"""단계별 실행 기록(job_step): 브리핑이 한 모든 호출(시세처리기·뉴스처리기 조회, LLM)을 정상까지 포함해 시도마다 한 줄 남긴다.

job_log는 특이한 일·판단만 남기고, 호출이 언제 시작해 얼마나 걸렸고 몇 번째 시도에 어떻게 끝났는지는 여기에 남는다.
메일 하나의 추적 ID(correlationId)로 이 표와 다섯 서비스의 로그 줄을 함께 찾는다. Kafka와 DB는 모른다.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Iterator, Protocol

from briefing.llm import LlmClient, LlmReply
from briefing.trace import current as current_correlation_id

log = logging.getLogger("briefing.job_step")

STEP_LLM = "llm"
LLM_TARGET_PREFIX = "llm/"


class StepResult(str, Enum):
    """호출 한 번의 결과. 값 제한은 DB 마이그레이션 V5와 같아야 한다."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class JobStep:
    """호출 한 번(시도 하나). job_id는 job_run의 작업이고, 재검색 호출은 재검색을 부른 작업이다."""

    job_id: str
    # 단계: 무엇을 하려던 호출인가(예: history-request, articles-request, llm)
    step: str
    # 부른 곳·무엇(예: market/history, news/articles, llm/<모델>)
    target: str
    started_at: datetime
    finished_at: datetime
    result: StepResult
    attempt: int
    correlation_id: str | None
    # 실패 종류(연결 실패, 시간 초과, 5xx 등). 성공이면 None
    failure_kind: str | None = None


class StepPort(Protocol):
    def record_step(self, step: JobStep) -> bool:
        """저장에 실패해도 예외를 내지 않고 False. 호출 흐름을 막지 않는다. 여러 스레드에서 불린다."""


class DiscardSteps:
    """기록을 버리는 구현. DB 없이 돌리는 테스트와 도구에서 쓴다."""

    def record_step(self, step: JobStep) -> bool:
        return True


_current_job: ContextVar[str | None] = ContextVar("briefingJobId", default=None)


def current_job() -> str | None:
    return _current_job.get()


@contextmanager
def job_scope(job_id: str | None) -> Iterator[None]:
    """이 안에서 부른 LLM 호출을 이 작업의 job_step으로 남긴다. LLM은 어느 작업의 호출인지 모르므로 부르는 쪽이 건다."""
    token = _current_job.set(job_id)
    try:
        yield
    finally:
        _current_job.reset(token)


class StepRecordingLlm:
    """LlmClient를 감싸 호출마다 job_step 한 줄을 남긴다. 응답과 예외는 그대로 넘긴다."""

    def __init__(self, client: LlmClient, steps: StepPort, clock: Callable[[], datetime]) -> None:
        self._client = client
        self._steps = steps
        self._clock = clock
        # 같은 작업 안의 LLM 호출 순번. 호출은 수신 루프 스레드에서만 일어난다.
        self._attempts: dict[str, int] = {}

    def __repr__(self) -> str:
        return f"StepRecordingLlm({self._client!r})"

    def complete(self, system: str, user: str, output_schema: dict[str, Any], addendum: str = "") -> LlmReply:
        started = self._clock()
        try:
            reply = self._client.complete(system, user, output_schema, addendum)
        except Exception as error:
            self._record(started, StepResult.FAILED, type(error).__name__)
            raise
        self._record(started, StepResult.SUCCEEDED, None)
        return reply

    def _record(self, started: datetime, result: StepResult, failure_kind: str | None) -> None:
        job_id = current_job()
        if job_id is None:
            # 작업 밖 호출은 job_run과 이을 수 없어 남기지 못한다. 빠지는 줄이 생기면 job_scope를 거는 곳을 고친다.
            log.warning("LLM call outside a job scope; job_step not recorded")
            return
        attempt = self._attempts.get(job_id, 0) + 1
        self._attempts[job_id] = attempt
        # 진행 중인 작업 수만큼만 기억한다. 오래된 작업의 순번은 버린다.
        while len(self._attempts) > 1000:
            self._attempts.pop(next(iter(self._attempts)))
        model = getattr(self._client, "model", None) or "unknown"
        self._steps.record_step(JobStep(job_id, STEP_LLM, LLM_TARGET_PREFIX + model, started, self._clock(), result,
                                        attempt, current_correlation_id(), failure_kind))
