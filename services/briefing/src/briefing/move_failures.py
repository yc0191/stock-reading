"""급변동 작업의 실패 기록 형식과 그것을 넘겨받는 포트. Kafka와 DB는 모르며, 지금 구현은 아무 데도 저장하지 않는다.

기존 실패 기록(ports.FailurePort, failed_jobs)은 작업이 끝내 실패했을 때 한 줄만 남긴다. 여기 기록은 어느 단계에서
왜 실패했는지와 관련 자료 전부를 담고, 끝내 성공한 작업의 중간 실패(거부된 LLM 시도, 제한 시간이 지나 다시 보낸 요청,
기사 안정 판정의 확인 라운드)도 하나씩 담는다. 저장할 테이블은 아직 정하지 않았다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

JOB_KIND_MOVE = "move"

# 단계: 근거 조회의 어느 요청인지, 또는 LLM 작성
STAGE_COLLECTION = "collection-wait"
STAGE_HISTORY = "market.history"
STAGE_CONTEXT = "market.context"
STAGE_FILINGS = "news.filings"
STAGE_ARTICLES = "news.articles"
STAGE_ARTICLES_RECHECK = "news.articles.recheck"
STAGE_LLM = "llm-report"

# 사유. 다시 보낸 요청과 기사 확인 라운드는 단계와 사유가 모두 다르다.
REASON_RESENT = "resent-after-timeout"
REASON_RECHECK_ROUND = "article-recheck-round"
REASON_LLM_REJECTED = "llm-output-rejected"
REASON_LLM_CALL_FAILED = "llm-call-failed"
# 작업이 끝내 실패한 경우는 기존 실패 사유(ports.FailureReason) 값을 그대로 쓴다.


@dataclass(frozen=True)
class MoveFailureRecord:
    """실패 하나의 기록. 키·수신 주소·보유 수량·금액은 담지 않는다."""

    job_kind: str
    job_id: str
    # 대상: 종목과 변동 시각
    target: dict[str, str]
    stage: str
    reason: str
    error_type: str
    attempts: int
    # 관련 자료 전부: 요청 범위, 받은·못 받은 응답, 재요청 기록, 빠진 항목, LLM이면 입력·원래 응답·거부 이유
    data: dict[str, Any]
    occurred_at: datetime


class MoveFailurePort(Protocol):
    def record(self, record: MoveFailureRecord) -> None: ...


class DiscardMoveFailures:
    """받기만 하고 버린다. 저장 테이블을 정하기 전까지의 기본 구현이다."""

    def record(self, record: MoveFailureRecord) -> None:
        return None


def move_target(symbol: str, move_at: str) -> dict[str, str]:
    return {"symbol": symbol, "moveAt": move_at}
