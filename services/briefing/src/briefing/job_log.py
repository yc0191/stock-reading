"""급변동·데일리·재검색이 함께 쓰는 작업 기록 형식과 그것을 넘겨받는 포트. Kafka와 DB는 모른다.

작업이 지나간 일(무응답 재요청, 기사 확인 라운드, LLM 시도, 끝내 실패, 최종 결과)을 한 줄씩 담고, 같은 작업의 줄은
작업 id로 묶는다. 키·수신 주소·보유 수량·금액은 담지 않는다. 저장은 JobLogPort 구현(job_log_store)이 맡는다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

log = logging.getLogger("briefing.job_log")

# 작업 종류: events.JOB_KIND_MOVE(move)와 데일리 판 이름(morning, post-close). 여기서 다시 내보낸다.
from briefing.events import JOB_KIND_MOVE  # noqa: E402,F401

# 기록 종류
TYPE_RESENT = "resent"                  # 무응답이라 같은 요청을 다시 보냄
TYPE_CHECK_ROUND = "check-round"        # 기사 안정 판정을 위한 확인 라운드
TYPE_LLM_REJECTED = "llm-rejected"      # 거부되거나 호출이 실패한 LLM 시도
TYPE_LLM_SUCCEEDED = "llm-succeeded"    # 받아들인 LLM 시도
TYPE_FAILED = "failed"                  # 끝내 실패(단계가 포기됨)
TYPE_MISSING_ITEMS = "missing-items"    # 판은 나갔지만 빠진 항목이 있음
TYPE_RESULT = "result"                  # 작업의 최종 결과. 작업마다 하나

# 급변동 단계: 근거 조회의 어느 요청인지, 또는 LLM 작성
STAGE_COLLECTION = "collection-wait"
STAGE_HISTORY = "market.history"
STAGE_CONTEXT = "market.context"
STAGE_FILINGS = "news.filings"
STAGE_ARTICLES = "news.articles"
STAGE_ARTICLES_RECHECK = "news.articles.recheck"
STAGE_LLM = "llm-report"
STAGE_PUBLISH = "publish"

# 급변동 사유. 끝내 실패한 경우는 ports.FailureReason 값을 그대로 쓴다.
REASON_RESENT = "resent-after-timeout"
REASON_RECHECK_ROUND = "article-recheck-round"
REASON_LLM_REJECTED = "llm-output-rejected"
REASON_LLM_CALL_FAILED = "llm-call-failed"
REASON_LLM_ACCEPTED = "llm-output-accepted"
REASON_PUBLISHED = "published"
REASON_NOT_SAVED = "log-not-saved"


@dataclass(frozen=True)
class JobLogEntry:
    """작업 기록 한 줄."""

    # move, morning, post-close
    job_kind: str
    # 뉴스 재검색 흐름에서 나온 줄인가
    researched: bool
    job_id: str
    # 대상: 급변동은 종목과 변동 시각, 데일리 판은 다루는 범위
    target: Mapping[str, str]
    stage: str
    entry_type: str
    reason: str
    error_type: str | None
    attempts: int
    # 관련 데이터 전부: 요청 범위, 받은 응답, 재요청 기록, 빠진 항목·제외 건수, 검색어, LLM이면 입력·원래 응답·거부 이유
    data: Mapping[str, Any]
    occurred_at: datetime


class JobLogPort(Protocol):
    def record(self, entry: JobLogEntry) -> bool:
        """한 줄을 남긴다. 끝내 저장하지 못했으면 False."""

    def has_failed(self, job_id: str) -> bool:
        """이 작업의 줄 중 끝내 저장하지 못한 것이 있는가."""


class DiscardJobLog:
    """받기만 하고 버린다. 저장소가 필요 없는 곳의 기본 구현이다."""

    def record(self, entry: JobLogEntry) -> bool:
        return True

    def has_failed(self, job_id: str) -> bool:
        return False


def record_result(job_log: JobLogPort, entry: JobLogEntry) -> bool:
    """최종 결과 줄을 남기고, 이 작업의 결과 이벤트를 발행해도 되는지 돌려준다.

    이 작업의 줄이 하나라도 끝내 저장되지 못했으면 False다. 부르는 쪽은 결과 이벤트를 발행하지 않는다.
    종목코드와 티커는 로그에 쓰지 않는다.
    """
    saved = job_log.record(entry)
    if saved and not job_log.has_failed(entry.job_id):
        return True
    log.error("job log was not saved; result event not published: job=%s kind=%s", entry.job_id, entry.job_kind)
    return False


def move_target(symbol: str, move_at: str) -> dict[str, str]:
    return {"symbol": symbol, "moveAt": move_at}


def range_target(range_start: str, range_end: str) -> dict[str, str]:
    return {"rangeStart": range_start, "rangeEnd": range_end}
