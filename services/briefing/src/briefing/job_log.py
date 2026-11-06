"""급변동·데일리·재검색이 함께 쓰는 작업 기록 형식과 그것을 넘겨받는 포트. Kafka와 DB는 모른다.

기록은 테이블 둘로 나뉜다. 작업마다 한 줄인 job_run(시작 때 만들고 끝날 때 고침)과, 그 작업에서 일어난 일마다
한 줄인 job_log(job_run을 가리킴)다. 같은 작업의 줄은 작업 id로 묶는다. 키·수신 주소·보유 수량·금액은 담지 않는다.
저장은 JobLogPort 구현(job_log_store)이 맡는다.

작업 종류·상태·작성 방식·기록 종류의 값은 아래 enum이 유일한 원본이다. DB의 CHECK 제약도 시작할 때 이 enum으로 다시 만든다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol

log = logging.getLogger("briefing.job_log")


class JobKind(str, Enum):
    """작업 종류. 급변동과 데일리 판 둘. 재검색 요청 이벤트의 jobKind와 같은 값이다."""

    MOVE = "move"
    MORNING = "morning"
    POST_CLOSE = "post-close"


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class GeneratedBy(str, Enum):
    """본문을 쓴 방식. briefing.generated의 generatedBy와 같은 값이다."""

    LLM = "llm"
    RULE_BASED = "rule-based"


class EntryType(str, Enum):
    """작업 기록(job_log) 한 줄의 종류."""

    REPORT_WRITTEN = "report-written"          # 보고서 LLM 응답을 받아들임
    LLM_REJECTED = "llm-rejected"              # 거부되거나 호출이 실패한 LLM 시도(보고서·검색어·다시 쓰기)
    QUERY_WRITTEN = "query-written"            # 검색어 LLM 응답을 받아들임
    RESEARCH_REQUESTED = "research-requested"  # 문장 하나의 재검색 요청을 보냄
    ARTICLES_RECEIVED = "articles-received"    # 재검색 기사를 받음(받은·쓴·버린 기사와 이유)
    SENTENCE_REWRITTEN = "sentence-rewritten"  # 문장 하나를 다시 씀(before·after)
    CHECK_ROUND = "check-round"                # 기사 안정 판정을 위한 확인 라운드
    RESENT = "resent"                          # 무응답이라 같은 요청을 다시 보냄
    MISSING_ITEMS = "missing-items"            # 판은 나갔지만 빠진 항목이 있음
    STEP_FAILED = "step-failed"                # 단계가 끝내 실패하거나 포기됨


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
class JobStart:
    """작업 시작 줄(job_run). 작업을 시작시킨 이벤트 원문을 그대로 담는다."""

    job_id: str
    job_kind: JobKind
    # 작업을 시작시킨 이벤트(market.move 또는 schedule.tick)의 원문 JSON
    input: str
    # 대상: 급변동은 종목과 변동 시각, 데일리 판은 다루는 범위
    target: Mapping[str, str]
    started_at: datetime


@dataclass(frozen=True)
class JobResult:
    """작업 끝 줄(job_run을 고침)."""

    job_id: str
    status: RunStatus
    finished_at: datetime
    # 끝내 실패했거나, 성공했어도 중간에 포기한 단계가 있으면 그 첫 사유
    failure_reason: str | None = None
    generated_by: GeneratedBy | None = None
    final_text: str | None = None
    # 최종 본문에 근거 약함이 남은 문장 수, 보낸 재검색 요청 수
    weak_count: int = 0
    research_count: int = 0
    # 발행한 결과 이벤트의 eventId(메일의 브리핑 ID). 발행하지 않았으면 None
    result_event_id: str | None = None


@dataclass(frozen=True)
class JobLogEntry:
    """작업 기록 한 줄."""

    job_id: str
    entry_type: EntryType
    stage: str
    reason: str
    occurred_at: datetime
    # 문장 단위로 일어난 일이면 그 문장 번호(1부터)와 그 문장의 재검색 회차
    sentence_no: int | None = None
    round: int | None = None
    # 문장을 고친 줄이면 고치기 전·후 문장
    before: str | None = None
    after: str | None = None
    error_type: str | None = None
    attempts: int = 0
    # 관련 데이터 전부: 요청 범위, 받은 응답, 재요청 기록, 빠진 항목·제외 건수, 검색어, LLM이면 입력·원래 응답·거부 이유
    data: Mapping[str, Any] = field(default_factory=dict)


class JobLogPort(Protocol):
    def start(self, run: JobStart) -> bool:
        """작업 한 줄을 running으로 만든다. 같은 작업을 다시 시작해도 한 줄만 남는다. 끝내 저장하지 못했으면 False."""

    def record(self, entry: JobLogEntry) -> bool:
        """일 한 줄을 남긴다. 끝내 저장하지 못했으면 False."""

    def finish(self, result: JobResult) -> bool:
        """작업 한 줄을 끝 상태로 고친다. 끝내 저장하지 못했으면 False."""

    def has_failed(self, job_id: str) -> bool:
        """이 작업의 줄 중 끝내 저장하지 못한 것이 있는가."""


class DiscardJobLog:
    """받기만 하고 버린다. 저장소가 필요 없는 곳의 기본 구현이다."""

    def start(self, run: JobStart) -> bool:
        return True

    def record(self, entry: JobLogEntry) -> bool:
        return True

    def finish(self, result: JobResult) -> bool:
        return True

    def has_failed(self, job_id: str) -> bool:
        return False


def record_result(job_log: JobLogPort, result: JobResult) -> bool:
    """작업의 최종 결과를 job_run에 적고, 이 작업의 결과 이벤트를 발행해도 되는지 돌려준다.

    결과 저장이 끝내 실패했거나 이 작업의 줄이 하나라도 끝내 저장되지 못했으면 False다.
    부르는 쪽은 결과 이벤트를 발행하지 않는다. 종목코드와 티커는 로그에 쓰지 않는다.
    """
    saved = job_log.finish(result)
    if saved and not job_log.has_failed(result.job_id):
        return True
    log.error("job result was not saved; result event not published: job=%s", result.job_id)
    return False


def move_target(symbol: str, move_at: str) -> dict[str, str]:
    return {"symbol": symbol, "moveAt": move_at}


def range_target(range_start: str, range_end: str) -> dict[str, str]:
    return {"rangeStart": range_start, "rangeEnd": range_end}
