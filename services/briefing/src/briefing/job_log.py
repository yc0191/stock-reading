"""급변동·데일리·재검색이 함께 쓰는 작업 기록 형식과 그것을 넘겨받는 포트. Kafka와 DB는 모른다.

기록은 테이블 둘로 나뉜다. 작업마다 한 줄인 job_run(시작 때 만들고 끝날 때 고침)과, 그 작업에서 일어난 일마다
한 줄인 job_log(job_run을 가리킴)다. 같은 작업의 줄은 작업 id로 묶는다. 키·수신 주소·보유 수량·금액은 담지 않는다.
저장은 JobLogPort 구현(job_log_store)이 맡는다.

작업 종류·상태·작성 방식·기록 종류의 값은 아래 enum이 원본이다. DB의 CHECK 제약은 Flyway 번호 파일에 같은 값으로 적혀 있고, 어긋나면 test_job_log_schema.py가 실패한다.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping, Protocol

from briefing.events import parse_timestamp
from briefing.job_step import JobStep

log = logging.getLogger("briefing.job_log")


# 급변동 레벨. 시세처리기가 정한 감지 유형(market.move의 detectionType)만 본다: prevCloseHigh면 상, 그 밖(window, prevClose,
# 감지 유형이 없는 이전 형식)은 중이다. 상은 리포트·메일을 만들고, 중은 LLM 없이 대표 기사만 작업 기록에 남긴다.
DETECTION_PREV_CLOSE_HIGH = "prevCloseHigh"
LEVEL_HIGH = "high"
LEVEL_MID = "mid"


def level_of(detection_type: str | None) -> str:
    return LEVEL_HIGH if detection_type == DETECTION_PREV_CLOSE_HIGH else LEVEL_MID


def level_of_input(raw_input: str | None) -> str:
    """job_run 입력(market.move 이벤트 JSON 글자)의 감지 유형으로 레벨을 정한다. 읽을 수 없으면 중이다."""
    try:
        return level_of(json.loads(raw_input)["payload"].get("detectionType"))
    except (KeyError, TypeError, ValueError, AttributeError):
        return LEVEL_MID


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
    # 아래 둘은 Kafka 요청·응답 때(2026-11-13까지)의 기록에만 있다. 지금은 조회 시도마다 job_step에 남는다.
    CHECK_ROUND = "check-round"                # 기사 안정 판정을 위한 확인 라운드
    RESENT = "resent"                          # 무응답이라 같은 요청을 다시 보냄
    MISSING_ITEMS = "missing-items"            # 판은 나갔지만 빠진 항목이 있음
    STEP_FAILED = "step-failed"                # 단계가 끝내 실패하거나 포기됨
    MOVES_GROUPED = "moves-grouped"            # 같은 종목의 변동 여럿을 모아 작업 하나로 만듦(모인 변동 전부)
    ETF_OUTLOOK = "etf-outlook"                # 아침 판이 쓴 ETF별 방향 예상(data.outlooks). 마감 후 판이 다시 읽는다
    MEMBER_BRIEFING = "member-briefing"        # 이 작업의 판을 멤버 한 명 몫으로 발행함(data.memberId, 결과 이벤트 ID)
    FINAL_CHECK = "final-check"                # 발행 직전 마지막 검사(코드 검사, LLM 검토, 최종으로 고른 본문과 이유)
    SENTENCE_FIXED = "sentence-fixed"          # 마지막 검사의 검토로 문장 하나를 고침(before·after)
    REPRESENTATIVE_ARTICLE = "representative-article"  # 레벨 중 급변동이 고른 대표 기사(없으면 관련 기사 없음)


# 급변동 단계: 근거 조회의 어느 조회인지, 또는 LLM 작성. 조회 단계 이름은 job_step의 단계에도 쓴다.
STAGE_COLLECTION = "collection-wait"
STAGE_HISTORY = "market.history"
# 레벨 상 메일의 장중 차트용 시세 조회(같은 시세 이력 조회를 다른 구간·간격으로 한 번 더 부른다)
STAGE_CHART_HISTORY = "market.chart-history"
STAGE_CONTEXT = "market.context"
STAGE_FILINGS = "news.filings"
STAGE_ARTICLES = "news.articles"
STAGE_LLM = "llm-report"
STAGE_PUBLISH = "publish"
STAGE_MOVE_GROUP = "move-group"
STAGE_REPRESENTATIVE = "representative-article"
STAGE_RESTART = "restart"

# 급변동 사유. 끝내 실패한 경우는 ports.FailureReason 값을 그대로 쓴다.
REASON_LLM_REJECTED = "llm-output-rejected"
REASON_LLM_CALL_FAILED = "llm-call-failed"
REASON_LLM_ACCEPTED = "llm-output-accepted"
REASON_PUBLISHED = "published"
REASON_NOT_SAVED = "log-not-saved"
REASON_GROUPED = "grouped-by-symbol"
# 레벨 중 급변동의 대표 기사를 고른 근거(data.basis): 같은 사건 묶음, 변동 직전 가장 최근 기사, 기사 없음
REASON_REPRESENTATIVE_GROUP = "group"
REASON_REPRESENTATIVE_LATEST = "latest-before"
REASON_REPRESENTATIVE_NONE = "no-article"
# 재시작으로 끝나지 못한 작업의 실패 사유(job_run.failure_reason). 코드의 단계 실패 사유(ports.FailureReason)와 달리
# 작업이 아니라 프로세스가 원인이라 여기에 둔다.
REASON_INTERRUPTED = "interrupted_by_restart"
# 장 시작~아침 판 시작 사이 급변동을 따로 보내지 않고 아침 판에 합쳤다(성공으로 끝난 작업의 사유이며 리포트는 없다)
REASON_MERGED_INTO_MORNING = "merged_into_morning"
# 최종 본문에 인용한 근거가 하나도 없어 결과 이벤트를 발행하지 않았다. 작업 상태는 succeeded, 발행 이벤트는 없다.
REASON_NO_EVIDENCE = "no_evidence"


@dataclass(frozen=True)
class JobStart:
    """작업 시작 줄(job_run). 작업을 시작시킨 이벤트 원문을 그대로 담는다."""

    job_id: str
    job_kind: JobKind
    # 작업을 시작시킨 이벤트(market.move 또는 판 시작 알림 daily.collection.completed)의 원문 JSON. 한도 시각으로 시작한 판은 그 사실을 담은 JSON
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


@dataclass(frozen=True)
class RepresentativeArticle:
    title: str
    publisher: str
    published_at: datetime


@dataclass(frozen=True)
class MoveRun:
    """급변동 작업 한 건(job_run 한 줄)에서 그날 요약에 필요한 값. change_percent는 변동 이벤트의 변동률(%)이다.

    today_change_percent는 전일 종가 대비 오늘 등락(%)이다. 이벤트의 todayChangePercent를 쓰고, 없을 때는 변동률이 전일 종가 대비인
    prevClose 유형만 그 변동률을 쓰며, 그 밖에는 None이다. level은 이벤트의 감지 유형으로 정한다.
    recorded가 True이면 레벨 중이 대표 기사 줄을 남긴 것이고, 그때 article이 None이면 '관련 기사 없음'이다."""

    job_id: str
    symbol: str
    move_at: datetime
    change_percent: float | None
    status: str
    generated_by: str | None
    failure_reason: str | None
    today_change_percent: float | None = None
    level: str = LEVEL_MID
    recorded: bool = False
    article: RepresentativeArticle | None = None


def parse_move_run(job_id: str, raw_input: str, target: str, status: str, generated_by: str | None,
                   failure_reason: str | None) -> MoveRun | None:
    """job_run 한 줄(입력과 대상은 JSON 글자)을 MoveRun으로 바꾼다. 종목이나 변동 시각을 읽을 수 없으면 None이다."""
    try:
        target_data = json.loads(target)
        symbol, move_at = target_data["symbol"], parse_timestamp(target_data["moveAt"])
    except (KeyError, TypeError, ValueError):
        return None
    percent = today = None
    detection_type = None
    try:
        payload = json.loads(raw_input)["payload"]
        percent = float(payload["changePercent"])
        detection_type = payload.get("detectionType")
        if payload.get("todayChangePercent") is not None:
            today = float(payload["todayChangePercent"])
        elif detection_type == "prevClose":
            today = percent
    except (KeyError, TypeError, ValueError, AttributeError):
        pass
    return MoveRun(job_id, symbol, move_at, percent, status, generated_by, failure_reason, today,
                   level_of(detection_type))


def with_representative(run: MoveRun, data: Mapping[str, Any]) -> MoveRun:
    """대표 기사 줄(representative-article)의 data를 읽어 run에 붙인다. 깨진 data는 '기록 없음'으로 둔다."""
    try:
        raw = data["article"]
        if raw is None:
            return replace(run, recorded=True, article=None)
        article = RepresentativeArticle(str(raw["title"]), str(raw["publisher"]), parse_timestamp(raw["publishedAt"]))
    except (KeyError, TypeError, ValueError):
        return run
    return replace(run, recorded=True, article=article)


class JobLogPort(Protocol):
    def start(self, run: JobStart) -> bool:
        """작업 한 줄을 running으로 만든다. 같은 작업을 다시 시작해도 한 줄만 남는다. 끝내 저장하지 못했으면 False."""

    def record(self, entry: JobLogEntry) -> bool:
        """일 한 줄을 남긴다. 끝내 저장하지 못했으면 False."""

    def finish(self, result: JobResult) -> bool:
        """작업 한 줄을 끝 상태로 고친다. 끝내 저장하지 못했으면 False."""

    def has_failed(self, job_id: str) -> bool:
        """이 작업의 줄 중 끝내 저장하지 못한 것이 있는가."""

    def fail_unfinished(self, now: datetime) -> list[str]:
        """시작할 때 부른다. 진행 중으로 남은 이전 실행의 작업을 실패로 끝내고 그 작업 id를 돌려준다."""

    def morning_outlooks(self, day: date) -> list[Mapping[str, Any]] | None:
        """그날(한국 시간) 아침 판이 남긴 etf-outlook 줄의 data를 기록이 늦은 것부터 돌려준다. 읽지 못하면 None."""

    def moves_between(self, since: datetime, until: datetime) -> list[MoveRun] | None:
        """시작 시각이 범위(양끝 포함) 안인 급변동 작업을 시작이 이른 것부터 돌려준다. 읽지 못하면 None."""

    def edition_started(self, kind: JobKind, day: date) -> bool | None:
        """그날(한국 시간) 시작한 이 종류의 작업 줄이 있는가(결과와 상관없이). 데일리 판을 하루 한 번만 만드는 데 쓴다. 읽지 못하면 None."""

    def mark_market_closed(self, kind: JobKind, day: date, event_id: str, at: datetime) -> bool:
        """그날(한국 시간) 이 판을 휴장이라 만들지 않는다고 남긴다. 같은 날 같은 판은 한 줄만 남는다. 끝내 저장하지 못했으면 False."""

    def market_closed(self, kind: JobKind, day: date) -> bool | None:
        """그날(한국 시간) 이 판에 휴장 기록이 있는가. 읽지 못하면 None."""

    def record_step(self, step: JobStep) -> bool:
        """호출 한 번(job_step)을 남긴다. 조회 작업 스레드에서도 불린다. 끝내 저장하지 못했으면 False."""


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

    def fail_unfinished(self, now: datetime) -> list[str]:
        return []

    def morning_outlooks(self, day: date) -> list[Mapping[str, Any]] | None:
        return []

    def moves_between(self, since: datetime, until: datetime) -> list[MoveRun] | None:
        return []

    def edition_started(self, kind: JobKind, day: date) -> bool | None:
        return False

    def mark_market_closed(self, kind: JobKind, day: date, event_id: str, at: datetime) -> bool:
        return True

    def record_step(self, step: JobStep) -> bool:
        return True

    def market_closed(self, kind: JobKind, day: date) -> bool | None:
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
