"""마감 후 판 본문 끝에 붙는 서버 상태 절(4줄과 '주의' 한 줄). Kafka와 DB는 모르며 받은 값만 센다.

숫자는 모두 코드가 세고 LLM에 넣지 않는다. 종목코드와 티커는 절에도 로그에도 쓰지 않는다.
범위는 판 범위(since 이상 asOf 이하, 양끝 포함)이고, 뉴스 수집 완료는 끝난 시각(finishedAt),
작업 기록은 시작 시각(started_at), 시세처리기 자료는 받은 시각으로 가른다.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Mapping, Protocol

from briefing.compose import KST
from briefing.events import parse_timestamp

log = logging.getLogger("briefing.status")

NO_DATA = "자료 없음"
JOB_TYPE_RESEARCH = "research-news"
# 수집 완료 이벤트 종류. 정기 수집은 collect-news, 재검색은 research-news다.
COLLECTION_COMPLETED = "collection.completed"

# 시세처리기 운영 상태의 수집 종류와 표시 이름(표시 순서)
COLLECTION_NAMES = (("pre-open", "장 시작 전"), ("quote-1s", "1초 현재가"), ("intraday", "장중"),
                    ("post-close", "장 마감 후"))
QUOTE_KIND = "quote-1s"

# 시세 실패 종류와 표시 이름(표시 순서). 모르는 종류는 계약 이름 그대로 뒤에 붙인다.
QUOTE_FAILURE_NAMES = (
    ("TOKEN_ISSUE_FAILED", "토큰 발급 실패"),
    ("HTTP_ERROR", "HTTP 오류"),
    ("RESULT_CODE_FAILED", "결과 코드 실패"),
    ("EMPTY_PRICE", "빈 현재가"),
    ("PRICE_PARSE_FAILED", "가격 해석 실패"),
    ("CONNECTION_FAILED", "연결 실패"),
    ("TIMEOUT", "시간 초과"),
    ("FIELD_MISMATCH", "칸 불일치"),
    ("TICK_SKIPPED_RATE_LIMITED", "호출 상한 건너뜀"),
    ("TICK_SKIPPED_SUPERSEDED", "새 tick에 밀림"),
    ("TICK_SKIPPED_OTHER", "오래된 tick 건너뜀"),
)
# 주의 대상인 시세 실패(코드·로직을 점검해야 하는 것). 연결·시간 초과·HTTP 오류·깨진 응답은 환경 문제라 뺀다.
ATTENTION_QUOTE_FAILURES = ("RESULT_CODE_FAILED", "PRICE_PARSE_FAILED", "TICK_SKIPPED_RATE_LIMITED",
                            "TOKEN_ISSUE_FAILED")

# 브리핑 실패 사유(ports.FailureReason 값과 재시작 중단 사유)의 표시 이름
FAILURE_REASON_NAMES = {
    "collection_not_covered": "기사 수집 미포함",
    "market_no_response": "시세 무응답",
    "news_no_response": "기사 무응답",
    "news_unstable": "기사 불안정",
    "context_no_response": "배경 자료 무응답",
    "filings_no_response": "공시 무응답",
    "llm_error": "LLM 오류",
    "interrupted_by_restart": "재시작으로 중단",
}

JOB_KIND_MOVE = "move"
JOB_KIND_MORNING = "morning"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_RUNNING = "running"
GENERATED_BY_RULE_BASED = "rule-based"


@dataclass(frozen=True)
class CollectionRecord:
    """받은 collection.completed 한 건. 받은 원문을 그대로 함께 든다."""

    event_id: str
    job_type: str
    scheduled_for: datetime
    started_at: datetime
    finished_at: datetime
    published_count: int
    incomplete_symbols: tuple[str, ...]
    research_job_id: str | None
    raw: str

    def is_research(self) -> bool:
        return self.job_type == JOB_TYPE_RESEARCH or self.research_job_id is not None


@dataclass(frozen=True)
class JobRunRow:
    """작업 기록(job_run) 한 줄 중 서버 상태에 필요한 값."""

    job_id: str
    job_kind: str
    status: str
    failure_reason: str | None
    generated_by: str | None
    started_at: datetime


@dataclass(frozen=True)
class CollectionStat:
    kind: str
    first_received_at: datetime | None
    last_received_at: datetime | None
    received_count: int


@dataclass(frozen=True)
class Operations:
    """market.daily.responded의 운영 상태(operations)를 읽은 값."""

    collections: tuple[CollectionStat, ...]
    quote_failures: Mapping[str, int]
    dropped_messages: int


class StatusPort(Protocol):
    def save_collection(self, record: CollectionRecord) -> bool:
        """수집 완료 한 건을 저장한다. 같은 eventId는 한 줄만 남는다. 끝내 저장하지 못했으면 False."""

    def collections_between(self, since: datetime, until: datetime) -> list[CollectionRecord] | None:
        """끝난 시각이 범위(양끝 포함) 안인 수집 완료. 조회에 실패하면 None."""

    def job_runs_between(self, since: datetime, until: datetime) -> list[JobRunRow] | None:
        """시작 시각이 범위(양끝 포함) 안인 작업 기록. 조회에 실패하면 None."""


@dataclass(frozen=True)
class StatusReport:
    """서버 상태 절. missing은 값을 못 받아 '자료 없음'으로 쓴 부분의 이름이다."""

    text: str
    missing: tuple[str, ...] = ()


def parse_collection(message: str | bytes) -> CollectionRecord:
    """collection.completed 원문을 읽는다. 형식이 잘못되면 ValueError."""
    raw = message.decode("utf-8") if isinstance(message, bytes) else message
    try:
        event = json.loads(raw)
        if event["eventType"] != COLLECTION_COMPLETED:
            raise ValueError(f"unexpected eventType: {event['eventType']}")
        payload = event["payload"]
        research = payload.get("researchJobId")
        return CollectionRecord(
            event_id=str(event["eventId"]),
            job_type=str(payload["jobType"]),
            scheduled_for=parse_timestamp(payload["scheduledFor"]),
            started_at=parse_timestamp(payload["startedAt"]),
            finished_at=parse_timestamp(payload["finishedAt"]),
            published_count=int(payload["publishedCount"]),
            incomplete_symbols=tuple(str(s) for s in payload["incompleteSymbols"]),
            research_job_id=str(research) if research is not None else None,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"malformed collection.completed event: {error!r}") from error


def parse_operations(value: Any) -> Operations | None:
    """응답의 operations 칸을 읽는다. 없거나 모양이 틀리면 None(그 부분을 '자료 없음'으로 쓴다)."""
    if not isinstance(value, dict):
        return None
    try:
        stats = tuple(CollectionStat(
            str(c["kind"]),
            parse_timestamp(c["firstReceivedAt"]) if c["firstReceivedAt"] is not None else None,
            parse_timestamp(c["lastReceivedAt"]) if c["lastReceivedAt"] is not None else None,
            int(c["receivedCount"])) for c in value["collections"])
        failures = {str(k): int(v) for k, v in value["quoteFailures"].items()}
        dropped = sum(int(d["count"]) for d in value["droppedMessages"])
    except (KeyError, TypeError, ValueError, AttributeError):
        log.warning("operations in the daily data response is malformed; the server status will say no data")
        return None
    return Operations(stats, failures, dropped)


# ---- 줄 만들기 ----

def _clock(at: datetime) -> str:
    return f"{at.astimezone(KST):%H:%M}"


def _clock_with_date(at: datetime) -> str:
    return f"{at.astimezone(KST):%m-%d %H:%M}"


def _times(stat: CollectionStat | None) -> str:
    if stat is None or stat.received_count == 0 or stat.first_received_at is None or stat.last_received_at is None:
        return "없음"
    first, last = stat.first_received_at.astimezone(KST), stat.last_received_at.astimezone(KST)
    if first.date() != last.date():
        return f"{_clock_with_date(first)}~{_clock_with_date(last)}"
    a, b = _clock(first), _clock(last)
    return a if a == b else f"{a}~{b}"


def _count(n: int) -> str:
    return f"{n:,}"


def _collection_line(ops: Operations | None) -> str:
    if ops is None:
        return f"- 수집: {NO_DATA}"
    by_kind = {c.kind: c for c in ops.collections}
    return "- 수집: " + " · ".join(f"{name} {_times(by_kind.get(kind))}" for kind, name in COLLECTION_NAMES)


def _quote_failure_parts(failures: Mapping[str, int]) -> list[tuple[str, str, int]]:
    """(계약 이름, 표시 이름, 건수). 0이 아닌 것만, 모르는 종류는 계약 이름 그대로 뒤에 둔다."""
    known = [(key, name, failures.get(key, 0)) for key, name in QUOTE_FAILURE_NAMES]
    names = {key for key, _ in QUOTE_FAILURE_NAMES}
    extra = [(key, key, n) for key, n in failures.items() if key not in names]
    return [part for part in (*known, *extra) if part[2] > 0]


def _quote_line(ops: Operations | None) -> str:
    if ops is None:
        return f"- 시세: {NO_DATA}"
    received = next((c.received_count for c in ops.collections if c.kind == QUOTE_KIND), 0)
    parts = _quote_failure_parts(ops.quote_failures)
    failed = sum(n for _, _, n in parts)
    line = f"- 시세: 받음 {_count(received)} · 실패 {_count(failed)}"
    if failed:
        # 비율의 분모는 시도한 건수(받은 건수 + 실패 건수)다.
        line += f"({failed / (received + failed) * 100:.2f}%) [" + " · ".join(
            f"{name} {_count(n)}" for _, name, n in parts) + "]"
    return line


def _news_line(collections: list[CollectionRecord] | None, current_research_count: int) -> str:
    if collections is None:
        return f"- 뉴스: {NO_DATA}"
    regular = [c for c in collections if not c.is_research()]
    # 이번 판의 재검색은 판 기준 시각 뒤에 끝나 범위 안 수집 완료에 없으므로 보낸 요청 수를 따로 더한다.
    research = sum(1 for c in collections if c.is_research()) + current_research_count
    incomplete = sum(1 for c in regular if c.incomplete_symbols)
    articles = sum(c.published_count for c in regular)
    return (f"- 뉴스: 정기 수집 {len(regular)}회 · 일부 종목 빠짐 {incomplete}회 · 기사 {_count(articles)}건"
            f" · 재검색 {research}회")


@dataclass
class _Moves:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    running: int = 0
    rule_based: int = 0
    reasons: dict[str, int] = field(default_factory=dict)


def _tally_moves(runs: list[JobRunRow]) -> _Moves:
    moves = _Moves()
    for r in runs:
        if r.job_kind != JOB_KIND_MOVE:
            continue
        moves.total += 1
        if r.status == STATUS_SUCCEEDED:
            moves.succeeded += 1
        elif r.status == STATUS_FAILED:
            moves.failed += 1
            reason = r.failure_reason or "unknown"
            moves.reasons[reason] = moves.reasons.get(reason, 0) + 1
        elif r.status == STATUS_RUNNING:
            moves.running += 1
        if r.generated_by == GENERATED_BY_RULE_BASED:
            moves.rule_based += 1
    return moves


def _reason_name(reason: str) -> str:
    return FAILURE_REASON_NAMES.get(reason, reason)


def _reasons_text(reasons: Mapping[str, int]) -> str:
    return " · ".join(f"{_reason_name(r)} {n}" for r, n in sorted(reasons.items()))


def _morning(runs: list[JobRunRow]) -> JobRunRow | None:
    mornings = [r for r in runs if r.job_kind == JOB_KIND_MORNING]
    return max(mornings, key=lambda r: r.started_at) if mornings else None


def _morning_text(morning: JobRunRow | None) -> str:
    if morning is None:
        return "기록 없음"
    names = {STATUS_SUCCEEDED: "성공", STATUS_FAILED: "실패", STATUS_RUNNING: "진행 중"}
    text = names.get(morning.status, morning.status)
    return text + "(규칙 기반)" if morning.generated_by == GENERATED_BY_RULE_BASED else text


def _briefing_line(runs: list[JobRunRow] | None) -> str:
    if runs is None:
        return f"- 브리핑: {NO_DATA}"
    m = _tally_moves(runs)
    failed = f"실패 {m.failed}" + (f"[{_reasons_text(m.reasons)}]" if m.reasons else "")
    return (f"- 브리핑: 급변동 {m.total}건(성공 {m.succeeded} · {failed} · 진행 중 {m.running} · 규칙 기반 {m.rule_based})"
            f" · 아침 판 {_morning_text(_morning(runs))}")


def _attention(ops: Operations | None, collections: list[CollectionRecord] | None, runs: list[JobRunRow] | None,
               as_of: datetime, current_rule_based: bool, stuck_after: timedelta) -> list[str]:
    """코드·로직을 점검해야 하는 상황만. 연결 실패·시간 초과·HTTP 오류·깨진 응답·뉴스 빠짐 같은 환경 문제는 넣지 않는다."""
    items: list[str] = []
    if ops is not None:
        if ops.dropped_messages:
            items.append(f"시세처리기 버린 메시지 {_count(ops.dropped_messages)}")
        names = dict(QUOTE_FAILURE_NAMES)
        for key in ATTENTION_QUOTE_FAILURES:
            n = ops.quote_failures.get(key, 0)
            if n:
                items.append(f"{names[key]} {_count(n)}")
    if collections is None:
        items.append("뉴스 수집 기록 읽기 실패")
    if runs is None:
        items.append("브리핑 기록 읽기 실패")
    else:
        m = _tally_moves(runs)
        if m.rule_based:
            items.append(f"규칙 기반 급변동 {m.rule_based}건")
        if m.failed:
            items.append(f"급변동 리포트 실패 {m.failed}[{_reasons_text(m.reasons)}]")
        stuck = sum(1 for r in runs if r.status == STATUS_RUNNING and as_of - r.started_at > stuck_after)
        if stuck:
            items.append(f"오래 진행 중인 작업 {stuck}건")
        morning = _morning(runs)
        if morning is None:
            items.append("아침 판 기록 없음")
        elif morning.status == STATUS_FAILED:
            items.append("아침 판 실패")
        elif morning.generated_by == GENERATED_BY_RULE_BASED:
            items.append("아침 판 규칙 기반")
    if current_rule_based:
        items.append("마감 후 판 규칙 기반")
    return items


def build_status_report(*, range_start: datetime, as_of: datetime, current_job_id: str, current_rule_based: bool,
                        operations: Operations | None, collections: list[CollectionRecord] | None,
                        runs: list[JobRunRow] | None, stuck_after: timedelta,
                        current_research_count: int = 0) -> StatusReport:
    """서버 상태 4줄과 주의 한 줄. 값을 받지 못한 부분은 '자료 없음'으로 쓰고 missing에 이름을 남긴다."""
    runs_without_current = None if runs is None else [r for r in runs if r.job_id != current_job_id]
    header = f"──────── 서버 상태 ({_clock_with_date(range_start)} ~ {_clock_with_date(as_of)}) ────────"
    attention = _attention(operations, collections, runs_without_current, as_of, current_rule_based, stuck_after)
    lines = [header,
             _collection_line(operations),
             _quote_line(operations),
             _news_line(collections, current_research_count),
             _briefing_line(runs_without_current),
             "- 주의: " + (" · ".join(attention) if attention else "없음")]
    missing = []
    if operations is None:
        missing.append("서버 상태: 수집·시세(시세처리기 운영 상태 없음)")
    if collections is None:
        missing.append("서버 상태: 뉴스(수집 완료 기록 조회 실패)")
    if runs is None:
        missing.append("서버 상태: 브리핑(작업 기록 조회 실패)")
    return StatusReport("\n".join(lines), tuple(missing))
