"""변동 한 건의 근거 조회 작업. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: market.move 수신 -> 쓸 수 있는 저장 끝 알림(news.stored) 대기(변동 T+5분이 되면 ETF 이름과 구성 종목 이름으로 먼저
재검색하고, 그 재검색의 저장 끝도 수집 완료로 본다. 둘 중 먼저 온 것으로 진행) -> 시세 이력·배경 자료·기사 조회(내부 조회 API) ->
배경 자료에 미국 종목 티커가 있으면 그 티커로 공시 조회 -> 모든 결과 수신(기사는 저장이 끝난 뒤라 한 번만 조회)
-> 받은 자료 재검사 -> 리포트 입력 완성, 또는 실패 기록(작업 기록 테이블).
"""
from __future__ import annotations

import json
import logging
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Protocol

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import NEWS_STORED, format_timestamp, parse_timestamp, research_request_event, stored_payload
from briefing.evidence import MARKET_WIDE, in_evidence_range
from briefing.job_log import (
    STAGE_ARTICLES,
    STAGE_COLLECTION,
    STAGE_CONTEXT,
    STAGE_FILINGS,
    STAGE_HISTORY,
    DiscardJobLog,
    EntryType,
    JobKind,
    JobLogEntry,
    JobLogPort,
    JobResult,
    JobStart,
    RunStatus,
    move_target,
    record_result,
)
from briefing.job_step import job_scope
from briefing.ports import FailureReason, RequestPort
from briefing.queries import Query, QueryKind, QueryPort, QueryResult
from briefing.research import MAX_QUERY_LENGTH, ResearchReason, ResearchStep
from briefing.sources import (
    BACKGROUND_MISSING,
    BACKGROUND_OK,
    BACKGROUND_TOO_LARGE,
    Article,
    Background,
    CloseRate,
    EtfInfo,
    Exchange,
    Filing,
    PricePoint,
    next_seq,
    parse_articles,
    parse_background,
    parse_close_rates,
    parse_etf_info,
    parse_filings,
)
from briefing.trace import correlation_context

log = logging.getLogger("briefing.jobs")

COLLECT_NEWS = "collect-news"
# 조회 결과가 왔지만 해석하지 못해 받지 못한 것으로 본 경우의 실패 종류
FAILURE_MALFORMED = "MalformedResponse"


@dataclass(frozen=True)
class ReportInput:
    job_id: str
    symbol: str
    correlation_id: str
    as_of: datetime
    previous_price: int
    price: int
    change_percent: float
    price_points: tuple[PricePoint, ...]
    articles: tuple[Article, ...]
    history_truncated: bool = False
    articles_truncated: bool = False
    # 변동 종목에 담긴 미국 종목. 비어 있으면 공시·해외 등락률은 해당 없음이다.
    tickers: tuple[str, ...] = ()
    tickers_truncated: bool = False
    close_rates: tuple[CloseRate, ...] = ()
    filings: tuple[Filing, ...] = ()
    filings_truncated: bool = False
    background: tuple[Background, ...] = ()
    # 변동 종목이 ETF일 때 응답에 담긴 ETF 정보. 이전 형식 응답에는 없다.
    etf: EtfInfo | None = None
    # 응답에 담긴 변동 종목 이름(ETF 또는 주식). 이전 형식 응답에는 없다.
    name: str | None = None
    # market.move를 감지한 시각(봉투의 occurredAt)과 감지 유형. 머리말의 "몇 분 사이"에 쓴다. 이전 형식에는 없을 수 있다.
    detected_at: datetime | None = None
    detection_type: str | None = None
    # 감지한 시세의 전일 종가 대비 오늘 등락률(%). 짧은 시간 변동을 시장의 하루 등락과 비교하고 첫 줄에 쓴다. 이전 형식에는 없다.
    today_change_percent: float | None = None
    # 근거 부족으로 다시 검색해 받은 기사. 기존 기사와 따로 두고, 근거 번호·링크 규칙은 기사와 같다.
    research_articles: tuple[Article, ...] = ()
    research_articles_truncated: bool = False


class ReadyPort(Protocol):
    def on_ready(self, report_input: ReportInput) -> None: ...


@dataclass(frozen=True)
class Completion:
    started_at: datetime
    job_type: str
    incomplete_symbols: frozenset[str]
    # 대기 한도 안에 저장되지 못한 기사 수. 그 기사는 조회에 없을 뿐 근거 조회는 진행한다(근거가 약하면 재검색이 채운다).
    unprocessed: int = 0

    def covers(self, symbol: str, as_of: datetime, settings: CoreSettings) -> bool:
        return (
            self.job_type == COLLECT_NEWS
            and self.started_at >= as_of + settings.delta
            and symbol not in self.incomplete_symbols
            and MARKET_WIDE not in self.incomplete_symbols
        )


class _Phase(Enum):
    WAITING_COLLECTION = "waiting_collection"
    FETCHING = "fetching"
    DONE = "done"


class _Early(Enum):
    """정기 수집을 기다리는 동안 하는 T+5분 재검색의 진행."""

    PENDING = "pending"            # 아직 시각이 안 됐다
    CONTEXT_SENT = "context_sent"  # ETF 이름을 알려고 배경 자료를 먼저 조회함
    REQUESTED = "requested"        # 재검색 요청을 보냄. 저장 끝 알림을 기다린다
    SKIPPED = "skipped"            # 이름을 모르거나 배경 자료가 오지 않아 정기 수집을 기다린다


@dataclass(frozen=True)
class _Context:
    tickers: tuple[str, ...]
    tickers_truncated: bool
    close_rates: tuple[CloseRate, ...]
    background: tuple[Background, ...]
    etf: EtfInfo | None = None
    name: str | None = None


@dataclass
class _Job:
    job_id: str
    symbol: str
    correlation_id: str
    as_of: datetime
    previous_price: int
    price: int
    change_percent: float
    created_at: datetime
    detected_at: datetime | None = None
    detection_type: str | None = None
    today_change_percent: float | None = None
    phase: _Phase = _Phase.WAITING_COLLECTION
    early: _Early = _Early.PENDING
    # 시세·배경 자료·공시·기사: 맡긴 조회의 순번과 시각, 받았는지
    market: Exchange = field(default_factory=Exchange)
    market_points: tuple[PricePoint, ...] | None = None
    market_truncated: bool = False
    context: Exchange = field(default_factory=Exchange)
    context_data: _Context | None = None
    filings: Exchange = field(default_factory=Exchange)
    filings_data: tuple[Filing, ...] = ()
    filings_truncated: bool = False
    news: Exchange = field(default_factory=Exchange)
    news_articles: tuple[Article, ...] | None = None
    news_truncated: bool = False
    # 실패 기록용: 조회마다 맡긴 순번·시각, 조회별 마지막 요청 범위
    request_log: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    request_ranges: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 실패 기록용: 조회별로 받은 응답 본문 원문 그대로
    responses: dict[str, dict[str, Any]] = field(default_factory=dict)


def parse_move(message: str | bytes, now: datetime) -> _Job:
    """market.move 원문을 작업으로 바꾼다. 형식이 잘못되면 ValueError."""
    try:
        move = json.loads(message)
        if move["eventType"] != "market.move":
            raise ValueError(f"unexpected eventType: {move['eventType']}")
        payload = move["payload"]
        return _Job(
            job_id=move["eventId"],
            symbol=payload["symbol"],
            correlation_id=move["correlationId"],
            # 변동 시각 T는 payload.asOf 하나로만 읽는다(detectionType·moveStartedAt 유무와 무관).
            as_of=parse_timestamp(payload["asOf"]),
            previous_price=int(payload["previousPrice"]),
            price=int(payload["price"]),
            change_percent=float(payload["changePercent"]),
            created_at=now,
            detected_at=parse_timestamp(move["occurredAt"]) if move.get("occurredAt") else None,
            detection_type=payload.get("detectionType"),
            today_change_percent=_optional_float(payload.get("todayChangePercent")),
        )
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"malformed market.move event: {error!r}") from error


def _optional_float(value: Any) -> float | None:
    """선택 칸의 숫자. 칸이 없으면 None이고, 숫자가 아니면 형식 오류로 본다."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"not a number: {value!r}")
    return float(value)


@dataclass
class BriefingCore:
    requests: RequestPort
    api: QueryPort
    ready: ReadyPort
    settings: CoreSettings = DEFAULT_SETTINGS
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    _jobs: OrderedDict[str, _Job] = field(default_factory=OrderedDict, init=False)
    _completions: deque[Completion] = field(init=False)
    # T+5분 재검색의 재검색 작업 id -> 급변동 작업 id
    _early_ids: OrderedDict[str, str] = field(default_factory=OrderedDict, init=False)

    def __post_init__(self) -> None:
        self._completions = deque(maxlen=self.settings.recent_completions)

    # ---- 입력 ----

    def on_move(self, message: str | bytes, now: datetime) -> None:
        """market.move 수신. 같은 eventId는 한 번만 처리한다. 형식이 잘못되면 ValueError."""
        job = parse_move(message, now)

        if job.job_id in self._jobs:
            log.info("duplicate market.move %s ignored", job.job_id)
            return
        self._jobs[job.job_id] = job
        while len(self._jobs) > self.settings.remembered_jobs:
            self._jobs.popitem(last=False)
        # 작업 한 줄은 시작 때 만들고 끝날 때 고친다. 입력은 시작시킨 이벤트 원문이다.
        raw = message.decode("utf-8") if isinstance(message, bytes) else message
        self.job_log.start(JobStart(job.job_id, JobKind.MOVE, raw, move_target(job.symbol, format_timestamp(job.as_of)),
                                    now))

        # 저장 끝 알림이 변동 알림보다 먼저 도착했을 수 있어 보관분부터 확인한다.
        if any(c.covers(job.symbol, job.as_of, self.settings) for c in self._completions):
            self._start_fetching(job, now)
        else:
            self._tick_early(job, now)

    def on_research_stored(self, message: str | bytes, now: datetime) -> bool:
        """T+5분 재검색의 저장 끝 알림이면 정기 수집 완료와 같이 '수집 완료'로 보고 True. 그 밖의 알림은 False로 돌려준다."""
        try:
            payload = stored_payload(message)
            research_job_id = payload.get("researchJobId")
            if research_job_id is None:
                return False
            incomplete = frozenset(payload["incompleteSymbols"])
        except (KeyError, TypeError, AttributeError) as error:
            raise ValueError(f"malformed {NEWS_STORED} event: {error!r}") from error

        job = self._jobs.get(self._early_ids.get(research_job_id, ""))
        if job is None:
            return False
        if job.phase is not _Phase.WAITING_COLLECTION:
            log.info("early research completion for finished or fetching job %s ignored", job.job_id)
            return True
        if job.symbol in incomplete or MARKET_WIDE in incomplete:
            # 하루 상한·검색 실패로 다 모으지 못했다. 정기 수집을 이어서 기다린다.
            log.warning("early research for job %s did not collect everything", job.job_id)
            self._record(job, ResearchStep.COMPLETION.value, EntryType.STEP_FAILED,
                         ResearchReason.COLLECTION_INCOMPLETE.value, "CollectionIncomplete", 1, now,
                         {"researchJobId": research_job_id, "incompleteSymbolCount": len(incomplete)})
            return True
        _warn_unprocessed(payload, job.job_id)
        log.info("early research completed for job %s; fetching evidence without the regular collection", job.job_id)
        self._start_fetching(job, now)
        return True

    def on_news_stored(self, message: str | bytes, now: datetime) -> None:
        """정기 수집의 저장 끝 알림(news.stored). 수집기가 보낸 기사가 모두 저장된 뒤라 기사는 한 번만 조회한다."""
        try:
            payload = stored_payload(message)
            completion = Completion(
                started_at=parse_timestamp(payload["startedAt"]),
                job_type=payload["jobType"],
                incomplete_symbols=frozenset(payload["incompleteSymbols"]),
                unprocessed=len(payload["unprocessedEventIds"]),
            )
        except (KeyError, TypeError, AttributeError) as error:
            raise ValueError(f"malformed {NEWS_STORED} event: {error!r}") from error

        self._completions.append(completion)
        for job in list(self._jobs.values()):
            if job.phase is _Phase.WAITING_COLLECTION:
                if completion.covers(job.symbol, job.as_of, self.settings):
                    _warn_unprocessed(payload, job.job_id)
                    self._start_fetching(job, now)
                else:
                    log.info("completion skipped for job %s (not usable)", job.job_id)

    def on_tick(self, now: datetime) -> None:
        """기다림이 끝난 작업을 처리한다. 주기적으로, 또는 next_deadline 시각에 부른다."""
        for job in list(self._jobs.values()):
            if job.phase is not _Phase.WAITING_COLLECTION:
                continue
            # 메시지 처리 밖에서 도는 작업이라 그 작업의 추적 ID를 로그 줄에 직접 건다.
            with correlation_context(job.correlation_id):
                if now - job.created_at >= self.settings.collection_wait_limit:
                    self._fail(job, FailureReason.COLLECTION_NOT_COVERED,
                               "no usable news.stored within the wait limit", 0, now)
                else:
                    self._tick_early(job, now)

    def next_deadline(self) -> datetime | None:
        """다음에 on_tick을 불러야 하는 가장 이른 시각. 할 일이 없으면 None. 조회 결과는 시각이 아니라 콜백으로 온다."""
        deadlines: list[datetime] = []
        for job in self._jobs.values():
            if job.phase is _Phase.WAITING_COLLECTION:
                deadlines.append(job.created_at + self.settings.collection_wait_limit)
                if job.early is _Early.PENDING:
                    deadlines.append(job.as_of + self.settings.early_research_delay)
        return min(deadlines) if deadlines else None

    # ---- 조회 결과 (수신 루프 스레드에서 불린다) ----

    def _on_history(self, result: QueryResult, now: datetime) -> None:
        job = self._fetching_job(result)
        if job is None:
            return
        points = truncated = None
        if result.received:
            payload = result.payload
            try:
                points = tuple(PricePoint(parse_timestamp(p["asOf"]), int(p["price"])) for p in payload["points"])
                truncated = bool(payload.get("truncated", False))
            except (KeyError, TypeError, ValueError) as error:
                _malformed(QueryKind.HISTORY, job.job_id, error)
        if points is None:
            job.market.given_up = True
            self._fail(job, FailureReason.MARKET_NO_RESPONSE, "no market history from the market service",
                       result.attempts, now, result.failure_kind or FAILURE_MALFORMED)
            return
        job.market.received = True
        job.responses[STAGE_HISTORY] = result.payload
        job.market_points, job.market_truncated = points, truncated
        self._complete_if_ready(job)

    def _on_context(self, result: QueryResult, now: datetime) -> None:
        job = self._jobs.get(result.query.payload["jobId"])
        if job is None or job.phase is _Phase.DONE:
            log.info("context result for unknown or finished job %s ignored", result.query.payload["jobId"])
            return
        context = None
        if result.received:
            try:
                context = self._parse_context(result.payload)
            except ValueError as error:
                _malformed(QueryKind.CONTEXT, job.job_id, error)
        if job.phase is _Phase.WAITING_COLLECTION:
            self._on_early_context(job, result, context, now)
            return
        if context is None:
            job.context.given_up = True
            self._fail(job, FailureReason.CONTEXT_NO_RESPONSE, "no market context from the market service",
                       result.attempts, now, result.failure_kind or FAILURE_MALFORMED)
            return
        self._accept_context(job, result.payload, context)
        # 미국 종목이 없으면 공시는 해당 없음이라 요청하지 않는다.
        if context.tickers:
            self._send_filings(job, now)
        self._complete_if_ready(job)

    def _accept_context(self, job: _Job, payload: dict[str, Any], context: _Context) -> None:
        job.context.received = True
        job.responses[STAGE_CONTEXT] = payload
        job.context_data = context

    @staticmethod
    def _parse_context(payload: dict) -> _Context:
        def _name_or_none(value: object) -> str | None:
            # 문자열이 아니거나 비면 이름 없음으로 본다(AttributeError로 실행 루프가 멈추지 않게).
            return value.strip() or None if isinstance(value, str) else None

        try:
            return _Context(
                tickers=tuple(payload["tickers"]),
                tickers_truncated=bool(payload["truncated"]),
                close_rates=parse_close_rates(payload["closeRates"]),
                background=tuple(parse_background(b) for b in payload["background"]),
                etf=parse_etf_info(payload.get("etf")),
                name=_name_or_none(payload.get("name")),
            )
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed {QueryKind.CONTEXT.value} response: {error!r}") from error

    def _on_filings(self, result: QueryResult, now: datetime) -> None:
        job = self._fetching_job(result)
        if job is None:
            return
        filings = truncated = None
        if result.received:
            try:
                filings = parse_filings(result.payload["filings"])
                truncated = bool(result.payload["truncated"])
            except (KeyError, TypeError, ValueError) as error:
                _malformed(QueryKind.FILINGS, job.job_id, error)
        if filings is None:
            job.filings.given_up = True
            self._fail(job, FailureReason.FILINGS_NO_RESPONSE, "no filings from the news service",
                       result.attempts, now, result.failure_kind or FAILURE_MALFORMED)
            return
        job.filings.received = True
        job.responses[STAGE_FILINGS] = result.payload
        job.filings_data, job.filings_truncated = filings, truncated
        self._complete_if_ready(job)

    def _on_articles(self, result: QueryResult, now: datetime) -> None:
        job = self._fetching_job(result)
        if job is None:
            return
        articles = None
        if result.received:
            try:
                articles = parse_articles(result.payload["articles"])
            except (KeyError, TypeError, ValueError) as error:
                _malformed(QueryKind.ARTICLES, job.job_id, error)
        if articles is None:
            job.news.given_up = True
            self._fail(job, FailureReason.NEWS_NO_RESPONSE, "no articles from the news service",
                       result.attempts, now, result.failure_kind or FAILURE_MALFORMED)
            return
        # 저장 끝 알림 뒤라 저장이 끝난 목록이다. 다시 조회해 같은지 확인하지 않는다.
        job.news.received = True
        job.responses[STAGE_ARTICLES] = result.payload
        job.news_articles = articles
        job.news_truncated = bool(result.payload.get("truncated", False))
        self._complete_if_ready(job)

    # ---- 내부 ----

    def _fetching_job(self, result: QueryResult) -> _Job | None:
        job_id = result.query.payload["jobId"]
        job = self._jobs.get(job_id)
        if job is None or job.phase is not _Phase.FETCHING:
            log.info("result for unknown or finished job %s ignored", job_id)
            return None
        return job

    def _tick_early(self, job: _Job, now: datetime) -> None:
        """정기 수집을 기다리는 작업의 T+5분 재검색을 시작한다. 배경 자료가 오면 _on_early_context가 이어 간다."""
        if job.early is _Early.PENDING and now >= job.as_of + self.settings.early_research_delay:
            job.early = _Early.CONTEXT_SENT
            self._send_context(job, now)

    def _on_early_context(self, job: _Job, result: QueryResult, context: _Context | None, now: datetime) -> None:
        """T+5분 재검색을 하려고 먼저 받은 배경 자료. ETF 이름을 알면 재검색을 요청하고, 모르면 정기 수집을 기다린다."""
        if context is None:
            log.info("early research skipped for job %s: no context result", job.job_id)
            job.context.given_up = True
            job.early = _Early.SKIPPED
            return
        self._accept_context(job, result.payload, context)
        queries = early_queries(context.etf, self.settings.max_research_queries, context.name)
        if not queries:
            log.info("early research skipped for job %s: ETF and stock name unknown", job.job_id)
            job.early = _Early.SKIPPED
            return
        research_id = early_research_id(job.job_id)
        event = research_request_event(research_id, 1, JobKind.MOVE.value, job.symbol, list(queries),
                                       job.correlation_id, now)
        self._early_ids[research_id] = job.job_id
        while len(self._early_ids) > self.settings.remembered_jobs:
            self._early_ids.popitem(last=False)
        job.early = _Early.REQUESTED
        self.requests.send(event)
        self._record(job, ResearchStep.REQUEST.value, EntryType.RESEARCH_REQUESTED, ResearchReason.REQUESTED.value,
                     None, 1, now, {"researchJobId": research_id, "queries": list(queries),
                                  "request": event.value["payload"]})
        log.info("early research requested job=%s queryCount=%d", job.job_id, len(queries))

    def _start_fetching(self, job: _Job, now: datetime) -> None:
        job.phase = _Phase.FETCHING
        self._send_market(job, now)
        if job.context_data is None:
            # T+5분 재검색을 하려고 먼저 맡긴 조회가 아직 끝나지 않았으면 그 결과를 이어 쓴다.
            if not job.context.awaiting():
                self._send_context(job, now)
        elif job.context_data.tickers:
            # T+5분 재검색을 하려고 배경 자료를 이미 받았다. 다시 받지 않고 공시만 요청한다.
            self._send_filings(job, now)
        self._send_news(job, now)

    def _submit(self, job: _Job, kind: QueryKind, stage: str, exchange: Exchange, now: datetime, on_result,
                payload: dict[str, Any]) -> None:
        payload = {"jobId": job.job_id, "seq": next_seq(exchange, now), **payload}
        job.request_log.setdefault(stage, []).append({"seq": payload["seq"], "sentAt": format_timestamp(now)})
        job.request_ranges[stage] = {k: v for k, v in payload.items() if k not in ("jobId", "seq")}
        self.api.submit(Query(kind, job.job_id, stage, job.correlation_id, payload), on_result)

    def _send_market(self, job: _Job, now: datetime) -> None:
        s = self.settings
        self._submit(job, QueryKind.HISTORY, STAGE_HISTORY, job.market, now, self._on_history, {
            "symbol": job.symbol,
            "since": format_timestamp(job.as_of - s.market_lookback),
            "until": format_timestamp(job.as_of + s.delta),
            "maxPoints": s.max_history_points,
            "intervalSeconds": int(s.history_interval.total_seconds()),
        })

    def _send_context(self, job: _Job, now: datetime) -> None:
        s = self.settings
        self._submit(job, QueryKind.CONTEXT, STAGE_CONTEXT, job.context, now, self._on_context, {
            "symbol": job.symbol,
            "asOf": format_timestamp(job.as_of),
            "since": format_timestamp(job.as_of - s.background_window),
            "maxResponseBytes": s.max_background_bytes,
            "maxTickers": s.max_tickers,
        })

    def _send_filings(self, job: _Job, now: datetime) -> None:
        s = self.settings
        self._submit(job, QueryKind.FILINGS, STAGE_FILINGS, job.filings, now, self._on_filings, {
            "symbol": job.symbol,
            "tickers": list(job.context_data.tickers),
            "since": format_timestamp(job.as_of - s.filings_window),
            "until": format_timestamp(job.as_of),
            "maxFilings": s.max_filings,
        })

    def _send_news(self, job: _Job, now: datetime) -> None:
        s = self.settings
        self._submit(job, QueryKind.ARTICLES, STAGE_ARTICLES, job.news, now, self._on_articles, {
            "symbol": job.symbol,
            "since": format_timestamp(job.as_of - s.evidence_window),
            "until": format_timestamp(job.as_of + s.delta),
            "maxArticles": s.max_articles,
        })

    def _complete_if_ready(self, job: _Job) -> None:
        # 필요한 응답을 모두 받아야 완성한다. 일부만으로 진행하지 않는다.
        context = job.context_data
        if not job.market.received or not job.news.received or context is None:
            return
        if context.tickers and not job.filings.received:
            return
        s = self.settings
        # 응답 쪽이 이미 범위를 지켰어야 하므로, 여기서 걸리는 자료는 응답 쪽 결함 신호로 경고를 남긴다.
        # 다른 종목 태그 기사는 news 조회가 걸러 주며(기사 응답에 태그가 없다) 여기서 다시 검사하지 않는다.
        articles = []
        for article in job.news_articles or ():
            if in_evidence_range(article.published_at, job.as_of, s.evidence_window, s.delta):
                articles.append(article)
            else:
                log.warning("dropping article %s outside the requested range (job=%s)",
                            article.article_id, job.job_id)
        filings = []
        for filing in job.filings_data:
            if in_evidence_range(filing.accepted_at, job.as_of, s.filings_window, timedelta(0)):
                filings.append(filing)
            else:
                log.warning("dropping filing %s accepted outside the requested range (job=%s)",
                            filing.accession_number, job.job_id)
        close_rates = []
        for rate in context.close_rates:
            if _within_state_range(rate.fetched_at, job.as_of, s.background_window) \
                    and _within_state_range(rate.close_at, job.as_of, s.background_window):
                close_rates.append(rate)
            else:
                # 티커는 보유 종목을 드러내므로 로그에 쓰지 않는다.
                log.warning("dropping an overseas close rate received or closed outside the range (job=%s)",
                            job.job_id)
        background = []
        background_excluded = 0
        for item in context.background:
            if item.status != BACKGROUND_MISSING and not _within_state_range(
                    item.fetched_at, job.as_of, s.background_window):
                # 대상(국내 일봉의 종목코드)은 로그에 쓰지 않고 종류만 남긴다.
                log.warning("dropping %s background received outside the range (job=%s)",
                            item.api_type, job.job_id)
                background_excluded += 1
                background.append(Background(item.api_type, item.target, BACKGROUND_MISSING))
            else:
                background.append(item)

        report_input = ReportInput(
            job_id=job.job_id, symbol=job.symbol, correlation_id=job.correlation_id,
            as_of=job.as_of, previous_price=job.previous_price, price=job.price,
            change_percent=job.change_percent, price_points=job.market_points,
            articles=tuple(articles), history_truncated=job.market_truncated,
            articles_truncated=job.news_truncated, tickers=context.tickers,
            tickers_truncated=context.tickers_truncated, close_rates=tuple(close_rates),
            filings=tuple(filings), filings_truncated=job.filings_truncated, background=tuple(background),
            etf=context.etf, name=context.name, detected_at=job.detected_at, detection_type=job.detection_type,
            today_change_percent=job.today_change_percent)
        _log_collection(report_input, _Excluded(
            articles=len(job.news_articles or ()) - len(articles),
            filings=len(job.filings_data) - len(filings),
            close_rates=len(context.close_rates) - len(close_rates),
            background=background_excluded))
        job.phase = _Phase.DONE
        # 리포트를 쓰는 LLM 호출을 이 작업의 job_step으로 남긴다.
        with job_scope(job.job_id):
            self.ready.on_ready(report_input)

    def _fail(self, job: _Job, reason: FailureReason, detail: str, attempts: int, now: datetime,
              error_type: str | None = None) -> None:
        job.phase = _Phase.DONE
        log.warning("job %s failed: %s", job.job_id, reason.value)
        stage, default_error = _FINAL_FAILURES[reason]
        error_type = error_type or default_error
        extra: dict[str, Any] = {"detail": detail}
        if reason is FailureReason.COLLECTION_NOT_COVERED:
            extra["waitStartedAt"] = format_timestamp(job.created_at)
            extra["waitLimitSeconds"] = int(self.settings.collection_wait_limit.total_seconds())
            extra["recentCompletions"] = [
                {"jobType": c.job_type, "startedAt": format_timestamp(c.started_at),
                 "incompleteSymbolCount": len(c.incomplete_symbols)}
                for c in self._completions]
        self._record(job, stage, EntryType.STEP_FAILED, reason.value, error_type, attempts, now, extra)
        # 작업 한 줄에는 사유만 적는다. 관련 데이터는 위의 실패 줄에 있다.
        record_result(self.job_log, JobResult(job.job_id, RunStatus.FAILED, now, failure_reason=reason.value))

    # ---- 작업 기록 ----

    def _record(self, job: _Job, stage: str, entry_type: EntryType, reason: str, error_type: str | None, attempts: int,
                now: datetime, extra: dict[str, Any]) -> None:
        received, not_received = _response_states(job)
        data: dict[str, Any] = {
            "requestRange": job.request_ranges.get(stage, {}),
            "requestRanges": dict(job.request_ranges),
            "requests": {k: list(v) for k, v in job.request_log.items()},
            "received": received,
            # 받은 응답의 내용 전부. 응답 payload를 가공하지 않고 담는다(배경 자료 원문, 지수별 계산값 포함).
            "receivedResponses": {stage: job.responses[stage] for stage in received},
            "notReceived": not_received,
            # 저장 끝을 기다리다 끝났으면 조회는 하나도 나가지 않았고 빠진 것은 저장 끝 알림이다.
            "missingItems": [NEWS_STORED] if stage == STAGE_COLLECTION else not_received,
        }
        data.update(extra)
        self.job_log.record(JobLogEntry(
            job_id=job.job_id, entry_type=entry_type, stage=stage, reason=reason, occurred_at=now,
            error_type=error_type, attempts=attempts, data=data))


def early_research_id(job_id: str) -> str:
    """T+5분 재검색의 재검색 작업 id. 요청 계약이 uuid를 요구하므로 급변동 작업 id에서 정한다."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"research-early:{job_id}"))


def early_queries(etf: EtfInfo | None, limit: int, stock_name: str | None = None) -> tuple[str, ...]:
    """ETF 이름과 비중 순위가 높은 구성 종목 이름으로 만든 검색어. ETF 정보가 없는 주식이면 그 주식 이름 하나.
    둘 다 모르면 빈 튜플(재검색하지 않는다)."""
    name = (etf.name or "").strip() if etf is not None else ""
    if not name:
        # 주식 급변동은 맥락 응답에 ETF 정보가 없고 name만 있다. 이 이름 하나로 찾는다.
        stock = (stock_name or "").strip() if etf is None else ""
        return (stock,) if stock and len(stock) <= MAX_QUERY_LENGTH else ()
    names = [name] + [m.name.strip() for m in sorted(etf.members, key=lambda m: m.rank) if m.name and m.name.strip()]
    unique = dict.fromkeys(n for n in names if len(n) <= MAX_QUERY_LENGTH)
    return tuple(unique)[:limit]


_FINAL_FAILURES: dict[FailureReason, tuple[str, str]] = {
    FailureReason.COLLECTION_NOT_COVERED: (STAGE_COLLECTION, "CollectionNotCovered"),
    FailureReason.MARKET_NO_RESPONSE: (STAGE_HISTORY, "ResponseTimeout"),
    FailureReason.CONTEXT_NO_RESPONSE: (STAGE_CONTEXT, "ResponseTimeout"),
    FailureReason.FILINGS_NO_RESPONSE: (STAGE_FILINGS, "ResponseTimeout"),
    FailureReason.NEWS_NO_RESPONSE: (STAGE_ARTICLES, "ResponseTimeout"),
}


def _warn_unprocessed(payload: dict[str, Any], job_id: str) -> None:
    """저장 끝이 대기 한도에 이르러 저장되지 않은 기사가 있다. 그 기사는 근거에 없을 뿐이므로 진행하고 건수만 남긴다."""
    unprocessed = len(payload.get("unprocessedEventIds") or ())
    if unprocessed:
        log.warning("news.stored for job %s left %d article(s) unprocessed; fetching what was stored",
                    job_id, unprocessed)


def _malformed(kind: QueryKind, job_id: str, error: Exception) -> None:
    # 응답 쪽 결함이라 받지 못한 것으로 본다. 원문은 로그에 쓰지 않는다.
    log.error("malformed %s response for job %s: %s", kind.value, job_id, type(error).__name__)


def _response_states(job: _Job) -> tuple[list[str], list[str]]:
    """요청한 응답 가운데 받은 것과 받지 못한 것. 보내지 않은 요청은 어느 쪽에도 넣지 않는다."""
    received: list[str] = []
    not_received: list[str] = []
    for stage, sent, got in (
            (STAGE_HISTORY, job.market.sent_at is not None, job.market.received),
            (STAGE_CONTEXT, job.context.sent_at is not None, job.context.received),
            (STAGE_FILINGS, job.filings.sent_at is not None, job.filings.received),
            (STAGE_ARTICLES, job.news.sent_at is not None, job.news.received)):
        if sent:
            (received if got else not_received).append(stage)
    return received, not_received


@dataclass(frozen=True)
class _Excluded:
    """재검사에서 버린 건수."""

    articles: int = 0
    filings: int = 0
    close_rates: int = 0
    background: int = 0


def _log_collection(i: ReportInput, excluded: _Excluded) -> None:
    """변동 1건의 수집 내역을 한 줄로 남긴다. 종목코드와 티커는 보유 종목을 드러내므로 쓰지 않는다."""
    us = bool(i.tickers)
    statuses = [b.status for b in i.background]
    log.info("collection job=%s history=%d historyTruncated=%s articles=%d articlesExcluded=%d articlesTruncated=%s "
             "filings=%s filingsExcluded=%d filingsTruncated=%s tickers=%d tickersTruncated=%s "
             "closeRates=%s closeRatesExcluded=%d closeRatesMissing=%s "
             "background=%d backgroundMissing=%d backgroundTooLarge=%d backgroundExcluded=%d",
             i.job_id, len(i.price_points), i.history_truncated, len(i.articles), excluded.articles,
             i.articles_truncated,
             len(i.filings) if us else "n/a", excluded.filings, i.filings_truncated, len(i.tickers),
             i.tickers_truncated,
             len(i.close_rates) if us else "n/a", excluded.close_rates,
             len(i.tickers) - len(i.close_rates) if us else "n/a",
             statuses.count(BACKGROUND_OK), statuses.count(BACKGROUND_MISSING),
             statuses.count(BACKGROUND_TOO_LARGE), excluded.background)


def _within_state_range(at: datetime | None, as_of: datetime, window: timedelta) -> bool:
    """상태형 자료의 시각이 [T - window, T] 안인가(양끝 포함). T 뒤 값은 변동 당시 알 수 없던 값이다."""
    return at is not None and as_of - window <= at <= as_of
