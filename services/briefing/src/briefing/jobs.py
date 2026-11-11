"""변동 한 건의 근거 조회 작업. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: market.move 수신 -> 쓸 수 있는 collection.completed 대기(변동 T+5분이 되면 ETF 이름과 구성 종목 이름으로 먼저 재검색하고,
그 재검색 완료도 수집 완료로 본다. 둘 중 먼저 온 것으로 진행) -> 시세 이력·배경 자료·기사 요청 ->
배경 자료 응답에 미국 종목 티커가 있으면 그 티커로 공시 요청 -> 모든 응답 수신(기사는 안정될 때까지 재확인)
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
from briefing.events import (
    ARTICLES_REQUESTED_TOPIC,
    CONTEXT_REQUESTED_TOPIC,
    FILINGS_REQUESTED_TOPIC,
    HISTORY_REQUESTED_TOPIC,
    format_timestamp,
    parse_timestamp,
    request_event,
    research_request_event,
)
from briefing.evidence import MARKET_WIDE, in_evidence_range
from briefing.job_log import (
    REASON_RECHECK_ROUND,
    REASON_RESENT,
    STAGE_ARTICLES,
    STAGE_ARTICLES_RECHECK,
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
from briefing.ports import FailureReason, RequestPort
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
    parse_background,
    parse_close_rates,
    parse_etf_info,
    parse_filings,
    response_payload,
)

log = logging.getLogger("briefing.jobs")

COLLECT_NEWS = "collect-news"


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
    # market.move를 감지한 시각(봉투의 occurredAt)과 감지 유형. 머리말의 "몇 분 사이"에 쓴다. 이전 형식에는 없을 수 있다.
    detected_at: datetime | None = None
    detection_type: str | None = None
    # 기사 요청에 마지막으로 쓴 순번. 재검색 기사 조회는 이 뒤 순번을 써서 늦게 온 이전 응답과 섞이지 않게 한다.
    news_seq: int = 0
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
    CONTEXT_SENT = "context_sent"  # ETF 이름을 알려고 배경 자료를 먼저 요청함
    REQUESTED = "requested"        # 재검색 요청을 보냄. 완료 알림을 기다린다
    SKIPPED = "skipped"            # 이름을 모르거나 배경 자료가 오지 않아 정기 수집을 기다린다


@dataclass(frozen=True)
class _Context:
    tickers: tuple[str, ...]
    tickers_truncated: bool
    close_rates: tuple[CloseRate, ...]
    background: tuple[Background, ...]
    etf: EtfInfo | None = None


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
    phase: _Phase = _Phase.WAITING_COLLECTION
    early: _Early = _Early.PENDING
    # 시세·배경 자료·공시: 가장 최근에 보낸 요청의 순번과 보낸 시각
    market: Exchange = field(default_factory=Exchange)
    market_points: tuple[PricePoint, ...] | None = None
    market_truncated: bool = False
    context: Exchange = field(default_factory=Exchange)
    context_data: _Context | None = None
    filings: Exchange = field(default_factory=Exchange)
    filings_data: tuple[Filing, ...] = ()
    filings_truncated: bool = False
    # 기사: 라운드(안정 확인용 재조회)와 라운드 안의 시도(재전송)를 따로 센다
    news_seq: int = 0
    news_sent_at: datetime | None = None
    news_round: int = 0
    news_attempt: int = 0
    news_requests: int = 0
    news_awaiting: bool = False
    news_recheck_at: datetime | None = None
    news_previous_ids: frozenset[str] | None = None
    news_articles: tuple[Article, ...] | None = None
    news_truncated: bool = False
    news_stable: bool = False
    # 실패 기록용: 요청마다 보낸 순번·시각, 요청별 마지막 요청 범위
    request_log: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    request_ranges: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 실패 기록용: 요청별로 마지막에 받은 응답 payload 원문 그대로(기사는 가장 최근 라운드)
    responses: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class BriefingCore:
    requests: RequestPort
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
        try:
            move = json.loads(message)
            if move["eventType"] != "market.move":
                raise ValueError(f"unexpected eventType: {move['eventType']}")
            payload = move["payload"]
            job = _Job(
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
            )
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"malformed market.move event: {error!r}") from error

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

        # 완료 알림이 변동 알림보다 먼저 도착했을 수 있어 보관분부터 확인한다.
        if any(c.covers(job.symbol, job.as_of, self.settings) for c in self._completions):
            self._start_fetching(job, now)
        else:
            self._tick_early(job, now)

    def on_research_completed(self, message: str | bytes, now: datetime) -> bool:
        """T+5분 재검색의 완료 알림이면 정기 수집 완료와 같이 '수집 완료'로 보고 True. 그 밖의 알림은 False로 돌려준다."""
        try:
            event = json.loads(message)
            if event["eventType"] != "collection.completed":
                raise ValueError(f"unexpected eventType: {event['eventType']}")
            payload = event["payload"]
            research_job_id = payload.get("researchJobId")
            if research_job_id is None:
                return False
            incomplete = frozenset(payload["incompleteSymbols"])
        except (KeyError, TypeError, AttributeError, json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"malformed collection.completed event: {error!r}") from error

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
        log.info("early research completed for job %s; fetching evidence without the regular collection", job.job_id)
        self._start_fetching(job, now)
        return True

    def on_collection_completed(self, message: str | bytes, now: datetime) -> None:
        try:
            event = json.loads(message)
            if event["eventType"] != "collection.completed":
                raise ValueError(f"unexpected eventType: {event['eventType']}")
            payload = event["payload"]
            completion = Completion(
                started_at=parse_timestamp(payload["startedAt"]),
                job_type=payload["jobType"],
                incomplete_symbols=frozenset(payload["incompleteSymbols"]),
            )
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"malformed collection.completed event: {error!r}") from error

        self._completions.append(completion)
        for job in list(self._jobs.values()):
            if job.phase is _Phase.WAITING_COLLECTION:
                if completion.covers(job.symbol, job.as_of, self.settings):
                    self._start_fetching(job, now)
                else:
                    log.info("completion skipped for job %s (not usable)", job.job_id)

    def on_history_responded(self, message: str | bytes, now: datetime) -> None:
        payload = response_payload(message, "market.history.responded")
        job = self._fetching_job(payload)
        if job is None:
            return
        if payload["seq"] != job.market.seq or job.market.received:
            log.info("stale market response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.market.seq)
            return
        try:
            points = tuple(
                PricePoint(parse_timestamp(p["asOf"]), int(p["price"])) for p in payload["points"]
            )
            truncated = bool(payload.get("truncated", False))
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed market.history.responded event: {error!r}") from error
        job.market.received = True
        job.responses[STAGE_HISTORY] = payload
        job.market_points, job.market_truncated = points, truncated
        self._complete_if_ready(job)

    def on_context_responded(self, message: str | bytes, now: datetime) -> None:
        payload = response_payload(message, "market.context.responded")
        early = self._jobs.get(payload["jobId"])
        if early is not None and early.phase is _Phase.WAITING_COLLECTION and early.early is _Early.CONTEXT_SENT:
            self._on_early_context(early, payload, now)
            return
        job = self._fetching_job(payload)
        if job is None:
            return
        if payload["seq"] != job.context.seq or job.context.received:
            log.info("stale context response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.context.seq)
            return
        context = self._parse_context(payload)
        job.context.received = True
        job.responses[STAGE_CONTEXT] = payload
        job.context_data = context
        # 미국 종목이 없으면 공시는 해당 없음이라 요청하지 않는다.
        if context.tickers:
            self._send_filings(job, now)
        self._complete_if_ready(job)

    @staticmethod
    def _parse_context(payload: dict) -> _Context:
        try:
            return _Context(
                tickers=tuple(payload["tickers"]),
                tickers_truncated=bool(payload["truncated"]),
                close_rates=parse_close_rates(payload["closeRates"]),
                background=tuple(parse_background(b) for b in payload["background"]),
                etf=parse_etf_info(payload.get("etf")),
            )
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed market.context.responded event: {error!r}") from error

    def on_filings_responded(self, message: str | bytes, now: datetime) -> None:
        payload = response_payload(message, "news.filings.responded")
        job = self._fetching_job(payload)
        if job is None:
            return
        if payload["seq"] != job.filings.seq or job.filings.received:
            log.info("stale filings response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.filings.seq)
            return
        try:
            filings = parse_filings(payload["filings"])
            truncated = bool(payload["truncated"])
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed news.filings.responded event: {error!r}") from error
        job.filings.received = True
        job.responses[STAGE_FILINGS] = payload
        job.filings_data, job.filings_truncated = filings, truncated
        self._complete_if_ready(job)

    def on_articles_responded(self, message: str | bytes, now: datetime) -> None:
        payload = response_payload(message, "news.articles.responded")
        job = self._fetching_job(payload)
        if job is None:
            return
        if payload["seq"] != job.news_seq or not job.news_awaiting:
            log.info("stale articles response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.news_seq)
            return
        try:
            articles = tuple(
                Article(a["articleId"], a["title"], a["summary"], parse_timestamp(a["publishedAt"]),
                        a["publisher"], a["url"])
                for a in payload["articles"]
            )
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed news.articles.responded event: {error!r}") from error

        job.news_awaiting = False
        job.responses[STAGE_ARTICLES] = payload
        ids = frozenset(a.article_id for a in articles)
        job.news_articles = articles
        job.news_truncated = bool(payload.get("truncated", False))
        if job.news_round >= 2:
            self._record_recheck_round(job, articles, ids, now)
        if job.news_round >= 2 and ids == job.news_previous_ids:
            job.news_stable = True
            self._complete_if_ready(job)
            return
        if job.news_round >= self.settings.max_rounds:
            self._fail(job, FailureReason.NEWS_UNSTABLE,
                       f"article set kept changing for {job.news_round} rounds", job.news_requests, now)
            return
        job.news_previous_ids = ids
        job.news_recheck_at = now + self.settings.recheck_interval

    def on_tick(self, now: datetime) -> None:
        """제한 시간이 지난 작업을 처리한다. 주기적으로, 또는 next_deadline 시각에 부른다."""
        for job in list(self._jobs.values()):
            if job.phase is _Phase.WAITING_COLLECTION:
                if now - job.created_at >= self.settings.collection_wait_limit:
                    self._fail(job, FailureReason.COLLECTION_NOT_COVERED,
                               "no usable collection.completed within the wait limit", 0, now)
                else:
                    self._tick_early(job, now)
            elif job.phase is _Phase.FETCHING:
                # 같은 시각에 여럿이 끝나도 실패는 한 번만 기록된다. 앞의 판정이 작업을 끝내면 나머지는 건너뛴다.
                for tick in (self._tick_market, self._tick_news, self._tick_context, self._tick_filings):
                    if job.phase is not _Phase.FETCHING:
                        break
                    tick(job, now)

    def next_deadline(self) -> datetime | None:
        """다음에 on_tick을 불러야 하는 가장 이른 시각. 할 일이 없으면 None."""
        deadlines: list[datetime] = []
        for job in self._jobs.values():
            if job.phase is _Phase.WAITING_COLLECTION:
                deadlines.append(job.created_at + self.settings.collection_wait_limit)
                if job.early is _Early.PENDING:
                    deadlines.append(job.as_of + self.settings.early_research_delay)
                elif job.early is _Early.CONTEXT_SENT:
                    deadlines.append(job.context.sent_at + self.settings.response_timeout)
            elif job.phase is _Phase.FETCHING:
                for exchange in (job.market, job.context, job.filings):
                    if exchange.awaiting():
                        deadlines.append(exchange.sent_at + self.settings.response_timeout)
                if job.news_awaiting and job.news_sent_at is not None:
                    deadlines.append(job.news_sent_at + self.settings.response_timeout)
                elif job.news_recheck_at is not None and not job.news_stable:
                    deadlines.append(job.news_recheck_at)
        return min(deadlines) if deadlines else None

    # ---- 내부 ----

    def _fetching_job(self, payload: dict) -> _Job | None:
        job = self._jobs.get(payload["jobId"])
        if job is None or job.phase is not _Phase.FETCHING:
            log.info("response for unknown or finished job %s ignored", payload["jobId"])
            return None
        return job

    def _tick_early(self, job: _Job, now: datetime) -> None:
        """정기 수집을 기다리는 작업의 T+5분 재검색을 진행한다. 배경 자료가 안 오면 포기하고 정기 수집을 기다린다."""
        if job.early is _Early.PENDING and now >= job.as_of + self.settings.early_research_delay:
            job.early = _Early.CONTEXT_SENT
            self._send_context(job, now)
        elif job.early is _Early.CONTEXT_SENT and now - job.context.sent_at >= self.settings.response_timeout:
            log.info("early research skipped for job %s: no context response", job.job_id)
            job.early = _Early.SKIPPED

    def _on_early_context(self, job: _Job, payload: dict, now: datetime) -> None:
        """T+5분 재검색을 하려고 먼저 받은 배경 자료. ETF 이름을 알면 재검색을 요청하고, 모르면 정기 수집을 기다린다."""
        if payload["seq"] != job.context.seq or job.context.received:
            log.info("stale context response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.context.seq)
            return
        context = self._parse_context(payload)
        job.context.received = True
        job.responses[STAGE_CONTEXT] = payload
        job.context_data = context
        queries = early_queries(context.etf, self.settings.max_research_queries)
        if not queries:
            log.info("early research skipped for job %s: ETF name unknown", job.job_id)
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
            # T+5분 재검색을 하려고 먼저 보낸 요청이 아직 답을 기다리는 중이면 그 요청을 이어 쓴다. 새 순번으로 다시 보내면
            # 곧 도착할 그 응답이 낡은 응답이 된다.
            if not job.context.awaiting():
                self._send_context(job, now)
        elif job.context_data.tickers:
            # T+5분 재검색을 하려고 배경 자료를 이미 받았다. 다시 받지 않고 공시만 요청한다.
            self._send_filings(job, now)
        job.news_round = 1
        self._send_news(job, now, attempt=1)

    def _send_market(self, job: _Job, now: datetime) -> None:
        seq = next_seq(job.market, now)
        s = self.settings
        self.requests.send(request_event(
            HISTORY_REQUESTED_TOPIC, "market.history.requested", job.correlation_id, now, self._sent(job, STAGE_HISTORY, now, {
                "jobId": job.job_id,
                "seq": seq,
                "symbol": job.symbol,
                "since": format_timestamp(job.as_of - s.market_lookback),
                "until": format_timestamp(job.as_of + s.delta),
                "maxPoints": s.max_history_points,
                "intervalSeconds": int(s.history_interval.total_seconds()),
            })))

    def _send_context(self, job: _Job, now: datetime) -> None:
        seq = next_seq(job.context, now)
        s = self.settings
        self.requests.send(request_event(
            CONTEXT_REQUESTED_TOPIC, "market.context.requested", job.correlation_id, now, self._sent(job, STAGE_CONTEXT, now, {
                "jobId": job.job_id,
                "seq": seq,
                "symbol": job.symbol,
                "asOf": format_timestamp(job.as_of),
                "since": format_timestamp(job.as_of - s.background_window),
                "maxResponseBytes": s.max_background_bytes,
                "maxTickers": s.max_tickers,
            })))

    def _send_filings(self, job: _Job, now: datetime) -> None:
        seq = next_seq(job.filings, now)
        s = self.settings
        self.requests.send(request_event(
            FILINGS_REQUESTED_TOPIC, "news.filings.requested", job.correlation_id, now, self._sent(job, STAGE_FILINGS, now, {
                "jobId": job.job_id,
                "seq": seq,
                "symbol": job.symbol,
                "tickers": list(job.context_data.tickers),
                "since": format_timestamp(job.as_of - s.filings_window),
                "until": format_timestamp(job.as_of),
                "maxFilings": s.max_filings,
            })))

    def _send_news(self, job: _Job, now: datetime, attempt: int) -> None:
        job.news_seq += 1
        job.news_attempt = attempt
        job.news_requests += 1
        job.news_sent_at = now
        job.news_awaiting = True
        job.news_recheck_at = None
        s = self.settings
        self.requests.send(request_event(
            ARTICLES_REQUESTED_TOPIC, "news.articles.requested", job.correlation_id, now, self._sent(job, STAGE_ARTICLES, now, {
                "jobId": job.job_id,
                "seq": job.news_seq,
                "symbol": job.symbol,
                "since": format_timestamp(job.as_of - s.evidence_window),
                "until": format_timestamp(job.as_of + s.delta),
                "maxArticles": s.max_articles,
            })))

    def _tick_market(self, job: _Job, now: datetime) -> None:
        self._tick_exchange(job, job.market, now, self._send_market, FailureReason.MARKET_NO_RESPONSE,
                            "market.history.responded", STAGE_HISTORY)

    def _tick_context(self, job: _Job, now: datetime) -> None:
        self._tick_exchange(job, job.context, now, self._send_context, FailureReason.CONTEXT_NO_RESPONSE,
                            "market.context.responded", STAGE_CONTEXT)

    def _tick_filings(self, job: _Job, now: datetime) -> None:
        self._tick_exchange(job, job.filings, now, self._send_filings, FailureReason.FILINGS_NO_RESPONSE,
                            "news.filings.responded", STAGE_FILINGS)

    def _tick_exchange(self, job: _Job, exchange: Exchange, now: datetime, resend, reason: FailureReason,
                       event_type: str, stage: str) -> None:
        if not exchange.awaiting() or now - exchange.sent_at < self.settings.response_timeout:
            return
        if exchange.seq >= self.settings.max_attempts:
            self._fail(job, reason, f"no {event_type} within the time limit", exchange.seq, now)
        else:
            resend(job, now)
            self._record(job, stage, EntryType.RESENT, REASON_RESENT, "ResponseTimeout", exchange.seq, now,
                         {"timedOutSeq": exchange.seq - 1, "resentSeq": exchange.seq})

    def _tick_news(self, job: _Job, now: datetime) -> None:
        if job.news_awaiting:
            if now - job.news_sent_at < self.settings.response_timeout:
                return
            if job.news_attempt >= self.settings.max_attempts:
                self._fail(job, FailureReason.NEWS_NO_RESPONSE,
                           "no news.articles.responded within the time limit", job.news_attempt, now)
            else:
                self._send_news(job, now, job.news_attempt + 1)
                self._record(job, STAGE_ARTICLES, EntryType.RESENT, REASON_RESENT, "ResponseTimeout", job.news_attempt, now,
                             {"round": job.news_round, "timedOutSeq": job.news_seq - 1, "resentSeq": job.news_seq})
        elif job.news_recheck_at is not None and not job.news_stable and now >= job.news_recheck_at:
            job.news_round += 1
            self._send_news(job, now, attempt=1)

    def _complete_if_ready(self, job: _Job) -> None:
        # 필요한 응답을 모두 받아야 완성한다. 일부만으로 진행하지 않는다.
        context = job.context_data
        if not job.market.received or not job.news_stable or context is None:
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
            etf=context.etf, detected_at=job.detected_at, detection_type=job.detection_type,
            news_seq=job.news_seq)
        _log_collection(report_input, _Excluded(
            articles=len(job.news_articles or ()) - len(articles),
            filings=len(job.filings_data) - len(filings),
            close_rates=len(context.close_rates) - len(close_rates),
            background=background_excluded))
        job.phase = _Phase.DONE
        self.ready.on_ready(report_input)

    def _fail(self, job: _Job, reason: FailureReason, detail: str, attempts: int, now: datetime) -> None:
        job.phase = _Phase.DONE
        log.warning("job %s failed: %s", job.job_id, reason.value)
        stage, error_type = _FINAL_FAILURES[reason]
        extra: dict[str, Any] = {"detail": detail}
        if reason is FailureReason.COLLECTION_NOT_COVERED:
            extra["waitStartedAt"] = format_timestamp(job.created_at)
            extra["waitLimitSeconds"] = int(self.settings.collection_wait_limit.total_seconds())
            extra["recentCompletions"] = [
                {"jobType": c.job_type, "startedAt": format_timestamp(c.started_at),
                 "incompleteSymbolCount": len(c.incomplete_symbols)}
                for c in self._completions]
        if reason is FailureReason.NEWS_UNSTABLE:
            extra["rounds"] = job.news_round
            extra["lastArticles"] = _article_list(job.news_articles or ())
        self._record(job, stage, EntryType.STEP_FAILED, reason.value, error_type, attempts, now, extra)
        # 작업 한 줄에는 사유만 적는다. 관련 데이터는 위의 실패 줄에 있다.
        record_result(self.job_log, JobResult(job.job_id, RunStatus.FAILED, now, failure_reason=reason.value))

    # ---- 작업 기록 ----

    def _sent(self, job: _Job, stage: str, now: datetime, payload: dict[str, Any]) -> dict[str, Any]:
        """보낸 요청을 실패 기록용으로 남기고 payload를 그대로 돌려준다."""
        job.request_log.setdefault(stage, []).append({"seq": payload["seq"], "sentAt": format_timestamp(now)})
        job.request_ranges[stage] = {k: v for k, v in payload.items() if k not in ("jobId", "seq")}
        return payload

    def _record(self, job: _Job, stage: str, entry_type: EntryType, reason: str, error_type: str | None, attempts: int,
                now: datetime, extra: dict[str, Any]) -> None:
        received, not_received = _response_states(job)
        request_stage = STAGE_ARTICLES if stage == STAGE_ARTICLES_RECHECK else stage
        data: dict[str, Any] = {
            "requestRange": job.request_ranges.get(request_stage, {}),
            "requestRanges": dict(job.request_ranges),
            "requests": {k: list(v) for k, v in job.request_log.items()},
            "received": received,
            # 받은 응답의 내용 전부. 응답 payload를 가공하지 않고 담는다(배경 자료 원문, 지수별 계산값 포함).
            "receivedResponses": {stage: job.responses[stage] for stage in received},
            "notReceived": not_received,
            # 수집 완료를 기다리다 끝났으면 요청은 하나도 나가지 않았고 빠진 것은 수집 완료 알림이다.
            "missingItems": ["collection.completed"] if stage == STAGE_COLLECTION else not_received,
        }
        data.update(extra)
        self.job_log.record(JobLogEntry(
            job_id=job.job_id, entry_type=entry_type, stage=stage, reason=reason, occurred_at=now,
            error_type=error_type, attempts=attempts, data=data))

    def _record_recheck_round(self, job: _Job, articles: tuple[Article, ...], ids: frozenset[str],
                              now: datetime) -> None:
        changed = ids != job.news_previous_ids
        self._record(job, STAGE_ARTICLES_RECHECK, EntryType.CHECK_ROUND, REASON_RECHECK_ROUND,
                     "ArticleSetChanged" if changed else "ArticleSetUnchanged", job.news_round, now, {
                         "round": job.news_round,
                         "receivedArticles": _article_list(articles),
                         "previousArticleIds": sorted(job.news_previous_ids or ()),
                         "stable": not changed,
                     })


def early_research_id(job_id: str) -> str:
    """T+5분 재검색의 재검색 작업 id. 요청 계약이 uuid를 요구하므로 급변동 작업 id에서 정한다."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"research-early:{job_id}"))


def early_queries(etf: EtfInfo | None, limit: int) -> tuple[str, ...]:
    """ETF 이름과 비중 순위가 높은 구성 종목 이름으로 만든 검색어. ETF 이름을 모르면 빈 튜플(재검색하지 않는다)."""
    name = (etf.name or "").strip() if etf is not None else ""
    if not name:
        return ()
    names = [name] + [m.name.strip() for m in sorted(etf.members, key=lambda m: m.rank) if m.name and m.name.strip()]
    unique = dict.fromkeys(n for n in names if len(n) <= MAX_QUERY_LENGTH)
    return tuple(unique)[:limit]


_FINAL_FAILURES: dict[FailureReason, tuple[str, str]] = {
    FailureReason.COLLECTION_NOT_COVERED: (STAGE_COLLECTION, "CollectionNotCovered"),
    FailureReason.MARKET_NO_RESPONSE: (STAGE_HISTORY, "ResponseTimeout"),
    FailureReason.CONTEXT_NO_RESPONSE: (STAGE_CONTEXT, "ResponseTimeout"),
    FailureReason.FILINGS_NO_RESPONSE: (STAGE_FILINGS, "ResponseTimeout"),
    FailureReason.NEWS_NO_RESPONSE: (STAGE_ARTICLES, "ResponseTimeout"),
    FailureReason.NEWS_UNSTABLE: (STAGE_ARTICLES_RECHECK, "ArticleSetUnstable"),
}


def _article_list(articles) -> list[dict[str, str]]:
    return [{"articleId": a.article_id, "title": a.title, "summary": a.summary, "publisher": a.publisher,
             "publishedAt": format_timestamp(a.published_at), "url": a.url} for a in articles]


def _response_states(job: _Job) -> tuple[list[str], list[str]]:
    """요청한 응답 가운데 받은 것과 받지 못한 것. 보내지 않은 요청은 어느 쪽에도 넣지 않는다."""
    received: list[str] = []
    not_received: list[str] = []
    for stage, sent, got in (
            (STAGE_HISTORY, job.market.sent_at is not None, job.market.received),
            (STAGE_CONTEXT, job.context.sent_at is not None, job.context.received),
            (STAGE_FILINGS, job.filings.sent_at is not None, job.filings.received),
            (STAGE_ARTICLES, job.news_sent_at is not None, job.news_articles is not None)):
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
