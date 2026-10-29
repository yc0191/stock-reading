"""변동 한 건의 근거 조회 작업. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: market.move 수신 -> 쓸 수 있는 collection.completed 대기 -> 시세·기사 요청 ->
두 응답 수신(기사는 안정될 때까지 재확인) -> 리포트 입력 완성, 또는 실패 기록.
"""
from __future__ import annotations

import json
import logging
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import (
    ARTICLES_REQUESTED_TOPIC,
    HISTORY_REQUESTED_TOPIC,
    OutboundEvent,
    format_timestamp,
    parse_timestamp,
    request_event,
)
from briefing.evidence import MARKET_WIDE, in_evidence_range

log = logging.getLogger("briefing.jobs")

COLLECT_NEWS = "collect-news"


class FailureReason(str, Enum):
    COLLECTION_NOT_COVERED = "collection_not_covered"
    MARKET_NO_RESPONSE = "market_no_response"
    NEWS_NO_RESPONSE = "news_no_response"
    NEWS_UNSTABLE = "news_unstable"
    LLM_ERROR = "llm_error"


@dataclass(frozen=True)
class Failure:
    job_id: str
    symbol: str
    reason: FailureReason
    detail: str
    attempts: int


@dataclass(frozen=True)
class PricePoint:
    as_of: datetime
    price: int


@dataclass(frozen=True)
class Article:
    article_id: str
    title: str
    summary: str
    published_at: datetime
    publisher: str
    url: str


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


class RequestPort(Protocol):
    def send(self, event: OutboundEvent) -> None: ...


class FailurePort(Protocol):
    def record(self, failure: Failure) -> None: ...


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
    phase: _Phase = _Phase.WAITING_COLLECTION
    # 시세: 가장 최근에 보낸 요청의 순번과 보낸 시각
    market_seq: int = 0
    market_sent_at: datetime | None = None
    market_points: tuple[PricePoint, ...] | None = None
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
    news_stable: bool = False


@dataclass
class BriefingCore:
    requests: RequestPort
    failures: FailurePort
    ready: ReadyPort
    settings: CoreSettings = DEFAULT_SETTINGS
    _jobs: OrderedDict[str, _Job] = field(default_factory=OrderedDict, init=False)
    _completions: deque[Completion] = field(init=False)

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
            )
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"malformed market.move event: {error!r}") from error

        if job.job_id in self._jobs:
            log.info("duplicate market.move %s ignored", job.job_id)
            return
        self._jobs[job.job_id] = job
        while len(self._jobs) > self.settings.remembered_jobs:
            self._jobs.popitem(last=False)

        # 완료 알림이 변동 알림보다 먼저 도착했을 수 있어 보관분부터 확인한다.
        if any(c.covers(job.symbol, job.as_of, self.settings) for c in self._completions):
            self._start_fetching(job, now)

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
        payload = _response_payload(message, "market.history.responded")
        job = self._fetching_job(payload)
        if job is None:
            return
        if payload["seq"] != job.market_seq or job.market_points is not None:
            log.info("stale market response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.market_seq)
            return
        try:
            job.market_points = tuple(
                PricePoint(parse_timestamp(p["asOf"]), int(p["price"])) for p in payload["points"]
            )
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed market.history.responded event: {error!r}") from error
        self._complete_if_ready(job)

    def on_articles_responded(self, message: str | bytes, now: datetime) -> None:
        payload = _response_payload(message, "news.articles.responded")
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
        ids = frozenset(a.article_id for a in articles)
        job.news_articles = articles
        if job.news_round >= 2 and ids == job.news_previous_ids:
            job.news_stable = True
            self._complete_if_ready(job)
            return
        if job.news_round >= self.settings.max_rounds:
            self._fail(job, FailureReason.NEWS_UNSTABLE,
                       f"article set kept changing for {job.news_round} rounds", job.news_requests)
            return
        job.news_previous_ids = ids
        job.news_recheck_at = now + self.settings.recheck_interval

    def on_tick(self, now: datetime) -> None:
        """제한 시간이 지난 작업을 처리한다. 주기적으로, 또는 next_deadline 시각에 부른다."""
        for job in list(self._jobs.values()):
            if job.phase is _Phase.WAITING_COLLECTION:
                if now - job.created_at >= self.settings.collection_wait_limit:
                    self._fail(job, FailureReason.COLLECTION_NOT_COVERED,
                               "no usable collection.completed within the wait limit", 0)
            elif job.phase is _Phase.FETCHING:
                self._tick_market(job, now)
                if job.phase is _Phase.FETCHING:
                    self._tick_news(job, now)

    def next_deadline(self) -> datetime | None:
        """다음에 on_tick을 불러야 하는 가장 이른 시각. 할 일이 없으면 None."""
        deadlines: list[datetime] = []
        for job in self._jobs.values():
            if job.phase is _Phase.WAITING_COLLECTION:
                deadlines.append(job.created_at + self.settings.collection_wait_limit)
            elif job.phase is _Phase.FETCHING:
                if job.market_points is None and job.market_sent_at is not None:
                    deadlines.append(job.market_sent_at + self.settings.response_timeout)
                if job.news_awaiting and job.news_sent_at is not None:
                    deadlines.append(job.news_sent_at + self.settings.response_timeout)
                elif job.news_recheck_at is not None and not job.news_stable:
                    deadlines.append(job.news_recheck_at)
        return min(deadlines) if deadlines else None

    def fail_llm_error(self, report_input: ReportInput, detail: str, attempts: int) -> None:
        """리포트 작성이 끝내 실패했을 때 llm-report 쪽에서 부른다."""
        self.failures.record(Failure(report_input.job_id, report_input.symbol,
                                     FailureReason.LLM_ERROR, detail, attempts))

    # ---- 내부 ----

    def _fetching_job(self, payload: dict) -> _Job | None:
        job = self._jobs.get(payload["jobId"])
        if job is None or job.phase is not _Phase.FETCHING:
            log.info("response for unknown or finished job %s ignored", payload["jobId"])
            return None
        return job

    def _start_fetching(self, job: _Job, now: datetime) -> None:
        job.phase = _Phase.FETCHING
        self._send_market(job, now)
        job.news_round = 1
        self._send_news(job, now, attempt=1)

    def _send_market(self, job: _Job, now: datetime) -> None:
        job.market_seq += 1
        job.market_sent_at = now
        s = self.settings
        self.requests.send(request_event(
            HISTORY_REQUESTED_TOPIC, "market.history.requested", job.correlation_id, now, {
                "jobId": job.job_id,
                "seq": job.market_seq,
                "symbol": job.symbol,
                "since": format_timestamp(job.as_of - s.market_lookback),
                "until": format_timestamp(job.as_of + s.delta),
                "maxPoints": s.max_history_points,
            }))

    def _send_news(self, job: _Job, now: datetime, attempt: int) -> None:
        job.news_seq += 1
        job.news_attempt = attempt
        job.news_requests += 1
        job.news_sent_at = now
        job.news_awaiting = True
        job.news_recheck_at = None
        s = self.settings
        self.requests.send(request_event(
            ARTICLES_REQUESTED_TOPIC, "news.articles.requested", job.correlation_id, now, {
                "jobId": job.job_id,
                "seq": job.news_seq,
                "symbol": job.symbol,
                "since": format_timestamp(job.as_of - s.evidence_window),
                "until": format_timestamp(job.as_of + s.delta),
                "maxArticles": s.max_articles,
            }))

    def _tick_market(self, job: _Job, now: datetime) -> None:
        if job.market_points is not None or job.market_sent_at is None:
            return
        if now - job.market_sent_at < self.settings.response_timeout:
            return
        if job.market_seq >= self.settings.max_attempts:
            self._fail(job, FailureReason.MARKET_NO_RESPONSE,
                       "no market.history.responded within the time limit", job.market_seq)
        else:
            self._send_market(job, now)

    def _tick_news(self, job: _Job, now: datetime) -> None:
        if job.news_awaiting:
            if now - job.news_sent_at < self.settings.response_timeout:
                return
            if job.news_attempt >= self.settings.max_attempts:
                self._fail(job, FailureReason.NEWS_NO_RESPONSE,
                           "no news.articles.responded within the time limit", job.news_attempt)
            else:
                self._send_news(job, now, job.news_attempt + 1)
        elif job.news_recheck_at is not None and not job.news_stable and now >= job.news_recheck_at:
            job.news_round += 1
            self._send_news(job, now, attempt=1)

    def _complete_if_ready(self, job: _Job) -> None:
        # 시세와 기사를 둘 다 받아야 완성한다. 일부만으로 진행하지 않는다.
        if job.market_points is None or not job.news_stable:
            return
        articles = []
        for article in job.news_articles or ():
            if in_evidence_range(article.published_at, job.as_of, self.settings.evidence_window,
                                 self.settings.delta):
                articles.append(article)
            else:
                log.warning("dropping article %s outside the requested range (job=%s)",
                            article.article_id, job.job_id)
        job.phase = _Phase.DONE
        self.ready.on_ready(ReportInput(
            job_id=job.job_id, symbol=job.symbol, correlation_id=job.correlation_id,
            as_of=job.as_of, previous_price=job.previous_price, price=job.price,
            change_percent=job.change_percent, price_points=job.market_points,
            articles=tuple(articles)))

    def _fail(self, job: _Job, reason: FailureReason, detail: str, attempts: int) -> None:
        job.phase = _Phase.DONE
        log.warning("job %s failed: %s", job.job_id, reason.value)
        self.failures.record(Failure(job.job_id, job.symbol, reason, detail, attempts))


def _response_payload(message: str | bytes, event_type: str) -> dict:
    try:
        event = json.loads(message)
        if event["eventType"] != event_type:
            raise ValueError(f"unexpected eventType: {event['eventType']}")
        payload = event["payload"]
        payload["jobId"], payload["seq"]  # noqa: B018 - 필수 필드 확인
        return payload
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"malformed {event_type} event: {error!r}") from error
