"""데일리 판(아침 판, 마감 후 판) 작업. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: schedule.tick(판마다 따로 받음) -> 범위 계산 -> 데일리 자료 요청(market.daily.requested)
-> 아침 판이고 미국 종목이 있으면 그 티커로 공시 요청 -> 받은 자료 재검사 -> 본문(LLM 또는 규칙 기반)
-> (LLM이 약한 문장을 냈으면 문장마다 뉴스 재검색 -> 그 문장만 다시 쓰기, 근거 부족이면 보고서 전체를 한 번 재검색)
-> briefing.daily.generated 발행. 응답을 끝내 받지 못하거나 LLM이 끝내 실패해도 판은 나간다.
예외는 작업 기록을 끝내 저장하지 못했을 때뿐이며, 그때는 이 판의 결과 이벤트를 발행하지 않고 ERROR 로그만 남긴다.
"""
from __future__ import annotations

import base64
import json
import logging
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.daily_chart import chart_bars, no_data_names, render_day_chart
from briefing.daily_html import ChartSlot, render_html
from briefing.daily_inputs import EtfArticles, member_issues, moves_with_market, notable_candidates
from briefing.daily_relevance import split_related
from briefing.daily_report import (
    Section,
    EDITIONS_WITH_ETF_ARTICLES,
    EDITIONS_WITH_FILINGS,
    GENERATED_BY_RULE,
    DailyInput,
    DailyLlmWriter,
    DailyOutput,
    EtfDay,
    GENERATED_BY_LLM,
    POINT_NOTABLE,
    build_llm_input,
    RELATION_HELD,
    RELATION_WATCH,
    build_sections,
    daily_text,
    has_evidence,
    llm_text,
    missing_items,
    rule_based_text,
)
from briefing.events import (
    EDITION_POST_CLOSE,
    ARTICLES_REQUESTED_TOPIC,
    FILINGS_REQUESTED_TOPIC,
    JOB_TYPE_BY_EDITION,
    OutboundEvent,
    daily_briefing_event,
    format_timestamp,
    parse_timestamp,
    request_event,
)
from briefing.job_log import (
    DiscardJobLog,
    EntryType,
    GeneratedBy,
    JobKind,
    JobLogEntry,
    JobLogPort,
    JobResult,
    JobStart,
    RunStatus,
    range_target,
    record_result,
)
from briefing.evidence import MARKET_WIDE
from briefing.ports import FailureReason, RequestPort
from briefing.research import ResearchTarget
from briefing.sentence_research import WHOLE_REPORT, SentenceResearch, WeakSentence
from briefing.status import Operations, StatusPort, StatusReport, build_status_report, parse_operations
from briefing.sources import (
    BACKGROUND_LABEL,
    BACKGROUND_MISSING,
    Article,
    Background,
    CloseRate,
    Exchange,
    Filing,
    next_seq,
    parse_background,
    parse_close_rates,
    parse_etf_info,
    parse_filings,
    response_payload,
)

log = logging.getLogger("briefing.daily_jobs")

# 차트를 싣지 못했을 때 자료 없음 목록(missingItems)에 남기는 줄.
CHART_OVERSIZED_ITEM = "하루 등락 차트: 크기 초과"
CHART_FAILED_ITEM = "하루 등락 차트: 그리지 못함"
# 서버 상태 절을 만들다 실패했을 때 본문 끝에 대신 붙이는 절.
NO_STATUS_TEXT = "──────── 서버 상태 ────────\n- 자료 없음"

SCHEDULE_TICK = "schedule.tick"
DAILY_DATA_REQUESTED_TOPIC = "market.daily.requested"
DAILY_DATA_RESPONDED = "market.daily.responded"
FILINGS_RESPONDED = "news.filings.responded"
ARTICLES_RESPONDED = "news.articles.responded"

# 작업 기록의 단계와 사유. 끝내 실패한 사유는 FailureReason 값을 그대로 쓴다.
STAGE_DAILY_DATA = "daily-data-request"
STAGE_FILINGS = "filings-request"
STAGE_ETF_ARTICLES = "etf-articles-request"
STAGE_LLM = "llm"
STAGE_COMPOSE = "compose"
REASON_RESENT = "no_response_resent"
REASON_LLM_REJECTED = "llm_attempt_rejected"
REASON_MISSING_ITEMS = "missing_items"
REASON_LLM_ACCEPTED = "llm_attempt_accepted"
REASON_PUBLISHED = "published"
# 작업 기록의 excluded에 남기는, ETF와 관계없어 뺀 기사 건수의 이름
EXCLUDED_UNRELATED = "irrelevantArticles"


def range_start(scheduled_for: datetime, settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> datetime:
    """직전 같은 판의 시각. 기록을 읽지 않고 예정 시각에서 직전 평일의 같은 시각으로 계산한다(휴장일은 보지 않음)."""
    local = scheduled_for.astimezone(KST)
    start = local - timedelta(days=1)
    while start.weekday() >= 5:
        start -= timedelta(days=1)
    return max(start, local - settings.max_lookback)


@dataclass
class _Sent:
    request: str
    seq: int
    sent_at: datetime
    payload: dict[str, Any]


@dataclass(frozen=True)
class _Draft:
    """LLM(또는 규칙 기반)이 쓴 본문 하나. 재검색을 할지 정하는 표시와 받아들인 LLM 출력을 함께 든다."""

    text: str
    generated_by: str
    cited: tuple[str, ...]
    insufficient_evidence: bool = False
    weak_shortfalls: tuple[str, ...] = ()
    output: DailyOutput | None = None


@dataclass
class _EtfNews:
    """ETF 하나의 기사 요청 상태. seq는 작업 안에서 겹치지 않는 번호이고 다시 보낼 때마다 새 번호를 쓴다."""

    seq: int = 0
    attempts: int = 0
    sent_at: datetime | None = None
    received: bool = False
    given_up: bool = False
    articles: tuple[Article, ...] = ()
    truncated: bool = False

    def awaiting(self) -> bool:
        return self.sent_at is not None and not self.received and not self.given_up


@dataclass
class _DailyJob:
    job_id: str
    edition: str
    correlation_id: str
    scheduled_for: datetime
    range_start: datetime
    data: Exchange = field(default_factory=Exchange)
    filings: Exchange = field(default_factory=Exchange)
    data_given_up: bool = False
    filings_given_up: bool = False
    data_payload: dict | None = None
    operations: Operations | None = None
    filings_payload: dict | None = None
    etfs: tuple[EtfDay, ...] | None = None
    etfs_truncated: bool = False
    close_rates: tuple[CloseRate, ...] | None = None
    background: tuple[Background, ...] | None = None
    filings_data: tuple[Filing, ...] | None = None
    filings_truncated: bool = False
    # 마감 후 판: ETF 종목코드마다 기사 요청 상태와, 받은 응답 원문(작업 기록용)
    news: dict[str, _EtfNews] = field(default_factory=dict)
    news_seq: int = 0
    news_payloads: list[dict] = field(default_factory=list)
    news_cut: int = 0
    unrelated_titles: list[dict[str, Any]] = field(default_factory=list)
    excluded: dict[str, int] = field(default_factory=dict)
    sent: list[_Sent] = field(default_factory=list)
    first_failure: tuple[FailureReason, str, int] | None = None
    # 문장 단위 재검색이 진행 중인가. 끝나면 재검색 흐름이 발행한다.
    researching: bool = False

    def waiting(self) -> bool:
        return (self.data.awaiting() and not self.data_given_up) or (self.filings.awaiting() and not self.filings_given_up) \
            or any(n.awaiting() for n in self.news.values())


@dataclass
class DailyCore:
    requests: RequestPort
    writer: DailyLlmWriter | None = None
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    # 있을 때만 재검색한다. 급변동과 함께 쓰는 문장 단위 재검색 흐름이다.
    sentences: SentenceResearch | None = None
    # 있을 때만 마감 후 판 끝에 서버 상태 절을 붙인다.
    status: StatusPort | None = None
    _jobs: dict[str, _DailyJob] = field(default_factory=dict, init=False)
    _seen: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)

    def __post_init__(self) -> None:
        if self.sentences is not None:
            adapter = DailySentences(self)
            for edition in EDITION_JOB_KINDS:
                self.sentences.register(edition, adapter)

    # ---- 입력 ----

    def on_schedule_tick(self, message: str | bytes, now: datetime, edition: str) -> None:
        """판 하나의 시작 신호. 그 판의 jobType만 처리하고 같은 eventId는 한 번만 처리한다. 형식이 잘못되면 ValueError."""
        try:
            tick = json.loads(message)
            if tick["eventType"] != SCHEDULE_TICK:
                raise ValueError(f"unexpected eventType: {tick['eventType']}")
            job_type = tick["payload"]["jobType"]
            if job_type != JOB_TYPE_BY_EDITION[edition]:
                return
            job_id = tick["eventId"]
            scheduled_for = parse_timestamp(tick["payload"]["scheduledFor"])
            correlation_id = tick["correlationId"]
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"malformed schedule.tick event: {error!r}") from error
        if job_id in self._seen:
            log.info("duplicate daily tick %s (%s) ignored", job_id, edition)
            return
        self._seen[job_id] = None
        while len(self._seen) > self.settings.remembered_ticks:
            self._seen.popitem(last=False)
        job = _DailyJob(job_id, edition, correlation_id, scheduled_for, range_start(scheduled_for, self.settings))
        self._jobs[job_id] = job
        # 작업 한 줄은 시작 때 만들고 끝날 때 고친다. 입력은 시작시킨 이벤트 원문이다.
        raw = message.decode("utf-8") if isinstance(message, bytes) else message
        self.job_log.start(JobStart(job_id, JobKind(edition), raw,
                                    range_target(format_timestamp(job.range_start), format_timestamp(scheduled_for)),
                                    now))
        log.info("daily %s started: job=%s range=%s~%s", edition, job_id,
                 format_timestamp(job.range_start), format_timestamp(scheduled_for))
        self._send_data(job, now)

    def on_daily_data_responded(self, message: str | bytes, now: datetime) -> None:
        payload = response_payload(message, DAILY_DATA_RESPONDED)
        job = self._jobs.get(payload["jobId"])
        if job is None or job.data_payload is not None or job.data_given_up:
            if job is not None:
                log.info("stale daily data response ignored: job=%s seq=%s", job.job_id, payload["seq"])
            return
        if payload["seq"] != job.data.seq:
            log.info("stale daily data response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.data.seq)
            return
        try:
            etfs = tuple(_parse_etf_day(e) for e in payload["etfs"])
            close_rates = parse_close_rates(payload["closeRates"])
            background = tuple(parse_background(b) for b in payload["background"])
            truncated = bool(payload["truncated"])
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed {DAILY_DATA_RESPONDED} event: {error!r}") from error
        job.data.received = True
        job.data_payload = payload
        job.operations = parse_operations(payload.get("operations"))
        job.etfs = self._recheck_etfs(job, etfs)
        job.close_rates, job.background = self._recheck_state(job, close_rates, background)
        job.etfs_truncated = truncated
        tickers = tuple(dict.fromkeys(t for etf in job.etfs for t in etf.tickers))
        if job.edition in EDITIONS_WITH_ETF_ARTICLES:
            for symbol in dict.fromkeys(etf.symbol for etf in job.etfs):
                self._send_articles(job, symbol, now)
        # 미국 종목이 없으면 공시는 해당 없음이라 요청하지 않는다.
        if job.edition in EDITIONS_WITH_FILINGS and tickers:
            self._send_filings(job, now)
        # 보낸 요청의 응답이 남아 있으면 _finish가 기다린다.
        self._finish(job, now)

    def on_articles_responded(self, message: str | bytes, now: datetime) -> None:
        """마감 후 판이 ETF마다 보낸 기사 요청의 응답. 급변동 작업의 응답도 같은 토픽으로 오므로 모르는 작업은 조용히 넘긴다."""
        payload = response_payload(message, ARTICLES_RESPONDED)
        job = self._jobs.get(payload["jobId"])
        if job is None:
            return
        state = job.news.get(payload.get("symbol"))
        if state is None or payload["seq"] != state.seq or not state.awaiting():
            log.info("stale daily articles response ignored: job=%s seq=%s", job.job_id, payload["seq"])
            return
        try:
            articles = tuple(
                Article(a["articleId"], a["title"], a["summary"], parse_timestamp(a["publishedAt"]),
                        a["publisher"], a["url"])
                for a in payload["articles"])
            truncated = bool(payload["truncated"])
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed {ARTICLES_RESPONDED} event: {error!r}") from error
        in_range = sorted((a for a in articles if job.range_start <= a.published_at <= job.scheduled_for),
                          key=lambda a: a.published_at, reverse=True)
        self._note_excluded(job, "articles", len(articles) - len(in_range))
        # 그 ETF와 관계없는 기사는 상한에 자리를 차지하기 전에 뺀다. 뺀 건수는 작업 기록의 excluded에 남는다.
        etf = next((e for e in job.etfs or () if e.symbol == payload.get("symbol")), None)
        related, unrelated = split_related(etf.etf if etf is not None else None, in_range, self.settings)
        if unrelated:
            job.excluded[EXCLUDED_UNRELATED] = job.excluded.get(EXCLUDED_UNRELATED, 0) + len(unrelated)
            # 실데이터에서 걸러낸 기사가 맞는지 볼 수 있게 제목을 작업 기록에 남긴다(로그 파일에는 쓰지 않는다).
            job.unrelated_titles.extend({"symbol": payload.get("symbol"), "title": a.title} for a in unrelated)
        kept = tuple(related[:self.settings.etf_max_articles])
        job.news_cut += len(related) - len(kept)
        state.received, state.articles, state.truncated = True, kept, truncated or len(kept) < len(related)
        job.news_payloads.append(payload)
        self._finish(job, now)

    def on_filings_responded(self, message: str | bytes, now: datetime) -> None:
        """급변동 작업의 공시 응답도 같은 토픽으로 오므로, 모르는 작업의 응답은 조용히 넘긴다."""
        payload = response_payload(message, FILINGS_RESPONDED)
        job = self._jobs.get(payload["jobId"])
        if job is None:
            return
        if payload["seq"] != job.filings.seq or job.filings.received or job.filings_given_up:
            log.info("stale daily filings response ignored: job=%s seq=%s latest=%s",
                     job.job_id, payload["seq"], job.filings.seq)
            return
        try:
            filings = parse_filings(payload["filings"])
            truncated = bool(payload["truncated"])
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed {FILINGS_RESPONDED} event: {error!r}") from error
        job.filings.received = True
        job.filings_payload = payload
        kept = tuple(f for f in filings if job.range_start <= f.accepted_at <= job.scheduled_for)
        self._note_excluded(job, "filings", len(filings) - len(kept))
        job.filings_data, job.filings_truncated = kept, truncated
        self._finish(job, now)

    def on_tick(self, now: datetime) -> None:
        """제한 시간이 지난 요청을 다시 보내거나 포기한다. 포기해도 판은 있는 자료로 나간다."""
        for job in list(self._jobs.values()):
            if job.data.awaiting() and not job.data_given_up \
                    and now - job.data.sent_at >= self.settings.response_timeout:
                if job.data.seq >= self.settings.max_attempts:
                    self._give_up(job, job.data, STAGE_DAILY_DATA, FailureReason.CONTEXT_NO_RESPONSE,
                                  DAILY_DATA_RESPONDED, now)
                    job.data_given_up = True
                    self._finish(job, now)
                else:
                    self._resent(job, job.data, STAGE_DAILY_DATA, DAILY_DATA_RESPONDED, now)
                    self._send_data(job, now)
            elif job.filings.awaiting() and not job.filings_given_up \
                    and now - job.filings.sent_at >= self.settings.response_timeout:
                if job.filings.seq >= self.settings.max_attempts:
                    self._give_up(job, job.filings, STAGE_FILINGS, FailureReason.FILINGS_NO_RESPONSE,
                                  FILINGS_RESPONDED, now)
                    job.filings_given_up = True
                    self._finish(job, now)
                else:
                    self._resent(job, job.filings, STAGE_FILINGS, FILINGS_RESPONDED, now)
                    self._send_filings(job, now)
            if job.job_id in self._jobs:
                self._tick_articles(job, now)

    def _tick_articles(self, job: _DailyJob, now: datetime) -> None:
        """답이 없는 ETF의 기사 요청을 다시 보내거나, 시도 횟수를 다 쓰면 그 ETF는 기사 없음으로 포기한다."""
        for symbol, state in list(job.news.items()):
            if not state.awaiting() or now - state.sent_at < self.settings.response_timeout:
                continue
            if state.attempts >= self.settings.max_attempts:
                self._give_up(job, state, STAGE_ETF_ARTICLES, FailureReason.NEWS_NO_RESPONSE, ARTICLES_RESPONDED, now,
                              attempts=state.attempts)
                state.given_up = True
            else:
                self._resent(job, state, STAGE_ETF_ARTICLES, ARTICLES_RESPONDED, now, attempts=state.attempts)
                self._send_articles(job, symbol, now)
        self._finish(job, now)

    def next_deadline(self) -> datetime | None:
        deadlines = [exchange.sent_at + self.settings.response_timeout
                     for job in self._jobs.values()
                     for exchange, given_up in ((job.data, job.data_given_up), (job.filings, job.filings_given_up))
                     if exchange.awaiting() and not given_up]
        deadlines.extend(n.sent_at + self.settings.response_timeout
                         for job in self._jobs.values() for n in job.news.values() if n.awaiting())
        return min(deadlines) if deadlines else None

    # ---- 요청 ----

    def _send_data(self, job: _DailyJob, now: datetime) -> None:
        seq = next_seq(job.data, now)
        s = self.settings
        payload = {
            "jobId": job.job_id,
            "seq": seq,
            "asOf": format_timestamp(job.scheduled_for),
            "since": format_timestamp(job.range_start),
            "maxResponseBytes": s.max_response_bytes,
            "maxEtfs": s.max_etfs,
            "maxTickers": s.max_tickers,
        }
        if job.edition == EDITION_POST_CLOSE and self.status is not None:
            # 서버 상태 절의 시세처리기 운영 상태. 마감 후 판만 요청한다.
            payload["includeOperations"] = True
        job.sent.append(_Sent(DAILY_DATA_REQUESTED_TOPIC, seq, now, payload))
        self.requests.send(daily_data_request(job.correlation_id, now, payload))

    def _send_articles(self, job: _DailyJob, symbol: str, now: datetime) -> None:
        state = job.news.setdefault(symbol, _EtfNews())
        job.news_seq += 1
        state.seq, state.sent_at = job.news_seq, now
        state.attempts += 1
        payload = {
            "jobId": job.job_id,
            "seq": state.seq,
            "symbol": symbol,
            "since": format_timestamp(job.range_start),
            "until": format_timestamp(job.scheduled_for),
            "maxArticles": self.settings.etf_max_articles,
        }
        job.sent.append(_Sent(ARTICLES_REQUESTED_TOPIC, state.seq, now, payload))
        self.requests.send(request_event(ARTICLES_REQUESTED_TOPIC, "news.articles.requested", job.correlation_id, now,
                                         payload))

    def _send_filings(self, job: _DailyJob, now: datetime) -> None:
        seq = next_seq(job.filings, now)
        with_tickers = [etf for etf in sorted(job.etfs, key=lambda e: e.symbol) if etf.tickers]
        payload = {
            "jobId": job.job_id,
            "seq": seq,
            # 계약상 필수인 종목코드. news는 메시지 키와 짝짓기에만 쓰므로 미국 종목이 있는 첫 ETF를 넣는다.
            "symbol": with_tickers[0].symbol,
            "tickers": list(dict.fromkeys(t for etf in with_tickers for t in etf.tickers)),
            "since": format_timestamp(job.range_start),
            "until": format_timestamp(job.scheduled_for),
            "maxFilings": self.settings.max_filings,
        }
        job.sent.append(_Sent(FILINGS_REQUESTED_TOPIC, seq, now, payload))
        self.requests.send(request_event(FILINGS_REQUESTED_TOPIC, "news.filings.requested", job.correlation_id, now,
                                         payload))

    # ---- 재검사 ----

    def _recheck_etfs(self, job: _DailyJob, etfs: tuple[EtfDay, ...]) -> tuple[EtfDay, ...]:
        # 일봉이 없는 ETF(받은 시각 없음)는 버리지 않고 자료 없음으로 싣는다.
        kept = tuple(e for e in etfs if e.fetched_at is None or job.range_start <= e.fetched_at <= job.scheduled_for)
        self._note_excluded(job, "etfs", len(etfs) - len(kept))
        return kept

    def _recheck_state(self, job: _DailyJob, close_rates: tuple[CloseRate, ...],
                       background: tuple[Background, ...]) -> tuple[tuple[CloseRate, ...], tuple[Background, ...]]:
        lo, hi = job.range_start, job.scheduled_for
        rates = tuple(r for r in close_rates if r.fetched_at <= hi and lo <= r.close_at <= hi)
        self._note_excluded(job, "closeRates", len(close_rates) - len(rates))
        kept: list[Background] = []
        dropped = 0
        for item in background:
            if item.fetched_at is not None and not lo <= item.fetched_at <= hi:
                dropped += 1
                kept.append(Background(item.api_type, item.target, BACKGROUND_MISSING))
            else:
                kept.append(item)
        self._note_excluded(job, "background", dropped)
        return rates, tuple(kept)

    def _note_excluded(self, job: _DailyJob, kind: str, count: int) -> None:
        if count:
            job.excluded[kind] = job.excluded.get(kind, 0) + count
            # 응답 쪽 결함 신호. 종목코드와 티커는 로그에 쓰지 않는다.
            log.warning("daily job %s: dropped %d %s outside the range", job.job_id, count, kind)

    # ---- 작업 기록 ----

    def _related_data(self, job: _DailyJob, **extra: Any) -> dict[str, Any]:
        data = {
            "requestRange": {"since": format_timestamp(job.range_start), "asOf": format_timestamp(job.scheduled_for)},
            "requests": [{"request": s.request, "seq": s.seq, "sentAt": format_timestamp(s.sent_at),
                          "payload": s.payload} for s in job.sent],
            "received": {**{name: payload for name, payload in ((DAILY_DATA_RESPONDED, job.data_payload),
                                                                (FILINGS_RESPONDED, job.filings_payload))
                            if payload is not None},
                         **({ARTICLES_RESPONDED: job.news_payloads} if job.news_payloads else {})},
            "notReceived": [*(name for name, exchange in ((DAILY_DATA_RESPONDED, job.data),
                                                           (FILINGS_RESPONDED, job.filings))
                              if exchange.seq and not exchange.received),
                            *([ARTICLES_RESPONDED] if any(not n.received for n in job.news.values()) else [])],
            "excluded": dict(job.excluded),
            **({"irrelevantArticleTitles": list(job.unrelated_titles)} if job.unrelated_titles else {}),
        }
        data.update(extra)
        return data

    def _detail(self, job: _DailyJob, stage: str, entry_type: EntryType, reason: str, error_type: str | None,
                attempts: int, now: datetime, *, sentence_no: int | None = None, round: int | None = None,
                **extra: Any) -> None:
        self.job_log.record(JobLogEntry(
            job_id=job.job_id, entry_type=entry_type, stage=stage, reason=reason, occurred_at=now,
            sentence_no=sentence_no, round=round, error_type=error_type, attempts=attempts,
            data=self._related_data(job, **extra)))

    def _resent(self, job: _DailyJob, exchange: Exchange | _EtfNews, stage: str, event_type: str, now: datetime,
                attempts: int | None = None) -> None:
        self._detail(job, stage, EntryType.RESENT, REASON_RESENT, "Timeout",
                     attempts if attempts is not None else exchange.seq, now,
                     timedOut={"response": event_type, "seq": exchange.seq})

    def _give_up(self, job: _DailyJob, exchange: Exchange | _EtfNews, stage: str, reason: FailureReason,
                 event_type: str, now: datetime, attempts: int | None = None) -> None:
        detail = f"no {event_type} within the time limit"
        tries = attempts if attempts is not None else exchange.seq
        self._detail(job, stage, EntryType.STEP_FAILED, reason.value, "Timeout", tries, now,
                     timedOut={"response": event_type, "seq": exchange.seq})
        self._first_failure(job, reason, detail, tries)

    def _first_failure(self, job: _DailyJob, reason: FailureReason, detail: str, attempts: int) -> None:
        if job.first_failure is None:
            job.first_failure = (reason, detail, attempts)

    # ---- 완성 ----

    def _finish(self, job: _DailyJob, now: datetime) -> None:
        if job.waiting() or job.researching:
            return
        daily_input = DailyInput(
            job_id=job.job_id,
            edition=job.edition,
            range_start=job.range_start,
            as_of=job.scheduled_for,
            etfs=job.etfs,
            etfs_truncated=job.etfs_truncated,
            close_rates=job.close_rates,
            background=job.background,
            filings=job.filings_data,
            filings_truncated=job.filings_truncated,
            settings=self.settings,
        )
        if job.edition in EDITIONS_WITH_ETF_ARTICLES:
            daily_input = self._with_etf_inputs(job, daily_input)
        sections = build_sections(daily_input, self.settings)
        draft = self._write(job, now, daily_input, sections)
        # 문장 단위 재검색이 시작되면 그 흐름이 끝난 뒤 발행한다. 시작하지 않으면 바로 발행한다.
        job.researching = self.sentences is not None and draft.generated_by == GENERATED_BY_LLM \
            and self.sentences.begin(job.edition, job.job_id, DailyDraft(job, daily_input, sections, draft), now)
        if not job.researching:
            self._publish(job, now, daily_input, sections, draft)

    def _with_etf_inputs(self, job: _DailyJob, daily_input: DailyInput) -> DailyInput:
        """마감 후 판의 ETF별 기사, 미국 구성 종목 이슈, 눈에 띄는 점 후보를 모아 입력에 더한다."""
        if daily_input.etfs is None:
            return daily_input
        groups = tuple(EtfArticles(symbol, state.articles, state.received, state.truncated)
                       for symbol, state in job.news.items())
        filings = daily_input.filings or ()
        issues, issues_cut = member_issues(daily_input.etfs, daily_input.close_rates or (), filings, self.settings)
        notables = notable_candidates(daily_input.etfs, self.settings)
        # 종목코드와 티커는 로그에 쓰지 않는다. 건수만 남긴다.
        log.info("daily %s collection: job=%s etfs=%d articles=%d articlesExcluded=%d articlesCut=%d "
                 "articlesTruncated=%d articlesNoResponse=%d filings=%d filingsExcluded=%d memberIssues=%d "
                 "memberIssuesCut=%d notables=%d articlesIrrelevant=%d",
                 job.edition, job.job_id, len(daily_input.etfs), sum(len(g.articles) for g in groups),
                 job.excluded.get("articles", 0), job.news_cut, sum(1 for g in groups if g.truncated),
                 sum(1 for g in groups if not g.received), len(filings), job.excluded.get("filings", 0),
                 len(issues), issues_cut, len(notables), job.excluded.get(EXCLUDED_UNRELATED, 0))
        return replace(daily_input, etf_articles=groups, member_issues=issues, notables=notables)

    def _write(self, job: _DailyJob, now: datetime, daily_input: DailyInput,
               sections: tuple[Section, ...]) -> _Draft:
        """본문을 쓴다. LLM이 없거나 끝내 실패하면 규칙 기반 본문이다. LLM 시도는 줄마다 남긴다."""
        missing = missing_items(sections)
        # 장 시작 전에는 그날 등락이 없어 Claude에 ETF별 이유를 묻지 않는다. 코드가 쓴 본문과 안내 문구로 나간다.
        if daily_input.before_open(self.settings):
            log.info("daily %s before the market opened: job=%s, no LLM call", job.edition, job.job_id)
        elif self.writer is not None and has_evidence(sections):
            result = self.writer.write(daily_input, sections)
            for attempt in result.rejected:
                self._detail(job, STAGE_LLM, EntryType.LLM_REJECTED, REASON_LLM_REJECTED, attempt.error_type,
                             attempt.attempt, now,
                             missingItems=missing,
                             llm={"input": attempt.llm_input, "rawOutput": attempt.raw_output,
                                  "rejectReason": attempt.reason})
            if result.body is None:
                detail = f"daily LLM failed after {len(result.rejected)} attempts"
                last = result.rejected[-1] if result.rejected else None
                self._detail(job, STAGE_LLM, EntryType.STEP_FAILED, FailureReason.LLM_ERROR.value,
                             last.error_type if last else None, len(result.rejected), now, missingItems=missing,
                             llm={"input": last.llm_input if last else None,
                                  "rawOutput": last.raw_output if last else None,
                                  "rejectReason": last.reason if last else None})
                self._first_failure(job, FailureReason.LLM_ERROR, detail, len(result.rejected))
            else:
                body = result.body
                if result.accepted is not None:
                    self._accepted(job, now, result.accepted, missing)
                return _Draft(body.text, body.generated_by, body.cited, body.insufficient_evidence,
                              body.weak_shortfalls, body.output)
        return _Draft(rule_based_text(daily_input, sections), GENERATED_BY_RULE, ())

    def _accepted(self, job: _DailyJob, now: datetime, a: Any, missing: list[str], *,
                  sentence_no: int | None = None, round: int | None = None) -> None:
        self._detail(job, STAGE_LLM, EntryType.REPORT_WRITTEN, REASON_LLM_ACCEPTED, None, a.attempt, now,
                     sentence_no=sentence_no, round=round, missingItems=missing,
                     llm={"input": a.llm_input, "rawOutput": a.response, "rejectReason": None})

    # ---- 문장 단위 재검색(어댑터가 부른다) ----

    def rewrite_point(self, draft: DailyDraft, no: int, round: int, articles: tuple[Article, ...],
                      research_articles: tuple[Article, ...], now: datetime) -> DailyDraft | None:
        """근거가 약한 문장 하나만 다시 쓰고 본문을 다시 만든다. 끝내 못 쓰면 None."""
        current = draft.draft.output.points[no - 1]
        rewritten_input = replace(draft.input, research_articles=research_articles)
        sections = build_sections(rewritten_input, self.settings)
        result = self.writer.rewrite_point(rewritten_input, sections, current, no, tuple(a.article_id for a in articles))
        missing = missing_items(sections)
        for attempt in result.rejected:
            self._detail(draft.job, STAGE_LLM, EntryType.LLM_REJECTED, REASON_LLM_REJECTED, attempt.error_type,
                         attempt.attempt, now, sentence_no=no, round=round, missingItems=missing,
                         llm={"input": attempt.llm_input, "rawOutput": attempt.raw_output,
                              "rejectReason": attempt.reason})
        if result.point is None:
            return None
        self._accepted(draft.job, now, result.accepted, missing, sentence_no=no, round=round)
        points = list(draft.draft.output.points)
        points[no - 1] = result.point
        output = replace(draft.draft.output, points=tuple(points))
        # 환율·금리 이슈 칸의 근거도 초안 단계처럼 인용한 번호에 들어간다.
        cited = tuple(dict.fromkeys(ref for p in (*points, *output.issues) for ref in p.refs))
        weak = tuple(p.shortfall for p in points if p.shortfall)
        new = _Draft(llm_text(rewritten_input, sections, output), GENERATED_BY_LLM, cited, False, weak, output)
        return DailyDraft(draft.job, rewritten_input, sections, new)

    def rewrite_whole(self, draft: DailyDraft, round: int, research_articles: tuple[Article, ...],
                      now: datetime) -> DailyDraft | None:
        """근거 부족이던 보고서 전체를 재검색 기사를 더해 다시 쓴다. 끝내 못 쓰면 None(첫 결과를 낸다)."""
        rewritten_input = replace(draft.input, research_articles=research_articles)
        sections = build_sections(rewritten_input, self.settings)
        new = self._write(draft.job, now, rewritten_input, sections)
        if new.generated_by != GENERATED_BY_LLM:
            return None
        return DailyDraft(draft.job, rewritten_input, sections, new)

    # ---- 서버 상태 절 ----

    def _status_report(self, job: _DailyJob, now: datetime, draft: _Draft) -> StatusReport | None:
        """마감 후 판 끝에 붙일 서버 상태 절. 만들지 못해도 판은 나간다."""
        if job.edition != EDITION_POST_CLOSE or self.status is None:
            return None
        try:
            return build_status_report(
                range_start=job.range_start, as_of=job.scheduled_for, current_job_id=job.job_id,
                current_rule_based=draft.generated_by == GENERATED_BY_RULE, operations=job.operations,
                collections=self.status.collections_between(job.range_start, job.scheduled_for),
                runs=self.status.job_runs_between(job.range_start, job.scheduled_for),
                stuck_after=self.settings.stuck_after)
        except Exception as error:  # noqa: BLE001 - 서버 상태는 덤이라 실패해도 본문을 막지 않는다
            log.warning("server status section failed for job %s: %s", job.job_id, type(error).__name__)
            return StatusReport(NO_STATUS_TEXT, ("서버 상태: 전체(만들지 못함)",))

    # ---- 발행 ----

    def publish_draft(self, draft: DailyDraft, now: datetime, research_count: int) -> None:
        self._publish(draft.job, now, draft.input, draft.sections, draft.draft, research_count)

    def _chart_image(self, daily_input: DailyInput) -> tuple[bytes | None, bool, str | None]:
        """마감 후 판의 하루 등락 차트. (그림, 크기 초과 여부, 자료 없음 목록에 더할 줄). 그릴 막대가 없으면 그림이 없다."""
        bars = chart_bars(daily_input)
        if not bars:
            return None, False, None
        try:
            png = render_day_chart(bars)
        except Exception as error:  # noqa: BLE001 - 차트는 덤이라 실패해도 메일은 나간다
            log.warning("daily chart failed for job %s: %s", daily_input.job_id, type(error).__name__)
            return None, False, CHART_FAILED_ITEM
        # 그림은 한 장씩 만들지만 상한(장 수)도 지켜서, 여러 장이 되는 날에도 같은 방식으로 뺀다.
        image_count = 1
        if png is not None and (image_count > self.settings.max_chart_images
                                or len(png) > self.settings.max_chart_image_bytes):
            return None, True, CHART_OVERSIZED_ITEM
        return png, False, None

    def _daily_event(self, job: _DailyJob, now: datetime, daily_input: DailyInput, sections: tuple[Section, ...],
                     draft: _Draft, text: str, status_text: str | None,
                     missing: list[str]) -> tuple[OutboundEvent, list[str]]:
        """글자판·HTML·차트를 담은 메일 본문 이벤트. 이벤트가 상한을 넘으면 그림만 빼고 다시 만든다."""
        post_close = job.edition == EDITION_POST_CLOSE
        png, oversized, chart_missing = self._chart_image(daily_input) if post_close else (None, False, None)

        def build(image: bytes | None, too_large: bool, items: list[str]) -> OutboundEvent:
            slot = None
            if post_close:
                slot = ChartSlot("chart-day" if image is not None else None, too_large, no_data_names(daily_input))
            html = render_html(daily_input, sections, draft.output, status_text, slot)
            images = [{"contentId": "chart-day", "contentType": "image/png",
                       "data": base64.b64encode(image).decode("ascii")}] if image is not None else None
            return daily_briefing_event(job.job_id, job.correlation_id, job.scheduled_for, now, job.edition,
                                        job.range_start, job.scheduled_for, text, items, draft.generated_by,
                                        html, images)

        items = [*missing, chart_missing] if chart_missing else missing
        event = build(png, oversized, items)
        if png is not None and len(json.dumps(event.value, ensure_ascii=False).encode("utf-8")) > self.settings.max_event_bytes:
            items = [*missing, CHART_OVERSIZED_ITEM]
            event = build(None, True, items)
        return event, items

    def _publish(self, job: _DailyJob, now: datetime, daily_input: DailyInput, sections: tuple[Section, ...],
                 draft: _Draft, research_count: int = 0) -> None:
        missing = missing_items(sections)
        by_ref = {e.ref: e for s in sections for e in s.entries}
        report = self._status_report(job, now, draft)
        status_text = report.text if report is not None else None
        if report is not None:
            missing = [*missing, *report.missing]
        # 글자판은 서버 상태를 면책 아래·근거 목록 위에 끼워 다시 만든다.
        text = daily_text(daily_input, sections, draft.output, status_text)
        event, missing = self._daily_event(job, now, daily_input, sections, draft, text, status_text, missing)
        if missing:
            self._detail(job, STAGE_COMPOSE, EntryType.MISSING_ITEMS, REASON_MISSING_ITEMS, None, 0, now,
                         missingItems=missing,
                         # 일봉이 빈 ETF와 이유 코드. 화면 로그가 아니라 작업 기록에만 남는다.
                         missingEtfs=[{"symbol": e.symbol, "name": e.label(), "reason": e.missing_reason}
                                      for e in daily_input.etfs or () if not e.has_day()],
                         evidence=[{"ref": ref, "describe": by_ref[ref].describe} for ref in draft.cited],
                         weakItems=list(draft.weak_shortfalls))
        result = JobResult(
            job.job_id, RunStatus.SUCCEEDED, now, generated_by=GeneratedBy(draft.generated_by),
            final_text=text, weak_count=len(draft.weak_shortfalls), research_count=research_count,
            # 성공했어도 중간에 포기한 단계가 있으면 첫 사유를 남긴다.
            failure_reason=job.first_failure[0].value if job.first_failure is not None else None,
            result_event_id=event.value["eventId"])
        if not record_result(self.job_log, result):
            del self._jobs[job.job_id]
            return
        self.requests.send(event)
        log.info("daily %s ready: job=%s generatedBy=%s missing=%d etfs=%s filings=%s excluded=%s",
                 job.edition, job.job_id, draft.generated_by, len(missing),
                 "n/a" if job.etfs is None else len(job.etfs),
                 "n/a" if job.filings_data is None else len(job.filings_data), sum(job.excluded.values()))
        del self._jobs[job.job_id]


# ---- 문장 단위 재검색 어댑터 ----

EDITION_JOB_KINDS = (JobKind.MORNING.value, JobKind.POST_CLOSE.value)


@dataclass(frozen=True)
class DailyDraft:
    """데일리 판 본문 한 벌. 다시 쓰면 재검색 기사가 든 입력·항목과 새 본문으로 바뀐다."""

    job: _DailyJob
    input: DailyInput
    sections: tuple[Section, ...]
    draft: _Draft


@dataclass
class DailySentences:
    """데일리 판 요약의 문장(point) 하나를 재검색의 문장 하나로 본다."""

    core: DailyCore

    def can_research(self, draft: DailyDraft) -> bool:
        # 기사 조회 요청에는 종목코드가 필수라 보유 ETF 자료가 없으면 재검색하지 않는다.
        return bool(draft.input.etfs) and self.core.writer is not None

    def weak_sentences(self, draft: DailyDraft) -> tuple[WeakSentence, ...]:
        output = draft.draft.output
        if output is None or output.insufficient_evidence:
            return ()
        # 근거 약한 ETF 이유만 다시 찾는다. 눈에 띄는 점은 '근거 약함'을 그대로 보여 주고, 시장과 같은 방향(부호가 같고
        # 차이가 설정값 이내)으로 움직인 ETF는 시장 전체의 움직임으로 설명되므로 따로 찾지 않는다.
        etfs = {e.symbol: e for e in draft.input.etfs or ()}
        return tuple(WeakSentence(n, p.statement, p.shortfall)
                     for n, p in enumerate(output.points, start=1)
                     if p.shortfall and p.kind != POINT_NOTABLE
                     and not (p.symbol in etfs and moves_with_market(etfs[p.symbol].rate, draft.input.background or (),
                                                       self.core.settings)))

    def whole_insufficient(self, draft: DailyDraft) -> bool:
        return draft.draft.insufficient_evidence

    def query_context(self, draft: DailyDraft, sentence: WeakSentence | None) -> dict[str, Any]:
        llm = build_llm_input(draft.input, draft.sections, self.core.settings)
        background = [e.llm for s in draft.sections for e in s.entries if e.llm.get("category") == BACKGROUND_LABEL]
        context: dict[str, Any] = {
            "edition": llm["edition"], "rangeStart": llm["rangeStart"], "asOf": llm["asOf"],
            "foundArticleTitles": [a.title for a in draft.input.research_articles],
            "background": background,
        }
        if "etfInfo" in llm:
            context["etfInfo"] = llm["etfInfo"]
        if sentence is not None:
            context["sentence"] = sentence.text
            context["shortfall"] = sentence.shortfall
            output = draft.draft.output
            point = output.points[sentence.no - 1] if output is not None and 0 < sentence.no <= len(output.points) else None
            if point is not None and point.symbol is not None:
                context["etf"] = next(({"symbol": e.symbol, "name": e.label()}
                                       for e in draft.input.etfs or () if e.symbol == point.symbol), None)
        return context

    def target(self, draft: DailyDraft, research_id: str, sentence_no: int, round: int) -> ResearchTarget:
        job = draft.job
        return ResearchTarget(
            job_kind=job.edition, job_id=research_id, parent_job_id=job.job_id, sentence_no=sentence_no,
            round=round, correlation_id=job.correlation_id, tag=MARKET_WIDE,
            # 데일리 공시 요청처럼 정렬한 첫 ETF 코드. 재검색 조회는 종목 조건을 보지 않고 응답 짝짓기에만 쓴다.
            reply_symbol=min(etf.symbol for etf in draft.input.etfs),
            since=job.range_start, until=job.scheduled_for)

    def rewrite_sentence(self, draft: DailyDraft, sentence_no: int, round: int, articles: tuple[Article, ...],
                         research_articles: tuple[Article, ...], now: datetime) -> DailyDraft | None:
        return self.core.rewrite_point(draft, sentence_no, round, articles, research_articles, now)

    def rewrite_whole(self, draft: DailyDraft, round: int, articles: tuple[Article, ...],
                      research_articles: tuple[Article, ...], now: datetime) -> DailyDraft | None:
        return self.core.rewrite_whole(draft, round, research_articles, now)

    def sentence_text(self, draft: DailyDraft, sentence_no: int) -> str | None:
        output = draft.draft.output
        if output is None or sentence_no == WHOLE_REPORT or sentence_no > len(output.points):
            return None
        return output.points[sentence_no - 1].statement

    def publish(self, draft: DailyDraft, now: datetime, research_count: int) -> None:
        self.core.publish_draft(draft, now, research_count)


def _parse_etf_day(e: dict[str, Any]) -> EtfDay:
    """v2 응답은 일봉이 없으면 값 칸이 null이다. v1 응답에는 missingReason이 없다."""
    fetched = e["fetchedAt"]
    return EtfDay(e["symbol"], e["date"], e["close"], e["prevClose"], e["rate"],
                  parse_timestamp(fetched) if fetched is not None else None,
                  tuple(e["tickers"]), bool(e["tickersTruncated"]), parse_etf_info(e.get("etf")),
                  e.get("missingReason"), _relation(e.get("relation")))


def _relation(value: Any) -> str | None:
    """보유/관심 구분. 칸이 없거나 모르는 값이면 구분을 모르는 것으로 본다."""
    return value if value in (RELATION_HELD, RELATION_WATCH) else None


def daily_data_request(correlation_id: str, now: datetime, payload: dict[str, Any]) -> OutboundEvent:
    """데일리 자료 요청 봉투. 종목 하나가 아니라 판 단위라 메시지 키는 jobId다."""
    event_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{DAILY_DATA_REQUESTED_TOPIC}:{payload['jobId']}:{payload['seq']}")
    return OutboundEvent(
        topic=DAILY_DATA_REQUESTED_TOPIC,
        key=payload["jobId"],
        value={
            "eventId": str(event_id),
            "eventType": DAILY_DATA_REQUESTED_TOPIC,
            "schemaVersion": 1,
            "occurredAt": format_timestamp(now),
            "producedAt": format_timestamp(now),
            "correlationId": correlation_id,
            "payload": payload,
        },
    )

