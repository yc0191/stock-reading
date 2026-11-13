"""데일리 판(아침 판, 마감 후 판) 작업. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: 수집기의 판 시작 알림(daily.collection.completed) 또는 한도 시각(알림이 끝내 오지 않을 때) -> 같은 날 같은 판을
이미 만들었으면 그만둠 -> 범위 계산 -> 데일리 자료 조회(시세처리기 내부 조회 API)
-> ETF마다 기사 조회, 미국 종목이 있으면 그 티커로 공시 조회(뉴스처리기) -> 받은 자료 재검사 -> 본문(LLM 또는 규칙 기반)
-> (LLM이 약한 문장을 냈으면 문장마다 뉴스 재검색 -> 그 문장만 다시 쓰기, 근거 부족이면 보고서 전체를 한 번 재검색)
-> 멤버마다 그 멤버의 포트폴리오 ETF만 담은 판을 짜서 briefing.daily.generated 발행(LLM 본문은 판 하나에 한 번만 쓴다).
조회를 끝내 받지 못하거나 LLM이 끝내 실패해도 판은 나간다. 예외는 작업 기록을 끝내 저장하지 못했을 때와 받을 멤버를 읽지 못했을 때뿐이며,
그때는 이 판의 결과 이벤트를 발행하지 않고 ERROR 로그만 남긴다.
"""
from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from typing import Any, Callable

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DEFAULT_SETTINGS, CoreSettings, DailySettings
from briefing.daily_chart import chart_bars, no_data_names, render_day_chart
from briefing.daily_html import ChartSlot, render_html
from briefing.daily_members import MemberPort, MemberPortfolio, member_input, member_output, member_sections
from briefing.daily_morning import outlook_records
from briefing.daily_inputs import EtfArticles, member_issues, moves_with_market, notable_candidates
from briefing.daily_relevance import split_related
from briefing.daily_start import DailyStart
from briefing.daily_review import MORNING_READ_FAILED, NO_ACTUAL_RATES, NO_MORNING_CALLS, ReviewInput, parse_calls
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
    build_sections,
    daily_text,
    has_evidence,
    llm_text,
    missing_items,
    rule_based_text,
)
from briefing.llm import usage_record
from briefing.events import (
    EDITION_MORNING,
    EDITION_POST_CLOSE,
    EDITIONS,
    OutboundEvent,
    daily_briefing_event,
    format_timestamp,
    parse_timestamp,
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
from briefing.final_check import FinalCheck
from briefing.open_flow import in_range, open_flow_start, parse_open_flow
from briefing.job_step import job_scope
from briefing.ports import FailureReason, RequestPort
from briefing.queries import Query, QueryKind, QueryPort, QueryResult
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
    parse_articles,
    parse_background,
    parse_close_rates,
    parse_etf_info,
    parse_filings,
)

log = logging.getLogger("briefing.daily_jobs")

# 차트를 싣지 못했을 때 자료 없음 목록(missingItems)에 남기는 줄.
CHART_OVERSIZED_ITEM = "하루 등락 차트: 크기 초과"
CHART_FAILED_ITEM = "하루 등락 차트: 그리지 못함"
# 서버 상태 절을 만들다 실패했을 때 본문 끝에 대신 붙이는 절.
NO_STATUS_TEXT = "──────── 서버 상태 ────────\n- 자료 없음"

# 작업 기록(requests, received, notReceived)에 남기는 조회 이름
DAILY_DATA = QueryKind.DAILY.value
FILINGS = QueryKind.FILINGS.value
ARTICLES = QueryKind.ARTICLES.value
# 조회 결과가 왔지만 해석하지 못해 받지 못한 것으로 본 경우의 실패 종류
FAILURE_MALFORMED = "MalformedResponse"

# 작업 기록의 단계와 사유. 끝내 실패한 사유는 FailureReason 값을 그대로 쓴다.
STAGE_DAILY_DATA = "daily-data-request"
STAGE_FILINGS = "filings-request"
STAGE_ETF_ARTICLES = "etf-articles-request"
STAGE_LLM = "llm"
STAGE_COMPOSE = "compose"
REASON_LLM_REJECTED = "llm_attempt_rejected"
REASON_MISSING_ITEMS = "missing_items"
REASON_LLM_ACCEPTED = "llm_attempt_accepted"
REASON_PUBLISHED = "published"
# 판을 멤버 한 명 몫으로 발행함 / 그 멤버의 판을 짜다 실패함
REASON_MEMBER_PUBLISHED = "member_published"
# 판을 받을 멤버를 읽지 못해 발행하지 못한 작업의 실패 사유
REASON_MEMBERS_UNREADABLE = "members_unreadable"
REASON_MEMBER_FAILED = "member_failed"
REASON_OUTLOOK_RECORDED = "morning_outlook_recorded"
# 마감 후 판이 같은 날 아침 예상을 읽지 못해 대조를 뺀 사유
REASON_OUTLOOK_MISSING = "morning_outlook_missing"
# 아침 예상은 있는데 대조할 실제 등락이 하나도 없어 대조를 뺀 사유
REASON_OUTLOOK_NO_RATES = "morning_outlook_no_actual_rates"
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
    # 문장 단위 재검색으로 다시 쓴 문장(points) 번호. 보고서 전체를 다시 썼으면 WHOLE_REPORT. 마지막 검사가 검토를 부를지 정한다.
    rewritten: tuple[int, ...] = ()


@dataclass
class _EtfNews(Exchange):
    """ETF 하나의 기사 조회 상태. seq는 작업 안에서 겹치지 않는 번호다."""

    articles: tuple[Article, ...] = ()
    truncated: bool = False


@dataclass
class _DailyJob:
    job_id: str
    edition: str
    correlation_id: str
    scheduled_for: datetime
    range_start: datetime
    data: Exchange = field(default_factory=Exchange)
    filings: Exchange = field(default_factory=Exchange)
    data_payload: dict | None = None
    operations: Operations | None = None
    filings_payload: dict | None = None
    etfs: tuple[EtfDay, ...] | None = None
    etfs_truncated: bool = False
    close_rates: tuple[CloseRate, ...] | None = None
    background: tuple[Background, ...] | None = None
    filings_data: tuple[Filing, ...] | None = None
    filings_truncated: bool = False
    # ETF 종목코드마다 기사 조회 상태와, 받은 응답 원문(작업 기록용)
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
    # 아침 판만: 개장 시세를 읽을 구간의 시작. 판 시각이 장 시작 전이라 읽지 않으면 None.
    open_flow_since: datetime | None = None
    # 판을 시작시킨 쪽이 알린 빠진 자료(못 받은 수집 자료, 알림 없이 시작함). 판의 자료 없음 목록 맨 앞에 붙는다.
    notes: tuple[str, ...] = ()

    def waiting(self) -> bool:
        return self.data.awaiting() or self.filings.awaiting() or any(n.awaiting() for n in self.news.values())


@dataclass
class DailyCore:
    requests: RequestPort
    # 데일리 자료·기사·공시 조회(시세처리기·뉴스처리기 내부 조회 API)
    api: QueryPort
    writer: DailyLlmWriter | None = None
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    # 아침 판의 개장 시세 범위(장 시작 시각, 거슬러 읽는 상한)는 근거 조회와 같은 CoreSettings에서 온다.
    core_settings: CoreSettings = DEFAULT_SETTINGS
    # 있을 때만 재검색한다. 급변동과 함께 쓰는 문장 단위 재검색 흐름이다.
    sentences: SentenceResearch | None = None
    # 있을 때만 마감 후 판 끝에 서버 상태 절을 붙인다.
    status: StatusPort | None = None
    # 판을 받을 멤버와 그 포트폴리오를 읽는 곳. 없으면 받을 멤버를 모르므로 판을 발행하지 않는다.
    members: MemberPort | None = None
    # 아침 판의 결과(판 시각, 나갔는가, 판에 개장 흐름이 담긴 종목코드, 개장 흐름을 읽은 구간 시작)를 받는 곳.
    # 장 시작 뒤 급변동을 아침 판에 합쳤는지 정하는 데 쓴다.
    on_morning_result: Callable[[datetime, bool, datetime, frozenset[str], datetime | None], None] | None = None
    # 있을 때만 LLM 본문을 발행 직전 마지막 검사(코드 검사, 필요하면 LLM 검토와 고침)에 거친다.
    final: FinalCheck | None = None
    _jobs: dict[str, _DailyJob] = field(default_factory=dict, init=False)
    _starter: DailyStart = field(init=False)

    def __post_init__(self) -> None:
        self._starter = DailyStart(self.start, self.job_log, self.settings)
        if self.sentences is not None:
            adapter = DailySentences(self)
            for edition in EDITION_JOB_KINDS:
                self.sentences.register(edition, adapter)

    # ---- 입력 ----

    def on_collection_ready(self, message: str | bytes, now: datetime) -> None:
        """수집기의 판 시작 알림(daily.collection.completed). 판을 시작할지는 DailyStart가 정한다. 형식이 잘못되면 ValueError."""
        self._starter.on_collection_ready(message, now)

    def check_deadlines(self, now: datetime) -> None:
        """한도 시각까지 판 시작 알림이 없으면 있는 자료로 판을 시작한다."""
        self._starter.check_deadlines(now)

    def start(self, job_id: str, edition: str, correlation_id: str, scheduled_for: datetime, raw: str,
               now: datetime, notes: tuple[str, ...]) -> None:
        job = _DailyJob(job_id, edition, correlation_id, scheduled_for, range_start(scheduled_for, self.settings),
                        notes=notes)
        if edition == EDITION_MORNING:
            job.open_flow_since = open_flow_start(scheduled_for, self.core_settings)
        self._jobs[job_id] = job
        # 작업 한 줄은 시작 때 만들고 끝날 때 고친다. 입력은 시작시킨 알림 원문(한도 시각이면 그 사실)이다.
        self.job_log.start(JobStart(job_id, JobKind(edition), raw,
                                    range_target(format_timestamp(job.range_start), format_timestamp(scheduled_for)),
                                    now))
        log.info("daily %s started: job=%s range=%s~%s notes=%d", edition, job_id,
                 format_timestamp(job.range_start), format_timestamp(scheduled_for), len(notes))
        self._send_data(job, now)

    # ---- 조회 결과 (수신 루프 스레드에서 불린다) ----

    def _on_daily_data(self, result: QueryResult, now: datetime) -> None:
        job = self._jobs.get(result.query.payload["jobId"])
        if job is None or not job.data.awaiting():
            return
        parsed = None
        if result.received:
            payload = result.payload
            try:
                parsed = (tuple(_parse_etf_day(e) for e in payload["etfs"]), parse_close_rates(payload["closeRates"]),
                          tuple(parse_background(b) for b in payload["background"]), bool(payload["truncated"]))
            except (KeyError, TypeError, ValueError) as error:
                _malformed(QueryKind.DAILY, job.job_id, error)
        if parsed is None:
            # 데일리 자료 없이도 판은 나간다(자료 없음 표시).
            self._give_up(job, job.data, STAGE_DAILY_DATA, FailureReason.CONTEXT_NO_RESPONSE, DAILY_DATA, result, now)
            self._finish(job, now)
            return
        etfs, close_rates, background, truncated = parsed
        job.data.received = True
        job.data_payload = result.payload
        job.operations = parse_operations(result.payload.get("operations"))
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
        # 맡긴 조회의 결과가 남아 있으면 _finish가 기다린다.
        self._finish(job, now)

    def _on_articles(self, result: QueryResult, now: datetime) -> None:
        """ETF 하나의 기사 조회 결과. 끝내 받지 못하면 그 ETF는 기사 없음으로 판을 낸다."""
        payload = result.query.payload
        job = self._jobs.get(payload["jobId"])
        state = job.news.get(payload["symbol"]) if job is not None else None
        if state is None or not state.awaiting():
            return
        articles = None
        if result.received:
            try:
                articles = parse_articles(result.payload["articles"])
                truncated = bool(result.payload["truncated"])
            except (KeyError, TypeError, ValueError) as error:
                _malformed(QueryKind.ARTICLES, job.job_id, error)
        if articles is None:
            self._give_up(job, state, STAGE_ETF_ARTICLES, FailureReason.NEWS_NO_RESPONSE, ARTICLES, result, now)
            self._finish(job, now)
            return
        in_range = sorted((a for a in articles if job.range_start <= a.published_at <= job.scheduled_for),
                          key=lambda a: a.published_at, reverse=True)
        self._note_excluded(job, "articles", len(articles) - len(in_range))
        # 그 ETF와 관계없는 기사는 상한에 자리를 차지하기 전에 뺀다. 뺀 건수는 작업 기록의 excluded에 남는다.
        symbol = payload["symbol"]
        etf = next((e for e in job.etfs or () if e.symbol == symbol), None)
        related, unrelated = split_related(etf.etf if etf is not None else None, in_range, self.settings)
        if unrelated:
            job.excluded[EXCLUDED_UNRELATED] = job.excluded.get(EXCLUDED_UNRELATED, 0) + len(unrelated)
            # 실데이터에서 걸러낸 기사가 맞는지 볼 수 있게 제목을 작업 기록에 남긴다(로그 파일에는 쓰지 않는다).
            job.unrelated_titles.extend({"symbol": symbol, "title": a.title} for a in unrelated)
        kept = tuple(related[:self.settings.etf_max_articles])
        job.news_cut += len(related) - len(kept)
        state.received, state.articles, state.truncated = True, kept, truncated or len(kept) < len(related)
        job.news_payloads.append(result.payload)
        self._finish(job, now)

    def _on_filings(self, result: QueryResult, now: datetime) -> None:
        job = self._jobs.get(result.query.payload["jobId"])
        if job is None or not job.filings.awaiting():
            return
        filings = None
        if result.received:
            try:
                filings = parse_filings(result.payload["filings"])
                truncated = bool(result.payload["truncated"])
            except (KeyError, TypeError, ValueError) as error:
                _malformed(QueryKind.FILINGS, job.job_id, error)
        if filings is None:
            self._give_up(job, job.filings, STAGE_FILINGS, FailureReason.FILINGS_NO_RESPONSE, FILINGS, result, now)
            self._finish(job, now)
            return
        job.filings.received = True
        job.filings_payload = result.payload
        kept = tuple(f for f in filings if job.range_start <= f.accepted_at <= job.scheduled_for)
        self._note_excluded(job, "filings", len(filings) - len(kept))
        job.filings_data, job.filings_truncated = kept, truncated
        self._finish(job, now)

    def morning_in_progress(self, day: date) -> bool:
        """그 날의 아침 판이 시작됐고 아직 끝나지 않았는가."""
        return any(j.edition == EDITION_MORNING and j.scheduled_for.astimezone(KST).date() == day
                   for j in self._jobs.values())

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
        if job.edition == EDITION_POST_CLOSE:
            # 일봉 없는 ETF의 장중 시세 대체값(#64). 아침 판은 쓰지 않으므로 마감 후 판만 요청한다.
            payload["includeIntradayFallback"] = True
        if job.edition == EDITION_POST_CLOSE and self.status is not None:
            # 서버 상태 절의 시세처리기 운영 상태. 마감 후 판만 요청한다.
            payload["includeOperations"] = True
        if job.open_flow_since is not None:
            payload["openFlowSince"] = format_timestamp(job.open_flow_since)
        self._submit(job, QueryKind.DAILY, STAGE_DAILY_DATA, payload, now, self._on_daily_data)

    def _send_articles(self, job: _DailyJob, symbol: str, now: datetime) -> None:
        state = job.news.setdefault(symbol, _EtfNews())
        job.news_seq += 1
        state.seq, state.sent_at = job.news_seq, now
        payload = {
            "jobId": job.job_id,
            "seq": state.seq,
            "symbol": symbol,
            "since": format_timestamp(job.range_start),
            "until": format_timestamp(job.scheduled_for),
            "maxArticles": self.settings.etf_max_articles,
        }
        self._submit(job, QueryKind.ARTICLES, STAGE_ETF_ARTICLES, payload, now, self._on_articles)

    def _send_filings(self, job: _DailyJob, now: datetime) -> None:
        seq = next_seq(job.filings, now)
        with_tickers = [etf for etf in sorted(job.etfs, key=lambda e: e.symbol) if etf.tickers]
        payload = {
            "jobId": job.job_id,
            "seq": seq,
            # 계약상 필수인 종목코드. 조회 조건으로 쓰이지 않으므로 미국 종목이 있는 첫 ETF를 넣는다.
            "symbol": with_tickers[0].symbol,
            "tickers": list(dict.fromkeys(t for etf in with_tickers for t in etf.tickers)),
            "since": format_timestamp(job.range_start),
            "until": format_timestamp(job.scheduled_for),
            "maxFilings": self.settings.max_filings,
        }
        self._submit(job, QueryKind.FILINGS, STAGE_FILINGS, payload, now, self._on_filings)

    def _submit(self, job: _DailyJob, kind: QueryKind, stage: str, payload: dict[str, Any], now: datetime,
                on_result) -> None:
        job.sent.append(_Sent(kind.value, payload["seq"], now, payload))
        self.api.submit(Query(kind, job.job_id, stage, job.correlation_id, payload), on_result)

    # ---- 재검사 ----

    def _recheck_etfs(self, job: _DailyJob, etfs: tuple[EtfDay, ...]) -> tuple[EtfDay, ...]:
        # 일봉이 없는 ETF(받은 시각 없음)는 버리지 않고 자료 없음으로 싣는다.
        kept = tuple(e for e in etfs if e.fetched_at is None or job.range_start <= e.fetched_at <= job.scheduled_for)
        self._note_excluded(job, "etfs", len(etfs) - len(kept))
        # 개장 흐름은 읽은 구간(장 시작~판 시각) 밖이거나 요청하지 않았는데 온 것을 버리고 자료 없음으로 싣는다.
        rechecked = tuple(replace(e, open_flow=None) if e.open_flow is not None and (
            job.open_flow_since is None or not in_range(e.open_flow, job.open_flow_since, job.scheduled_for)) else e
                          for e in kept)
        self._note_excluded(job, "openFlow", sum(1 for a, b in zip(kept, rechecked) if a is not b))
        # 장중 시세 대체값은 마감 후 판에서만, 판 범위 안에 받은 것만 쓴다. 아침 판의 하루 등락은 전날 일봉이라
        # 그날 아침 시세로 대신하면 다른 날 등락이 된다.
        final = tuple(e.without_fallback() if e.intraday_at is not None and (
            job.edition != EDITION_POST_CLOSE or not job.range_start <= e.intraday_at <= job.scheduled_for) else e
                      for e in rechecked)
        self._note_excluded(job, "intradayFallback", sum(1 for a, b in zip(rechecked, final) if a is not b))
        return final

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
            "received": {**{name: payload for name, payload in ((DAILY_DATA, job.data_payload),
                                                                (FILINGS, job.filings_payload))
                            if payload is not None},
                         **({ARTICLES: job.news_payloads} if job.news_payloads else {})},
            "notReceived": [*(name for name, exchange in ((DAILY_DATA, job.data), (FILINGS, job.filings))
                              if exchange.seq and not exchange.received),
                            *([ARTICLES] if any(not n.received for n in job.news.values()) else [])],
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

    def _give_up(self, job: _DailyJob, exchange: Exchange, stage: str, reason: FailureReason, name: str,
                 result: QueryResult, now: datetime) -> None:
        """조회를 끝내 받지 못했다. 그 자료가 빠진 채로 판을 낸다. 시도마다의 기록은 job_step에 있다."""
        exchange.given_up = True
        error_type = result.failure_kind or FAILURE_MALFORMED
        self._detail(job, stage, EntryType.STEP_FAILED, reason.value, error_type, result.attempts, now,
                     failed={"query": name, "seq": exchange.seq, "failureKind": error_type})
        self._first_failure(job, reason, f"no {name} result after {result.attempts} attempt(s)", result.attempts)

    def _first_failure(self, job: _DailyJob, reason: FailureReason, detail: str, attempts: int) -> None:
        if job.first_failure is None:
            job.first_failure = (reason, detail, attempts)

    # ---- 완성 ----

    def _finish(self, job: _DailyJob, now: datetime) -> None:
        if job.waiting() or job.researching:
            return
        # 본문을 쓰고 발행하는 LLM 호출을 이 판의 job_step으로 남긴다.
        with job_scope(job.job_id):
            self._compose(job, now)

    def _compose(self, job: _DailyJob, now: datetime) -> None:
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
            open_flow_start=job.open_flow_since,
            settings=self.settings,
        )
        if job.edition in EDITIONS_WITH_ETF_ARTICLES:
            daily_input = self._with_etf_inputs(job, daily_input)
        if job.edition == EDITION_POST_CLOSE and daily_input.etfs and not daily_input.before_open(self.settings):
            daily_input = replace(daily_input, review=self._read_review(job, now, daily_input))
        sections = build_sections(daily_input, self.settings)
        draft = self._write(job, now, daily_input, sections)
        # 문장 단위 재검색이 시작되면 그 흐름이 끝난 뒤 발행한다. 시작하지 않으면 바로 발행한다.
        job.researching = self.sentences is not None and draft.generated_by == GENERATED_BY_LLM \
            and self.sentences.begin(job.edition, job.job_id, DailyDraft(job, daily_input, sections, draft), now)
        if not job.researching:
            self._finalize(job, now, daily_input, sections, draft)

    def _finalize(self, job: _DailyJob, now: datetime, daily_input: DailyInput, sections: tuple[Section, ...],
                  draft: _Draft, research_count: int = 0) -> None:
        """마지막 검사를 거친 뒤 아침 판의 ETF별 예상을 작업 기록에 남기고 발행한다. 최종 본문 기준으로 한 번만 남긴다."""
        if self.final is not None and draft.generated_by == GENERATED_BY_LLM:
            draft = self.final.run(DailyDraft(job, daily_input, sections, draft), now).draft
        outlooks = outlook_records(draft.output)
        if job.edition == EDITION_MORNING and outlooks:
            # 마감 후 판이 같은 날의 이 기록을 읽어 예상과 실제 등락을 대조한다(작업 종류 morning, 범위 끝 날짜로 찾음).
            self.job_log.record(JobLogEntry(
                job_id=job.job_id, entry_type=EntryType.ETF_OUTLOOK, stage=STAGE_COMPOSE,
                reason=REASON_OUTLOOK_RECORDED, occurred_at=now,
                data={"asOf": format_timestamp(job.scheduled_for), "outlooks": outlooks}))
        self._publish(job, now, daily_input, sections, draft, research_count)

    def _read_review(self, job: _DailyJob, now: datetime, daily_input: DailyInput) -> ReviewInput:
        """같은 날 아침 판의 ETF별 예상과 그날 급변동을 작업 기록에서 읽는다. 못 읽어도 판은 나가고, 대조를 뺀 이유를 기록에 남긴다."""
        day = job.scheduled_for.astimezone(KST).date()
        day_start = datetime.combine(day, time.min, tzinfo=KST)
        records = self.job_log.morning_outlooks(day)
        calls = parse_calls(records) if records is not None else ()
        note = None if calls else MORNING_READ_FAILED if records is None else NO_MORNING_CALLS
        reason = REASON_OUTLOOK_MISSING
        if calls and not replace(daily_input, review=ReviewInput(calls, None, ())).reviews():
            note, reason = NO_ACTUAL_RATES, REASON_OUTLOOK_NO_RATES
        if note is not None:
            self._detail(job, STAGE_COMPOSE, EntryType.MISSING_ITEMS, reason,
                         "ReadFailed" if records is None else None, 0, now, missingItems=[note])
        moves = self.job_log.moves_between(day_start, job.scheduled_for)
        # 전날 밤 변동이 자정 직후에 시작된 경우를 빼려고 작업 시작이 아닌 변동 시각으로 거른다
        moves = tuple(m for m in moves if m.move_at >= day_start) if moves is not None else None
        return ReviewInput(calls, note, moves)

    def _with_etf_inputs(self, job: _DailyJob, daily_input: DailyInput) -> DailyInput:
        """ETF별 기사를 모아 입력에 더한다. 마감 후 판은 미국 구성 종목 이슈와 눈에 띄는 점 후보도 더한다."""
        if daily_input.etfs is None:
            return daily_input
        groups = tuple(EtfArticles(symbol, state.articles, state.received, state.truncated)
                       for symbol, state in job.news.items())
        if job.edition != EDITION_POST_CLOSE:
            # 아침 판은 기사만 더한다. 구성 종목 이슈와 눈에 띄는 점 후보는 마감 후 판의 것이다.
            log.info("daily %s collection: job=%s etfs=%d articles=%d articlesExcluded=%d articlesCut=%d "
                     "articlesNoResponse=%d articlesIrrelevant=%d",
                     job.edition, job.job_id, len(daily_input.etfs), sum(len(g.articles) for g in groups),
                     job.excluded.get("articles", 0), job.news_cut, sum(1 for g in groups if not g.received),
                     job.excluded.get(EXCLUDED_UNRELATED, 0))
            return replace(daily_input, etf_articles=groups)
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
                                  "rejectReason": attempt.reason, "usage": usage_record(attempt.usage)})
            if result.body is None:
                detail = f"daily LLM failed after {len(result.rejected)} attempts"
                last = result.rejected[-1] if result.rejected else None
                self._detail(job, STAGE_LLM, EntryType.STEP_FAILED, FailureReason.LLM_ERROR.value,
                             last.error_type if last else None, len(result.rejected), now, missingItems=missing,
                             llm={"input": last.llm_input if last else None,
                                  "rawOutput": last.raw_output if last else None,
                                  "rejectReason": last.reason if last else None,
                                  "usage": usage_record(last.usage) if last else None})
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
                     llm={"input": a.llm_input, "rawOutput": a.response, "rejectReason": None,
                          "usage": usage_record(a.usage), "articleGroups": a.article_groups})

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
                              "rejectReason": attempt.reason, "usage": usage_record(attempt.usage)})
        if result.point is None:
            return None
        self._accepted(draft.job, now, result.accepted, missing, sentence_no=no, round=round)
        points = list(draft.draft.output.points)
        points[no - 1] = result.point
        output = replace(draft.draft.output, points=tuple(points))
        # 환율·금리 이슈 칸과 아침 판 ETF 예상의 근거도 초안 단계처럼 인용한 번호에 들어간다.
        new = _Draft(llm_text(rewritten_input, sections, output), GENERATED_BY_LLM, output.cited_refs(), False,
                     output.weak_shortfalls(), output, draft.draft.rewritten + (no,))
        return DailyDraft(draft.job, rewritten_input, sections, new)

    def rewrite_whole(self, draft: DailyDraft, round: int, research_articles: tuple[Article, ...],
                      now: datetime) -> DailyDraft | None:
        """근거 부족이던 보고서 전체를 재검색 기사를 더해 다시 쓴다. 끝내 못 쓰면 None(첫 결과를 낸다)."""
        rewritten_input = replace(draft.input, research_articles=research_articles)
        sections = build_sections(rewritten_input, self.settings)
        new = self._write(draft.job, now, rewritten_input, sections)
        if new.generated_by != GENERATED_BY_LLM:
            return None
        return DailyDraft(draft.job, rewritten_input, sections, replace(new, rewritten=(WHOLE_REPORT,)))

    # ---- 서버 상태 절 ----

    def _status_report(self, job: _DailyJob, now: datetime, draft: _Draft,
                       research_count: int) -> StatusReport | None:
        """마감 후 판 끝에 붙일 서버 상태 절. 만들지 못해도 판은 나간다."""
        if job.edition != EDITION_POST_CLOSE or self.status is None:
            return None
        try:
            return build_status_report(
                range_start=job.range_start, as_of=job.scheduled_for, current_job_id=job.job_id,
                current_rule_based=draft.generated_by == GENERATED_BY_RULE, operations=job.operations,
                collections=self.status.collections_between(job.range_start, job.scheduled_for),
                runs=self.status.job_runs_between(job.range_start, job.scheduled_for),
                stuck_after=self.settings.stuck_after, current_research_count=research_count)
        except Exception as error:  # noqa: BLE001 - 서버 상태는 덤이라 실패해도 본문을 막지 않는다
            log.warning("server status section failed for job %s: %s", job.job_id, type(error).__name__)
            return StatusReport(NO_STATUS_TEXT, ("서버 상태: 전체(만들지 못함)",))

    # ---- 발행 ----

    def publish_draft(self, draft: DailyDraft, now: datetime, research_count: int) -> None:
        self._finalize(draft.job, now, draft.input, draft.sections, draft.draft, research_count)

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

    def _daily_event(self, job: _DailyJob, now: datetime, member_id: int, daily_input: DailyInput,
                     sections: tuple[Section, ...], draft: _Draft, text: str, status_text: str | None,
                     missing: list[str]) -> tuple[OutboundEvent, list[str]]:
        """멤버 한 명의 글자판·HTML·차트를 담은 메일 본문 이벤트. 이벤트가 상한을 넘으면 그림만 빼고 다시 만든다."""
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
                                        member_id, html, images)

        items = [*missing, chart_missing] if chart_missing else missing
        event = build(png, oversized, items)
        if png is not None and len(json.dumps(event.value, ensure_ascii=False).encode("utf-8")) > self.settings.max_event_bytes:
            items = [*missing, CHART_OVERSIZED_ITEM]
            event = build(None, True, items)
        return event, items

    def _member_events(self, job: _DailyJob, now: datetime, daily_input: DailyInput, sections: tuple[Section, ...],
                       draft: _Draft, status_text: str | None, status_missing: list[str],
                       members: tuple[MemberPortfolio, ...]) -> list[tuple[MemberPortfolio, OutboundEvent, int]]:
        """멤버마다 판을 짠다. 같은 LLM 본문에서 그 멤버의 ETF에 관련된 부분만 남기고, 한 멤버가 실패해도 다른 멤버의 판은 나간다."""
        events = []
        for member in members:
            try:
                mine = member_input(daily_input, member)
                mine_sections = member_sections(mine, sections, self.settings)
                mine_draft = replace(draft, output=member_output(draft.output, mine, mine_sections))
                text = daily_text(mine, mine_sections, mine_draft.output, status_text)
                missing = [*job.notes, *missing_items(mine_sections), *status_missing]
                event, _ = self._daily_event(job, now, member.member_id, mine, mine_sections, mine_draft, text,
                                             status_text, missing)
            except Exception as error:  # noqa: BLE001 - 멤버 한 명의 판이 실패해도 다른 멤버의 판은 낸다
                log.error("daily %s for one member failed: job=%s %s", job.edition, job.job_id, type(error).__name__)
                self._detail(job, STAGE_COMPOSE, EntryType.STEP_FAILED, REASON_MEMBER_FAILED, type(error).__name__, 0,
                             now, memberId=member.member_id)
                continue
            events.append((member, event, len(mine.etfs or ())))
        return events

    def _publish(self, job: _DailyJob, now: datetime, daily_input: DailyInput, sections: tuple[Section, ...],
                 draft: _Draft, research_count: int = 0) -> None:
        missing = [*job.notes, *missing_items(sections)]
        by_ref = {e.ref: e for s in sections for e in s.entries}
        report = self._status_report(job, now, draft, research_count)
        status_text = report.text if report is not None else None
        if report is not None:
            missing = [*missing, *report.missing]
        # 작업 기록에 남기는 본문은 멤버를 가르지 않은 판 전체 글이다(멤버마다 다른 글자판은 메일로만 나간다).
        text = daily_text(daily_input, sections, draft.output, status_text)
        members = self.members.daily_members(job.edition) if self.members is not None else None
        if members is None:
            # 받을 멤버를 모르면 누구에게 보낼지 알 수 없다. 틀린 사람에게 보내지 않도록 발행하지 않고 실패로 남긴다.
            self._detail(job, STAGE_COMPOSE, EntryType.STEP_FAILED, REASON_MEMBERS_UNREADABLE, "ReadFailed", 0, now)
            events: list[tuple[MemberPortfolio, OutboundEvent, int]] = []
        else:
            events = self._member_events(job, now, daily_input, sections, draft, status_text,
                                         list(report.missing) if report is not None else [], members)
        if missing:
            self._detail(job, STAGE_COMPOSE, EntryType.MISSING_ITEMS, REASON_MISSING_ITEMS, None, 0, now,
                         missingItems=missing,
                         # 일봉이 빈 ETF와 이유 코드. 화면 로그가 아니라 작업 기록에만 남는다.
                         missingEtfs=[{"symbol": e.symbol, "name": e.label(), "reason": e.missing_reason}
                                      for e in daily_input.etfs or () if not e.has_day()],
                         evidence=[{"ref": ref, "describe": by_ref[ref].describe} for ref in draft.cited],
                         weakItems=list(draft.weak_shortfalls))
        for member, event, etf_count in events:
            # 같은 작업 안에서 이 판이 어느 멤버 몫인지 남긴다. 멤버 번호와 결과 이벤트 ID만 담고 주소는 알지 못한다.
            self._detail(job, STAGE_COMPOSE, EntryType.MEMBER_BRIEFING, REASON_MEMBER_PUBLISHED, None, 0, now,
                         memberId=member.member_id, resultEventId=event.value["eventId"], etfCount=etf_count,
                         missingItems=event.value["payload"]["missingItems"])
        if members is not None and not members:
            log.warning("daily %s: no member to send to: job=%s", job.edition, job.job_id)
        # 멤버가 있는데 판이 한 건도 안 나가면 모두 실패한 것이라 성공으로 남기지 않는다(일부만 실패하면 나머지는 보낸다).
        all_failed = bool(members) and not events
        if all_failed:
            log.error("daily %s: every member briefing failed: job=%s members=%d", job.edition, job.job_id, len(members))
        failed = members is None or all_failed
        failure = REASON_MEMBERS_UNREADABLE if members is None else REASON_MEMBER_FAILED if all_failed else None
        result = JobResult(
            job.job_id, RunStatus.FAILED if failed else RunStatus.SUCCEEDED, now,
            generated_by=GeneratedBy(draft.generated_by),
            final_text=text, weak_count=len(draft.weak_shortfalls), research_count=research_count,
            # 성공했어도 중간에 포기한 단계가 있으면 첫 사유를 남긴다.
            failure_reason=failure if failed
            else job.first_failure[0].value if job.first_failure is not None else None,
            # 멤버마다 이벤트가 따로라 작업 한 줄에는 첫 멤버의 것만 싣고, 나머지는 member-briefing 줄에 있다.
            result_event_id=events[0][1].value["eventId"] if events else None)
        saved = record_result(self.job_log, result)
        if job.edition == EDITION_MORNING and self.on_morning_result is not None:
            # 나가지 않은 아침 판(실패, 기록 저장 실패)은 합쳐 둔 급변동을 받지 못했으므로 그 변동을 따로 알린다.
            flowed = frozenset(e.symbol for e in job.etfs or () if e.open_flow is not None and e.open_flow.present)
            self.on_morning_result(job.scheduled_for, saved and not failed, now, flowed, job.open_flow_since)
        if not saved:
            del self._jobs[job.job_id]
            return
        for _, event, _ in events:
            self.requests.send(event)
        log.info("daily %s ready: job=%s generatedBy=%s members=%d missing=%d etfs=%s filings=%s excluded=%s",
                 job.edition, job.job_id, draft.generated_by, len(events), len(missing),
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
        llm = build_llm_input(draft.input, draft.sections, self.core.settings, group=False)
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

    def correlation_id(self, draft: DailyDraft) -> str:
        return draft.job.correlation_id

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
    """v2 응답은 일봉이 없으면 값 칸이 null이다. v1 응답에는 missingReason이 없다. 장중 시세 대체값(intradayFallback)이
    있으면 prevClose·rate가 그 값이다."""
    fetched = e["fetchedAt"]
    fallback = e.get("intradayFallback")
    return EtfDay(e["symbol"], e["date"], e["close"], e["prevClose"], e["rate"],
                  parse_timestamp(fetched) if fetched is not None else None,
                  tuple(e["tickers"]), bool(e["tickersTruncated"]), parse_etf_info(e.get("etf")),
                  e.get("missingReason"), open_flow=parse_open_flow(e.get("openFlow")),
                  intraday_at=parse_timestamp(fallback["receivedAt"]) if fallback is not None else None,
                  intraday_price=fallback["price"] if fallback is not None else None)


def _malformed(kind: QueryKind, job_id: str, error: Exception) -> None:
    # 응답 쪽 결함이라 받지 못한 것으로 본다. 원문은 로그에 쓰지 않는다.
    log.error("malformed %s response for job %s: %s", kind.value, job_id, type(error).__name__)
