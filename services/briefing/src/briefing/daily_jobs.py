"""데일리 판(아침 판, 마감 후 판) 작업. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: schedule.tick(판마다 따로 받음) -> 범위 계산 -> 데일리 자료 요청(market.daily.requested)
-> 아침 판이고 미국 종목이 있으면 그 티커로 공시 요청 -> 받은 자료 재검사 -> 본문(LLM 또는 규칙 기반)
-> (LLM이 근거 부족 또는 약한 문장을 냈으면 뉴스 재검색 1번 -> 받은 기사를 더해 한 번 다시 쓰기)
-> briefing.daily.generated 발행. 응답을 끝내 받지 못하거나 LLM이 끝내 실패해도 판은 나간다.
예외는 작업 기록을 끝내 저장하지 못했을 때뿐이며, 그때는 이 판의 결과 이벤트를 발행하지 않고 ERROR 로그만 남긴다.
"""
from __future__ import annotations

import json
import logging
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.daily_report import (
    Section,
    EDITIONS_WITH_FILINGS,
    GENERATED_BY_RULE,
    DailyInput,
    DailyLlmWriter,
    EtfDay,
    GENERATED_BY_LLM,
    build_llm_input,
    build_sections,
    has_evidence,
    missing_items,
    rule_based_text,
)
from briefing.events import (
    FILINGS_REQUESTED_TOPIC,
    JOB_TYPE_BY_EDITION,
    OutboundEvent,
    daily_briefing_event,
    format_timestamp,
    parse_timestamp,
    request_event,
)
from briefing.job_log import (
    STAGE_PUBLISH,
    TYPE_FAILED,
    TYPE_LLM_REJECTED,
    TYPE_LLM_SUCCEEDED,
    TYPE_MISSING_ITEMS,
    TYPE_RESENT,
    TYPE_RESULT,
    DiscardJobLog,
    JobLogEntry,
    JobLogPort,
    range_target,
    record_result,
)
from briefing.evidence import MARKET_WIDE
from briefing.ports import FailureReason, RequestPort
from briefing.research import (
    NewsResearch,
    QueryWriter,
    ResearchArticles,
    ResearchReason,
    ResearchTarget,
    ask_queries,
    query_context,
    record_rewrite,
)
from briefing.sources import (
    BACKGROUND_MISSING,
    Background,
    CloseRate,
    Exchange,
    Filing,
    next_seq,
    parse_background,
    parse_close_rates,
    parse_filings,
    response_payload,
)

log = logging.getLogger("briefing.daily_jobs")

SCHEDULE_TICK = "schedule.tick"
DAILY_DATA_REQUESTED_TOPIC = "market.daily.requested"
DAILY_DATA_RESPONDED = "market.daily.responded"
FILINGS_RESPONDED = "news.filings.responded"

# 작업 기록의 단계와 사유. 끝내 실패한 사유는 FailureReason 값을 그대로 쓴다.
STAGE_DAILY_DATA = "daily-data-request"
STAGE_FILINGS = "filings-request"
STAGE_LLM = "llm"
STAGE_COMPOSE = "compose"
REASON_RESENT = "no_response_resent"
REASON_LLM_REJECTED = "llm_attempt_rejected"
REASON_MISSING_ITEMS = "missing_items"
REASON_LLM_ACCEPTED = "llm_attempt_accepted"
REASON_PUBLISHED = "published"


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
    """LLM(또는 규칙 기반)이 쓴 본문 하나. 재검색을 할지 정하는 표시를 함께 든다."""

    text: str
    generated_by: str
    cited: tuple[str, ...]
    insufficient_evidence: bool = False
    weak_shortfalls: tuple[str, ...] = ()


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
    filings_payload: dict | None = None
    etfs: tuple[EtfDay, ...] | None = None
    etfs_truncated: bool = False
    close_rates: tuple[CloseRate, ...] | None = None
    background: tuple[Background, ...] | None = None
    filings_data: tuple[Filing, ...] | None = None
    filings_truncated: bool = False
    excluded: dict[str, int] = field(default_factory=dict)
    sent: list[_Sent] = field(default_factory=list)
    first_failure: tuple[FailureReason, str, int] | None = None
    # 재검색을 시도했는가(작업 기록 줄에 표시), 재검색 결과를 기다리는 중인가와 그동안 쥐고 있는 첫 결과
    researched: bool = False
    researching: bool = False
    pending: tuple[DailyInput, tuple[Section, ...], _Draft] | None = None

    def waiting(self) -> bool:
        return (self.data.awaiting() and not self.data_given_up) or (self.filings.awaiting() and not self.filings_given_up)


@dataclass
class DailyCore:
    requests: RequestPort
    writer: DailyLlmWriter | None = None
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    # 둘 다 있을 때만 재검색한다. research는 급변동과 함께 쓰는 재검색 흐름이다.
    queries: QueryWriter | None = None
    research: NewsResearch | None = None
    _jobs: dict[str, _DailyJob] = field(default_factory=dict, init=False)
    _seen: OrderedDict[str, None] = field(default_factory=OrderedDict, init=False)

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
            etfs = tuple(
                EtfDay(e["symbol"], e["date"], e["close"], e["prevClose"], e["rate"], parse_timestamp(e["fetchedAt"]),
                       tuple(e["tickers"]), bool(e["tickersTruncated"]))
                for e in payload["etfs"]
            )
            close_rates = parse_close_rates(payload["closeRates"])
            background = tuple(parse_background(b) for b in payload["background"])
            truncated = bool(payload["truncated"])
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed {DAILY_DATA_RESPONDED} event: {error!r}") from error
        job.data.received = True
        job.data_payload = payload
        job.etfs = self._recheck_etfs(job, etfs)
        job.close_rates, job.background = self._recheck_state(job, close_rates, background)
        job.etfs_truncated = truncated
        tickers = tuple(dict.fromkeys(t for etf in job.etfs for t in etf.tickers))
        # 공시는 아침 판만 쓰고, 미국 종목이 없으면 해당 없음이라 요청하지 않는다.
        if job.edition in EDITIONS_WITH_FILINGS and tickers:
            self._send_filings(job, now)
        else:
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

    def next_deadline(self) -> datetime | None:
        deadlines = [exchange.sent_at + self.settings.response_timeout
                     for job in self._jobs.values()
                     for exchange, given_up in ((job.data, job.data_given_up), (job.filings, job.filings_given_up))
                     if exchange.awaiting() and not given_up]
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
        job.sent.append(_Sent(DAILY_DATA_REQUESTED_TOPIC, seq, now, payload))
        self.requests.send(daily_data_request(job.correlation_id, now, payload))

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
        kept = tuple(e for e in etfs if job.range_start <= e.fetched_at <= job.scheduled_for)
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
            "received": {name: payload for name, payload in ((DAILY_DATA_RESPONDED, job.data_payload),
                                                             (FILINGS_RESPONDED, job.filings_payload))
                         if payload is not None},
            "notReceived": [name for name, exchange in ((DAILY_DATA_RESPONDED, job.data),
                                                        (FILINGS_RESPONDED, job.filings))
                            if exchange.seq and not exchange.received],
            "excluded": dict(job.excluded),
        }
        data.update(extra)
        return data

    def _entry(self, job: _DailyJob, stage: str, entry_type: str, reason: str, error_type: str | None, attempts: int,
               now: datetime, data: dict[str, Any]) -> JobLogEntry:
        return JobLogEntry(job.edition, job.researched, job.job_id,
                           range_target(format_timestamp(job.range_start), format_timestamp(job.scheduled_for)),
                           stage, entry_type, reason, error_type, attempts, data, now)

    def _detail(self, job: _DailyJob, stage: str, entry_type: str, reason: str, error_type: str | None,
                attempts: int, now: datetime, **extra: Any) -> None:
        self.job_log.record(self._entry(job, stage, entry_type, reason, error_type, attempts, now,
                                        self._related_data(job, **extra)))

    def _resent(self, job: _DailyJob, exchange: Exchange, stage: str, event_type: str, now: datetime) -> None:
        self._detail(job, stage, TYPE_RESENT, REASON_RESENT, "Timeout", exchange.seq, now,
                     timedOut={"response": event_type, "seq": exchange.seq})

    def _give_up(self, job: _DailyJob, exchange: Exchange, stage: str, reason: FailureReason, event_type: str,
                 now: datetime) -> None:
        detail = f"no {event_type} within the time limit"
        self._detail(job, stage, TYPE_FAILED, reason.value, "Timeout", exchange.seq, now,
                     timedOut={"response": event_type, "seq": exchange.seq})
        self._first_failure(job, reason, detail, exchange.seq)

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
        )
        sections = build_sections(daily_input, self.settings)
        draft = self._write(job, now, daily_input, sections)
        if self._start_research(job, now, daily_input, sections, draft):
            return
        self._publish(job, now, daily_input, sections, draft)

    def _write(self, job: _DailyJob, now: datetime, daily_input: DailyInput,
               sections: tuple[Section, ...]) -> _Draft:
        """본문을 쓴다. LLM이 없거나 끝내 실패하면 규칙 기반 본문이다. LLM 시도는 줄마다 남긴다."""
        missing = missing_items(sections)
        if self.writer is not None and has_evidence(sections):
            result = self.writer.write(daily_input, sections)
            for attempt in result.rejected:
                self._detail(job, STAGE_LLM, TYPE_LLM_REJECTED, REASON_LLM_REJECTED, attempt.error_type,
                             attempt.attempt, now,
                             missingItems=missing,
                             llm={"input": attempt.llm_input, "rawOutput": attempt.raw_output,
                                  "rejectReason": attempt.reason})
            if result.body is None:
                detail = f"daily LLM failed after {len(result.rejected)} attempts"
                last = result.rejected[-1] if result.rejected else None
                self._detail(job, STAGE_LLM, TYPE_FAILED, FailureReason.LLM_ERROR.value,
                             last.error_type if last else None, len(result.rejected), now, missingItems=missing,
                             llm={"input": last.llm_input if last else None,
                                  "rawOutput": last.raw_output if last else None,
                                  "rejectReason": last.reason if last else None})
                self._first_failure(job, FailureReason.LLM_ERROR, detail, len(result.rejected))
            else:
                body = result.body
                if result.accepted is not None:
                    a = result.accepted
                    self._detail(job, STAGE_LLM, TYPE_LLM_SUCCEEDED, REASON_LLM_ACCEPTED, None, a.attempt, now,
                                 missingItems=missing,
                                 llm={"input": a.llm_input, "rawOutput": a.response, "rejectReason": None})
                return _Draft(body.text, body.generated_by, body.cited, body.insufficient_evidence,
                              body.weak_shortfalls)
        return _Draft(rule_based_text(daily_input, sections), GENERATED_BY_RULE, ())

    # ---- 재검색 ----

    def _start_research(self, job: _DailyJob, now: datetime, daily_input: DailyInput,
                        sections: tuple[Section, ...], draft: _Draft) -> bool:
        """LLM이 근거 부족이나 약한 문장을 냈으면 검색어를 받아 재검색을 시작한다. 시작했으면 True."""
        needs = draft.generated_by == GENERATED_BY_LLM and (draft.insufficient_evidence or draft.weak_shortfalls)
        if not needs or self.queries is None or self.research is None:
            return False
        if not daily_input.etfs:
            # 기사 조회 요청에는 종목코드가 필수라 보유 ETF 자료가 없으면 재검색하지 않는다.
            return False
        job.researched = True
        target = ResearchTarget(
            job_kind=job.edition, job_id=job.job_id, correlation_id=job.correlation_id, tag=MARKET_WIDE,
            # 데일리 공시 요청처럼 정렬한 첫 ETF 코드. 재검색 조회는 종목 조건을 보지 않고 응답 짝짓기에만 쓴다.
            reply_symbol=min(etf.symbol for etf in daily_input.etfs),
            since=job.range_start, until=job.scheduled_for)
        queries = ask_queries(self.queries, self.job_log, target,
                              query_context(build_llm_input(daily_input, sections), draft.weak_shortfalls), now)
        if not queries:
            return False
        job.researching = True
        job.pending = (daily_input, sections, draft)
        if not self.research.start(target, queries, now):
            job.researching = False
            job.pending = None
            return False
        return True

    def on_research_done(self, target: ResearchTarget, result: ResearchArticles | None, now: datetime) -> None:
        """재검색이 끝났다. 받은 기사로 한 번 다시 쓰고, 실패했거나 쓸 기사가 없으면 첫 결과를 낸다."""
        job = self._jobs.get(target.job_id)
        if job is None or not job.researching or job.pending is None:
            return
        daily_input, sections, first = job.pending
        job.researching, job.pending = False, None
        if result is None:
            self._publish(job, now, daily_input, sections, first)
            return
        rewritten_input = replace(daily_input, research_articles=result.articles,
                                  research_articles_truncated=result.truncated)
        rewritten_sections = build_sections(rewritten_input, self.settings)
        second = self._write(job, now, rewritten_input, rewritten_sections)
        if second.generated_by != GENERATED_BY_LLM:
            record_rewrite(self.job_log, target, ResearchReason.REWRITE_FAILED, now, result.articles)
            self._publish(job, now, daily_input, sections, first)
            return
        if second.insufficient_evidence:
            record_rewrite(self.job_log, target, ResearchReason.STILL_INSUFFICIENT, now, result.articles)
            if not first.insufficient_evidence:
                # 첫 결과에는 쓸 만한 요약이 있었다. 다시 쓰다가 전부 잃으면 첫 결과를 낸다.
                self._publish(job, now, daily_input, sections, first)
                return
        elif second.weak_shortfalls:
            record_rewrite(self.job_log, target, ResearchReason.STILL_WEAK, now, result.articles)
        self._publish(job, now, rewritten_input, rewritten_sections, second)

    # ---- 발행 ----

    def _publish(self, job: _DailyJob, now: datetime, daily_input: DailyInput, sections: tuple[Section, ...],
                 draft: _Draft) -> None:
        missing = missing_items(sections)
        if missing:
            self._detail(job, STAGE_COMPOSE, TYPE_MISSING_ITEMS, REASON_MISSING_ITEMS, None, 0, now,
                         missingItems=missing)
        by_ref = {e.ref: e for s in sections for e in s.entries}
        result_data: dict[str, Any] = {
            "outcome": "published", "text": draft.text, "generatedBy": draft.generated_by, "missingItems": missing,
            "evidence": [{"ref": ref, "describe": by_ref[ref].describe} for ref in draft.cited],
            "weakItems": list(draft.weak_shortfalls),
        }
        if job.first_failure is not None:
            reason, detail, attempts = job.first_failure
            result_data["firstFailure"] = {"reason": reason.value, "detail": detail, "attempts": attempts}
        result = self._entry(job, STAGE_PUBLISH, TYPE_RESULT, REASON_PUBLISHED, None, 0, now, result_data)
        if not record_result(self.job_log, result):
            del self._jobs[job.job_id]
            return
        self.requests.send(daily_briefing_event(job.job_id, job.correlation_id, job.scheduled_for, now, job.edition,
                                                job.range_start, job.scheduled_for, draft.text, missing,
                                                draft.generated_by))
        log.info("daily %s ready: job=%s generatedBy=%s missing=%d etfs=%s filings=%s excluded=%s",
                 job.edition, job.job_id, draft.generated_by, len(missing),
                 "n/a" if job.etfs is None else len(job.etfs),
                 "n/a" if job.filings_data is None else len(job.filings_data), sum(job.excluded.values()))
        del self._jobs[job.job_id]


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

