"""근거가 부족할 때의 뉴스 재검색. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: 보고서 LLM이 근거 부족을 냄 -> 검색어 LLM이 검색어를 냄 -> news.research.requested(작업 하나당 1번) ->
같은 재검색 작업 id의 collection.completed 대기 -> news.articles.requested(researchJobId)를 안정될 때까지 재확인 ->
기존 시간 범위로 다시 거르고 이미 받은 기사를 뺌 -> 재검색 기사를 더해 보고서를 한 번 더 씀.
어디서 끝나든 실패는 새 실패 사유 없이 로그와 작업 기록(JobLogPort)에만 남기고 기존 근거로 보고서를 낸다.

재검색(NewsResearch)과 검색어 LLM(QueryWriter)은 작업 종류와 작업 id로만 움직여 데일리 판도 부를 수 있다.
지금 연결된 곳은 급변동(ResearchReportStage)뿐이다.
"""
from __future__ import annotations

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Mapping, Protocol

from briefing.compose import KST
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import (
    ARTICLES_REQUESTED_TOPIC,
    JOB_KIND_MOVE,
    format_timestamp,
    parse_timestamp,
    request_event,
    research_request_event,
)
from briefing.evidence import MARKET_WIDE
from briefing.job_log import (
    TYPE_CHECK_ROUND,
    TYPE_FAILED,
    TYPE_LLM_REJECTED,
    TYPE_LLM_SUCCEEDED,
    TYPE_RESENT,
    DiscardJobLog,
    JobLogEntry,
    JobLogPort,
    move_target,
    range_target,
)
from briefing.jobs import ReportInput
from briefing.llm import LlmCallError, LlmClient, llm_config
from briefing.ports import RequestPort
from briefing.report import Report, ReportStage, build_llm_input
from briefing.sources import Article, response_payload

log = logging.getLogger("briefing.research")

RESEARCH_JOB_TYPE = "research-news"
RESEARCH_REQUEST_SEQ = 1
MAX_QUERY_LENGTH = 100

QUERY_SYSTEM_PROMPT = """당신은 ETF 가격 변동의 원인을 설명할 뉴스 기사를 찾기 위한 검색어를 정하는 도우미입니다.
입력은 JSON이며 변동 종목 코드, 변동 방향과 변동률, 변동 시각, 이미 찾은 기사·공시·해외 등락률·배경 자료를 담고 있습니다.
이미 찾은 자료만으로는 변동 원인을 설명하기에 근거가 부족했습니다.

규칙:
- 뉴스 검색에 넣을 검색어만 냅니다. 설명이나 문장은 쓰지 않습니다.
- 검색어 하나는 짧은 한국어 낱말 2~3개로 씁니다(예: 반도체 수출). 이미 찾은 기사와 같은 내용만 다시 찾을 검색어는 피합니다.
- 검색어의 모든 낱말이 기사 제목이나 요약에 그대로 나와야 그 기사가 걸립니다. 낱말이 많을수록 거의 걸리지 않으므로 낱말을 늘리지 않습니다.
- 추측한 주제를 쓰지 않고 입력 자료에 근거해 고릅니다(예: 배경 자료에서 크게 움직인 항목, 이미 찾은 기사·공시에 나온 낱말). 입력에 근거가 없으면 빈 목록을 냅니다.
- 변동 시각 전후에 나왔을 만한 기사를 찾을 검색어를 고릅니다.
- 검색어는 {max_queries}개 이하로, 서로 다르게 냅니다. 찾을 만한 검색어가 없으면 빈 목록을 냅니다.

출력은 다음 JSON 하나만 냅니다:
{{"queries":["..."]}}"""


def query_output_schema(max_queries: int) -> dict[str, Any]:
    # 개수 상한은 지시문과 설명에 적고 코드가 검사한다(parse_queries).
    return {
        "type": "object",
        "properties": {
            "queries": {
                "type": "array",
                "items": {"type": "string"},
                "description": f"뉴스 검색어. {max_queries}개 이하, 하나는 {MAX_QUERY_LENGTH}자 이하.",
            },
        },
        "required": ["queries"],
        "additionalProperties": False,
    }


class InvalidQueries(ValueError):
    pass


def parse_queries(text: str, max_queries: int) -> tuple[str, ...]:
    """검색어 LLM 응답을 검증한다. 형식이 틀리면 InvalidQueries. 빈 목록은 '찾을 검색어 없음'이라는 정상 응답이다."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise InvalidQueries("output is not valid JSON") from error
    if not isinstance(data, dict) or not isinstance(data.get("queries"), list):
        raise InvalidQueries("output does not have queries")
    if not all(isinstance(q, str) for q in data["queries"]):
        raise InvalidQueries("a query is not a string")
    # 수집기도 앞뒤 공백을 지우고 빈 것과 같은 것은 한 번만 쓰므로 같은 규칙으로 정리한다.
    queries = tuple(dict.fromkeys(q.strip() for q in data["queries"] if q.strip()))
    if len(queries) > max_queries:
        raise InvalidQueries(f"{len(queries)} queries over the limit of {max_queries}")
    if any(len(q) > MAX_QUERY_LENGTH for q in queries):
        raise InvalidQueries(f"a query is longer than {MAX_QUERY_LENGTH} characters")
    return queries


# ---- 재검색 기록 ----

class ResearchStep(str, Enum):
    """재검색 기록의 단계."""

    QUERY = "query"                        # 검색어 LLM
    COMPLETION = "completion"              # 재검색 완료 알림 대기
    ARTICLES = "articles"                  # 재검색 기사 조회(요청과 응답)
    ARTICLES_CONFIRM = "articles-confirm"  # 재검색 기사 조회의 안정 판정 확인 라운드
    FILTER = "filter"                      # 받은 기사 재검사
    REWRITE = "rewrite"                    # 보고서 다시 쓰기


class ResearchReason(str, Enum):
    """재검색 기록의 사유. 실패 사유(ports.FailureReason)가 아니며 작업을 실패로 만들지 않는다."""

    QUERY_REJECTED = "query_rejected"              # 검색어 LLM 응답이 형식에 맞지 않아 거부한 시도
    QUERY_CALL_FAILED = "query_call_failed"        # 검색어 LLM 호출이 실패한 시도
    QUERY_ACCEPTED = "query_accepted"              # 검색어 LLM 응답을 받아들인 시도
    QUERY_GAVE_UP = "query_gave_up"                # 모든 시도가 실패해 재검색하지 않음
    NO_QUERIES = "no_queries"                      # 검색어 LLM이 검색어를 내지 않음
    ALREADY_RESEARCHED = "already_researched"      # 같은 작업은 한 번만 재검색한다
    COLLECTION_INCOMPLETE = "collection_incomplete"  # 수집기가 하루 상한·검색 실패로 다 모으지 못함
    COMPLETION_TIMEOUT = "completion_timeout"      # 완료 알림이 대기 상한 안에 오지 않음
    ARTICLES_RESENT = "articles_resent"            # 응답이 없어 기사 조회를 다시 보냄
    ARTICLES_NO_RESPONSE = "articles_no_response"  # 다시 보내도 응답이 없음
    ARTICLES_CONFIRM_ROUND = "articles_confirm_round"  # 정상 동작: 안정 확인을 위해 같은 조회를 다시 보낸 라운드의 응답
    ARTICLES_UNSTABLE = "articles_unstable"        # 라운드 상한까지 기사 목록이 계속 바뀜
    NO_USABLE_ARTICLES = "no_usable_articles"      # 범위 밖 제외·중복을 빼니 남은 기사가 없음
    REWRITE_FAILED = "rewrite_failed"              # 다시 쓰기가 끝내 실패(llm_error로도 기록됨)
    STILL_INSUFFICIENT = "still_insufficient"      # 다시 써도 근거 부족


# 재검색 사유별 기록 종류. 나머지 사유는 재검색이 끝내 실패했거나 쓸 기사가 없는 경우라 failed다.
_ENTRY_TYPES = {
    ResearchReason.QUERY_REJECTED: TYPE_LLM_REJECTED,
    ResearchReason.QUERY_CALL_FAILED: TYPE_LLM_REJECTED,
    ResearchReason.QUERY_ACCEPTED: TYPE_LLM_SUCCEEDED,
    ResearchReason.ARTICLES_RESENT: TYPE_RESENT,
    ResearchReason.ARTICLES_CONFIRM_ROUND: TYPE_CHECK_ROUND,
}


@dataclass(frozen=True)
class ResearchTarget:
    """재검색을 부른 작업 하나. 작업 종류와 작업 id만 알면 급변동이든 데일리 판이든 같은 흐름을 쓴다."""

    job_kind: str
    job_id: str
    correlation_id: str
    # 재검색 요청에 붙일 태그 종목(급변동은 종목코드, 데일리 판은 *)
    tag: str
    # 기사 조회의 symbol(응답 짝짓기에만 쓰인다)
    reply_symbol: str
    # 기사 조회와 재검사에 쓰는 기존 시간 범위(양끝 포함)
    since: datetime
    until: datetime
    # 이미 받은 기사. 재검색 기사에서 뺀다.
    known_article_ids: frozenset[str] = frozenset()
    # 기사 조회 순번은 이 값 뒤부터 쓴다.
    after_seq: int = 0
    # 급변동의 변동 시각. 데일리 판은 None이고 since~until이 대상 범위다.
    move_at: datetime | None = None


def _record(target: ResearchTarget, stage: ResearchStep, reason: ResearchReason, now: datetime,
            attempts: int, *, error_type: str | None = None, queries: tuple[str, ...] = (),
            request: Mapping[str, Any] | None = None, received: tuple[Mapping[str, Any], ...] = (),
            awaited: Mapping[str, Any] | None = None, missing: tuple[str, ...] = (), excluded: int = 0,
            duplicates: int = 0, llm_input: str | None = None, raw_output: str | None = None,
            rejection: str | None = None) -> JobLogEntry:
    """재검색 기록 한 줄. 요청 payload와 받은 응답 payload는 가공하지 않고 담는다. 검색어는 로그에 쓰지 않지만 여기에는 담는다."""
    data: dict[str, Any] = {
        "queries": list(queries), "request": request, "received": list(received), "awaited": awaited,
        "missing": list(missing), "excluded": excluded, "duplicates": duplicates,
    }
    if llm_input is not None:
        data["llm"] = {"input": llm_input, "rawOutput": raw_output, "rejectReason": rejection}
    daily = target.move_at is None
    return JobLogEntry(
        job_kind=target.job_kind, researched=True, job_id=target.job_id,
        target=range_target(format_timestamp(target.since), format_timestamp(target.until)) if daily
        else move_target(target.tag, format_timestamp(target.move_at)),
        stage=stage.value, entry_type=_ENTRY_TYPES.get(reason, TYPE_FAILED), reason=reason.value,
        error_type=error_type, attempts=attempts, data=data, occurred_at=now)


# ---- 검색어 LLM ----

@dataclass(frozen=True)
class QueryAttempt:
    """검색어 LLM 시도 하나의 입력과 결과."""

    attempt: int
    llm_input: str
    raw_output: str | None
    rejection: str | None
    error_type: str | None


@dataclass(frozen=True)
class QueryResult:
    queries: tuple[str, ...]
    attempts: tuple[QueryAttempt, ...]
    gave_up: bool


@dataclass(frozen=True)
class QueryWriter:
    """검색어만 내는 LLM. 보고서 LLM과 지시문·출력 형식이 따로다."""

    client: LlmClient
    max_queries: int = DEFAULT_SETTINGS.max_research_queries
    max_attempts: int = DEFAULT_SETTINGS.research_query_attempts

    def ask(self, context: Mapping[str, Any]) -> QueryResult:
        user = json.dumps(context, ensure_ascii=False)
        system = QUERY_SYSTEM_PROMPT.format(max_queries=self.max_queries)
        schema = query_output_schema(self.max_queries)
        attempts: list[QueryAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw = None
            try:
                raw = self.client.complete(system, user, schema)
                queries = parse_queries(raw, self.max_queries)
            except (LlmCallError, InvalidQueries) as error:
                attempts.append(QueryAttempt(attempt, user, raw, str(error), type(error).__name__))
                continue
            attempts.append(QueryAttempt(attempt, user, raw, None, None))
            return QueryResult(queries, tuple(attempts), gave_up=False)
        return QueryResult((), tuple(attempts), gave_up=True)


def build_query_writer(env: Mapping[str, str], client_factory: Callable[[str, str], LlmClient],
                       settings: CoreSettings = DEFAULT_SETTINGS) -> QueryWriter | None:
    """LLM 키가 없으면 None. 보고서도 규칙 기반이라 근거 부족을 내지 않으므로 재검색하지 않는다."""
    config = llm_config(env)
    if config is None:
        return None
    return QueryWriter(client_factory(config.api_key, config.model), settings.max_research_queries,
                       settings.research_query_attempts)


# ---- 재검색 요청·대기·기사 조회 ----

@dataclass(frozen=True)
class ResearchArticles:
    articles: tuple[Article, ...]
    truncated: bool


class ResearchDonePort(Protocol):
    def on_research_done(self, target: ResearchTarget, result: ResearchArticles | None, now: datetime) -> None:
        """result가 None이면 재검색이 실패했거나 쓸 기사가 없다. 기존 근거로 진행한다."""


class _Phase(Enum):
    WAITING_COMPLETION = "waiting_completion"
    FETCHING = "fetching"
    DONE = "done"


@dataclass
class _Research:
    target: ResearchTarget
    queries: tuple[str, ...]
    requested_at: datetime
    request: dict[str, Any]
    phase: _Phase = _Phase.WAITING_COMPLETION
    seq: int = 0
    sent_at: datetime | None = None
    last_request: dict[str, Any] | None = None
    round: int = 0
    attempt: int = 0
    requests: int = 0
    awaiting: bool = False
    recheck_at: datetime | None = None
    previous_ids: frozenset[str] | None = None
    received: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class NewsResearch:
    requests: RequestPort
    job_log: JobLogPort
    done: ResearchDonePort
    settings: CoreSettings = DEFAULT_SETTINGS
    _jobs: OrderedDict[str, _Research] = field(default_factory=OrderedDict, init=False)

    def start(self, target: ResearchTarget, queries: tuple[str, ...], now: datetime) -> bool:
        """재검색 요청을 보낸다. 같은 작업 id는 한 번만 보내고 두 번째부터 False."""
        if target.job_id in self._jobs:
            log.info("research already requested for job %s; not requesting again", target.job_id)
            self.job_log.record(_record(target, ResearchStep.QUERY, ResearchReason.ALREADY_RESEARCHED, now, 0,
                                        queries=queries))
            return False
        event = research_request_event(target.job_id, RESEARCH_REQUEST_SEQ, target.job_kind, target.tag,
                                       list(queries), target.correlation_id, now)
        job = _Research(target, queries, now, event.value["payload"])
        self._jobs[target.job_id] = job
        while len(self._jobs) > self.settings.remembered_jobs:
            self._jobs.popitem(last=False)
        self.requests.send(event)
        log.info("research requested job=%s kind=%s queryCount=%d", target.job_id, target.job_kind, len(queries))
        return True

    def on_collection_completed(self, message: str | bytes, now: datetime) -> bool:
        """재검색 완료 알림이면 처리하고 True. researchJobId가 없는 정기 수집 완료는 False로 돌려준다."""
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

        job = self._jobs.get(research_job_id)
        if job is None or job.phase is not _Phase.WAITING_COMPLETION:
            log.info("research completion for unknown or finished job %s ignored", research_job_id)
            return True
        if job.target.tag in incomplete or MARKET_WIDE in incomplete:
            log.warning("research for job %s did not collect everything (daily limit or search failure)",
                        research_job_id)
            self._finish(job, now, None, ResearchStep.COMPLETION, ResearchReason.COLLECTION_INCOMPLETE, 1,
                         request=job.request, received=(payload,), missing=tuple(sorted(incomplete)))
            return True
        job.phase = _Phase.FETCHING
        job.round = 1
        job.seq = job.target.after_seq
        self._send(job, now, attempt=1)
        return True

    def on_articles_responded(self, message: str | bytes, now: datetime) -> bool:
        """재검색 기사 조회의 응답이면 처리하고 True. 첫 근거 조회의 응답이면 False로 돌려준다."""
        payload = response_payload(message, "news.articles.responded")
        job = self._jobs.get(payload["jobId"])
        if job is None or job.phase is not _Phase.FETCHING or payload["seq"] <= job.target.after_seq:
            return False
        if payload["seq"] != job.seq or not job.awaiting:
            log.info("stale research articles response ignored: job=%s seq=%s latest=%s",
                     job.target.job_id, payload["seq"], job.seq)
            return True
        try:
            articles = tuple(
                Article(a["articleId"], a["title"], a["summary"], parse_timestamp(a["publishedAt"]),
                        a["publisher"], a["url"])
                for a in payload["articles"]
            )
            truncated = bool(payload.get("truncated", False))
        except (KeyError, TypeError) as error:
            raise ValueError(f"malformed news.articles.responded event: {error!r}") from error

        job.awaiting = False
        job.received.append(payload)
        ids = frozenset(a.article_id for a in articles)
        if job.round >= 2:
            # 확인 라운드는 정상 동작이지만, 무엇을 받았는지 남기려고 라운드마다 기록한다.
            self.job_log.record(_record(
                job.target, ResearchStep.ARTICLES_CONFIRM, ResearchReason.ARTICLES_CONFIRM_ROUND, now, job.round,
                queries=job.queries, request=job.last_request, received=(payload,)))
        if job.round >= 2 and ids == job.previous_ids:
            self._finish_stable(job, articles, truncated, payload, now)
            return True
        if job.round >= self.settings.max_rounds:
            log.warning("research articles for job %s kept changing for %d rounds", job.target.job_id, job.round)
            self._finish(job, now, None, ResearchStep.ARTICLES, ResearchReason.ARTICLES_UNSTABLE, job.requests,
                         request=job.last_request, received=tuple(job.received))
            return True
        job.previous_ids = ids
        job.recheck_at = now + self.settings.recheck_interval
        return True

    def on_tick(self, now: datetime) -> None:
        for job in list(self._jobs.values()):
            if job.phase is _Phase.WAITING_COMPLETION:
                if now - job.requested_at >= self.settings.research_wait_limit:
                    log.warning("no research completion for job %s within the wait limit", job.target.job_id)
                    self._finish(job, now, None, ResearchStep.COMPLETION, ResearchReason.COMPLETION_TIMEOUT, 1,
                                 request=job.request, awaited={
                                     "eventType": "collection.completed",
                                     "jobType": RESEARCH_JOB_TYPE,
                                     "researchJobId": job.target.job_id,
                                     "requestedAt": format_timestamp(job.requested_at),
                                 })
            elif job.phase is _Phase.FETCHING:
                self._tick_fetching(job, now)

    def next_deadline(self) -> datetime | None:
        deadlines: list[datetime] = []
        for job in self._jobs.values():
            if job.phase is _Phase.WAITING_COMPLETION:
                deadlines.append(job.requested_at + self.settings.research_wait_limit)
            elif job.phase is _Phase.FETCHING:
                if job.awaiting and job.sent_at is not None:
                    deadlines.append(job.sent_at + self.settings.response_timeout)
                elif job.recheck_at is not None:
                    deadlines.append(job.recheck_at)
        return min(deadlines) if deadlines else None

    # ---- 내부 ----

    def _tick_fetching(self, job: _Research, now: datetime) -> None:
        if job.awaiting:
            if now - job.sent_at < self.settings.response_timeout:
                return
            awaited = {"eventType": "news.articles.responded", "seq": job.seq,
                       "sentAt": format_timestamp(job.sent_at)}
            if job.attempt >= self.settings.max_attempts:
                log.warning("no research articles response for job %s within the time limit", job.target.job_id)
                self._finish(job, now, None, ResearchStep.ARTICLES, ResearchReason.ARTICLES_NO_RESPONSE,
                             job.attempt, request=job.last_request, received=tuple(job.received), awaited=awaited)
                return
            self.job_log.record(_record(job.target, ResearchStep.ARTICLES, ResearchReason.ARTICLES_RESENT, now,
                                        job.attempt, queries=job.queries, request=job.last_request,
                                        awaited=awaited))
            self._send(job, now, job.attempt + 1)
        elif job.recheck_at is not None and now >= job.recheck_at:
            job.round += 1
            self._send(job, now, attempt=1)

    def _send(self, job: _Research, now: datetime, attempt: int) -> None:
        job.seq += 1
        job.attempt = attempt
        job.requests += 1
        job.sent_at = now
        job.awaiting = True
        job.recheck_at = None
        t = job.target
        event = request_event(ARTICLES_REQUESTED_TOPIC, "news.articles.requested", t.correlation_id, now, {
            "jobId": t.job_id,
            "seq": job.seq,
            "symbol": t.reply_symbol,
            "since": format_timestamp(t.since),
            "until": format_timestamp(t.until),
            "maxArticles": self.settings.max_articles,
            "researchJobId": t.job_id,
        })
        job.last_request = event.value["payload"]
        self.requests.send(event)

    def _finish_stable(self, job: _Research, articles: tuple[Article, ...], truncated: bool,
                       payload: dict[str, Any], now: datetime) -> None:
        t = job.target
        usable: list[Article] = []
        seen = set(t.known_article_ids)
        excluded = duplicates = 0
        for article in articles:
            if not t.since <= article.published_at <= t.until:
                # 응답 쪽이 이미 범위를 지켰어야 하므로 응답 쪽 결함 신호로 경고를 남긴다.
                log.warning("dropping research article %s outside the requested range (job=%s)",
                            article.article_id, t.job_id)
                excluded += 1
            elif article.article_id in seen:
                duplicates += 1
            else:
                seen.add(article.article_id)
                usable.append(article)
        _log_research(t.job_id, True, len(articles), excluded, duplicates, truncated,
                      "found" if usable else ResearchReason.NO_USABLE_ARTICLES.value)
        if not usable:
            self._finish(job, now, None, ResearchStep.FILTER, ResearchReason.NO_USABLE_ARTICLES, job.requests,
                         request=job.last_request, received=(payload,), excluded=excluded, duplicates=duplicates,
                         log_result=False)
            return
        job.phase = _Phase.DONE
        self.done.on_research_done(t, ResearchArticles(tuple(usable), truncated), now)

    def _finish(self, job: _Research, now: datetime, result: ResearchArticles | None, stage: ResearchStep,
                reason: ResearchReason, attempts: int, log_result: bool = True, **data: Any) -> None:
        job.phase = _Phase.DONE
        if log_result:
            _log_research(job.target.job_id, True, 0, 0, 0, False, reason.value)
        self.job_log.record(_record(job.target, stage, reason, now, attempts, queries=job.queries, **data))
        self.done.on_research_done(job.target, result, now)


def _log_research(job_id: str, researched: bool, articles: int, excluded: int, duplicates: int, truncated: bool,
                  result: str) -> None:
    """작업 1건의 재검색 내역을 한 줄로 남긴다. 검색어와 종목은 보유 종목을 드러내므로 쓰지 않는다."""
    log.info("research job=%s researched=%s articles=%d articlesExcluded=%d articlesDuplicate=%d "
             "articlesTruncated=%s result=%s", job_id, researched, articles, excluded, duplicates, truncated, result)


# ---- 급변동 연결 ----

@dataclass
class ResearchReportStage:
    """BriefingCore의 ReadyPort 구현. 보고서가 근거 부족이면 한 번 재검색하고 다시 쓴다.

    재검색이 없거나 실패하면 첫 보고서를 그대로 낸다. 실패는 새 실패 사유 없이 로그와 작업 기록에만 남긴다.
    """

    stage: ReportStage
    queries: QueryWriter | None
    requests: RequestPort
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    settings: CoreSettings = DEFAULT_SETTINGS
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))
    research: NewsResearch = field(init=False)
    _pending: dict[str, tuple[ReportInput, Report]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.research = NewsResearch(self.requests, self.job_log, self, self.settings)

    def on_ready(self, report_input: ReportInput) -> None:
        report = self.stage.write(report_input)
        if report is None:
            return
        if not report.insufficient_evidence or self.queries is None:
            if self.queries is not None:
                _log_research(report_input.job_id, False, 0, 0, 0, False, "not_needed")
            self.stage.publish(report_input, report)
            return
        now = self.clock()
        target = self._target(report_input)
        result = self.queries.ask(build_llm_input(report_input))
        for attempt in result.attempts:
            if attempt.rejection is None:
                self.job_log.record(self._attempt_record(target, attempt, now, ResearchReason.QUERY_ACCEPTED))
            else:
                log.warning("research search-term attempt %d/%d failed for job %s: %s",
                            attempt.attempt, self.queries.max_attempts, report_input.job_id, attempt.rejection)
                self.job_log.record(self._attempt_record(target, attempt, now, (
                    ResearchReason.QUERY_CALL_FAILED if attempt.error_type == LlmCallError.__name__
                    else ResearchReason.QUERY_REJECTED)))
        if result.gave_up or not result.queries:
            reason = ResearchReason.QUERY_GAVE_UP if result.gave_up else ResearchReason.NO_QUERIES
            _log_research(report_input.job_id, False, 0, 0, 0, False, reason.value)
            self.job_log.record(self._attempt_record(target, result.attempts[-1], now, reason))
            self.stage.publish(report_input, report, researched=True)
            return
        self._pending[report_input.job_id] = (report_input, report)
        if not self.research.start(target, result.queries, now):
            del self._pending[report_input.job_id]
            self.stage.publish(report_input, report, researched=True)

    def on_research_done(self, target: ResearchTarget, result: ResearchArticles | None, now: datetime) -> None:
        report_input, first = self._pending.pop(target.job_id)
        if result is None:
            self.stage.publish(report_input, first, researched=True)
            return
        rewritten = replace(report_input, research_articles=result.articles,
                            research_articles_truncated=result.truncated)
        report = self.stage.write(rewritten, researched=True)
        if report is None:
            self.job_log.record(_record(target, ResearchStep.REWRITE, ResearchReason.REWRITE_FAILED, now, 1,
                                        received=tuple(_article_entry(a) for a in result.articles)))
            return
        if report.insufficient_evidence:
            log.info("research job=%s rewritten report still has insufficient evidence", target.job_id)
            self.job_log.record(_record(target, ResearchStep.REWRITE, ResearchReason.STILL_INSUFFICIENT, now, 1,
                                        received=tuple(_article_entry(a) for a in result.articles)))
        self.stage.publish(rewritten, report, researched=True)

    def next_deadline(self) -> datetime | None:
        return self.research.next_deadline()

    def on_tick(self, now: datetime) -> None:
        self.research.on_tick(now)

    def _target(self, i: ReportInput) -> ResearchTarget:
        s = self.settings
        return ResearchTarget(
            job_kind=JOB_KIND_MOVE, job_id=i.job_id, correlation_id=i.correlation_id, tag=i.symbol,
            reply_symbol=i.symbol, since=i.as_of - s.evidence_window, until=i.as_of + s.delta,
            known_article_ids=frozenset(a.article_id for a in i.articles), after_seq=i.news_seq, move_at=i.as_of)

    @staticmethod
    def _attempt_record(target: ResearchTarget, attempt: QueryAttempt, now: datetime,
                        reason: ResearchReason) -> JobLogEntry:
        return _record(target, ResearchStep.QUERY, reason, now, attempt.attempt, error_type=attempt.error_type,
                       llm_input=attempt.llm_input, raw_output=attempt.raw_output, rejection=attempt.rejection)


def _article_entry(a: Article) -> dict[str, Any]:
    return {"articleId": a.article_id, "title": a.title, "summary": a.summary, "publisher": a.publisher,
            "publishedAt": format_timestamp(a.published_at), "url": a.url}
