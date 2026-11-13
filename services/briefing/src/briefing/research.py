"""뉴스 재검색 한 번(검색어 LLM과 기사 조회). Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: 검색어 LLM이 검색어를 냄 -> news.research.requested(재검색 작업 하나당 1번) ->
같은 재검색 작업 id의 저장 끝 알림(news.stored) 대기 -> 뉴스처리기 기사 조회(researchJobId)를 한 번 ->
기존 시간 범위로 다시 거르고 이미 받은 기사를 뺌 -> 끝났다고 알림(ResearchDonePort).
어디서 끝나든 실패는 새 실패 사유 없이 로그와 작업 기록(JobLogPort)에만 남긴다.

재검색 작업 하나는 약한 문장 하나(또는 보고서 전체)의 한 번 찾기다. 어느 문장을 언제 찾고 찾은 기사로 무엇을 다시 쓸지는
sentence_research가 정하고, 여기서는 그 흐름 하나만 맡는다. 급변동과 데일리 판이 함께 쓴다.
"""
from __future__ import annotations

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Mapping, Protocol

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import NEWS_STORED, format_timestamp, research_request_event, stored_payload
from briefing.evidence import MARKET_WIDE
from briefing.job_log import EntryType, JobLogEntry, JobLogPort
from briefing.job_step import job_scope
from briefing.llm import LlmCallError, LlmClient, LlmUsage, attempt_usage, llm_config, usage_record
from briefing.ports import RequestPort
from briefing.queries import Query, QueryKind, QueryPort, QueryResult
from briefing.sources import Article, parse_articles
from briefing.trace import correlation_context

log = logging.getLogger("briefing.research")

RESEARCH_JOB_TYPE = "research-news"
RESEARCH_REQUEST_SEQ = 1
MAX_QUERY_LENGTH = 100

QUERY_SYSTEM_PROMPT = """당신은 ETF 브리핑에서 근거가 약하다고 표시된 문장 하나를 뒷받침할 뉴스 기사를 찾기 위한 검색어를 정하는 도우미입니다.
입력은 JSON이며 약한 문장(sentence)과 그 문장의 부족한 점(shortfall), ETF 정보(etf), 이미 찾은 기사 제목(foundArticleTitles), 배경 숫자(figures), 브리핑이 다룬 변동 또는 판의 기본 정보를 담고 있습니다.
sentence는 보고서가 쓴 문장이고 shortfall은 그 문장에 어떤 자료가 더 있어야 하는지 적은 것입니다. shortfall을 채울 기사를 찾을 검색어를 고릅니다.
입력에 sentence가 없으면 보고서 전체가 근거 부족이라는 뜻이므로 기본 정보가 다룬 변동이나 시장 흐름을 설명할 기사를 찾습니다.

규칙:
- 뉴스 검색에 넣을 검색어만 냅니다. 설명이나 문장은 쓰지 않습니다.
- 검색어 하나는 짧은 한국어 낱말 2~3개로 씁니다(예: 반도체 수출). foundArticleTitles와 같은 내용만 다시 찾을 검색어는 피합니다.
- 검색어의 모든 낱말이 기사 제목이나 요약에 그대로 나와야 그 기사가 걸립니다. 낱말이 많을수록 거의 걸리지 않으므로 낱말을 늘리지 않습니다.
- 미국 종목은 etf의 구성 종목에 name(한글 이름)이 있으면 그 한글 이름으로 검색어를 씁니다. 이름이 없을 때만 티커를 씁니다.
- 추측한 주제를 쓰지 않고 입력 자료에 근거해 고릅니다(예: sentence와 shortfall에 나온 낱말, etf 정보, figures에서 크게 움직인 항목). 입력에 근거가 없으면 빈 목록을 냅니다.
- sentence나 shortfall이 막연하면(예: "시장 불안", "투자 심리 위축") 그 막연한 낱말로 찾지 않고, 그 무렵 실제로 일어난 구체적 사건의 주체와 행위(누가·무엇을, 예: 회사 이름 + 실적 발표, 기관 이름 + 금리 결정)를 찾는 검색어를 씁니다. 주체와 행위는 입력 자료에 나온 것에서 고릅니다.
- sentence가 이미 찾은 기사(foundArticleTitles)의 사건을 말하고 있으면 같은 기사를 다시 찾지 말고, 그 사건을 다룬 다른 기사(다른 수치, 관계자 발언, 후속 보도)를 더 모을 검색어를 씁니다(예: 그 사건의 주체 이름 + 전망, 주체 이름 + 발언).
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
    REQUEST = "research-request"           # 재검색 요청 발행
    COMPLETION = "completion"              # 재검색 저장 끝 알림 대기
    ARTICLES = "articles"                  # 재검색 기사 조회
    FILTER = "filter"                      # 받은 기사 재검사
    SELECT = "select"                      # 다른 문장이 이미 쓴 기사 빼기
    REWRITE = "rewrite"                    # 문장(또는 보고서 전체) 다시 쓰기
    LIMIT = "limit"                        # 상한(요청 합계, 전체 시간)에 막혀 찾지 않음


class ResearchReason(str, Enum):
    """재검색 기록의 사유. 실패 사유(ports.FailureReason)가 아니며 작업을 실패로 만들지 않는다."""

    QUERY_REJECTED = "query_rejected"              # 검색어 LLM 응답이 형식에 맞지 않아 거부한 시도
    QUERY_CALL_FAILED = "query_call_failed"        # 검색어 LLM 호출이 실패한 시도
    QUERY_ACCEPTED = "query_accepted"              # 검색어 LLM 응답을 받아들인 시도
    QUERY_GAVE_UP = "query_gave_up"                # 모든 시도가 실패해 재검색하지 않음
    NO_QUERIES = "no_queries"                      # 검색어 LLM이 검색어를 내지 않음
    REQUESTED = "research_requested"               # 재검색 요청을 보냄
    ALREADY_RESEARCHED = "already_researched"      # 같은 재검색 작업은 한 번만 요청한다
    COLLECTION_INCOMPLETE = "collection_incomplete"  # 수집기가 하루 상한·검색 실패로 다 모으지 못함
    COMPLETION_TIMEOUT = "completion_timeout"      # 저장 끝 알림이 대기 상한 안에 오지 않음
    ARTICLES_NO_RESPONSE = "articles_no_response"  # 기사 조회를 다시 불러도 끝내 받지 못함
    ARTICLES_RECEIVED = "articles_received"        # 기사 목록을 받아 시간 범위로 다시 거름
    NO_USABLE_ARTICLES = "no_usable_articles"      # 범위 밖 제외·중복을 빼니 남은 기사가 없음
    ARTICLES_SELECTED = "articles_selected"        # 다른 문장이 쓴 기사를 빼고 이 문장에 쓸 기사를 정함
    ALL_USED_BY_OTHERS = "all_used_by_other_sentences"  # 받은 기사를 다른 문장이 이미 모두 씀
    REWRITTEN = "sentence_rewritten"               # 문장을 다시 씀
    REWRITE_FAILED = "rewrite_failed"              # 다시 쓰기가 끝내 실패(문장은 그대로 둠)
    STILL_INSUFFICIENT = "still_insufficient"      # 보고서 전체를 다시 써도 근거 부족
    REQUEST_LIMIT = "request_limit"                # 작업 하나의 재검색 요청 합계 상한이라 찾지 않음
    SENTENCE_LIMIT = "sentence_limit"              # 문장 하나의 재검색 횟수 상한이라 더 찾지 않음
    TIME_LIMIT = "research_time_limit"             # 작업 하나의 재검색 전체 시간 상한이라 그때의 본문으로 냄


# 재검색 사유별 기록 종류. 나머지 사유는 재검색이 끝내 실패했거나 쓸 기사가 없는 경우라 step-failed다.
_ENTRY_TYPES = {
    ResearchReason.QUERY_REJECTED: EntryType.LLM_REJECTED,
    ResearchReason.QUERY_CALL_FAILED: EntryType.LLM_REJECTED,
    ResearchReason.QUERY_ACCEPTED: EntryType.QUERY_WRITTEN,
    ResearchReason.REQUESTED: EntryType.RESEARCH_REQUESTED,
    ResearchReason.ARTICLES_RECEIVED: EntryType.ARTICLES_RECEIVED,
    ResearchReason.ARTICLES_SELECTED: EntryType.ARTICLES_RECEIVED,
    ResearchReason.REWRITTEN: EntryType.SENTENCE_REWRITTEN,
}


@dataclass(frozen=True)
class ResearchTarget:
    """재검색 작업 하나. 약한 문장 하나(또는 보고서 전체)의 한 번 찾기이며, 그것을 부른 작업과 문장을 함께 든다."""

    job_kind: str
    # 이 재검색 작업의 id(uuid). 수집기는 이 값을 researchJobId로 돌려준다.
    job_id: str
    # 재검색을 부른 작업(급변동 eventId, 데일리 판은 tick eventId). 작업 기록은 이 id로 묶는다.
    parent_job_id: str
    # 찾는 문장 번호(1부터). 보고서 전체가 근거 부족이면 0
    sentence_no: int
    # 그 문장의 몇 번째 찾기인가(1부터)
    round: int
    correlation_id: str
    # 재검색 요청에 붙일 태그 종목(급변동은 종목코드, 데일리 판은 *)
    tag: str
    # 기사 조회의 symbol(계약상 필수 칸이며 조회 조건으로 쓰이지 않는다)
    reply_symbol: str
    # 기사 조회와 재검사에 쓰는 기존 시간 범위(양끝 포함)
    since: datetime
    until: datetime
    # 이미 받은 기사. 재검색 기사에서 뺀다.
    known_article_ids: frozenset[str] = frozenset()


def research_entry(target: ResearchTarget, stage: ResearchStep, reason: ResearchReason, now: datetime,
            attempts: int, *, error_type: str | None = None, queries: tuple[str, ...] = (),
            request: Mapping[str, Any] | None = None, received: tuple[Mapping[str, Any], ...] = (),
            awaited: Mapping[str, Any] | None = None, missing: tuple[str, ...] = (), excluded: int = 0,
            duplicates: int = 0, llm_input: str | None = None, raw_output: str | None = None,
            rejection: str | None = None, usage: Mapping[str, int] | None = None, extra: Mapping[str, Any] | None = None) -> JobLogEntry:
    """재검색 기록 한 줄. 요청 payload와 받은 응답 payload는 가공하지 않고 담는다. 검색어는 로그에 쓰지 않지만 여기에는 담는다."""
    data: dict[str, Any] = {
        "researchJobId": target.job_id,
        "queries": list(queries), "request": request, "received": list(received), "awaited": awaited,
        "missing": list(missing), "excluded": excluded, "duplicates": duplicates,
    }
    if llm_input is not None:
        data["llm"] = {"input": llm_input, "rawOutput": raw_output, "rejectReason": rejection, "usage": usage}
    if extra:
        data.update(extra)
    return JobLogEntry(
        job_id=target.parent_job_id, entry_type=_ENTRY_TYPES.get(reason, EntryType.STEP_FAILED),
        stage=stage.value, reason=reason.value, occurred_at=now,
        # 보고서 전체(0)의 재검색은 문장 번호가 없다.
        sentence_no=target.sentence_no or None, round=target.round,
        error_type=error_type, attempts=attempts, data=data)


# ---- 검색어 LLM ----

@dataclass(frozen=True)
class QueryAttempt:
    """검색어 LLM 시도 하나의 입력과 결과."""

    attempt: int
    llm_input: str
    raw_output: str | None
    rejection: str | None
    error_type: str | None
    # 응답을 받지 못한 호출 실패는 None이다.
    usage: LlmUsage | None = None


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
            usage: LlmUsage | None = None
            try:
                reply = self.client.complete(system, user, schema)
                raw, usage = reply.text, reply.usage
                queries = parse_queries(raw, self.max_queries)
            except (LlmCallError, InvalidQueries) as error:
                attempts.append(QueryAttempt(attempt, user, raw, str(error), type(error).__name__,
                                              attempt_usage(usage, error)))
                continue
            attempts.append(QueryAttempt(attempt, user, raw, None, None, usage))
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


def ask_queries(writer: QueryWriter, job_log: JobLogPort, target: ResearchTarget, context: Mapping[str, Any],
                now: datetime) -> tuple[str, ...]:
    """검색어 LLM을 불러 시도마다 기록하고, 재검색할 검색어를 돌려준다. 빈 튜플이면 재검색하지 않는다."""
    result = writer.ask(context)
    for attempt in result.attempts:
        if attempt.rejection is None:
            job_log.record(_query_record(target, attempt, now, ResearchReason.QUERY_ACCEPTED))
        else:
            log.warning("research search-term attempt %d/%d failed for job %s: %s",
                        attempt.attempt, writer.max_attempts, target.parent_job_id, attempt.rejection)
            job_log.record(_query_record(target, attempt, now, (
                ResearchReason.QUERY_CALL_FAILED if attempt.error_type == LlmCallError.__name__
                else ResearchReason.QUERY_REJECTED)))
    if result.gave_up or not result.queries:
        reason = ResearchReason.QUERY_GAVE_UP if result.gave_up else ResearchReason.NO_QUERIES
        _log_research(target.parent_job_id, False, 0, 0, 0, False, reason.value)
        job_log.record(_query_record(target, result.attempts[-1], now, reason))
        return ()
    return result.queries


def _query_record(target: ResearchTarget, attempt: QueryAttempt, now: datetime,
                  reason: ResearchReason) -> JobLogEntry:
    return research_entry(target, ResearchStep.QUERY, reason, now, attempt.attempt, error_type=attempt.error_type,
                   llm_input=attempt.llm_input, raw_output=attempt.raw_output, rejection=attempt.rejection,
                   usage=usage_record(attempt.usage))


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
    last_request: dict[str, Any] | None = None
    # 뉴스처리기가 보낸 저장 끝 알림 payload 원문. 수집기가 낸 건수와 저장 결과 건수를 작업 기록에 그대로 남긴다.
    completion: dict[str, Any] | None = None


@dataclass
class NewsResearch:
    requests: RequestPort
    api: QueryPort
    job_log: JobLogPort
    done: ResearchDonePort
    settings: CoreSettings = DEFAULT_SETTINGS
    _jobs: OrderedDict[str, _Research] = field(default_factory=OrderedDict, init=False)

    def start(self, target: ResearchTarget, queries: tuple[str, ...], now: datetime) -> bool:
        """재검색 요청을 보낸다. 같은 작업 id는 한 번만 보내고 두 번째부터 False."""
        if target.job_id in self._jobs:
            log.info("research already requested for job %s; not requesting again", target.job_id)
            self.job_log.record(research_entry(target, ResearchStep.QUERY, ResearchReason.ALREADY_RESEARCHED, now, 0,
                                        queries=queries))
            return False
        event = research_request_event(target.job_id, RESEARCH_REQUEST_SEQ, target.job_kind, target.tag,
                                       list(queries), target.correlation_id, now)
        job = _Research(target, queries, now, event.value["payload"])
        self._jobs[target.job_id] = job
        while len(self._jobs) > self.settings.remembered_jobs:
            self._jobs.popitem(last=False)
        self.requests.send(event)
        self.job_log.record(research_entry(target, ResearchStep.REQUEST, ResearchReason.REQUESTED, now, 1, queries=queries,
                                    request=job.request))
        log.info("research requested job=%s sentence=%d round=%d kind=%s queryCount=%d", target.parent_job_id,
                 target.sentence_no, target.round, target.job_kind, len(queries))
        return True

    def cancel(self, research_job_id: str) -> None:
        """전체 시간 상한이 지나 더 기다리지 않는다. 늦게 오는 저장 끝 알림과 기사 조회 결과는 무시된다."""
        job = self._jobs.get(research_job_id)
        if job is not None:
            job.phase = _Phase.DONE

    def on_stored(self, message: str | bytes, now: datetime) -> bool:
        """재검색의 저장 끝 알림이면 처리하고 True. researchJobId가 없는 정기 수집의 알림은 False로 돌려준다."""
        try:
            payload = stored_payload(message)
            research_job_id = payload.get("researchJobId")
            if research_job_id is None:
                return False
            incomplete = frozenset(payload["incompleteSymbols"])
        except (KeyError, TypeError, AttributeError) as error:
            raise ValueError(f"malformed {NEWS_STORED} event: {error!r}") from error

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
        if payload.get("unprocessedEventIds"):
            # 대기 한도까지 저장되지 않은 기사는 조회에 없을 뿐이다. 저장된 것으로 진행한다.
            log.warning("research for job %s: %d article(s) were not stored in time", research_job_id,
                        len(payload["unprocessedEventIds"]))
        job.completion = payload
        job.phase = _Phase.FETCHING
        self._send(job, now)
        return True

    def on_tick(self, now: datetime) -> None:
        for job in list(self._jobs.values()):
            if job.phase is not _Phase.WAITING_COMPLETION or now - job.requested_at < self.settings.research_wait_limit:
                continue
            # 메시지 처리 밖에서 도는 작업이라 재검색을 부른 작업의 추적 ID를 로그 줄에 직접 건다.
            with correlation_context(job.target.correlation_id):
                log.warning("no research completion for job %s within the wait limit", job.target.job_id)
                self._finish(job, now, None, ResearchStep.COMPLETION, ResearchReason.COMPLETION_TIMEOUT, 1,
                             request=job.request, awaited={
                                 "eventType": NEWS_STORED,
                                 "jobType": RESEARCH_JOB_TYPE,
                                 "researchJobId": job.target.job_id,
                                 "requestedAt": format_timestamp(job.requested_at),
                             })

    def next_deadline(self) -> datetime | None:
        deadlines = [job.requested_at + self.settings.research_wait_limit
                     for job in self._jobs.values() if job.phase is _Phase.WAITING_COMPLETION]
        return min(deadlines) if deadlines else None

    # ---- 내부 ----

    def _send(self, job: _Research, now: datetime) -> None:
        t = job.target
        payload = {
            "jobId": t.job_id,
            "seq": 1,
            "symbol": t.reply_symbol,
            "since": format_timestamp(t.since),
            "until": format_timestamp(t.until),
            "maxArticles": self.settings.max_articles,
            "researchJobId": t.job_id,
        }
        job.last_request = payload
        self.api.submit(Query(QueryKind.ARTICLES, t.parent_job_id, ResearchStep.ARTICLES.value, t.correlation_id,
                                  payload), self._on_articles)

    def _on_articles(self, result: QueryResult, now: datetime) -> None:
        job = self._jobs.get(result.query.payload["jobId"])
        if job is None or job.phase is not _Phase.FETCHING:
            log.info("research articles for unknown or finished job %s ignored", result.query.payload["jobId"])
            return
        articles = None
        if result.received:
            try:
                articles = parse_articles(result.payload["articles"])
                truncated = bool(result.payload.get("truncated", False))
            except (KeyError, TypeError, ValueError) as error:
                log.error("malformed research articles response for job %s: %s", job.target.job_id,
                          type(error).__name__)
        if articles is None:
            log.warning("no research articles for job %s (%s)", job.target.job_id, result.failure_kind)
            self._finish(job, now, None, ResearchStep.ARTICLES, ResearchReason.ARTICLES_NO_RESPONSE, result.attempts,
                         request=job.last_request, received=(), error_type=result.failure_kind or "MalformedResponse")
            return
        self._finish_received(job, articles, truncated, result.payload, result.attempts, now)

    def _finish_received(self, job: _Research, articles: tuple[Article, ...], truncated: bool,
                         payload: dict[str, Any], attempts: int, now: datetime) -> None:
        t = job.target
        usable: list[Article] = []
        seen = set(t.known_article_ids)
        dropped: list[dict[str, Any]] = []
        excluded = duplicates = 0
        for article in articles:
            if not t.since <= article.published_at <= t.until:
                # 응답 쪽이 이미 범위를 지켰어야 하므로 응답 쪽 결함 신호로 경고를 남긴다.
                log.warning("dropping research article %s outside the requested range (job=%s)",
                            article.article_id, t.job_id)
                excluded += 1
                dropped.append({**article_entry(article), "reason": "outside_range"})
            elif article.article_id in seen:
                duplicates += 1
                dropped.append({**article_entry(article), "reason": "already_known"})
            else:
                seen.add(article.article_id)
                usable.append(article)
        _log_research(t.parent_job_id, True, len(articles), excluded, duplicates, truncated,
                      "found" if usable else ResearchReason.NO_USABLE_ARTICLES.value)
        # 받은 것·쓸 것·버린 것과 이유. 뉴스처리기가 보낸 저장 끝 알림 원문(수집·저장 건수)도 함께 남긴다.
        self.job_log.record(research_entry(
            t, ResearchStep.FILTER, ResearchReason.ARTICLES_RECEIVED, now, attempts, queries=job.queries,
            request=job.last_request, received=(payload,), excluded=excluded, duplicates=duplicates,
            extra={"articles": [article_entry(a) for a in articles], "usable": [article_entry(a) for a in usable],
                   "dropped": dropped, "truncated": truncated, "collectorCompletion": job.completion}))
        if not usable:
            self._finish(job, now, None, ResearchStep.FILTER, ResearchReason.NO_USABLE_ARTICLES, attempts,
                         request=job.last_request, received=(payload,), excluded=excluded, duplicates=duplicates,
                         log_result=False)
            return
        job.phase = _Phase.DONE
        self._done(t, ResearchArticles(tuple(usable), truncated), now)

    def _finish(self, job: _Research, now: datetime, result: ResearchArticles | None, stage: ResearchStep,
                reason: ResearchReason, attempts: int, log_result: bool = True, **data: Any) -> None:
        job.phase = _Phase.DONE
        if log_result:
            _log_research(job.target.parent_job_id, True, 0, 0, 0, False, reason.value)
        self.job_log.record(research_entry(job.target, stage, reason, now, attempts, queries=job.queries, **data))
        self._done(job.target, result, now)

    def _done(self, target: ResearchTarget, result: ResearchArticles | None, now: datetime) -> None:
        # 끝난 재검색은 그 작업의 문장을 다시 쓰거나 판을 낸다(LLM 호출). 어느 흐름에서 끝나든 그 호출을 부른 작업의
        # job_step으로 남기도록 작업 id를 건다.
        with job_scope(target.parent_job_id):
            self.done.on_research_done(target, result, now)


def _log_research(job_id: str, researched: bool, articles: int, excluded: int, duplicates: int, truncated: bool,
                  result: str) -> None:
    """작업 1건의 재검색 내역을 한 줄로 남긴다. 검색어와 종목은 보유 종목을 드러내므로 쓰지 않는다."""
    log.info("research job=%s researched=%s articles=%d articlesExcluded=%d articlesDuplicate=%d "
             "articlesTruncated=%s result=%s", job_id, researched, articles, excluded, duplicates, truncated, result)


def article_entry(a: Article) -> dict[str, Any]:
    return {"articleId": a.article_id, "title": a.title, "summary": a.summary, "publisher": a.publisher,
            "publishedAt": format_timestamp(a.published_at), "url": a.url}
