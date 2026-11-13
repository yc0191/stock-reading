"""근거가 약한 문장 단위 재검색. Kafka를 모르며 메시지와 시각을 인자로 받아 상태만 움직인다.

흐름: 보고서 LLM이 약하다고 한 문장마다 검색어 LLM을 따로 불러 재검색 요청을 문장마다 별도 재검색 작업 id로 보낸다(동시에 진행) ->
문장마다 기사가 오면 다른 문장이 이미 쓴 기사를 빼고 그 문장만 다시 쓴다(약하지 않은 문장은 그대로) ->
다시 써도 약하면 그 문장만 한 번 더 찾는다(문장당 횟수 상한) -> 진행 중인 재검색이 없어지면 발행한다.
작업 하나의 요청 합계 상한을 넘는 문장은 찾지 않고 '근거 약함'을 남기며, 작업 하나의 전체 시간 상한이 지나면
그때의 본문으로 낸다. 보고서 전체가 근거 부족이면 문장이 없으므로 보고서 전체를 대상으로 한 번 찾아 다시 쓴다.

문장 하나를 찾고 다시 쓰는 흐름은 급변동(MoveSentences)과 데일리 판(daily_jobs)이 같고, 본문을 다루는 부분만 어댑터가 맡는다.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Mapping, Protocol

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.final_check import FinalCheck
from briefing.job_log import DiscardJobLog, JobKind, JobLogPort
from briefing.jobs import ReportInput
from briefing.job_step import job_scope
from briefing.ports import RequestPort
from briefing.queries import QueryPort
from briefing.report import Cause, Report, ReportStage, build_llm_input
from briefing.research import (
    NewsResearch,
    QueryWriter,
    ResearchArticles,
    ResearchReason,
    ResearchStep,
    ResearchTarget,
    article_entry,
    ask_queries,
    research_entry,
)
from briefing.sources import Article, etf_info_input
from briefing.trace import correlation_context

log = logging.getLogger("briefing.sentence_research")

# 보고서 전체가 근거 부족일 때의 문장 번호. 문장 번호는 1부터다.
WHOLE_REPORT = 0


@dataclass(frozen=True)
class WeakSentence:
    """보고서 LLM이 근거가 약하다고 한 문장 하나. text는 작업 기록의 before에 남길 사람이 읽는 문장이다."""

    no: int
    text: str
    shortfall: str


class SentenceAdapter(Protocol):
    """본문(draft)을 다루는 쪽. 급변동과 데일리 판이 각자 구현한다. draft는 어댑터만 아는 값이다."""

    def can_research(self, draft: Any) -> bool: ...

    def weak_sentences(self, draft: Any) -> tuple[WeakSentence, ...]: ...

    def whole_insufficient(self, draft: Any) -> bool: ...

    def query_context(self, draft: Any, sentence: WeakSentence | None) -> Mapping[str, Any]:
        """검색어 LLM 입력. 문장, 부족한 점, ETF 정보, 이미 찾은 기사 제목, 배경 숫자를 담는다."""

    def target(self, draft: Any, research_id: str, sentence_no: int, round: int) -> ResearchTarget: ...

    def correlation_id(self, draft: Any) -> str:
        """작업의 추적 ID. 메시지 처리 밖(전체 시간 상한)에서 찍는 로그 줄에 건다."""

    def rewrite_sentence(self, draft: Any, sentence_no: int, round: int, articles: tuple[Article, ...],
                         research_articles: tuple[Article, ...], now: datetime) -> Any | None:
        """그 문장만 다시 쓴 새 draft. 끝내 못 쓰면 None(원래 draft를 그대로 둔다)."""

    def rewrite_whole(self, draft: Any, round: int, articles: tuple[Article, ...],
                      research_articles: tuple[Article, ...], now: datetime) -> Any | None: ...

    def sentence_text(self, draft: Any, sentence_no: int) -> str | None: ...

    def publish(self, draft: Any, now: datetime, research_count: int) -> None: ...


def research_job_id(job_id: str, sentence_no: int, round: int) -> str:
    """문장 하나의 한 번 찾기의 재검색 작업 id. 요청 계약이 uuid를 요구하므로 (작업, 문장, 회차)에서 정한다."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"research:{job_id}:{sentence_no}:{round}"))


@dataclass
class _Sentence:
    rounds: int = 0


@dataclass
class _Run:
    job_id: str
    adapter: SentenceAdapter
    draft: Any
    deadline: datetime
    # 보낸 재검색 요청 합계
    requests: int = 0
    sentences: dict[int, _Sentence] = field(default_factory=dict)
    # 찾는 중인 재검색 작업 id -> 문장 번호
    inflight: dict[str, int] = field(default_factory=dict)
    # 문장을 다시 쓰는 데 이미 쓴 기사. 같은 articleId는 다른 문장에 다시 쓰지 않는다.
    used_ids: set[str] = field(default_factory=set)
    research_articles: list[Article] = field(default_factory=list)


@dataclass
class SentenceResearch:
    """작업 하나의 약한 문장들을 동시에 찾고 그 문장만 다시 쓰는 흐름. 급변동과 데일리 판이 함께 쓴다."""

    queries: QueryWriter | None
    requests: RequestPort
    # 재검색 기사 조회(뉴스처리기 내부 조회 API)
    api: QueryPort
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    settings: CoreSettings = DEFAULT_SETTINGS
    research: NewsResearch = field(init=False)
    _adapters: dict[str, SentenceAdapter] = field(default_factory=dict, init=False)
    _runs: dict[str, _Run] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.research = NewsResearch(self.requests, self.api, self.job_log, self, self.settings)

    def register(self, job_kind: str, adapter: SentenceAdapter) -> None:
        self._adapters[job_kind] = adapter

    # ---- 시작 ----

    def begin(self, job_kind: str, job_id: str, draft: Any, now: datetime) -> bool:
        """약한 문장(또는 근거 부족)이 있으면 재검색을 시작한다. 시작했으면 True이고 발행은 이 흐름이 맡는다.

        False이면 재검색할 것이 없거나 할 수 없으므로 부르는 쪽이 바로 발행한다.
        """
        adapter = self._adapters[job_kind]
        if self.queries is None or not adapter.can_research(draft):
            return False
        weak = adapter.weak_sentences(draft)
        whole = adapter.whole_insufficient(draft)
        if not weak and not whole:
            log.info("research job=%s researched=False result=not_needed", job_id)
            return False
        run = _Run(job_id, adapter, draft, now + self.settings.research_total_limit)
        self._runs[job_id] = run
        if whole:
            self._search(run, None, now)
        else:
            for sentence in weak:
                self._search(run, sentence, now)
        self._finish_if_idle(run, now)
        return True

    def _search(self, run: _Run, sentence: WeakSentence | None, now: datetime) -> None:
        """문장 하나의 재검색 요청을 보낸다. 상한에 막히면 찾지 않고 그 문장은 근거 약함으로 남는다."""
        no = sentence.no if sentence is not None else WHOLE_REPORT
        state = run.sentences.setdefault(no, _Sentence())
        limit = None
        if state.rounds >= self.settings.research_rounds_per_sentence:
            limit = ResearchReason.SENTENCE_LIMIT
        elif now >= run.deadline:
            limit = ResearchReason.TIME_LIMIT
        elif run.requests >= self.settings.max_research_requests:
            limit = ResearchReason.REQUEST_LIMIT
        if limit is not None:
            self._note(run, no, state.rounds + 1, ResearchStep.LIMIT, limit, now)
            return
        state.rounds += 1
        research_id = research_job_id(run.job_id, no, state.rounds)
        target = run.adapter.target(run.draft, research_id, no, state.rounds)
        found = ask_queries(self.queries, self.job_log, target, run.adapter.query_context(run.draft, sentence), now)
        if not found:
            return
        if self.research.start(target, found, now):
            run.requests += 1
            run.inflight[research_id] = no

    # ---- 재검색이 끝남 ----

    def on_research_done(self, target: ResearchTarget, result: ResearchArticles | None, now: datetime) -> None:
        """재검색 작업 하나가 끝났다. 받은 기사로 그 문장만 다시 쓰고, 실패했거나 쓸 기사가 없으면 문장을 그대로 둔다."""
        run = self._runs.get(target.parent_job_id)
        if run is None or run.inflight.pop(target.job_id, None) is None:
            return
        if now >= run.deadline:
            self._expire(run, now)
            return
        no = target.sentence_no
        if result is not None:
            self._rewrite(run, target, result, now)
        self._finish_if_idle(run, now)

    def _rewrite(self, run: _Run, target: ResearchTarget, result: ResearchArticles, now: datetime) -> None:
        no = target.sentence_no
        fresh = tuple(a for a in result.articles if a.article_id not in run.used_ids)
        used_elsewhere = [a for a in result.articles if a.article_id in run.used_ids]
        # 쓸 기사와, 다른 문장이 이미 써서 뺀 기사를 이유와 함께 남긴다.
        self.job_log.record(research_entry(
            target, ResearchStep.SELECT, ResearchReason.ARTICLES_SELECTED, now, 1,
            extra={"used": [article_entry(a) for a in fresh],
                   "dropped": [{**article_entry(a), "reason": "used_by_other_sentence"} for a in used_elsewhere]}))
        if not fresh:
            self.job_log.record(research_entry(target, ResearchStep.SELECT, ResearchReason.ALL_USED_BY_OTHERS, now, 1))
            return
        run.used_ids.update(a.article_id for a in fresh)
        run.research_articles.extend(fresh)
        adapter, before = run.adapter, run.adapter.sentence_text(run.draft, no)
        if no == WHOLE_REPORT:
            rewritten = adapter.rewrite_whole(run.draft, target.round, fresh, tuple(run.research_articles), now)
        else:
            rewritten = adapter.rewrite_sentence(run.draft, no, target.round, fresh, tuple(run.research_articles), now)
        if rewritten is None:
            self.job_log.record(research_entry(target, ResearchStep.REWRITE, ResearchReason.REWRITE_FAILED, now, 1))
            return
        run.draft = rewritten
        entry = research_entry(target, ResearchStep.REWRITE, ResearchReason.REWRITTEN, now, 1,
                               extra={"articleRefs": [a.article_id for a in fresh]})
        self.job_log.record(replace(entry, before=before, after=adapter.sentence_text(rewritten, no)))
        if no == WHOLE_REPORT:
            if adapter.whole_insufficient(rewritten):
                log.info("research job=%s rewritten report still has insufficient evidence", run.job_id)
                self.job_log.record(research_entry(
                    target, ResearchStep.REWRITE, ResearchReason.STILL_INSUFFICIENT, now, 1,
                    received=tuple(article_entry(a) for a in fresh)))
            # 보고서 전체를 다시 쓴 결과에 약한 문장이 있으면 그 문장은 이미 한 번 찾은 것으로 보고 이어서 찾는다.
            for sentence in adapter.weak_sentences(rewritten):
                run.sentences[sentence.no] = _Sentence(rounds=1)
                self._search(run, sentence, now)
            return
        again = {w.no: w for w in adapter.weak_sentences(rewritten)}.get(no)
        if again is not None:
            log.info("research job=%s sentence %d is still weak after the rewrite", run.job_id, no)
            self._search(run, again, now)

    # ---- 시간 ----

    def on_stored(self, message: str | bytes, now: datetime) -> bool:
        return self.research.on_stored(message, now)

    def on_tick(self, now: datetime) -> None:
        self.research.on_tick(now)
        for run in list(self._runs.values()):
            if run.job_id in self._runs and now >= run.deadline:
                # 메시지 처리 밖에서 판을 내보내므로 그 작업의 추적 ID를 로그 줄에, 작업 id를 LLM 호출 기록(job_step)에 직접 건다.
                with correlation_context(run.adapter.correlation_id(run.draft)), job_scope(run.job_id):
                    self._expire(run, now)

    def next_deadline(self) -> datetime | None:
        deadlines = [run.deadline for run in self._runs.values()]
        research = self.research.next_deadline()
        if research is not None:
            deadlines.append(research)
        return min(deadlines) if deadlines else None

    def _expire(self, run: _Run, now: datetime) -> None:
        """전체 시간 상한이 지났다. 아직 찾는 중인 문장은 포기하고 그때의 본문으로 낸다."""
        log.warning("research job=%s reached the total time limit with %d search(es) in progress",
                    run.job_id, len(run.inflight))
        for research_id, no in run.inflight.items():
            self.research.cancel(research_id)
            round_no = run.sentences[no].rounds
            self._note(run, no, round_no, ResearchStep.LIMIT, ResearchReason.TIME_LIMIT, now)
        run.inflight.clear()
        self._finish(run, now)

    # ---- 끝 ----

    def _finish_if_idle(self, run: _Run, now: datetime) -> None:
        if not run.inflight and run.job_id in self._runs:
            self._finish(run, now)

    def _finish(self, run: _Run, now: datetime) -> None:
        self._runs.pop(run.job_id, None)
        run.adapter.publish(run.draft, now, run.requests)

    def _note(self, run: _Run, no: int, round: int, step: ResearchStep, reason: ResearchReason,
              now: datetime) -> None:
        target = run.adapter.target(run.draft, research_job_id(run.job_id, no, round), no, round)
        self.job_log.record(research_entry(
            target, step, reason, now, 0,
            extra={"requests": run.requests, "requestLimit": self.settings.max_research_requests,
                   "roundLimit": self.settings.research_rounds_per_sentence,
                   "totalLimitSeconds": int(self.settings.research_total_limit.total_seconds())}))


# ---- 급변동 ----

@dataclass(frozen=True)
class MoveDraft:
    """급변동 본문 한 벌. 다시 쓰면 재검색 기사가 든 입력과 새 보고서로 바뀐다."""

    input: ReportInput
    report: Report
    # 문장 단위 재검색으로 다시 쓴 원인 번호(보고서 전체를 다시 썼으면 WHOLE_REPORT). 마지막 검사가 검토를 부를지 정한다.
    rewritten: tuple[int, ...] = ()


def checked_report(final: FinalCheck | None, draft: MoveDraft, now: datetime) -> Report:
    """발행 직전 마지막 검사를 거친 보고서. 검사가 없거나 LLM이 쓴 본문이 아니면 그대로 낸다."""
    if final is None or draft.report.output is None:
        return draft.report
    return final.run(draft, now).report


def cause_text(cause: Cause) -> str:
    return f"{cause.title}: {cause.detail} {cause.meaning}"


@dataclass
class MoveSentences:
    """급변동 보고서의 원인 하나를 문장 하나로 본다."""

    stage: ReportStage
    settings: CoreSettings = DEFAULT_SETTINGS
    final: FinalCheck | None = None

    def can_research(self, draft: MoveDraft) -> bool:
        return True

    def weak_sentences(self, draft: MoveDraft) -> tuple[WeakSentence, ...]:
        output = draft.report.output
        if output is None or output.insufficient_evidence:
            return ()
        return tuple(WeakSentence(n, cause_text(c), c.shortfall)
                     for n, c in enumerate(output.causes, start=1) if c.shortfall)

    def whole_insufficient(self, draft: MoveDraft) -> bool:
        return draft.report.insufficient_evidence

    def query_context(self, draft: MoveDraft, sentence: WeakSentence | None) -> Mapping[str, Any]:
        i = draft.input
        llm = build_llm_input(i, self.settings, group=False)
        context: dict[str, Any] = {
            "symbol": llm["symbol"], "direction": llm["direction"], "changePercent": llm["changePercent"],
            "moveAt": llm["moveAt"],
            "foundArticleTitles": [a.title for a in i.articles + i.research_articles],
            "figures": llm["figures"],
        }
        if i.etf is not None:
            context["etf"] = etf_info_input(i.etf, self.settings.max_etf_members)
        if sentence is not None:
            context["sentence"] = sentence.text
            context["shortfall"] = sentence.shortfall
        return context

    def target(self, draft: MoveDraft, research_id: str, sentence_no: int, round: int) -> ResearchTarget:
        i, s = draft.input, self.settings
        return ResearchTarget(
            job_kind=JobKind.MOVE.value, job_id=research_id, parent_job_id=i.job_id, sentence_no=sentence_no,
            round=round, correlation_id=i.correlation_id, tag=i.symbol, reply_symbol=i.symbol,
            since=i.as_of - s.evidence_window, until=i.as_of + s.delta,
            known_article_ids=frozenset(a.article_id for a in i.articles))

    def correlation_id(self, draft: MoveDraft) -> str:
        return draft.input.correlation_id

    def rewrite_sentence(self, draft: MoveDraft, sentence_no: int, round: int, articles: tuple[Article, ...],
                         research_articles: tuple[Article, ...], now: datetime) -> MoveDraft | None:
        rewritten = replace(draft.input, research_articles=research_articles)
        report = self.stage.rewrite_sentence(rewritten, draft.report, sentence_no, round,
                                             tuple(a.article_id for a in articles))
        return MoveDraft(rewritten, report, draft.rewritten + (sentence_no,)) if report is not None else None

    def rewrite_whole(self, draft: MoveDraft, round: int, articles: tuple[Article, ...],
                      research_articles: tuple[Article, ...], now: datetime) -> MoveDraft | None:
        rewritten = replace(draft.input, research_articles=research_articles)
        # 다시 쓰기가 끝내 실패해도 첫 보고서를 낼 수 있으므로 작업을 실패로 닫지 않는다.
        report = self.stage.write(rewritten, fail_job=False)
        return MoveDraft(rewritten, report, (WHOLE_REPORT,)) if report is not None else None

    def sentence_text(self, draft: MoveDraft, sentence_no: int) -> str | None:
        output = draft.report.output
        if output is None or sentence_no == WHOLE_REPORT or sentence_no > len(output.causes):
            return None
        return cause_text(output.causes[sentence_no - 1])

    def publish(self, draft: MoveDraft, now: datetime, research_count: int) -> None:
        self.stage.publish(draft.input, checked_report(self.final, draft, now), research_count)


@dataclass
class ResearchReportStage:
    """BriefingCore의 ReadyPort 구현. 보고서가 근거 약함·근거 부족이면 문장 단위로 재검색하고 그 문장만 다시 쓴다.

    재검색이 없거나 실패하면 첫 보고서를 그대로 낸다. 실패는 새 실패 사유 없이 로그와 작업 기록에만 남긴다.
    """

    stage: ReportStage
    sentences: SentenceResearch
    # 있을 때만 발행 직전 마지막 검사(코드 검사, 필요하면 LLM 검토와 고침)를 한다.
    final: FinalCheck | None = None

    def __post_init__(self) -> None:
        self.sentences.register(JobKind.MOVE.value, MoveSentences(self.stage, self.sentences.settings, self.final))

    def on_ready(self, report_input: ReportInput) -> None:
        report = self.stage.write(report_input)
        if report is None:
            return
        draft = MoveDraft(report_input, report)
        now = self.stage.clock()
        if not self.sentences.begin(JobKind.MOVE.value, report_input.job_id, draft, now):
            self.stage.publish(report_input, checked_report(self.final, draft, now))
