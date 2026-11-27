"""데일리 판의 문장 단위 재검색 어댑터. 재검색 흐름(SentenceResearch)이 판 본문(DailyDraft)의 약한 문장을 묻고,
다시 쓰기와 발행은 DailyCore에 맡긴다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from briefing.daily_inputs import moves_with_market
from briefing.daily_sections import Section, DailyInput, RELATION_HELD
from briefing.daily_report import build_llm_input
from briefing.daily_output import POINT_ETF, POINT_NOTABLE, sentence_slots, slot_point
from briefing.job_log import JobKind
from briefing.evidence import MARKET_WIDE
from briefing.research import ResearchTarget
from briefing.sentence_research import WHOLE_REPORT, WeakSentence
from briefing.sources import BACKGROUND_LABEL, Article

if TYPE_CHECKING:
    from briefing.daily_jobs import DailyCore, _DailyJob, _Draft

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
    """데일리 판 본문의 문장 하나(sentence_slots의 한 자리)를 재검색의 문장 하나로 본다."""

    core: DailyCore
    _held_by_job: dict[str, frozenset[str] | None] = field(default_factory=dict, init=False)

    def can_research(self, draft: DailyDraft) -> bool:
        # 기사 조회 요청에는 종목코드가 필수라 보유 ETF 자료가 없으면 재검색하지 않는다.
        return bool(draft.input.etfs) and self.core.writer is not None

    def request_limit(self) -> int | None:
        return self.core.settings.max_research_requests

    def weak_sentences(self, draft: DailyDraft) -> tuple[WeakSentence, ...]:
        """다시 찾을 약한 문장(밤사이 이슈, ETF 이유, 환율·금리 칸, 아침 판 ETF별 예상)을 찾을 차례로 돌려준다.

        차례는 보유 ETF → 환율·금리 → 밤사이 이슈 → 관심 ETF다. 요청 합계 상한 안에서 모두 한 번씩 먼저 찾고, 남은 요청으로
        두 번째를 찾으므로 차례가 뒤인 문장이 상한에 걸린다. 보유는 멤버 중 한 명이라도 보유한 ETF이고, 멤버를 읽지 못하면
        모두 보유로 본다. 눈에 띄는 점은 '근거 약함'을 그대로 보여 주고, 마감 후 판에서 시장과 같은 방향(부호가 같고 차이가
        설정값 이내)으로 움직인 ETF의 이유는 시장 전체의 움직임으로 설명되므로 따로 찾지 않는다."""
        output = draft.draft.output
        if output is None or output.insufficient_evidence:
            return ()
        etfs = {e.symbol: e for e in draft.input.etfs or ()}
        held = self._held(draft)
        found = []
        for no, (name, _) in enumerate(sentence_slots(output), start=1):
            p = slot_point(output, no)
            if p is None or not p.shortfall or p.kind == POINT_NOTABLE:
                continue
            if p.kind == POINT_ETF and p.symbol in etfs and moves_with_market(
                    etfs[p.symbol].rate, draft.input.background or (), self.core.settings):
                continue
            if p.symbol is not None:
                rank = 0 if held is None or p.symbol in held else 3
            else:
                rank = 1 if name == "issues" else 2
            found.append((rank, no, WeakSentence(no, p.statement, p.shortfall)))
        return tuple(w for _, _, w in sorted(found, key=lambda f: (f[0], f[1])))

    def _held(self, draft: DailyDraft) -> frozenset[str] | None:
        """멤버 중 누구라도 보유한 ETF. 판마다 한 번 읽어 둔다. 멤버를 모르거나 읽지 못하면 None."""
        job = draft.job
        if job is None:
            return None
        if job.job_id not in self._held_by_job:
            members = self.core.members.daily_members(job.edition) if self.core.members is not None else None
            self._held_by_job[job.job_id] = None if members is None else frozenset(
                symbol for m in members for symbol, relation in m.etfs if relation == RELATION_HELD)
        return self._held_by_job[job.job_id]

    def whole_insufficient(self, draft: DailyDraft) -> bool:
        return draft.draft.insufficient_evidence

    def query_context(self, draft: DailyDraft, sentence: WeakSentence | None) -> dict[str, Any]:
        llm = build_llm_input(draft.input, draft.sections, self.core.settings, group=False)
        # 배경은 원문(response)을 빼고 코드가 꺼낸 숫자(지수 등락률, 원/달러, 해외 시세 등)와 받은 시각만 준다. 검색어를 고르는 데
        # 원문은 필요 없고, 원문을 넣으면 검색어 한 번에 입력이 수만 토큰이 된다(급변동은 처음부터 숫자만 준다).
        background = [{k: v for k, v in e.llm.items() if k != "response"}
                      for s in draft.sections for e in s.entries if e.llm.get("category") == BACKGROUND_LABEL]
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
            point = slot_point(draft.draft.output, sentence.no)
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
        if sentence_no == WHOLE_REPORT:
            return None
        point = slot_point(draft.draft.output, sentence_no)
        return point.statement if point is not None else None

    def publish(self, draft: DailyDraft, now: datetime, research_count: int) -> None:
        self.core.publish_draft(draft, now, research_count)
