"""데일리 판을 멤버마다 따로 짜는 부분. Kafka와 DB를 모르며 모은 자료와 멤버의 포트폴리오를 인자로 받는다.

LLM 본문은 판 하나에 한 번만 쓴다(ETF마다 한 번). 이 모듈은 그 결과를 멤버 한 명의 포트폴리오에 맞게 줄인다:
그 멤버의 ETF만 남기고, 보유/관심 구분은 그 멤버의 값으로 붙이고, 다른 멤버의 ETF에만 얽힌 자료·문장은 뺀다.
근거 번호는 판 전체에서 정한 번호를 그대로 쓴다(멤버마다 번호를 새로 매기면 LLM이 인용한 번호가 어긋난다).
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol

from briefing.daily_inputs import EtfArticles
from briefing.daily_report import (
    KIND_DAY,
    KIND_OPEN_FLOW,
    KIND_MEMBER,
    KIND_US_CLOSE,
    POINT_NOTABLE,
    DailyInput,
    DailyOutput,
    Entry,
    Point,
    Section,
    build_sections,
)
from briefing.daily_review import ReviewInput
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings


@dataclass(frozen=True)
class MemberPortfolio:
    """이 판을 받을 멤버 한 명. etfs는 (종목코드, 보유/관심) 목록이다. 메일 주소는 브리핑이 알지 못한다."""

    member_id: int
    etfs: tuple[tuple[str, str], ...]


class MemberPort(Protocol):
    def daily_members(self, edition: str) -> tuple[MemberPortfolio, ...] | None:
        """사용 중이고 그 판 수신을 켠 멤버 중 포트폴리오에 국내 ETF가 있는 멤버(번호 순). 읽지 못하면 None."""
        ...


def member_input(full: DailyInput, member: MemberPortfolio) -> DailyInput:
    """판 전체 자료에서 이 멤버의 ETF에 관련된 것만 남긴 자료. 응답을 받지 못해 ETF 목록이 없으면 그대로 None이다."""
    relations = dict(member.etfs)
    etfs = None if full.etfs is None else tuple(replace(e, relation=relations[e.symbol])
                                                for e in full.etfs if e.symbol in relations)
    mine = frozenset(e.symbol for e in etfs) if etfs is not None else frozenset(relations)
    tickers = frozenset(t for e in etfs or () for t in e.tickers)
    review = full.review
    if review is not None:
        review = ReviewInput(tuple(c for c in review.calls if c.symbol in mine), review.note,
                             None if review.moves is None else tuple(m for m in review.moves if m.symbol in mine))
    return replace(
        full,
        etfs=etfs,
        close_rates=None if full.close_rates is None else tuple(r for r in full.close_rates if r.ticker in tickers),
        filings=None if full.filings is None else tuple(f for f in full.filings if tickers.intersection(f.tickers)),
        etf_articles=None if full.etf_articles is None else tuple(g for g in full.etf_articles if g.symbol in mine),
        member_issues=tuple(i for i in full.member_issues if i.etf in mine),
        notables=tuple(n for n in full.notables if all(s in mine for s in n.symbols)),
        review=review,
    )


def _entry_key(entry: Entry) -> tuple | None:
    """ETF·구성 종목에 묶인 근거를 가리키는 값. 멤버마다 목록이 달라 번호가 밀리는 종류만 해당한다."""
    llm = entry.llm
    if entry.kind == KIND_DAY:
        return (KIND_DAY, llm.get("symbol"))
    if entry.kind == KIND_OPEN_FLOW:
        return (KIND_OPEN_FLOW, llm.get("symbol"))
    if entry.kind == KIND_US_CLOSE:
        return (KIND_US_CLOSE, llm.get("ticker"))
    if entry.kind == KIND_MEMBER:
        return (KIND_MEMBER, llm.get("etf"), llm.get("ticker"))
    return None


def member_sections(mine: DailyInput, full_sections: tuple[Section, ...],
                    settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[Section, ...]:
    """멤버 자료로 항목을 만들되 근거 번호는 판 전체 항목의 번호로 맞춘다."""
    numbers = {key: e.ref for s in full_sections for e in s.entries if (key := _entry_key(e)) is not None}
    sections = []
    for s in build_sections(mine, settings):
        entries = []
        for e in s.entries:
            ref = numbers.get(_entry_key(e)) if _entry_key(e) is not None else None
            entries.append(e if ref is None else replace(e, ref=ref, llm={**e.llm, "ref": ref}))
        sections.append(replace(s, entries=tuple(entries)))
    return tuple(sections)


def member_output(output: DailyOutput | None, mine: DailyInput, sections: tuple[Section, ...]) -> DailyOutput | None:
    """LLM 본문에서 이 멤버에게 보여도 되는 문장만 남긴다. 다른 멤버의 ETF를 다룬 문장과, 이 멤버의 항목에 없는 근거를
    인용한 문장은 뺀다. 요약 줄과 환율·금리 이슈는 ETF 하나하나를 나열하지 않는 시장 전체 글이라 그대로 둔다."""
    if output is None:
        return None
    symbols = frozenset(e.symbol for e in mine.etfs or ())
    refs = frozenset(e.ref for s in sections for e in s.entries)

    def visible(p: Point) -> bool:
        if p.kind != POINT_NOTABLE and p.symbol is not None and p.symbol not in symbols:
            return False
        return all(ref in refs for ref in p.refs)

    return replace(output,
                   points=tuple(p for p in output.points if visible(p)),
                   outlooks=tuple(p for p in output.outlooks if visible(p)),
                   reviews=tuple(p for p in output.reviews if visible(p)))
