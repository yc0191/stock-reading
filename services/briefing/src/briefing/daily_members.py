"""데일리 판을 멤버마다 따로 짜는 부분. Kafka와 DB를 모르며 모은 자료와 멤버의 포트폴리오를 인자로 받는다.

LLM 본문은 판 하나에 한 번만 쓴다(ETF마다 한 번). 이 모듈은 그 결과를 멤버 한 명의 포트폴리오에 맞게 줄인다:
그 멤버의 ETF만 남기고, 보유/관심 구분은 그 멤버의 값으로 붙이고, 다른 멤버의 ETF에만 얽힌 자료·문장은 뺀다.
근거 번호는 판 전체에서 정한 번호를 그대로 쓴다(멤버마다 번호를 새로 매기면 LLM이 인용한 번호가 어긋난다).
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol

from briefing.daily_inputs import EtfArticles
from briefing.daily_sections import (
    KIND_DAY,
    KIND_OPEN_FLOW,
    KIND_MEMBER,
    KIND_US_CLOSE,
    DailyInput,
    Entry,
    Section,
    build_sections,
)
from briefing.daily_output import POINT_NOTABLE, DailyOutput, Point
from briefing.daily_review import ReviewInput
from briefing.llm_output import ticker_pattern
from briefing.sources import US_MARKET, EtfMember
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings


@dataclass(frozen=True)
class MemberPortfolio:
    """이 판을 받을 멤버 한 명. etfs는 (종목코드, 보유/관심) 목록이다. 메일 주소는 브리핑이 알지 못한다.
    stocks는 포트폴리오의 국내 주식 (종목코드, 이름) 목록이다. 데일리 자료는 ETF만 다루므로 주식은 그날 급변동 목록에만 쓴다."""

    member_id: int
    etfs: tuple[tuple[str, str], ...]
    stocks: tuple[tuple[str, str | None], ...] = ()


class MemberPort(Protocol):
    def daily_members(self, edition: str) -> tuple[MemberPortfolio, ...] | None:
        """사용 중이고 그 판 수신을 켠 멤버 중 포트폴리오에 국내 ETF가 있는 멤버(번호 순)와 그 멤버의 국내 주식. 읽지 못하면 None."""
        ...


def member_input(full: DailyInput, member: MemberPortfolio) -> DailyInput:
    """판 전체 자료에서 이 멤버의 ETF에 관련된 것만 남긴 자료. 응답을 받지 못해 ETF 목록이 없으면 그대로 None이다.
    그날 급변동은 시세 대상인 멤버의 국내 주식에서도 나므로 ETF와 주식 둘 다로 거른다."""
    relations = dict(member.etfs)
    etfs = None if full.etfs is None else tuple(replace(e, relation=relations[e.symbol])
                                                for e in full.etfs if e.symbol in relations)
    mine = frozenset(e.symbol for e in etfs) if etfs is not None else frozenset(relations)
    tickers = frozenset(t for e in etfs or () for t in e.tickers)
    review = full.review
    if review is not None:
        moved = mine | frozenset(symbol for symbol, _ in member.stocks)
        review = ReviewInput(tuple(c for c in review.calls if c.symbol in mine), review.note,
                             None if review.moves is None else tuple(m for m in review.moves if m.symbol in moved),
                             member.stocks)
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


def whole_input(full: DailyInput, members: tuple[MemberPortfolio, ...] | None) -> DailyInput:
    """작업 기록에 남길 판 전체 글의 자료. 그날 급변동 줄에 모든 멤버의 국내 주식 이름을 붙인다.
    같은 주식을 여러 멤버가 담으면 이름을 아는 멤버의 것을 쓴다."""
    if full.review is None or not members:
        return full
    names: dict[str, str | None] = {}
    for m in members:
        for symbol, name in m.stocks:
            if names.get(symbol) is None:
                names[symbol] = name
    return replace(full, review=replace(full.review, stock_names=tuple(names.items())))


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


def foreign_us_names(full: DailyInput, mine: DailyInput) -> tuple[frozenset[str], frozenset[str]]:
    """다른 멤버의 ETF에만 담긴 미국 구성 종목의 (티커, 이름). 이 멤버의 ETF에도 담긴 종목은 빼며, 한 글자 이름은
    일반 낱말과 겹쳐 뺀다."""
    def members(i: DailyInput) -> list[EtfMember]:
        return [m for e in i.etfs or () if e.etf is not None for m in e.etf.members if m.market == US_MARKET]

    own = members(mine)
    own_tickers, own_names = {m.symbol for m in own}, {m.name for m in own if m.name}
    others = [m for m in members(full) if m.symbol not in own_tickers]
    return (frozenset(m.symbol for m in others),
            frozenset(m.name for m in others if m.name and len(m.name) >= 2 and m.name not in own_names))


def member_output(output: DailyOutput | None, mine: DailyInput, sections: tuple[Section, ...],
                  foreign: tuple[frozenset[str], frozenset[str]] = (frozenset(), frozenset())) -> DailyOutput | None:
    """LLM 본문에서 이 멤버에게 보여도 되는 문장만 남긴다. 다른 멤버의 ETF를 다룬 문장, 이 멤버의 항목에 없는 근거를
    인용한 문장, 다른 멤버의 ETF에만 담긴 미국 종목(foreign: 티커, 이름)을 쓴 문장은 뺀다. 마감 후 판 요약 줄은 근거
    번호가 없는 시장 전체 글이라 그대로 둔다. 환율·금리 이슈가 빠지면 본문은 코드가 받은 숫자로 그 칸을 채운다."""
    if output is None:
        return None
    symbols = frozenset(e.symbol for e in mine.etfs or ())
    refs = frozenset(e.ref for s in sections for e in s.entries)
    tickers, names = ticker_pattern(foreign[0]), foreign[1]

    def names_foreign(text: str) -> bool:
        return bool(tickers is not None and tickers.search(text)) or any(name in text for name in names)

    def visible(p: Point) -> bool:
        if p.kind != POINT_NOTABLE and p.symbol is not None and p.symbol not in symbols:
            return False
        # 시장 전체 근거만 인용해도 Claude가 ETF 구성 정보에서 본 남의 보유 종목을 쓸 수 있어 문장으로도 본다.
        if names_foreign(p.statement) or names_foreign(p.shortfall or ""):
            return False
        return all(ref in refs for ref in p.refs)

    return replace(output,
                   points=tuple(p for p in output.points if visible(p)),
                   issues=tuple(p for p in output.issues if visible(p)),
                   outlooks=tuple(p for p in output.outlooks if visible(p)),
                   reviews=tuple(p for p in output.reviews if visible(p)),
                   others=tuple(p for p in output.others if visible(p)))
