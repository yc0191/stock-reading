"""마감 후 판이 Claude에 넘기는 ETF별 자료 중 코드가 계산하는 부분: 미국 구성 종목 이슈, 눈에 띄는 점 후보, 이유를 찾지 못한 ETF.

Kafka를 모르며 받은 자료만 인자로 받는다. 기준 숫자는 모두 DailySettings에서 온다.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Iterable

from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.move_figures import KOSPI_NAME, index_figure
from briefing.sources import US_MARKET, Article, Background, CloseRate, Filing

if TYPE_CHECKING:
    from briefing.daily_report import EtfDay

KIND_OPPOSITE = "opposite"
KIND_CLUSTER = "cluster"


@dataclass(frozen=True)
class EtfArticles:
    """ETF 하나가 받은 기사. received가 False면 끝내 응답을 받지 못해 기사 없음으로 진행한 것이다."""

    symbol: str
    articles: tuple[Article, ...]
    received: bool
    truncated: bool = False


@dataclass(frozen=True)
class MemberIssue:
    """ETF에 담긴 미국 구성 종목 하나의 이슈. 직전 미국 장 등락이 크거나 범위 안 공시가 있다."""

    etf: str
    rank: int
    ticker: str
    name: str | None
    rate: str | None
    filing_refs: tuple[str, ...]


@dataclass(frozen=True)
class NotableCandidate:
    kind: str
    # 관련 ETF 종목코드
    symbols: tuple[str, ...]


def decimal_rate(text: str | None) -> Decimal | None:
    """등락률 문자열의 십진 값. 숫자가 아니거나 유한하지 않으면 None."""
    if text is None:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def moves_with_market(etf_rate: str | None, background: Iterable[Background],
                      settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> bool:
    """ETF의 하루 등락이 그날 시장(코스피종합 등락률)과 같은 방향인가. 부호가 같고 두 등락률 차이가 설정값 이내일 때만이다.
    어느 한쪽이라도 모르거나 0이면 같은 방향이 아니다."""
    value = decimal_rate(etf_rate)
    kospi = index_figure(tuple(background), KOSPI_NAME)
    if value is None or kospi is None or kospi.rate is None:
        return False
    if not ((value > 0 and kospi.rate > 0) or (value < 0 and kospi.rate < 0)):
        return False
    return abs(value - kospi.rate) <= _threshold(settings.market_same_direction_gap_percent)


def _threshold(value: float) -> Decimal:
    return Decimal(str(value))


def member_issues(etfs: Iterable[EtfDay], close_rates: Iterable[CloseRate], filings: Iterable[Filing],
                  settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[tuple[MemberIssue, ...], int]:
    """ETF마다 비중 상위 member_rank_limit개 안의 미국 종목 중 이슈가 있는 종목을 비중 순으로 member_issues_per_etf개까지.

    이슈는 직전 장 등락률의 절댓값이 member_move_percent 이상이거나 범위 안 공시가 있는 경우다. 같은 종목은 가장 늦은
    장 마감의 등락률을 쓴다. (이슈 목록, 개수 상한 때문에 뺀 종목 수)를 돌려준다."""
    latest: dict[str, CloseRate] = {}
    for r in close_rates:
        if r.ticker not in latest or r.close_at > latest[r.ticker].close_at:
            latest[r.ticker] = r
    refs: dict[str, list[str]] = {}
    for f in sorted(filings, key=lambda f: f.accession_number):
        for ticker in f.tickers:
            refs.setdefault(ticker, []).append(f.accession_number)
    limit = _threshold(settings.member_move_percent)
    issues: list[MemberIssue] = []
    cut = 0
    for etf in etfs:
        if etf.etf is None:
            continue
        top = sorted(etf.etf.members, key=lambda m: m.rank)[:settings.member_rank_limit]
        found = []
        for m in top:
            if m.market != US_MARKET:
                continue
            rate = latest[m.symbol].rate if m.symbol in latest else None
            value = decimal_rate(rate)
            filing_refs = tuple(refs.get(m.symbol, ()))
            if (value is not None and abs(value) >= limit) or filing_refs:
                found.append(MemberIssue(etf.symbol, m.rank, m.symbol, m.name, rate, filing_refs))
        cut += max(0, len(found) - settings.member_issues_per_etf)
        issues.extend(found[:settings.member_issues_per_etf])
    return tuple(issues), cut


def notable_candidates(etfs: Iterable[EtfDay],
                       settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[NotableCandidate, ...]:
    """눈에 띄는 점 후보는 여러 ETF를 묶는 것만이다. 순서는 ① 업종이 같은데 반대로 움직임 ② 같은 방향으로 크게 움직인 묶음.
    ETF 하나의 구성 종목 이슈나 이유 없음은 ETF 줄이 이미 보여 주므로 후보로 만들지 않는다. 하루 등락이 없는 ETF는 뺀다."""
    rated = [(e, v) for e in etfs if e.has_day() for v in [decimal_rate(e.rate)] if v is not None]
    found: list[NotableCandidate] = []

    opposite = _threshold(settings.opposite_move_percent)
    for n, (a, va) in enumerate(rated):
        sector = a.etf.sector if a.etf is not None else None
        for b, vb in rated[n + 1:]:
            if sector and b.etf is not None and b.etf.sector == sector \
                    and abs(va) >= opposite and abs(vb) >= opposite and (va > 0) != (vb > 0):
                found.append(NotableCandidate(KIND_OPPOSITE, (a.symbol, b.symbol)))

    cluster = _threshold(settings.cluster_move_percent)
    for same_side in ([e.symbol for e, v in rated if v >= cluster], [e.symbol for e, v in rated if v <= -cluster]):
        if len(same_side) >= settings.cluster_min_size:
            found.append(NotableCandidate(KIND_CLUSTER, tuple(same_side)))
    return tuple(found)


def no_reason_count(etfs: Iterable[EtfDay], issues: Iterable[MemberIssue],
                    etf_articles: Iterable[EtfArticles], filings: Iterable[Filing]) -> int:
    """이유를 찾지 못한 ETF 수: 하루 등락이 있고 기사 응답을 받았으며 기사·공시·구성 종목 이슈가 모두 없는 ETF.
    기사 응답을 못 받은 ETF는 이유가 없는 것이 아니라 모르는 것이라 세지 않는다."""
    etf_articles = tuple(etf_articles)
    answered = {a.symbol for a in etf_articles if a.received}
    with_articles = {a.symbol for a in etf_articles if a.articles}
    with_issue = {i.etf for i in issues}
    filing_tickers = {t for f in filings for t in f.tickers}
    return sum(1 for e in etfs
               if e.has_day() and decimal_rate(e.rate) is not None and e.symbol in answered
               and e.symbol not in with_articles and e.symbol not in with_issue
               and not filing_tickers.intersection(e.tickers))
