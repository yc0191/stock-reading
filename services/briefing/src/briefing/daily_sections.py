"""데일리 판 입력(DailyInput)과 항목별 섹션 만들기. Kafka를 모르며 모은 자료를 인자로 받는다.

항목별 숫자·목록, 근거 번호, '자료 없음' 표시는 모두 여기서 코드가 쓴다. LLM 입력과 본문 글은 daily_report.py가 만든다.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.daily_inputs import EtfArticles, MemberIssue, NotableCandidate, is_before_open, decimal_rate
from briefing.daily_review import Review, ReviewInput, review_rows
from briefing.events import EDITION_MORNING, EDITION_POST_CLOSE
from briefing.llm_output import ticker_pattern
from briefing.open_flow import OPEN_SOURCE_TEXT, OpenFlow, signed
from briefing.price_series import DailyCloses, IntradaySeries
from briefing.move_figures import (KOSDAQ_NAME, KOSPI_NAME, USD_KRW_NAME, IndexFigure, display, exchange_rate_figure,
                                   index_figure, investor_flows, overseas_quotes, round_cent)
from briefing.sources import (
    BACKGROUND_LABEL,
    BACKGROUND_MISSING,
    BACKGROUND_NAMES,
    BACKGROUND_OK,
    BACKGROUND_SOURCE,
    BACKGROUND_TOO_LARGE,
    CLOSE_RATE_SOURCE,
    FILING_SOURCE,
    MARK_MISSING,
    MARK_NOT_APPLICABLE,
    MARK_PRESENT,
    MARK_TOO_LARGE,
    MARK_TRUNCATED,
    US_MARKET,
    Article,
    Background,
    CloseRate,
    EtfInfo,
    Filing,
)

NO_DATA = "자료 없음"
DAILY_INSUFFICIENT = "받은 자료만으로는 요약할 근거가 부족합니다."
DAILY_DISCLAIMER = "※ 투자 조언이 아니며, 위 내용은 받은 자료를 정리한 것일 뿐 원인을 단정하지 않습니다."

RELATION_HELD = "HELD"
RELATION_WATCH = "WATCH"
RELATION_TITLES = ((RELATION_HELD, "보유"), (RELATION_WATCH, "관심"))
RELATION_UNKNOWN_TITLE = "구분 없음"
CONCLUSION_PREFIX = "결론: "
CONCLUSION_NO_DATA = "등락 자료 없음"
MINUS = "\u2212"

EDITION_NAMES = {EDITION_MORNING: "아침 판", EDITION_POST_CLOSE: "마감 후 판"}

ITEM_US_INDEX = "미국 지수"
ITEM_EXCHANGE = "환율"
ITEM_US_STOCKS = "미국 종목 직전 장 등락"
ITEM_FILINGS = "범위 안 미국 공시"
NO_FILINGS_IN_RANGE = "범위 안에 접수된 공시 없음"
# 수집기가 공시를 못 받았다고 알린 판에서 저장된 공시가 0건이면, 없었다가 아니라 모른다고 쓴다.
FILINGS_NOT_COLLECTED = "공시 수집 실패"
# 아침 판의 시장종합(해외 시세·투자자별 순매수). Claude 입력에만 담고, 인용되면 근거 목록에 나온다.
ITEM_MARKET_SUMMARY = "시장종합"
ITEM_KR_TODAY = "그날 한국 증시"
# 아침 판의 전 거래일 한국 증시: 전 거래일 장 마감 뒤 받은 시장종합에서 코드가 꺼낸 코스피·코스닥 마감 등락률. 그날 아침에 받은
# 시장종합(ITEM_MARKET_SUMMARY)의 지수 등락률은 개장 직후 값이라 섞지 않는다.
ITEM_KR_PREV = "전 거래일 한국 증시"
ITEM_OPEN_FLOW = "ETF별 개장 흐름"
ITEM_ETFS = "보유 ETF별 하루 등락"
# 보유/관심으로 나뉠 때 글자판·HTML에 보이는 절 제목. LLM 입력과 자료 없음 목록은 ITEM_ETFS 이름을 그대로 쓴다.
ITEM_ETFS_SPLIT_TITLE = "보유·관심 ETF별 하루 등락"
# 마감 후 판이 ETF마다 모아 둔 기사, ETF에 담긴 미국 종목 중 이슈가 있는 종목. 글자판·HTML에는 아직 싣지 않는다.
ITEM_ETF_ARTICLES = "ETF별 기사"
ITEM_MEMBER_ISSUES = "미국 구성 종목 이슈"
# 근거가 부족해 다시 검색해 찾은 기사. 판 종류와 상관없이 다시 쓸 때만 맨 뒤에 붙는다.
ITEM_RESEARCH = "재검색 기사"

EDITION_ITEMS = {
    EDITION_MORNING: (ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_US_STOCKS, ITEM_FILINGS, ITEM_MARKET_SUMMARY, ITEM_KR_PREV,
                      ITEM_OPEN_FLOW, ITEM_ETF_ARTICLES),
    EDITION_POST_CLOSE: (ITEM_KR_TODAY, ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_ETFS, ITEM_ETF_ARTICLES, ITEM_FILINGS,
                         ITEM_MEMBER_ISSUES),
}
# 마감 후 판이 Claude 입력과 출처로만 쓰고 글자판·HTML 절로는 보여 주지 않는 항목
POST_CLOSE_HIDDEN_ITEMS = frozenset({ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_FILINGS})
EDITIONS_WITH_FILINGS = frozenset(e for e, items in EDITION_ITEMS.items() if ITEM_FILINGS in items)
EDITIONS_WITH_ETF_ARTICLES = frozenset(e for e, items in EDITION_ITEMS.items() if ITEM_ETF_ARTICLES in items)

# 마감 후 판 출처 목록의 자료 종류
KIND_NEWS = "기사"
KIND_FILING = "공시"
KIND_DAY = "일봉"
KIND_US_CLOSE = "해외 등락률"
KIND_MEMBER = "구성 종목 이슈"
KIND_OPEN_FLOW = "개장 시세"
NO_REASON_LABEL = "이유를 찾지 못한 ETF"
# 규칙 기반 본문은 ETF마다 이유를 쓰지 않으므로 '이유를 찾지 못한' 수 대신 코드가 실제로 세는 것을 그대로 쓴다.
NO_RELATED_LABEL = "관련 기사·공시·종목 이슈 없는 ETF"
KIND_BACKGROUND = "시장 자료"

# 코드가 붙이는 근거 번호의 머리글. 공시는 접수번호를 그대로 쓴다.
ETF_REF_PREFIX = "E"
CLOSE_RATE_REF_PREFIX = "U"
BACKGROUND_REF_PREFIX = "B"
MEMBER_ISSUE_REF_PREFIX = "M"
OPEN_FLOW_REF_PREFIX = "O"

# 본문에는 미국 티커를 쓰지 않는다(보유 종목이 드러남). LLM 입력에는 넣는다.
US_STOCK_LABEL = "미국 종목"
WEEKDAYS = "월화수목금토일"

# ---- 입력 ----

@dataclass(frozen=True)
class EtfDay:
    """보유 ETF 하나의 하루 등락. 값은 일봉 원문 그대로다. 일봉이 없으면 값 칸이 None이고 이유가 missing_reason에 있다.
    일봉이 없어도 시세처리기가 장중 시세로 계산한 대체값이 있으면 prev_close·rate가 그 값이고 intraday_at이 그 시세를 받은 시각이다."""

    symbol: str
    date: str | None
    close: str | None
    prev_close: str | None
    rate: str | None
    fetched_at: datetime | None
    tickers: tuple[str, ...]
    tickers_truncated: bool
    # 응답의 ETF 정보(선택 칸). 이전 형식 응답에는 없다.
    etf: EtfInfo | None = None
    # 값 칸이 빈 이유(데일리 자료 응답 v2의 missingReason). 값이 있으면 None이고, v1 응답에도 없다.
    missing_reason: str | None = None
    # 보유(HELD)·관심(WATCH) 구분. 멤버마다 달라 응답에는 없고, 멤버의 판을 짤 때 그 멤버의 포트폴리오 값으로 붙인다.
    relation: str | None = None
    # 아침 판의 개장 흐름(선택 칸). 응답에 칸이 없으면 None이고, 범위 안 시세가 없으면 present가 False다.
    open_flow: OpenFlow | None = None
    # 하루 등락 대체값(응답의 intradayFallback). 마감 후 판에서 판 범위 안에 받은 것만 남는다.
    intraday_at: datetime | None = None
    intraday_price: str | None = None
    # 그래프용 장중 흐름·기간 종가(응답의 선택 칸 intradaySeries·dailyCloses). 칸이 없거나 범위 밖이면 None이다.
    # 그래프에만 쓰고 LLM 입력에는 넣지 않는다.
    intraday_series: IntradaySeries | None = None
    daily_closes: DailyCloses | None = None

    def has_day(self) -> bool:
        if self.is_fallback():
            return True
        return self.missing_reason is None and None not in (self.date, self.close, self.prev_close, self.rate)

    def is_fallback(self) -> bool:
        """일봉 대신 장중 시세로 계산한 하루 등락인가."""
        return self.intraday_at is not None and None not in (self.prev_close, self.rate)

    def without_fallback(self) -> "EtfDay":
        """대체값을 버리고 일봉 없음으로 되돌린다."""
        return replace(self, prev_close=None, rate=None, intraday_at=None, intraday_price=None)

    def label(self) -> str:
        """본문에 쓰는 ETF 이름. 이름을 모르면 종목코드."""
        return self.etf.name if self.etf is not None and self.etf.name else self.symbol


@dataclass(frozen=True)
class DailyInput:
    """판 하나를 쓰는 데 모은 자료. None은 응답을 끝내 받지 못했다는 뜻이다."""

    job_id: str
    edition: str
    range_start: datetime
    as_of: datetime
    etfs: tuple[EtfDay, ...] | None
    etfs_truncated: bool
    close_rates: tuple[CloseRate, ...] | None
    background: tuple[Background, ...] | None
    filings: tuple[Filing, ...] | None
    filings_truncated: bool
    # 재검색으로 찾은 기사. 다시 쓸 때만 있다. 첫 작성의 입력 모양은 바꾸지 않는다.
    research_articles: tuple[Article, ...] = ()
    research_articles_truncated: bool = False
    # 마감 후 판만: ETF마다 받은 기사(None이면 요청하지 않은 판), 미국 구성 종목 이슈, 눈에 띄는 점 후보.
    etf_articles: tuple[EtfArticles, ...] | None = None
    member_issues: tuple[MemberIssue, ...] = ()
    notables: tuple[NotableCandidate, ...] = ()
    # 마감 후 판만: 같은 날 아침 예상과 그날 급변동을 작업 기록에서 읽은 값(None이면 읽지 않은 판).
    review: ReviewInput | None = None
    # 아침 판만: 개장 시세를 읽은 구간의 시작. 판 시각이 장 시작 전이라 읽지 않았으면 None.
    open_flow_start: datetime | None = None
    # 아침 판만: 전 거래일 장 마감 뒤 받은 시장종합. 조회를 끝내 받지 못했거나 구간 안에 마감 뒤 받은 값이 없으면 None.
    prev_market: Background | None = None
    # 수집기 판 시작 알림이 공시(sec-filings)를 못 받았다고 알렸는가.
    filings_not_collected: bool = False
    # 판을 만든 설정. 글자판을 다시 만드는 곳까지 설정을 넘기지 않아도 같은 장 시작 시각으로 판정하게 한다.
    settings: DailySettings = DEFAULT_DAILY_SETTINGS

    def tickers(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(t for etf in self.etfs or () for t in etf.tickers))

    def before_open(self, settings: DailySettings | None = None) -> bool:
        """장 시작 전에 만든 마감 후 판인가. 그날 등락이 아직 없어 ETF별 이유를 쓰지 않는다."""
        return self.edition == EDITION_POST_CLOSE and is_before_open(self.etfs or (), self.as_of, settings or self.settings)

    def reviews(self) -> tuple[Review, ...]:
        """아침 예상과 그날 실제 등락의 대조. 장 시작 전 판이거나 아침 예상 기록이 없으면 비어 있다."""
        if self.review is None or self.before_open():
            return ()
        return review_rows(self.etfs or (), self.review.calls, self.settings.flat_band_percent)

    def member_us_symbols(self) -> frozenset[str]:
        """ETF 정보의 구성 종목 중 미국 종목의 티커."""
        return frozenset(t for etf in self.etfs or () if etf.etf for t in etf.etf.us_tickers())


@dataclass(frozen=True)
class Entry:
    """근거 번호가 붙은 자료 하나. llm은 LLM 입력에 넣는 값, describe는 본문 근거 목록에 쓰는 설명이다."""

    ref: str
    llm: dict[str, Any]
    describe: str
    # 마감 후 판 출처 목록용: 자료 종류와 원문 링크(기사·공시만)
    kind: str = ""
    url: str | None = None
    # 기사만: 같은 사건 기사 묶기에 쓰는 발행 시각
    published_at: datetime | None = None


@dataclass(frozen=True)
class Section:
    name: str
    status: str
    lines: tuple[str, ...]
    entries: tuple[Entry, ...] = ()
    # 항목 안에서 일부만 빈 경우 빈 것마다 한 줄("<이름>, <이유>"). 항목 전체가 비면 status가 없음이다.
    gaps: tuple[str, ...] = ()
    # gaps와 같은 자리의 LLM 입력(어느 대상의 무엇이 왜 비었는지)
    gaps_llm: tuple[dict[str, Any], ...] = ()
    # 보유/관심으로 나눠 보여 줄 때 묶음 이름과 그 묶음의 줄. 구분을 모르면 비어 있다(lines 한 묶음).
    groups: tuple[tuple[str, tuple[str, ...]], ...] = ()
    # True면 Claude 입력에는 들어가지만 글자판·HTML 항목으로는 보여 주지 않는다.
    hidden: bool = False

    @property
    def missing(self) -> bool:
        return self.status in (MARK_MISSING, MARK_TOO_LARGE)


# ---- 항목 ----

def _at(value: datetime) -> str:
    return value.astimezone(KST).isoformat(timespec="seconds")


def _short(value: datetime) -> str:
    local = value.astimezone(KST)
    return f"{local:%m-%d}({WEEKDAYS[local.weekday()]}) {local:%H:%M}"


def _minutes_before(at: datetime, as_of: datetime) -> int:
    return int((as_of - at).total_seconds() // 60)


def _hours_before(at: datetime, as_of: datetime) -> float:
    return round((as_of - at).total_seconds() / 3600, 1)


def _rate(text: str | None) -> float | None:
    """등락률 문자열의 숫자. 숫자가 아니거나 nan·inf처럼 유한하지 않으면 자료 없음(None)이다."""
    try:
        value = float(text) if text is not None else None
    except ValueError:
        return None
    return value if value is None or math.isfinite(value) else None


def _shown(rate: float) -> float:
    """결론에 표시하는 자리(소수 한 자리)로 반올림한 값. 가장 크게 오름·내림 판정은 보이는 값을 기준으로 한다."""
    # 이진 float 의 .1f 는 0.25 를 0.2 로 내리므로 십진 반올림(0.25→0.3, -0.15→-0.2)으로 한다.
    return float(Decimal(str(rate)).quantize(Decimal("0.1"), ROUND_HALF_UP))


def _missing(name: str, why: str | None = None) -> Section:
    return Section(name, MARK_MISSING, (f"{NO_DATA} ({why})" if why else NO_DATA,))


def _find_background(i: DailyInput, api_type: str, target: str | None) -> Background | None:
    for item in i.background or ():
        if item.api_type == api_type and item.target == target:
            return item
    return None


def _background_entry(ref: str, item: Background, as_of: datetime, with_rates: bool) -> Entry:
    data: dict[str, Any] = {
        "ref": ref,
        "category": BACKGROUND_LABEL,
        "kind": BACKGROUND_NAMES.get(item.api_type, item.api_type),
        "target": item.target,
        "status": MARK_PRESENT if item.status == BACKGROUND_OK else MARK_TOO_LARGE,
        "receivedAt": _at(item.fetched_at),
        "minutesBeforeAsOf": _minutes_before(item.fetched_at, as_of),
    }
    if item.status == BACKGROUND_OK:
        data["response"] = item.response
    if with_rates:
        data["indexRates"] = [
            {"code": r.code, "name": r.name, "businessDay": r.business_day, "close": r.close,
             "prevBusinessDay": r.prev_business_day if r.prev_business_day is not None else MARK_MISSING,
             "prevClose": r.prev_close if r.prev_close is not None else MARK_MISSING,
             "rate": r.rate if r.rate is not None else MARK_MISSING}
            for r in item.index_rates
        ]
    name = BACKGROUND_NAMES.get(item.api_type, item.api_type)
    return Entry(ref, data, f"{name} — {BACKGROUND_SOURCE}, {_short(item.fetched_at)} 받음", KIND_BACKGROUND)


def _raw_background_section(name: str, i: DailyInput, api_type: str, refs: Callable[[], str],
                            with_usd_krw: bool = False, with_market_figures: bool = False) -> Section:
    """환율종합·시장종합. 원문은 해석하지 않으므로 본문에는 받았는지만 쓰고 숫자는 LLM 요약에 맡긴다.
    with_usd_krw면 환율종합 원문에서 꺼낸 원/달러 값을, with_market_figures면 시장종합 원문에서 꺼낸 해외 시세와
    투자자별 순매수를 근거에 덧붙인다(원문은 그대로 둔다)."""
    if i.background is None:
        return _missing(name)
    item = _find_background(i, api_type, None)
    label = BACKGROUND_NAMES.get(api_type, api_type)
    if item is None or item.status == BACKGROUND_MISSING:
        return _missing(name, "범위 안에 받은 값이 없음")
    if item.status == BACKGROUND_TOO_LARGE:
        return Section(name, MARK_TOO_LARGE, (f"{NO_DATA} ({label} 원문이 너무 커서 빠짐)",))
    entry = _background_entry(refs(), item, i.as_of, with_rates=False)
    if with_usd_krw:
        entry = _with_usd_krw(entry, item)
    if with_market_figures:
        entry = _with_market_figures(entry, item)
    return Section(name, MARK_PRESENT, (f"{label} {_short(item.fetched_at)} 받은 값 있음",), (entry,))


def _with_market_figures(entry: Entry, item: Background) -> Entry:
    """시장종합 원문의 칸 이름이 약어라 해외 시세(원유·반도체지수·선물 등)와 투자자별 순매수를 따로 꺼내 준다.
    둘 다 없으면 그대로다."""
    quotes, flows = overseas_quotes((item,)), investor_flows((item,))
    if not quotes and not flows:
        return entry
    llm = {**entry.llm,
           "overseas": [{"name": q.name, "price": _plain(q.price), "ratePercent": _plain(q.rate), "at": q.at}
                        for q in quotes],
           "investorNetBuy": [{"investor": f.investor, "kospi": _plain(f.kospi), "kosdaq": _plain(f.kosdaq)}
                              for f in flows]}
    describe = (f"{BACKGROUND_NAMES.get(item.api_type, item.api_type)}(해외 시세·투자자별 순매수) — "
                f"{BACKGROUND_SOURCE}, {_short(item.fetched_at)} 받음")
    return replace(entry, llm=llm, describe=describe)


def _plain(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _with_usd_krw(entry: Entry, item: Background) -> Entry:
    """원문 칸 이름이 약어라 Claude가 읽기 쉽게 원/달러 값을 따로 주고, 출처 줄에도 숫자를 보인다. 행이 없으면 그대로다."""
    fx = exchange_rate_figure((item,))
    if fx is None:
        return entry
    values = {"value": display(fx.value), "change": display(fx.change, sign=True),
              "ratePercent": display(fx.rate, sign=True)}
    describe = (f"{BACKGROUND_NAMES.get(item.api_type, item.api_type)} {USD_KRW_NAME} {values['value']}"
                f"(전일대비 {values['change']}, {values['ratePercent']}%) — {BACKGROUND_SOURCE}, {_short(item.fetched_at)} 받음")
    return replace(entry, llm={**entry.llm, "usdKrw": values}, describe=describe)


def kr_prev_line(item: Background) -> str:
    """전 거래일 한국 증시 줄: '코스피 −2.14% · 코스닥 +0.35% (11-20(금) 마감)'. 원문에서 못 꺼낸 지수는 자료 없음이다."""
    def part(label: str, figure: IndexFigure | None) -> str:
        if figure is None or figure.rate is None:
            return f"{label} {NO_DATA}"
        rate = round_cent(figure.rate)
        return f"{label} {'+' if rate >= 0 else MINUS}{abs(rate)}%"

    local = item.fetched_at.astimezone(KST)
    return (f"{part('코스피', index_figure((item,), KOSPI_NAME))} · {part('코스닥', index_figure((item,), KOSDAQ_NAME))} "
            f"({local:%m-%d}({WEEKDAYS[local.weekday()]}) 마감)")


def _index_llm(figure: IndexFigure | None) -> dict[str, str] | str:
    if figure is None:
        return MARK_MISSING
    return {"value": display(figure.value), "change": display(figure.change, sign=True),
            "ratePercent": display(figure.rate, sign=True)}


def _kr_prev_section(i: DailyInput, refs: Callable[[], str]) -> Section:
    """아침 판의 전 거래일 한국 증시. 원문 대신 코드가 꺼낸 지수와 투자자별 순매수만 Claude에 준다(그날 아침 시장종합 원문이
    이미 들어가므로 같은 모양의 원문을 두 번 넣지 않는다)."""
    item = i.prev_market
    if item is None:
        return _missing(ITEM_KR_PREV, "전 거래일 장 마감 뒤 받은 값이 없음")
    kospi, kosdaq = index_figure((item,), KOSPI_NAME), index_figure((item,), KOSDAQ_NAME)
    if all(f is None or f.rate is None for f in (kospi, kosdaq)):
        return _missing(ITEM_KR_PREV, "시장종합 원문에서 지수를 찾지 못함")
    line = kr_prev_line(item)
    data: dict[str, Any] = {
        "ref": refs(),
        "category": BACKGROUND_LABEL,
        "kind": BACKGROUND_NAMES[item.api_type],
        "closeDate": f"{item.fetched_at.astimezone(KST):%Y-%m-%d}",
        "receivedAt": _at(item.fetched_at),
        "minutesBeforeAsOf": _minutes_before(item.fetched_at, i.as_of),
        "kospi": _index_llm(kospi),
        "kosdaq": _index_llm(kosdaq),
    }
    flows = investor_flows((item,))
    if flows:
        data["investorNetBuy"] = [{"investor": f.investor, "kospi": _plain(f.kospi), "kosdaq": _plain(f.kosdaq)}
                                  for f in flows]
    describe = f"{BACKGROUND_NAMES[item.api_type]}(전 거래일 마감) {line} — {BACKGROUND_SOURCE}, {_short(item.fetched_at)} 받음"
    return Section(ITEM_KR_PREV, MARK_PRESENT, (line,), (Entry(data["ref"], data, describe, KIND_BACKGROUND),))


def _us_index_section(i: DailyInput, settings: DailySettings, refs: Callable[[], str]) -> Section:
    if i.background is None:
        return _missing(ITEM_US_INDEX)
    item = _find_background(i, "world-index", settings.us_index_target)
    if item is None or item.status == BACKGROUND_MISSING or not item.index_rates:
        return _missing(ITEM_US_INDEX, "범위 안에 받은 값이 없음")
    lines = []
    for r in item.index_rates:
        if r.rate is None:
            lines.append(f"{r.name}({r.code}) 등락률 {NO_DATA} ({r.business_day} 종가 {r.close}, 직전 영업일 종가 없음)")
        else:
            lines.append(f"{r.name}({r.code}) {_signed_rate(r.rate)}% ({r.business_day} 종가 {r.close}, "
                         f"직전 {r.prev_business_day} {r.prev_close})")
    if all(r.rate is None for r in item.index_rates):
        return Section(ITEM_US_INDEX, MARK_MISSING, tuple(lines))
    return Section(ITEM_US_INDEX, MARK_PRESENT, tuple(lines),
                   (_background_entry(refs(), item, i.as_of, with_rates=True),))


def _us_stocks_section(i: DailyInput, refs: Callable[[], str]) -> Section:
    if i.etfs is None or i.close_rates is None:
        return _missing(ITEM_US_STOCKS)
    if not i.tickers():
        return Section(ITEM_US_STOCKS, MARK_NOT_APPLICABLE, (f"{MARK_NOT_APPLICABLE} (보유 ETF에 미국 종목이 없음)",))
    rated = [(r, _rate(r.rate)) for r in i.close_rates]
    values = [v for _, v in rated if v is not None]
    if not values:
        return _missing(ITEM_US_STOCKS, f"{US_STOCK_LABEL} {len(i.tickers())}개의 직전 장 등락률을 찾지 못함")
    # 등락률 집계는 코드가 한다. 본문에는 티커를 쓰지 않고 숫자만 쓴다.
    up = sum(1 for v in values if v > 0)
    down = sum(1 for v in values if v < 0)
    lines = [f"{US_STOCK_LABEL} {len(values)}개(전체 {len(i.tickers())}개 중): 오름 {up}, 내림 {down}, "
             f"보합 {len(values) - up - down}, 평균 {sum(values) / len(values):+.2f}%",
             f"가장 큰 상승 {max(values):+.2f}%, 가장 큰 하락 {min(values):+.2f}%"]
    entries = []
    for r, _ in rated:
        ref = refs()
        entries.append(Entry(ref, {"ref": ref, "ticker": r.ticker, "date": r.date, "rate": r.rate,
                                   "hoursBeforeAsOf": _hours_before(r.close_at, i.as_of)},
                             f"{US_STOCK_LABEL} 직전 정규장 등락률 {r.rate}% ({r.date}) — {CLOSE_RATE_SOURCE}", KIND_US_CLOSE))
    status = MARK_TRUNCATED if any(e.tickers_truncated for e in i.etfs) else MARK_PRESENT
    return Section(ITEM_US_STOCKS, status, tuple(lines), tuple(entries))


# 미국 수시 공시(8-K) 항목 번호의 뜻. 첨부 서류 목록(9.01)은 다른 항목에 딸려 오므로 뜻을 붙이지 않는다.
FILING_ITEM_NAMES = {
    "1.01": "중요 계약 체결", "1.02": "중요 계약 종료", "1.03": "파산·회생 절차", "1.05": "중요 사이버 보안 사고",
    "2.01": "자산 인수·처분 완료", "2.02": "실적 발표", "2.03": "새 금융 채무", "2.04": "금융 채무 조기 상환 사유",
    "2.05": "구조조정 비용", "2.06": "중요 자산 손상", "3.01": "상장 유지 기준 미달·상장 이전",
    "3.02": "미등록 지분 증권 매각", "3.03": "증권 보유자 권리 변경", "4.01": "감사인 변경",
    "4.02": "과거 재무제표 신뢰 불가", "5.01": "지배권 변경", "5.02": "임원·이사 변경", "5.03": "정관·회계연도 변경",
    "5.04": "직원 연금 거래 일시 중지", "5.05": "윤리강령 변경", "5.06": "페이퍼컴퍼니 지위 변경",
    "5.07": "주주총회 안건 결과", "5.08": "주주 추천 이사 후보", "7.01": "공정 공시(설명 자료)", "8.01": "기타 중요 사항",
}


def filing_item_names(items: tuple[str, ...]) -> list[str]:
    """공시 항목 번호의 뜻. 모르는 번호와 첨부 서류(9.01)는 뺀다."""
    return [FILING_ITEM_NAMES[item.strip()] for item in items if item.strip() in FILING_ITEM_NAMES]


def _us_member_names(i: DailyInput) -> dict[str, str]:
    """ETF 정보에 담긴 미국 구성 종목의 티커별 이름. 공시에 회사 이름을 붙이는 데 쓴다."""
    return {m.symbol: m.name for e in i.etfs or () if e.etf is not None for m in e.etf.members
            if m.market == US_MARKET and m.name}


def _filings_section(i: DailyInput) -> Section:
    if i.etfs is not None and not i.tickers():
        return Section(ITEM_FILINGS, MARK_NOT_APPLICABLE, (f"{MARK_NOT_APPLICABLE} (보유 ETF에 미국 종목이 없음)",))
    if i.filings is None:
        return _missing(ITEM_FILINGS)
    if not i.filings:
        if i.filings_not_collected:
            return _missing(ITEM_FILINGS, FILINGS_NOT_COLLECTED)
        return Section(ITEM_FILINGS, MARK_PRESENT, (NO_FILINGS_IN_RANGE,))
    names = _us_member_names(i)
    lines, entries = [], []
    for f in sorted(i.filings, key=lambda f: f.accepted_at, reverse=True):
        companies = [names[t] for t in f.tickers if t in names]
        item_names = filing_item_names(f.items)
        title = f"{', '.join(companies) or US_STOCK_LABEL} {f.form} 공시"
        if item_names:
            title += f"({', '.join(item_names)})"
        lines.append(f"{title} ({_short(f.accepted_at)} 접수)")
        data: dict[str, Any] = {"ref": f.accession_number, "tickers": list(f.tickers), "form": f.form,
                                "items": list(f.items), "acceptedAt": _at(f.accepted_at),
                                "hoursBeforeAsOf": _hours_before(f.accepted_at, i.as_of)}
        # 이름과 항목 뜻은 아는 것만 싣는다. 모르는 공시의 입력 모양은 바꾸지 않는다.
        if companies:
            data["companyNames"] = companies
        if item_names:
            data["itemNames"] = item_names
        entries.append(Entry(f.accession_number, data,
                             f"{title} — {FILING_SOURCE}, {_short(f.accepted_at)} 접수", KIND_FILING, f.url))
    status = MARK_TRUNCATED if i.filings_truncated else MARK_PRESENT
    if i.filings_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    return Section(ITEM_FILINGS, status, tuple(lines), tuple(entries))


ETF_MISSING_REASONS = {
    "no-daily-price-in-range": "범위 안에 받은 일봉이 없음",
    "daily-price-unreadable": "일봉 원문을 읽지 못함",
}
ETF_MISSING_UNKNOWN = "일봉 값이 비어 있고 이유를 받지 못함"


def _etf_missing_reason(etf: EtfDay) -> str:
    return ETF_MISSING_REASONS.get(etf.missing_reason or "", ETF_MISSING_UNKNOWN)


def etf_groups(etfs: tuple[EtfDay, ...]) -> tuple[tuple[str, tuple[EtfDay, ...]], ...]:
    """보유/관심 묶음. 구분을 아는 ETF가 하나도 없으면 묶음 이름 없이 한 묶음("")이다."""
    if not any(e.relation in dict(RELATION_TITLES) for e in etfs):
        return (("", tuple(etfs)),) if etfs else ()
    groups = [(title, tuple(e for e in etfs if e.relation == key)) for key, title in RELATION_TITLES]
    groups.append((RELATION_UNKNOWN_TITLE, tuple(e for e in etfs if e.relation not in dict(RELATION_TITLES))))
    return tuple((title, members) for title, members in groups if members)


def etf_name(etf: EtfDay) -> str:
    """본문 줄에 쓰는 ETF 표기. 이름을 알면 이름(종목코드)."""
    return etf.symbol if etf.label() == etf.symbol else f"{etf.label()}({etf.symbol})"


def fallback_note(etf: EtfDay) -> str:
    """대체값 뒤에 붙이는 기준 표시. 대체값이 아니면 빈 글."""
    if not etf.is_fallback():
        return ""
    return f"(장중 시세 {etf.intraday_at.astimezone(KST):%H:%M} 기준, 일봉 없음)"


def etf_line(etf: EtfDay) -> str:
    if not etf.has_day():
        return f"{NO_DATA}: {etf.label()}, {_etf_missing_reason(etf)}"
    rate = _rate(etf.rate)
    shown = f"+{etf.rate}" if rate is not None and rate > 0 and not etf.rate.startswith("+") else etf.rate
    if etf.is_fallback():
        return f"{etf_name(etf)} {shown}% {fallback_note(etf)}"
    return f"{etf_name(etf)} {shown}% (종가 {etf.close}, 전일 {etf.prev_close}, {etf.date})"


def _etfs_section(i: DailyInput, refs: Callable[[], str]) -> Section:
    if not i.etfs:
        return _missing(ITEM_ETFS, None if i.etfs is None else "받은 ETF가 없음")
    lines, entries, gaps, gaps_llm = [], [], [], []
    for etf in i.etfs:
        if not etf.has_day():
            reason = _etf_missing_reason(etf)
            gaps.append(f"{etf.label()}, {reason}")
            gaps_llm.append({"symbol": etf.symbol, "name": etf.label(), "status": MARK_MISSING, "reason": reason})
            continue
        ref = refs()
        if etf.is_fallback():
            note = fallback_note(etf)
            entries.append(Entry(ref, {"ref": ref, "symbol": etf.symbol, "basis": note, "price": etf.intraday_price,
                                       "prevClose": etf.prev_close, "rate": etf.rate,
                                       "receivedAt": _at(etf.intraday_at), "tickers": list(etf.tickers)},
                                 f"{etf.symbol} 하루 등락 {etf.rate}% {note} — {BACKGROUND_SOURCE}", KIND_DAY))
            continue
        entries.append(Entry(ref, {"ref": ref, "symbol": etf.symbol, "date": etf.date, "close": etf.close,
                                   "prevClose": etf.prev_close, "rate": etf.rate, "receivedAt": _at(etf.fetched_at),
                                   "tickers": list(etf.tickers)},
                             f"{etf.symbol} 하루 등락 {etf.rate}% ({etf.date}) — {BACKGROUND_SOURCE}", KIND_DAY))
    named = etf_groups(i.etfs)
    # 구분이 없으면 받은 순서 그대로 한 묶음이고, 있으면 묶음 안에서 받은 순서를 지킨다.
    lines = [etf_line(e) for _, members in named for e in members]
    groups = tuple((title, tuple(etf_line(e) for e in members)) for title, members in named if title)
    if i.etfs_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    if not entries:
        # 모든 ETF가 비면 항목 전체가 자료 없음이다.
        return Section(ITEM_ETFS, MARK_MISSING, tuple(lines), (), tuple(gaps), tuple(gaps_llm), groups)
    return Section(ITEM_ETFS, MARK_TRUNCATED if i.etfs_truncated else MARK_PRESENT, tuple(lines), tuple(entries),
                   tuple(gaps), tuple(gaps_llm), groups)


OPEN_FLOW_NOT_OPEN = "장 시작 전이라 개장 시세가 없음"
OPEN_FLOW_NO_QUOTE = "범위 안 시세 없음"
OPEN_FLOW_NO_RESPONSE = "응답에 개장 흐름 칸이 없음"


def _open_flow_line(etf: EtfDay, flow: OpenFlow) -> str:
    start = flow.start_at.astimezone(KST) if flow.start_at else None
    end = flow.end_at.astimezone(KST) if flow.end_at else None
    parts = [f"{etf_name(etf)} 시가 {signed(flow.open_rate)}"]
    if flow.open is not None:
        parts[0] += f" (전일 종가 {flow.prev_close or NO_DATA}, 시가 {flow.open}"
        parts[0] += f", {OPEN_SOURCE_TEXT.get(flow.open_source or '', '출처 모름')})"
    if start is not None and end is not None:
        parts.append(f"{start:%H:%M}~{end:%H:%M} {signed(flow.flow_rate)}")
    parts.append(f"{end:%H:%M} 현재가 전일 종가 대비 {signed(flow.rate_vs_prev_close)}" if end else "")
    return " · ".join(p for p in parts if p)


def _open_flow_section(i: DailyInput, refs: Callable[[], str]) -> Section:
    """아침 판 ETF별 개장 흐름. 시세처리기가 저장한 시세로 코드가 계산한 값이며, 없는 ETF는 자료 없음 줄로 둔다."""
    if not i.etfs:
        return _missing(ITEM_OPEN_FLOW, None if i.etfs is None else "받은 ETF가 없음")
    if i.open_flow_start is None:
        return _missing(ITEM_OPEN_FLOW, OPEN_FLOW_NOT_OPEN)
    lines, entries, gaps, gaps_llm = [], [], [], []
    for etf in i.etfs:
        flow = etf.open_flow
        if flow is None or not flow.present:
            reason = OPEN_FLOW_NO_RESPONSE if flow is None else OPEN_FLOW_NO_QUOTE
            gaps.append(f"{etf.label()}, {reason}")
            gaps_llm.append({"symbol": etf.symbol, "name": etf.label(), "status": MARK_MISSING, "reason": reason})
            continue
        ref = refs()
        line = _open_flow_line(etf, flow)
        lines.append(line)
        entries.append(Entry(ref, {
            "ref": ref, "symbol": etf.symbol, "prevClose": flow.prev_close, "open": flow.open,
            "openSource": flow.open_source, "openRatePercent": flow.open_rate,
            "startAt": _at(flow.start_at), "startPrice": flow.start_price, "endAt": _at(flow.end_at),
            "endPrice": flow.end_price, "flowRatePercent": flow.flow_rate,
            "rateVsPrevClosePercent": flow.rate_vs_prev_close,
            "minutesBeforeAsOf": _minutes_before(flow.end_at, i.as_of)},
            f"{etf.symbol} 개장 흐름 시가 {signed(flow.open_rate)}, 현재가 전일 종가 대비 "
            f"{signed(flow.rate_vs_prev_close)} ({_at(flow.end_at)}) — {BACKGROUND_SOURCE}", KIND_OPEN_FLOW))
    if not entries:
        return Section(ITEM_OPEN_FLOW, MARK_MISSING, (NO_DATA,), (), tuple(gaps), tuple(gaps_llm))
    return Section(ITEM_OPEN_FLOW, MARK_PRESENT, tuple(lines), tuple(entries), tuple(gaps), tuple(gaps_llm))


ARTICLES_NO_RESPONSE = "기사 응답을 받지 못함"


def _etf_articles_section(i: DailyInput) -> Section:
    """ETF마다 모아 둔 기사. 같은 기사가 시장 전체 기사로 여러 ETF에 오면 한 번만 담고 etf 칸에 ETF 목록을 둔다."""
    if i.etf_articles is None:
        return Section(ITEM_ETF_ARTICLES, MARK_MISSING, (NO_DATA,), hidden=True)
    names = {e.symbol: e.label() for e in i.etfs or ()}
    lines, entries, gaps, gaps_llm = [], [], [], []
    by_id: dict[str, list[str]] = {}
    for group in i.etf_articles:
        if not group.received:
            gaps.append(f"{names.get(group.symbol, group.symbol)}, {ARTICLES_NO_RESPONSE}")
            gaps_llm.append({"symbol": group.symbol, "name": names.get(group.symbol, group.symbol),
                             "status": MARK_MISSING, "reason": ARTICLES_NO_RESPONSE})
            continue
        for a in group.articles:
            seen = by_id.get(a.article_id)
            if seen is not None:
                seen.append(group.symbol)
                continue
            by_id[a.article_id] = etfs_of = [group.symbol]
            lines.append(f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행")
            entries.append(Entry(a.article_id,
                                 {"ref": a.article_id, "etf": etfs_of, "articleId": a.article_id,
                                  "title": a.title, "summary": a.summary, "source": a.publisher,
                                  "publishedAt": _at(a.published_at),
                                  "minutesBeforeAsOf": _minutes_before(a.published_at, i.as_of)},
                                 f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행 {a.url}", KIND_NEWS, a.url,
                                 a.published_at))
    truncated = any(g.truncated for g in i.etf_articles)
    if not entries and gaps:
        return Section(ITEM_ETF_ARTICLES, MARK_MISSING, tuple(lines), (), tuple(gaps), tuple(gaps_llm), hidden=True)
    if not entries:
        return Section(ITEM_ETF_ARTICLES, MARK_PRESENT, ("범위 안에 모아 둔 기사 없음",), hidden=True)
    return Section(ITEM_ETF_ARTICLES, MARK_TRUNCATED if truncated else MARK_PRESENT, tuple(lines), tuple(entries),
                   tuple(gaps), tuple(gaps_llm), hidden=True)


def _member_issues_section(i: DailyInput, refs: Callable[[], str]) -> Section:
    """미국 구성 종목 이슈. 티커는 Claude 입력에만 담고, 근거 설명에는 종목 이름(모르면 '미국 종목')을 쓴다."""
    if i.etfs is None:
        return Section(ITEM_MEMBER_ISSUES, MARK_MISSING, (NO_DATA,), hidden=True)
    if not i.member_issues:
        return Section(ITEM_MEMBER_ISSUES, MARK_PRESENT, ("기준에 맞는 미국 구성 종목 없음",), hidden=True)
    lines, entries = [], []
    for issue in i.member_issues:
        ref = refs()
        data: dict[str, Any] = {"ref": ref, "etf": issue.etf, "rank": issue.rank, "ticker": issue.ticker,
                                "rate": issue.rate if issue.rate is not None else MARK_MISSING,
                                "filingRefs": list(issue.filing_refs)}
        if issue.name:
            data["name"] = issue.name
        reasons = ([f"직전 장 등락률 {_signed_rate(issue.rate)}%"] if issue.rate is not None else []) \
            + (["범위 안 공시"] if issue.filing_refs else [])
        what = f"비중 {issue.rank}위 {member_label(issue)} {', '.join(reasons)}"
        entries.append(Entry(ref, data, f"{what} — {CLOSE_RATE_SOURCE}", KIND_MEMBER))
        lines.append(what)
    return Section(ITEM_MEMBER_ISSUES, MARK_PRESENT, tuple(lines), tuple(entries), hidden=True)


def _research_section(i: DailyInput) -> Section:
    lines, entries = [], []
    for a in sorted(i.research_articles, key=lambda a: a.published_at, reverse=True):
        lines.append(f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행")
        entries.append(Entry(a.article_id,
                             {"ref": a.article_id, "articleId": a.article_id, "title": a.title,
                              "summary": a.summary, "source": a.publisher, "publishedAt": _at(a.published_at),
                              "minutesBeforeAsOf": _minutes_before(a.published_at, i.as_of)},
                             f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행 {a.url}", KIND_NEWS, a.url,
                             a.published_at))
    if i.research_articles_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    status = MARK_TRUNCATED if i.research_articles_truncated else MARK_PRESENT
    return Section(ITEM_RESEARCH, status, tuple(lines), tuple(entries))


def build_sections(i: DailyInput, settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[Section, ...]:
    """판의 항목을 차례대로 만든다. 근거 번호는 이 순서로 붙는다."""
    counters = {ETF_REF_PREFIX: 0, CLOSE_RATE_REF_PREFIX: 0, BACKGROUND_REF_PREFIX: 0, MEMBER_ISSUE_REF_PREFIX: 0,
                OPEN_FLOW_REF_PREFIX: 0}

    def next_ref(prefix: str) -> Callable[[], str]:
        def make() -> str:
            counters[prefix] += 1
            return f"{prefix}{counters[prefix]}"
        return make

    background_ref = next_ref(BACKGROUND_REF_PREFIX)
    builders = {
        ITEM_US_INDEX: lambda: _us_index_section(i, settings, background_ref),
        ITEM_EXCHANGE: lambda: _raw_background_section(ITEM_EXCHANGE, i, "exchange-rate-summary", background_ref,
                                                       with_usd_krw=True),
        ITEM_US_STOCKS: lambda: _us_stocks_section(i, next_ref(CLOSE_RATE_REF_PREFIX)),
        ITEM_FILINGS: lambda: _filings_section(i),
        ITEM_MARKET_SUMMARY: lambda: _raw_background_section(ITEM_MARKET_SUMMARY, i, "market-summary", background_ref,
                                                             with_market_figures=True),
        ITEM_KR_TODAY: lambda: _raw_background_section(ITEM_KR_TODAY, i, "market-summary", background_ref),
        ITEM_KR_PREV: lambda: _kr_prev_section(i, background_ref),
        ITEM_ETFS: lambda: _etfs_section(i, next_ref(ETF_REF_PREFIX)),
        ITEM_OPEN_FLOW: lambda: _open_flow_section(i, next_ref(OPEN_FLOW_REF_PREFIX)),
        ITEM_ETF_ARTICLES: lambda: _etf_articles_section(i),
        ITEM_MEMBER_ISSUES: lambda: _member_issues_section(i, next_ref(MEMBER_ISSUE_REF_PREFIX)),
    }
    sections = [builders[name]() for name in EDITION_ITEMS[i.edition]]
    if i.edition == EDITION_POST_CLOSE:
        # 마감 후 판 공시·미국 지수·환율은 Claude 입력에만 담고, 인용되면 출처 목록에 나온다.
        sections = [replace(s, hidden=True) if s.name in POST_CLOSE_HIDDEN_ITEMS else s for s in sections]
    if i.research_articles:
        sections.append(_research_section(i))
    return tuple(sections)


def missing_items(sections: tuple[Section, ...]) -> list[str]:
    """빠진 항목 이름. 항목 안에서 빈 대상이 있으면 '<항목>: <대상>, <이유>'를 이어 붙인다."""
    items: list[str] = []
    for s in sections:
        # 빈 대상이 있으면 대상별 줄이 이미 항목을 가리키므로 항목 이름만 따로 싣지 않는다.
        if s.missing and not s.gaps:
            items.append(s.name)
        items.extend(f"{s.name}: {gap}" for gap in s.gaps)
    return items


def has_evidence(sections: tuple[Section, ...]) -> bool:
    return any(s.entries for s in sections)


def _signed_rate(rate: str) -> str:
    value = decimal_rate(rate)
    return f"+{rate}" if value is not None and value > 0 and not rate.startswith("+") else rate


def member_label(issue: MemberIssue) -> str:
    """본문에 쓰는 구성 종목 표기. 종목 이름을 모르거나 이름에 티커가 들어 있으면 '미국 종목'이다(티커는 본문에 쓰지 않는다)."""
    name = (issue.name or "").strip()
    pattern = ticker_pattern(frozenset({issue.ticker}))
    return name if name and not (pattern is not None and pattern.search(name)) else US_STOCK_LABEL


def member_line(issue: MemberIssue) -> str:
    """ETF 줄 아래 '↳' 줄. 비중 순위, 종목 이름, 직전 장 등락률, 공시 여부를 쓰고 티커는 쓰지 않는다."""
    parts = []
    if issue.rate is not None:
        parts.append(f"직전 장 {_signed_rate(issue.rate)}%")
    if issue.filing_refs:
        parts.append("범위 안 공시")
    return f"↳ 비중 {issue.rank}위 {member_label(issue)} " + " · ".join(parts)
