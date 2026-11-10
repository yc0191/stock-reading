"""데일리 판(아침 판, 마감 후 판) 본문 만들기. Kafka를 모르며 모은 자료(DailyInput)를 인자로 받는다.

항목별 숫자·목록과 '자료 없음' 표시, 머리말, 면책 문구는 코드가 쓴다. LLM은 코드가 붙인 근거 번호를 인용한
요약 문장만 만들고, LLM이 끝내 실패하면 요약 없이 코드가 쓴 숫자·목록 본문(규칙 기반)으로 나간다.
"""
from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Mapping

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.daily_inputs import (
    PRE_OPEN_NOTICE,
    EtfArticles,
    MemberIssue,
    NotableCandidate,
    is_before_open,
    no_reason_count,
    decimal_rate,
)
from briefing.daily_prompts import (
    DIRECTION_UNSURE,
    DIRECTIONS,
    NO_REASON_STATEMENT,
    OUTPUT_SCHEMA,
    OUTPUT_SCHEMA_MORNING,
    OUTPUT_SCHEMA_POST_CLOSE,
    REWRITE_ADDENDUM,
    REWRITE_ADDENDUM_POST_CLOSE,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_POST_CLOSE,
)
from briefing.daily_review import Review, ReviewInput, review_rows
from briefing.events import EDITION_MORNING, EDITION_POST_CLOSE
from briefing.job_log import GeneratedBy
from briefing.llm_output import (
    InvalidLlmOutput,
    check_items_present,
    output_cited_refs,
    output_item,
    output_sentence,
    output_weak_shortfall,
    check_no_ticker,
    parse_output_object,
    ticker_pattern,
)
from briefing.llm import API_KEY_ENV, AcceptedAttempt, AnthropicClient, LlmCallError, LlmClient, llm_config
from briefing.move_figures import USD_KRW_NAME, display, exchange_rate_figure
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
    Article,
    Background,
    CloseRate,
    EtfInfo,
    Filing,
    etf_info_input,
)

log = logging.getLogger("briefing.daily_report")

GENERATED_BY_LLM = GeneratedBy.LLM.value
GENERATED_BY_RULE = GeneratedBy.RULE_BASED.value

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
ITEM_KR_REVIEW = "전날 한국 증시"
ITEM_KR_TODAY = "그날 한국 증시"
ITEM_ETFS = "보유 ETF별 하루 등락"
# 보유/관심으로 나뉠 때 글자판·HTML에 보이는 절 제목. LLM 입력과 자료 없음 목록은 ITEM_ETFS 이름을 그대로 쓴다.
ITEM_ETFS_SPLIT_TITLE = "보유·관심 ETF별 하루 등락"
# 마감 후 판이 ETF마다 모아 둔 기사, ETF에 담긴 미국 종목 중 이슈가 있는 종목. 글자판·HTML에는 아직 싣지 않는다.
ITEM_ETF_ARTICLES = "ETF별 기사"
ITEM_MEMBER_ISSUES = "미국 구성 종목 이슈"
# 근거가 부족해 다시 검색해 찾은 기사. 판 종류와 상관없이 다시 쓸 때만 맨 뒤에 붙는다.
ITEM_RESEARCH = "재검색 기사"

EDITION_ITEMS = {
    EDITION_MORNING: (ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_US_STOCKS, ITEM_FILINGS, ITEM_KR_REVIEW, ITEM_ETF_ARTICLES),
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
NO_REASON_LABEL = "이유를 찾지 못한 ETF"
KIND_BACKGROUND = "시장 자료"

# 마감 후 판 출력의 문장 종류. 재검색은 아침 판과 같은 번호 체계로 문장을 가리키므로 두 종류를 한 목록에 담는다.
POINT_ETF = "etf"
POINT_NOTABLE = "notable"
# 아침 판 출력의 ETF별 방향 예상 문장 종류. 이유 문장(POINT_ETF)과 달리 방향(direction)을 함께 든다.
POINT_OUTLOOK = "outlook"
# 마감 후 판 출력의 아침 예상 대조 이유 문장 종류. 맞음·빗나감은 코드가 정하고 문장은 그 이유만 쓴다.
POINT_REVIEW = "review"
# 환율 이슈·금리 이슈 칸의 문장 종류와 Claude 출력 칸 이름. 이 문장은 points가 아니라 issues에 담는다(다시 쓰기 대상이 아님).
POINT_FX = "fx"
POINT_RATE = "rate"
ISSUE_KEYS = (("fxIssue", POINT_FX), ("rateIssue", POINT_RATE))
SUMMARY_LINES = 2

# 코드가 붙이는 근거 번호의 머리글. 공시는 접수번호를 그대로 쓴다.
ETF_REF_PREFIX = "E"
CLOSE_RATE_REF_PREFIX = "U"
BACKGROUND_REF_PREFIX = "B"
MEMBER_ISSUE_REF_PREFIX = "M"

# 본문에는 미국 티커를 쓰지 않는다(보유 종목이 드러남). LLM 입력에는 넣는다.
US_STOCK_LABEL = "미국 종목"
WEEKDAYS = "월화수목금토일"

# ---- 입력 ----

@dataclass(frozen=True)
class EtfDay:
    """보유 ETF 하나의 하루 등락. 값은 일봉 원문 그대로다. 일봉이 없으면 값 칸이 None이고 이유가 missing_reason에 있다."""

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
    # 값 칸이 빈 이유(market.daily.responded v2). 값이 있으면 None이고, v1 응답에도 없다.
    missing_reason: str | None = None
    # 보유(HELD)·관심(WATCH) 구분(market.daily.responded v2 선택 칸). 칸이 없는 응답이면 None.
    relation: str | None = None

    def has_day(self) -> bool:
        return self.missing_reason is None and None not in (self.date, self.close, self.prev_close, self.rate)

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
                            with_usd_krw: bool = False) -> Section:
    """환율종합·시장종합. 원문은 해석하지 않으므로 본문에는 받았는지만 쓰고 숫자는 LLM 요약에 맡긴다.
    with_usd_krw면 환율종합 원문에서 꺼낸 원/달러 값을 근거에 덧붙인다(원문은 그대로 둔다)."""
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
    return Section(name, MARK_PRESENT, (f"{label} {_short(item.fetched_at)} 받은 값 있음",), (entry,))


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
            lines.append(f"{r.name}({r.code}) {r.rate}% ({r.business_day} 종가 {r.close}, "
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


def _filings_section(i: DailyInput) -> Section:
    if i.etfs is not None and not i.tickers():
        return Section(ITEM_FILINGS, MARK_NOT_APPLICABLE, (f"{MARK_NOT_APPLICABLE} (보유 ETF에 미국 종목이 없음)",))
    if i.filings is None:
        return _missing(ITEM_FILINGS)
    if not i.filings:
        return Section(ITEM_FILINGS, MARK_PRESENT, ("범위 안에 접수된 공시 없음",))
    lines, entries = [], []
    for f in sorted(i.filings, key=lambda f: f.accepted_at, reverse=True):
        items = f" 항목 {', '.join(f.items)}" if f.items else ""
        title = f"{US_STOCK_LABEL} {f.form} 공시{items}"
        lines.append(f"{title} ({_short(f.accepted_at)} 접수)")
        entries.append(Entry(f.accession_number,
                             {"ref": f.accession_number, "tickers": list(f.tickers), "form": f.form,
                              "items": list(f.items), "acceptedAt": _at(f.accepted_at),
                              "hoursBeforeAsOf": _hours_before(f.accepted_at, i.as_of)},
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


def etf_line(etf: EtfDay) -> str:
    if not etf.has_day():
        return f"{NO_DATA}: {etf.label()}, {_etf_missing_reason(etf)}"
    rate = _rate(etf.rate)
    shown = f"+{etf.rate}" if rate is not None and rate > 0 and not etf.rate.startswith("+") else etf.rate
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
                                 f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행 {a.url}", KIND_NEWS, a.url))
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
                             f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행 {a.url}", KIND_NEWS, a.url))
    if i.research_articles_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    status = MARK_TRUNCATED if i.research_articles_truncated else MARK_PRESENT
    return Section(ITEM_RESEARCH, status, tuple(lines), tuple(entries))


def build_sections(i: DailyInput, settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[Section, ...]:
    """판의 항목을 차례대로 만든다. 근거 번호는 이 순서로 붙는다."""
    counters = {ETF_REF_PREFIX: 0, CLOSE_RATE_REF_PREFIX: 0, BACKGROUND_REF_PREFIX: 0, MEMBER_ISSUE_REF_PREFIX: 0}

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
        ITEM_KR_REVIEW: lambda: _raw_background_section(ITEM_KR_REVIEW, i, "market-summary", background_ref),
        ITEM_KR_TODAY: lambda: _raw_background_section(ITEM_KR_TODAY, i, "market-summary", background_ref),
        ITEM_ETFS: lambda: _etfs_section(i, next_ref(ETF_REF_PREFIX)),
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


def _llm_item(s: Section) -> dict[str, Any]:
    item: dict[str, Any] = {"name": s.name, "status": s.status, "entries": [e.llm for e in s.entries]}
    # 값이 빈 대상은 있을 때만 싣는다. 빈 것이 없는 항목의 입력 모양은 바꾸지 않는다.
    if s.gaps_llm:
        item["missing"] = list(s.gaps_llm)
    return item


def build_llm_input(i: DailyInput, sections: tuple[Section, ...],
                    settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> dict[str, Any]:
    data: dict[str, Any] = {
        "edition": EDITION_NAMES[i.edition],
        "rangeStart": _at(i.range_start),
        "asOf": _at(i.as_of),
        "items": [_llm_item(s) for s in sections],
    }
    # ETF 정보는 응답에 있을 때만 넣는다. 정보가 없는 이전 형식 응답의 입력 모양은 바꾸지 않는다.
    etf_info = [{"symbol": e.symbol, **etf_info_input(e.etf, settings.max_etf_members)}
                for e in i.etfs or () if e.etf is not None]
    if etf_info:
        data["etfInfo"] = etf_info
    if i.edition == EDITION_POST_CLOSE:
        data["notableCandidates"] = _candidates_input(i, sections)
        # 대조할 ETF가 있을 때만 싣는다. 없으면 입력 모양은 그대로다.
        reviews = _reviews_input(i)
        if reviews:
            data["reviewEtfs"] = reviews
    else:
        data["outlookEtfs"] = _outlook_etfs_input(i)
    return data


def _outlook_etfs_input(i: DailyInput) -> list[dict[str, Any]]:
    """아침 판이 방향을 예상할 ETF. 보유·관심 모두이고, 일봉이 비어 있어도 기사로 예상하므로 빼지 않는다."""
    titles = dict(RELATION_TITLES)
    return [{"symbol": e.symbol, "name": e.label(), "relation": titles.get(e.relation, RELATION_UNKNOWN_TITLE)}
            for e in i.etfs or ()]


def _reviews_input(i: DailyInput) -> list[dict[str, Any]]:
    """코드가 정한 맞음·빗나감을 Claude에 준다. Claude는 판정을 바꾸지 않고 이유만 쓴다."""
    return [{"symbol": r.symbol, "morningDirection": r.call.direction, "morningReason": r.call.reason,
             "actualRate": str(r.rate), "verdict": r.verdict}
            for r in i.reviews() if r.verdict is not None]


def _candidates_input(i: DailyInput, sections: tuple[Section, ...]) -> list[dict[str, Any]]:
    """눈에 띄는 점 후보. 후보에 얽힌 ETF의 등락 근거 번호를 함께 준다."""
    day_refs = {e.llm["symbol"]: e.ref for s in sections if s.name == ITEM_ETFS for e in s.entries}
    return [{"kind": c.kind, "symbols": list(c.symbols), "refs": [day_refs[sym] for sym in c.symbols if sym in day_refs]}
            for c in i.notables]


# ---- 출력 검증 ----

class InvalidDailyOutput(InvalidLlmOutput):
    pass


@dataclass(frozen=True)
class Point:
    statement: str
    refs: tuple[str, ...]
    # 근거가 약하다고 한 문장의 부족한 점. 약하지 않으면 None.
    shortfall: str | None = None
    # 마감 후 판의 문장 종류(POINT_ETF, POINT_NOTABLE)와 ETF 이유의 종목코드. 아침 판은 둘 다 비어 있다.
    kind: str = ""
    symbol: str | None = None
    # 아침 판 ETF별 예상의 방향(DIRECTIONS 중 하나). 그 밖의 문장은 None.
    direction: str | None = None


@dataclass(frozen=True)
class DailyOutput:
    points: tuple[Point, ...]
    insufficient_evidence: bool
    # 마감 후 판의 요약 2줄. 아침 판은 비어 있다.
    summary: tuple[str, ...] = ()
    # 환율 이슈·금리 이슈 문장(kind가 POINT_FX, POINT_RATE). Claude가 내지 않았으면 없다.
    issues: tuple[Point, ...] = ()
    # 아침 판의 ETF별 방향 예상(kind가 POINT_OUTLOOK). 문장 단위 재검색의 대상이 아니라 points와 따로 둔다.
    outlooks: tuple[Point, ...] = ()
    # 마감 후 판의 아침 예상 대조 이유 문장(kind가 POINT_REVIEW). 대조할 ETF가 없으면 비어 있다.
    reviews: tuple[Point, ...] = ()

    def cited_refs(self) -> tuple[str, ...]:
        """본문이 인용한 근거 번호. 근거 부족 출력은 문장을 쓰지 않으므로 없다."""
        if self.insufficient_evidence:
            return ()
        return tuple(dict.fromkeys(ref for p in (*self.points, *self.issues, *self.outlooks, *self.reviews) for ref in p.refs))

    def weak_shortfalls(self) -> tuple[str, ...]:
        """근거가 약하다고 한 문장들의 부족한 점. 환율·금리 이슈 칸은 따로 보이므로 세지 않는다."""
        if self.insufficient_evidence:
            return ()
        return tuple(p.shortfall for p in (*self.points, *self.outlooks) if p.shortfall)


def parse_output(text: str, allowed_refs: frozenset[str],
                 forbidden_tickers: frozenset[str] = frozenset(),
                 weak_without_refs: bool = False) -> DailyOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나 준 적 없는 근거 번호를 인용하거나
    LLM 입력에 있던 미국 티커를 문장에 쓰면 InvalidDailyOutput.
    weak_without_refs이면 근거 약함인 문장만 근거 번호 없이 둘 수 있다(마감 후 판 ETF 이유 다시 쓰기)."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "points", InvalidDailyOutput)
    points = _parse_points(data, allowed_refs, tickers, weak_without_refs)
    insufficient = check_items_present(data, "points", "point", InvalidDailyOutput)
    return DailyOutput(points, insufficient)


def _parse_points(data: dict[str, Any], allowed_refs: frozenset[str], tickers: re.Pattern[str] | None,
                  weak_without_refs: bool) -> tuple[Point, ...]:
    points = []
    for raw in data["points"]:
        item = output_item(raw, "point", InvalidDailyOutput)
        statement = output_sentence(item, "statement", "point", InvalidDailyOutput)
        no_refs = weak_without_refs and item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "point", allowed_refs, InvalidDailyOutput)
        check_no_ticker(statement, tickers, "point", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "point", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "point shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("point cites no refs but is not marked weak")
        points.append(Point(statement, refs, shortfall))
    return tuple(points)


def parse_morning_output(text: str, allowed_refs: frozenset[str], forbidden_tickers: frozenset[str],
                         required_etfs: frozenset[str]) -> DailyOutput:
    """아침 판 LLM 응답을 검증한다. 요약 문장(points)은 parse_output과 같은 검사이고, 더해 ETF별 예상(etfs)이 빠지거나
    겹치거나 모르는 ETF이거나, 방향이 목록 밖이거나, 방향을 정하고도 근거 번호가 없거나(근거 약한 '예상 어려움'만 번호 없이 둘 수 있다),
    근거 번호 없는 예상이 근거 약함이 아니면 InvalidDailyOutput. 근거 부족이면 문장을 쓰지 않으므로 이슈 칸과 etfs는 보지 않는다."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "points", InvalidDailyOutput)
    points = _parse_points(data, allowed_refs, tickers, False)
    insufficient = check_items_present(data, "points", "point", InvalidDailyOutput)
    if insufficient:
        return DailyOutput((), True)
    if not isinstance(data.get("etfs"), list):
        raise InvalidDailyOutput("output does not have etfs")
    outlooks, seen = [], set()
    for raw in data["etfs"]:
        item = output_item(raw, "etf", InvalidDailyOutput)
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or symbol not in required_etfs:
            raise InvalidDailyOutput("etf is not one that was given for an outlook")
        if symbol in seen:
            raise InvalidDailyOutput("etf appears more than once")
        seen.add(symbol)
        direction = item.get("direction")
        if direction not in DIRECTIONS:
            raise InvalidDailyOutput("etf direction is not one of the allowed values")
        reason = output_sentence(item, "reason", "etf", InvalidDailyOutput)
        no_refs = item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "etf", allowed_refs, InvalidDailyOutput)
        check_no_ticker(reason, tickers, "etf", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "etf", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "etf shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("etf cites no refs but is not marked weak")
        elif direction == DIRECTION_UNSURE:
            # 방향을 못 정했다는 것은 근거가 모자라다는 뜻이라 근거 약함 표시가 함께 있어야 한다.
            raise InvalidDailyOutput("etf direction is unsure but is not marked weak")
        if no_refs and direction != DIRECTION_UNSURE:
            raise InvalidDailyOutput("etf has a direction but cites no refs")
        outlooks.append(Point(reason, refs, shortfall, POINT_OUTLOOK, symbol, direction))
    if required_etfs - seen:
        raise InvalidDailyOutput(f"{len(required_etfs - seen)} etf(s) have no outlook")
    issues = [_parse_issue(data[key], key, kind, allowed_refs, tickers)
              for key, kind in ISSUE_KEYS if data.get(key) is not None]
    return DailyOutput(points, False, (), tuple(issues), tuple(outlooks))


MEMBER_REPEAT_MIN_NAME = 2


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


def member_repeat_reason(text: str, issues: tuple[MemberIssue, ...]) -> str | None:
    """이유 문장이 '↳' 줄에 이미 있는 내용(종목 이름, 직전 장 등락률 숫자)을 되풀이하면 그 내용을 돌려준다. 티커와 이름은 적지 않는다."""
    lowered = text.lower()
    for issue in issues:
        if issue.name and len(issue.name) >= MEMBER_REPEAT_MIN_NAME and issue.name.lower() in lowered:
            return "종목 이름"
        value = decimal_rate(issue.rate)
        if value is None:
            continue
        forms = {str(abs(value)), format(abs(value).normalize(), "f")}
        for form in sorted(forms):
            if re.search(rf"(?<![\d.]){re.escape(form)}\s*%", text):
                return f"등락률 숫자 {form}%"
    return None


def repeats_member_line(text: str, issues: tuple[MemberIssue, ...]) -> bool:
    return member_repeat_reason(text, issues) is not None


def _repeat_error(reason: str) -> InvalidDailyOutput:
    return InvalidDailyOutput(f"etf reason repeats the member issue line ({reason})")


def parse_post_close_output(text: str, allowed_refs: frozenset[str], forbidden_tickers: frozenset[str],
                            required_etfs: frozenset[str],
                            member_issues: Mapping[str, tuple[MemberIssue, ...]],
                            notables_given: bool = True,
                            required_reviews: frozenset[str] = frozenset()) -> DailyOutput:
    """마감 후 판 LLM 응답을 검증한다. 아침 판 검사(형식, 목록 밖 근거 번호, 티커)에 더해 요약이 2줄이 아니거나,
    등락 자료가 있는 ETF가 빠지거나 겹치거나 모르는 ETF가 있거나, 이유가 '↳' 줄을 되풀이하면 InvalidDailyOutput.
    눈에 띄는 점 후보가 없는데(notables_given=False) 쓴 눈에 띄는 점은 버린다.
    아침 예상을 대조할 ETF(required_reviews)는 대조 이유(reviews)가 빠지거나 겹치면 거부하고, 대조할 ETF가 아닌 항목은 버린다."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "etfs", InvalidDailyOutput)
    summary_raw, notables_raw = data.get("summary"), data.get("notables")
    if not isinstance(summary_raw, list) or not isinstance(notables_raw, list):
        raise InvalidDailyOutput("output does not have summary and notables")
    if not notables_given:
        # 거부하면 멀쩡한 요약과 ETF 이유까지 버리고, 세 번 모두 그러면 판이 규칙 기반으로 나간다.
        notables_raw = []
    insufficient = data["insufficientEvidence"]
    summary = []
    for raw in summary_raw:
        line = output_sentence({"summary": raw}, "summary", "summary", InvalidDailyOutput)
        check_no_ticker(line, tickers, "summary", InvalidDailyOutput)
        summary.append(line)
    etf_points, seen = [], set()
    for raw in data["etfs"]:
        item = output_item(raw, "etf", InvalidDailyOutput)
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or symbol not in required_etfs:
            raise InvalidDailyOutput("etf is not one that was given with a daily move")
        if symbol in seen:
            raise InvalidDailyOutput("etf appears more than once")
        seen.add(symbol)
        reason = output_sentence(item, "reason", "etf", InvalidDailyOutput)
        # 이유를 찾지 못한 ETF는 근거 없이 둔다. 자기 일봉을 붙이면 근거가 있는 것처럼 보인다. 대신 근거 약함이어야 한다.
        no_refs = item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "etf", allowed_refs, InvalidDailyOutput)
        check_no_ticker(reason, tickers, "etf", InvalidDailyOutput)
        repeated = member_repeat_reason(reason, member_issues.get(symbol, ()))
        if repeated is not None:
            raise _repeat_error(repeated)
        shortfall = output_weak_shortfall(item, "etf", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "etf shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("etf cites no refs but is not marked weak")
        etf_points.append(Point(reason, refs, shortfall, POINT_ETF, symbol))
    notable_points = []
    for raw in notables_raw:
        item = output_item(raw, "notable", InvalidDailyOutput)
        statement = output_sentence(item, "statement", "notable", InvalidDailyOutput)
        refs = output_cited_refs(item, "refs", "notable", allowed_refs, InvalidDailyOutput)
        check_no_ticker(statement, tickers, "notable", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "notable", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "notable shortfall", InvalidDailyOutput)
        notable_points.append(Point(statement, refs, shortfall, POINT_NOTABLE))
    if insufficient:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않으므로 이슈 칸이 비었거나 깨져 있어도 거부하지 않는다.
        return DailyOutput((), True)
    issue_points = [_parse_issue(data[key], key, kind, allowed_refs, tickers)
                    for key, kind in ISSUE_KEYS if data.get(key) is not None]
    if required_etfs - seen:
        raise InvalidDailyOutput(f"{len(required_etfs - seen)} etf(s) with a daily move have no reason")
    if len(summary) != SUMMARY_LINES:
        raise InvalidDailyOutput(f"summary must have {SUMMARY_LINES} lines")
    reviews = _parse_reviews(data.get("reviews"), allowed_refs, tickers, required_reviews)
    return DailyOutput(tuple(etf_points) + tuple(notable_points), False, tuple(summary), tuple(issue_points),
                       reviews=reviews)


def _parse_reviews(raw_reviews: Any, allowed_refs: frozenset[str], tickers: re.Pattern[str] | None,
                   required: frozenset[str]) -> tuple[Point, ...]:
    """아침 예상 대조 이유. 대조할 ETF마다 정확히 하나이고, 이유를 찾지 못한 문장만 근거 번호 없이 둔다(근거 약함 표시 필요)."""
    if not required:
        # 대조할 ETF가 없는데 쓴 문장을 거부하면 멀쩡한 본문까지 버리게 되므로 버리기만 한다.
        return ()
    if not isinstance(raw_reviews, list):
        raise InvalidDailyOutput("output does not have reviews")
    points, seen = [], set()
    for raw in raw_reviews:
        item = output_item(raw, "review", InvalidDailyOutput)
        symbol = item.get("symbol")
        if symbol not in required:
            continue
        if symbol in seen:
            raise InvalidDailyOutput("review appears more than once")
        seen.add(symbol)
        reason = output_sentence(item, "reason", "review", InvalidDailyOutput)
        no_refs = item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "review", allowed_refs, InvalidDailyOutput)
        check_no_ticker(reason, tickers, "review", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "review", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "review shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("review cites no refs but is not marked weak")
        points.append(Point(reason, refs, shortfall, POINT_REVIEW, symbol))
    if required - seen:
        raise InvalidDailyOutput(f"{len(required - seen)} review(s) have no reason")
    return tuple(points)


def _parse_issue(raw: Any, key: str, kind: str, allowed_refs: frozenset[str],
                 tickers: re.Pattern[str] | None) -> Point:
    """환율 이슈·금리 이슈 문장 하나. 근거가 없다고 쓴 문장(금리 기사 없음 등)은 근거 약함일 때만 근거 번호 없이 둔다."""
    item = output_item(raw, key, InvalidDailyOutput)
    statement = output_sentence(item, "statement", key, InvalidDailyOutput)
    no_refs = item.get("refs") == []
    refs = () if no_refs else output_cited_refs(item, "refs", key, allowed_refs, InvalidDailyOutput)
    check_no_ticker(statement, tickers, key, InvalidDailyOutput)
    shortfall = output_weak_shortfall(item, key, InvalidDailyOutput)
    if shortfall is not None:
        check_no_ticker(shortfall, tickers, f"{key} shortfall", InvalidDailyOutput)
    elif no_refs:
        raise InvalidDailyOutput(f"{key} cites no refs but is not marked weak")
    return Point(statement, refs, shortfall, kind)


# ---- 본문 ----

def headline(i: DailyInput) -> list[str]:
    local = i.as_of.astimezone(KST)
    return [f"[{EDITION_NAMES[i.edition]}] {local:%Y-%m-%d}({WEEKDAYS[local.weekday()]}) {local:%H:%M} 데일리 브리핑",
            f"다룬 범위: {_short(i.range_start)} ~ {_short(i.as_of)}"]


def section_title(s: Section) -> str:
    """글자판·HTML에 보이는 절 제목."""
    return ITEM_ETFS_SPLIT_TITLE if s.name == ITEM_ETFS and s.groups else s.name


def section_lines(sections: tuple[Section, ...]) -> list[str]:
    """항목마다 제목과 숫자·목록 줄. 재검색 기사와 숨김 항목은 싣지 않는다."""
    lines: list[str] = []
    for s in sections:
        if s.name == ITEM_RESEARCH or s.hidden:
            # 재검색 기사는 요약이 인용할 때만 근거 목록에 나온다. 항목으로 나열하지 않는다.
            continue
        lines.append("")
        lines.append(f"■ {section_title(s)}")
        if s.groups:
            grouped = {line for _, members in s.groups for line in members}
            for title, members in s.groups:
                lines.append(f"  [{title}]")
                lines.extend(f"  - {line}" for line in members)
            # 묶음에 들지 않은 줄(잘림 표시)은 맨 끝에 둔다.
            lines.extend(f"  - {line}" for line in s.lines if line not in grouped)
        else:
            lines.extend(f"  - {line}" for line in s.lines)
    return lines


def _signed(rate: float) -> str:
    """결론에 쓰는 등락률. 소수 한 자리에 부호를 붙이고 내림은 빼기 기호(−)로 쓴다."""
    shown = _shown(rate)
    return f"{'+' if shown >= 0 else MINUS}{abs(shown):.1f}%"


def _count_text(label: str, group: tuple[EtfDay, ...]) -> str:
    # 개수는 ETF 줄에 보이는 원래 등락률의 부호로 센다. 반올림하면 +0.04%가 보합이 되어 목록과 어긋난다.
    up = sum(1 for e in group if e.has_day() and (_rate(e.rate) or 0.0) > 0)
    nodata = sum(1 for e in group if not e.has_day() or _rate(e.rate) is None)
    text = f"{label} {len(group)}개 중 {up}개 상승"
    return f"{text}(자료 없음 {nodata})" if nodata else text


def claude_no_reason_count(output: DailyOutput) -> int:
    """Claude가 이유를 '받은 자료에서 이유를 찾지 못했습니다'로 쓴 ETF 수."""
    target = NO_REASON_STATEMENT.rstrip(".")
    return sum(1 for p in output.points if p.kind == POINT_ETF and p.statement.strip().rstrip(".") == target)


def conclusion(i: DailyInput, output: DailyOutput | None = None) -> str:
    """마감 후 판의 한 줄 결론. 개수와 최대 등락은 코드가 센다. 자료 없는 ETF도 개수에 넣는다.
    이유를 찾지 못한 ETF 수는 Claude 본문이 있으면 본문에 그렇게 쓴 ETF 수이고, 규칙 기반 본문이면 코드가 자료로 센다."""
    etfs = i.etfs or ()
    if i.before_open():
        return CONCLUSION_PREFIX + PRE_OPEN_NOTICE
    rated = [(e, v) for e in etfs if e.has_day() for v in [_rate(e.rate)] if v is not None]
    if not rated:
        return CONCLUSION_PREFIX + CONCLUSION_NO_DATA
    parts = [_count_text(title or "ETF", members) for title, members in etf_groups(etfs)]
    best = max(rated, key=lambda r: r[1])
    worst = min(rated, key=lambda r: r[1])
    if _shown(best[1]) > 0:
        parts.append(f"가장 크게 오름 {best[0].label()} {_signed(best[1])}")
    if _shown(worst[1]) < 0:
        parts.append(f"가장 크게 내림 {worst[0].label()} {_signed(worst[1])}")
    if output is not None and not output.insufficient_evidence:
        unexplained = claude_no_reason_count(output)
    else:
        unexplained = no_reason_count(etfs, i.member_issues, i.etf_articles or (), i.filings or ())
    if unexplained:
        parts.append(f"{NO_REASON_LABEL} {unexplained}개")
    return CONCLUSION_PREFIX + " · ".join(parts)


def daily_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None,
               status_text: str | None = None) -> str:
    """글자판 본문. output이 None이면 숫자·목록만으로 된 규칙 기반 본문이다. 판마다 배치가 달라
    아침 판은 daily_morning, 마감 후 판은 daily_post_close가 쓴다."""
    # 두 모듈은 이 모듈의 자료 모양을 쓰므로 순환을 피하려고 여기서 가져온다.
    if i.edition == EDITION_POST_CLOSE:
        from briefing.daily_post_close import post_close_text
        return post_close_text(i, sections, output, status_text)
    from briefing.daily_morning import morning_text
    return morning_text(i, sections, output, status_text)


def rule_based_text(i: DailyInput, sections: tuple[Section, ...]) -> str:
    """숫자·목록만으로 된 본문. LLM 키가 없거나 LLM이 끝내 실패했을 때 쓴다."""
    return daily_text(i, sections, None)


def llm_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput) -> str:
    return daily_text(i, sections, output)


# ---- LLM 작성기 ----

@dataclass(frozen=True)
class DailyBody:
    text: str
    generated_by: str
    insufficient_evidence: bool = False
    # 본문이 인용한 근거 번호(규칙 기반이면 없음)
    cited: tuple[str, ...] = ()
    # 근거가 약하다고 한 문장들의 부족한 점. 재검색 여부를 정하고 검색어 LLM 입력에 들어간다.
    weak_shortfalls: tuple[str, ...] = ()
    # 받아들인 LLM 출력. 문장 하나만 다시 쓸 때 나머지 문장을 그대로 두고 본문을 다시 만드는 데 쓴다(규칙 기반이면 None).
    output: DailyOutput | None = None


@dataclass(frozen=True)
class LlmAttempt:
    """거부되거나 실패한 LLM 시도 하나. 작업 기록에 그대로 넘긴다."""

    attempt: int
    error_type: str
    reason: str
    llm_input: str
    raw_output: str | None


@dataclass(frozen=True)
class DailyWriteResult:
    body: DailyBody | None
    rejected: tuple[LlmAttempt, ...]
    # 받아들인 시도(끝내 실패했으면 None)
    accepted: AcceptedAttempt | None = None


@dataclass(frozen=True)
class PointRewrite:
    """문장 하나를 다시 쓴 결과. point가 None이면 끝내 실패했고 원래 문장을 그대로 둔다."""

    point: Point | None
    rejected: tuple[LlmAttempt, ...]
    accepted: AcceptedAttempt | None = None


def _etfs_with_day(i: DailyInput) -> frozenset[str]:
    """이유를 써야 하는 ETF. 하루 등락이 없는 ETF는 코드가 자료 없음 줄로 쓴다."""
    return frozenset(e.symbol for e in i.etfs or () if e.has_day())


def _etfs_to_forecast(i: DailyInput) -> frozenset[str]:
    """아침 판이 방향을 예상해야 하는 ETF. 보유·관심 모두이고 일봉이 비어 있어도 뺀다고 정하지 않는다."""
    return frozenset(e.symbol for e in i.etfs or ())


def _reviews_to_explain(i: DailyInput) -> frozenset[str]:
    """Claude가 맞음·빗나감의 이유를 써야 하는 ETF. 예상 어려움은 대조하지 않으므로 뺀다."""
    return frozenset(r.symbol for r in i.reviews() if r.verdict is not None)


def _issues_by_etf(i: DailyInput) -> dict[str, tuple[MemberIssue, ...]]:
    grouped: dict[str, list[MemberIssue]] = {}
    for issue in i.member_issues:
        grouped.setdefault(issue.etf, []).append(issue)
    return {etf: tuple(issues) for etf, issues in grouped.items()}


@dataclass(frozen=True)
class DailyLlmWriter:
    client: LlmClient
    max_attempts: int = DEFAULT_DAILY_SETTINGS.llm_max_attempts
    settings: DailySettings = DEFAULT_DAILY_SETTINGS

    @staticmethod
    def _forbidden_tickers(i: DailyInput) -> frozenset[str]:
        return frozenset(i.tickers()) | frozenset(r.ticker for r in i.close_rates or ()) \
            | frozenset(t for f in i.filings or () for t in f.tickers) | i.member_us_symbols()

    def write(self, i: DailyInput, sections: tuple[Section, ...]) -> DailyWriteResult:
        user = json.dumps(build_llm_input(i, sections, self.settings), ensure_ascii=False)
        allowed = frozenset(e.ref for s in sections for e in s.entries)
        tickers = self._forbidden_tickers(i)
        post_close = i.edition == EDITION_POST_CLOSE
        prompt, schema = (SYSTEM_PROMPT_POST_CLOSE, OUTPUT_SCHEMA_POST_CLOSE) if post_close \
            else (SYSTEM_PROMPT, OUTPUT_SCHEMA_MORNING)
        rejected: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw: str | None = None
            try:
                raw = self.client.complete(prompt, user, schema)
                output = parse_post_close_output(raw, allowed, tickers, _etfs_with_day(i), _issues_by_etf(i),
                                                 bool(i.notables), _reviews_to_explain(i)) \
                    if post_close else parse_morning_output(raw, allowed, tickers, _etfs_to_forecast(i))
            except (LlmCallError, InvalidDailyOutput) as error:
                rejected.append(LlmAttempt(attempt, type(error).__name__, str(error), user, raw))
                log.warning("daily LLM attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, i.job_id, error)
                continue
            return DailyWriteResult(DailyBody(llm_text(i, sections, output), GENERATED_BY_LLM,
                                              output.insufficient_evidence, output.cited_refs(),
                                              output.weak_shortfalls(), output), tuple(rejected),
                                    AcceptedAttempt(attempt, user, raw))
        return DailyWriteResult(None, tuple(rejected))

    def rewrite_point(self, i: DailyInput, sections: tuple[Section, ...], current: Point, no: int,
                      new_refs: tuple[str, ...]) -> PointRewrite:
        """근거가 약한 문장 하나만 다시 쓴다. 나머지 문장은 건드리지 않는다. i와 sections에는 새로 찾은 기사까지 들어 있다."""
        data = build_llm_input(i, sections, self.settings)
        data["rewrite"] = {"sentenceNo": no, "sentence": {"statement": current.statement, "refs": list(current.refs)},
                           "shortfall": current.shortfall, "newRefs": list(new_refs)}
        if current.symbol is not None:
            data["rewrite"]["symbol"] = current.symbol
        user = json.dumps(data, ensure_ascii=False)
        allowed = frozenset(e.ref for s in sections for e in s.entries)
        tickers = self._forbidden_tickers(i)
        prompt = SYSTEM_PROMPT_POST_CLOSE + REWRITE_ADDENDUM_POST_CLOSE if i.edition == EDITION_POST_CLOSE \
            else SYSTEM_PROMPT + REWRITE_ADDENDUM
        rejected: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw: str | None = None
            try:
                raw = self.client.complete(prompt, user, OUTPUT_SCHEMA)
                output = parse_output(raw, allowed, tickers, weak_without_refs=current.kind == POINT_ETF)
                if output.insufficient_evidence or len(output.points) != 1:
                    raise InvalidDailyOutput("rewrite must return exactly one point")
                repeated = member_repeat_reason(
                    output.points[0].statement, _issues_by_etf(i).get(current.symbol or "", ())) \
                    if current.kind == POINT_ETF else None
                if repeated is not None:
                    raise _repeat_error(repeated)
            except (LlmCallError, InvalidDailyOutput) as error:
                rejected.append(LlmAttempt(attempt, type(error).__name__, str(error), user, raw))
                log.warning("daily LLM sentence rewrite attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, i.job_id, error)
                continue
            # 다시 쓴 문장은 원래 문장의 종류와 ETF를 이어받는다.
            point = replace(output.points[0], kind=current.kind, symbol=current.symbol)
            return PointRewrite(point, tuple(rejected), AcceptedAttempt(attempt, user, raw))
        return PointRewrite(None, tuple(rejected))


def build_daily_writer(env: Mapping[str, str],
                       client_factory: Callable[[str, str], LlmClient] = AnthropicClient,
                       settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> DailyLlmWriter | None:
    """시작할 때 한 번 부른다. 키가 없으면 None이고 데일리 판은 규칙 기반 본문으로 나간다."""
    config = llm_config(env)
    if config is None:
        log.warning("%s is not set; daily briefings will be rule-based", API_KEY_ENV)
        return None
    return DailyLlmWriter(client_factory(config.api_key, config.model), settings.llm_max_attempts, settings)
