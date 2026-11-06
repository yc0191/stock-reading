"""데일리 판(아침 판, 마감 후 판) 본문 만들기. Kafka를 모르며 모은 자료(DailyInput)를 인자로 받는다.

항목별 숫자·목록과 '자료 없음' 표시, 머리말, 면책 문구는 코드가 쓴다. LLM은 코드가 붙인 근거 번호를 인용한
요약 문장만 만들고, LLM이 끝내 실패하면 요약 없이 코드가 쓴 숫자·목록 본문(규칙 기반)으로 나간다.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
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
    weak_note_line,
)
from briefing.llm import API_KEY_ENV, AcceptedAttempt, AnthropicClient, LlmCallError, LlmClient, llm_config
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

EDITION_NAMES = {EDITION_MORNING: "아침 판", EDITION_POST_CLOSE: "마감 후 판"}

ITEM_US_INDEX = "미국 지수"
ITEM_EXCHANGE = "환율"
ITEM_US_STOCKS = "미국 종목 직전 장 등락"
ITEM_FILINGS = "범위 안 미국 공시"
ITEM_KR_REVIEW = "전날 한국 증시"
ITEM_KR_TODAY = "그날 한국 증시"
ITEM_ETFS = "보유 ETF별 하루 등락"
# 근거가 부족해 다시 검색해 찾은 기사. 판 종류와 상관없이 다시 쓸 때만 맨 뒤에 붙는다.
ITEM_RESEARCH = "재검색 기사"

EDITION_ITEMS = {
    EDITION_MORNING: (ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_US_STOCKS, ITEM_FILINGS, ITEM_KR_REVIEW),
    EDITION_POST_CLOSE: (ITEM_KR_TODAY, ITEM_ETFS),
}
# 공시는 아침 판만 쓰므로 마감 후 판은 공시를 요청하지 않는다.
EDITIONS_WITH_FILINGS = frozenset(e for e, items in EDITION_ITEMS.items() if ITEM_FILINGS in items)

# 코드가 붙이는 근거 번호의 머리글. 공시는 접수번호를 그대로 쓴다.
ETF_REF_PREFIX = "E"
CLOSE_RATE_REF_PREFIX = "U"
BACKGROUND_REF_PREFIX = "B"

# 본문에는 미국 티커를 쓰지 않는다(보유 종목이 드러남). LLM 입력에는 넣는다.
US_STOCK_LABEL = "미국 종목"
WEEKDAYS = "월화수목금토일"

SYSTEM_PROMPT = """당신은 ETF 투자자에게 하루 시장 흐름을 짧게 정리해 주는 도우미입니다.
입력은 JSON이며 판 종류(아침 판, 마감 후 판), 다룬 범위, 항목(items) 목록을 담고 있습니다.
항목마다 status(상태)와 entries(자료)가 있고, 근거로 쓸 수 있는 자료에는 ref(근거 번호)가 붙어 있습니다.
- 배경 자료(category가 "배경")는 환율종합·시장종합·세계지수 응답 원문(response)을 그대로 준 것이며, receivedAt은 받은 시각, minutesBeforeAsOf는 판 기준 시각보다 몇 분 전에 받은 값인지입니다.
- 세계지수의 indexRates는 코드가 지수마다 직전 영업일 종가와 비교해 계산한 등락률입니다. 원문의 전일대비·등락률은 장이 끝난 지수에서 0.00으로 오므로 0.00을 "변동 없음"으로 읽지 말고, 등락률은 indexRates의 계산된 rate를 씁니다. rate가 "없음"이면 그 지수의 등락률은 모르는 것입니다.
- 미국 종목 등락률은 보유 ETF에 담긴 미국 종목의 직전 정규장 등락률(%)이며 hoursBeforeAsOf는 그 장 마감이 기준 시각보다 몇 시간 전인지입니다.
- 공시의 hoursBeforeAsOf는 공시 접수 시각이 기준 시각보다 몇 시간 전인지입니다.
- 보유 ETF별 하루 등락의 rate는 일봉 원문의 등락률(%)입니다.
- 항목 "재검색 기사"는 근거가 부족해 다시 검색해 찾은 기사이며(있을 때만 담깁니다) 기사 번호(ref)로 인용합니다. minutesBeforeAsOf는 기사 발행 시각이 판 기준 시각보다 몇 분 전인지입니다.
- etfInfo(있을 때만 담깁니다)는 보유 ETF의 이름(name)·추종 대상(tracks)·업종(sector)과 비중 순위(rank, 1이 가장 큼) 순의 구성 종목(members)입니다. ETF가 무엇을 따라가고 무엇이 담겨 있는지 이해하는 맥락으로만 쓰고, 이것을 등락의 원인으로 단정하지 않습니다. ref가 없어 인용할 수 없으며, 문장의 근거는 ref가 있는 자료에서 찾습니다.
- 상태 표시: "있음"은 자료가 있다는 뜻, "없음"은 범위 안에 자료가 없다는 뜻, "잘림"은 상한 때문에 일부만 받았다는 뜻, "너무 큼"은 원문이 커서 빠졌다는 뜻, "해당 없음"은 보유 ETF에 미국 종목이 없어 조회하지 않았다는 뜻입니다.

규칙:
- 원인을 단정하지 않습니다. "~때문에 올랐다"가 아니라 "~가 영향을 줬을 수 있다"처럼 씁니다.
- 각 문장은 근거가 된 자료의 ref를 refs에 하나 이상 넣습니다. 입력에 없는 ref는 쓰지 않습니다.
- 상태가 "없음"인 항목을 추측으로 채우지 않습니다.
- 문장마다 weak(근거 약함 여부)와 shortfall(부족한 점)을 냅니다. 그 문장을 뒷받침하는 자료가 약하면(범위가 맞지 않거나, 간접 정보뿐이거나, 세부가 자료에 없으면) weak를 true로 하고 shortfall에 어떤 자료가 더 있어야 하는지 한 문장으로 씁니다. 근거가 충분하면 weak를 false로, shortfall을 빈 문자열로 둡니다. shortfall은 찾아야 할 자료를 말할 뿐 원인을 단정하지 않으며 미국 종목의 티커를 쓰지 않습니다.
- 요약할 근거가 부족하면 insufficientEvidence를 true로 하고 points는 빈 목록으로 둡니다.
- 투자 조언(매수·매도·보유 권유, 가격 전망)을 하지 않습니다.
- 숫자를 새로 계산하지 않습니다. 입력에 있는 값만 씁니다.
- 문장에 미국 종목의 티커(ticker, tickers 칸의 값)를 쓰지 않습니다. 개별 종목을 말해야 하면 "미국 종목"이라고 씁니다.
- 문장은 한국어로 3~6개 정도로 씁니다.

출력은 다음 JSON 하나만 냅니다:
{"points":[{"statement":"...","refs":["..."],"weak":false,"shortfall":""}],"insufficientEvidence":false}"""

OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "points": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "statement": {"type": "string"},
                    "refs": {"type": "array", "items": {"type": "string"}},
                    "weak": {"type": "boolean"},
                    "shortfall": {"type": "string"},
                },
                "required": ["statement", "refs", "weak", "shortfall"],
                "additionalProperties": False,
            },
        },
        "insufficientEvidence": {"type": "boolean"},
    },
    "required": ["points", "insufficientEvidence"],
    "additionalProperties": False,
}


# ---- 입력 ----

@dataclass(frozen=True)
class EtfDay:
    """보유 ETF 하나의 하루 등락. 값은 일봉 원문 그대로다."""

    symbol: str
    date: str
    close: str
    prev_close: str
    rate: str
    fetched_at: datetime
    tickers: tuple[str, ...]
    tickers_truncated: bool
    # 응답의 ETF 정보(선택 칸). 이전 형식 응답에는 없다.
    etf: EtfInfo | None = None


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

    def tickers(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(t for etf in self.etfs or () for t in etf.tickers))

    def member_us_symbols(self) -> frozenset[str]:
        """ETF 정보의 구성 종목 중 미국 종목의 티커."""
        return frozenset(t for etf in self.etfs or () if etf.etf for t in etf.etf.us_tickers())


@dataclass(frozen=True)
class Entry:
    """근거 번호가 붙은 자료 하나. llm은 LLM 입력에 넣는 값, describe는 본문 근거 목록에 쓰는 설명이다."""

    ref: str
    llm: dict[str, Any]
    describe: str


@dataclass(frozen=True)
class Section:
    name: str
    status: str
    lines: tuple[str, ...]
    entries: tuple[Entry, ...] = ()

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
    try:
        return float(text) if text is not None else None
    except ValueError:
        return None


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
    return Entry(ref, data, f"{name} — {BACKGROUND_SOURCE}, {_short(item.fetched_at)} 받음")


def _raw_background_section(name: str, i: DailyInput, api_type: str, refs: Callable[[], str]) -> Section:
    """환율종합·시장종합. 원문은 해석하지 않으므로 본문에는 받았는지만 쓰고 숫자는 LLM 요약에 맡긴다."""
    if i.background is None:
        return _missing(name)
    item = _find_background(i, api_type, None)
    label = BACKGROUND_NAMES.get(api_type, api_type)
    if item is None or item.status == BACKGROUND_MISSING:
        return _missing(name, "범위 안에 받은 값이 없음")
    if item.status == BACKGROUND_TOO_LARGE:
        return Section(name, MARK_TOO_LARGE, (f"{NO_DATA} ({label} 원문이 너무 커서 빠짐)",))
    entry = _background_entry(refs(), item, i.as_of, with_rates=False)
    return Section(name, MARK_PRESENT, (f"{label} {_short(item.fetched_at)} 받은 값 있음",), (entry,))


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
                             f"{US_STOCK_LABEL} 직전 정규장 등락률 {r.rate}% ({r.date}) — {CLOSE_RATE_SOURCE}"))
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
                             f"{title} — {FILING_SOURCE}, {_short(f.accepted_at)} 접수"))
    status = MARK_TRUNCATED if i.filings_truncated else MARK_PRESENT
    if i.filings_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    return Section(ITEM_FILINGS, status, tuple(lines), tuple(entries))


def _etfs_section(i: DailyInput, refs: Callable[[], str]) -> Section:
    if not i.etfs:
        return _missing(ITEM_ETFS, None if i.etfs is None else "범위 안에 받은 일봉이 없음")
    lines, entries = [], []
    for etf in i.etfs:
        ref = refs()
        lines.append(f"{etf.symbol} {etf.rate}% (종가 {etf.close}, 전일 {etf.prev_close}, {etf.date})")
        entries.append(Entry(ref, {"ref": ref, "symbol": etf.symbol, "date": etf.date, "close": etf.close,
                                   "prevClose": etf.prev_close, "rate": etf.rate, "receivedAt": _at(etf.fetched_at),
                                   "tickers": list(etf.tickers)},
                             f"{etf.symbol} 하루 등락 {etf.rate}% ({etf.date}) — {BACKGROUND_SOURCE}"))
    if i.etfs_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    return Section(ITEM_ETFS, MARK_TRUNCATED if i.etfs_truncated else MARK_PRESENT, tuple(lines), tuple(entries))


def _research_section(i: DailyInput) -> Section:
    lines, entries = [], []
    for a in sorted(i.research_articles, key=lambda a: a.published_at, reverse=True):
        lines.append(f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행")
        entries.append(Entry(a.article_id,
                             {"ref": a.article_id, "articleId": a.article_id, "title": a.title,
                              "summary": a.summary, "source": a.publisher, "publishedAt": _at(a.published_at),
                              "minutesBeforeAsOf": _minutes_before(a.published_at, i.as_of)},
                             f"{a.title} — {a.publisher}, {_short(a.published_at)} 발행 {a.url}"))
    if i.research_articles_truncated:
        lines.append(f"({MARK_TRUNCATED}: 상한까지만 받음)")
    status = MARK_TRUNCATED if i.research_articles_truncated else MARK_PRESENT
    return Section(ITEM_RESEARCH, status, tuple(lines), tuple(entries))


def build_sections(i: DailyInput, settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[Section, ...]:
    """판의 항목을 차례대로 만든다. 근거 번호는 이 순서로 붙는다."""
    counters = {ETF_REF_PREFIX: 0, CLOSE_RATE_REF_PREFIX: 0, BACKGROUND_REF_PREFIX: 0}

    def next_ref(prefix: str) -> Callable[[], str]:
        def make() -> str:
            counters[prefix] += 1
            return f"{prefix}{counters[prefix]}"
        return make

    background_ref = next_ref(BACKGROUND_REF_PREFIX)
    builders = {
        ITEM_US_INDEX: lambda: _us_index_section(i, settings, background_ref),
        ITEM_EXCHANGE: lambda: _raw_background_section(ITEM_EXCHANGE, i, "exchange-rate-summary", background_ref),
        ITEM_US_STOCKS: lambda: _us_stocks_section(i, next_ref(CLOSE_RATE_REF_PREFIX)),
        ITEM_FILINGS: lambda: _filings_section(i),
        ITEM_KR_REVIEW: lambda: _raw_background_section(ITEM_KR_REVIEW, i, "market-summary", background_ref),
        ITEM_KR_TODAY: lambda: _raw_background_section(ITEM_KR_TODAY, i, "market-summary", background_ref),
        ITEM_ETFS: lambda: _etfs_section(i, next_ref(ETF_REF_PREFIX)),
    }
    sections = [builders[name]() for name in EDITION_ITEMS[i.edition]]
    if i.research_articles:
        sections.append(_research_section(i))
    return tuple(sections)


def missing_items(sections: tuple[Section, ...]) -> list[str]:
    return [s.name for s in sections if s.missing]


def has_evidence(sections: tuple[Section, ...]) -> bool:
    return any(s.entries for s in sections)


def build_llm_input(i: DailyInput, sections: tuple[Section, ...],
                    settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> dict[str, Any]:
    data: dict[str, Any] = {
        "edition": EDITION_NAMES[i.edition],
        "rangeStart": _at(i.range_start),
        "asOf": _at(i.as_of),
        "items": [{"name": s.name, "status": s.status, "entries": [e.llm for e in s.entries]} for s in sections],
    }
    # ETF 정보는 응답에 있을 때만 넣는다. 정보가 없는 이전 형식 응답의 입력 모양은 바꾸지 않는다.
    etf_info = [{"symbol": e.symbol, **etf_info_input(e.etf, settings.max_etf_members)}
                for e in i.etfs or () if e.etf is not None]
    if etf_info:
        data["etfInfo"] = etf_info
    return data


# ---- 출력 검증 ----

class InvalidDailyOutput(InvalidLlmOutput):
    pass


@dataclass(frozen=True)
class Point:
    statement: str
    refs: tuple[str, ...]
    # 근거가 약하다고 한 문장의 부족한 점. 약하지 않으면 None.
    shortfall: str | None = None


@dataclass(frozen=True)
class DailyOutput:
    points: tuple[Point, ...]
    insufficient_evidence: bool


def parse_output(text: str, allowed_refs: frozenset[str],
                 forbidden_tickers: frozenset[str] = frozenset()) -> DailyOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나 준 적 없는 근거 번호를 인용하거나
    LLM 입력에 있던 미국 티커를 문장에 쓰면 InvalidDailyOutput."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "points", InvalidDailyOutput)
    points = []
    for raw in data["points"]:
        item = output_item(raw, "point", InvalidDailyOutput)
        statement = output_sentence(item, "statement", "point", InvalidDailyOutput)
        refs = output_cited_refs(item, "refs", "point", allowed_refs, InvalidDailyOutput)
        check_no_ticker(statement, tickers, "point", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "point", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "point shortfall", InvalidDailyOutput)
        points.append(Point(statement, refs, shortfall))
    insufficient = check_items_present(data, "points", "point", InvalidDailyOutput)
    return DailyOutput(tuple(points), insufficient)


# ---- 본문 ----

def headline(i: DailyInput) -> list[str]:
    local = i.as_of.astimezone(KST)
    return [f"[{EDITION_NAMES[i.edition]}] {local:%Y-%m-%d}({WEEKDAYS[local.weekday()]}) {local:%H:%M} 데일리 브리핑",
            f"다룬 범위: {_short(i.range_start)} ~ {_short(i.as_of)}"]


def _facts(sections: tuple[Section, ...]) -> list[str]:
    lines: list[str] = []
    for s in sections:
        if s.name == ITEM_RESEARCH:
            # 재검색 기사는 요약이 인용할 때만 근거 목록에 나온다. 항목으로 나열하지 않는다.
            continue
        lines.append("")
        lines.append(f"■ {s.name}")
        lines.extend(f"  - {line}" for line in s.lines)
    return lines


def rule_based_text(i: DailyInput, sections: tuple[Section, ...]) -> str:
    """숫자·목록만으로 된 본문. LLM 키가 없거나 LLM이 끝내 실패했을 때 쓴다."""
    return "\n".join([*headline(i), *_facts(sections), "", DAILY_DISCLAIMER])


def llm_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput) -> str:
    lines = [*headline(i), *_facts(sections), "", "요약:"]
    if output.insufficient_evidence:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않는다.
        lines.append(f"  {DAILY_INSUFFICIENT}")
        return "\n".join([*lines, "", DAILY_DISCLAIMER])
    by_ref = {e.ref: e for s in sections for e in s.entries}
    cited = list(dict.fromkeys(ref for p in output.points for ref in p.refs))
    number = {ref: n for n, ref in enumerate(cited, start=1)}
    for p in output.points:
        lines.append(f"  - {p.statement} [근거 {', '.join(str(number[x]) for x in p.refs)}]")
        if p.shortfall:
            lines.append(weak_note_line(p.shortfall, "    "))
    lines.append("근거:")
    lines.extend(f"  {number[ref]}. {by_ref[ref].describe}" for ref in cited)
    return "\n".join([*lines, "", DAILY_DISCLAIMER])


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


REWRITE_ADDENDUM = """

다시 쓰기 요청:
- 입력의 rewrite 칸은 근거가 약하다고 한 문장 하나(sentence)와 그 부족한 점(shortfall)입니다. newRefs는 그 부족한 점을 채우려고 새로 찾아 항목 "재검색 기사"에 담은 기사의 ref입니다.
- 그 문장 하나만 다시 씁니다. points에는 다시 쓴 문장 하나만 담고 다른 문장은 쓰지 않으며, insufficientEvidence는 false로 둡니다.
- 새 기사로 부족한 점이 채워지면 weak를 false로, 그래도 약하면 weak를 true로 하고 shortfall에 아직 부족한 점을 씁니다. 새 기사에 없는 내용은 지어내지 않습니다.
- 근거 번호·단정 금지·티커 금지 같은 위의 규칙은 그대로 지킵니다."""


@dataclass(frozen=True)
class PointRewrite:
    """문장 하나를 다시 쓴 결과. point가 None이면 끝내 실패했고 원래 문장을 그대로 둔다."""

    point: Point | None
    rejected: tuple[LlmAttempt, ...]
    accepted: AcceptedAttempt | None = None


@dataclass(frozen=True)
class DailyLlmWriter:
    client: LlmClient
    max_attempts: int = DEFAULT_DAILY_SETTINGS.llm_max_attempts
    settings: DailySettings = DEFAULT_DAILY_SETTINGS

    def write(self, i: DailyInput, sections: tuple[Section, ...]) -> DailyWriteResult:
        user = json.dumps(build_llm_input(i, sections, self.settings), ensure_ascii=False)
        allowed = frozenset(e.ref for s in sections for e in s.entries)
        tickers = frozenset(i.tickers()) | frozenset(r.ticker for r in i.close_rates or ()) \
            | frozenset(t for f in i.filings or () for t in f.tickers) | i.member_us_symbols()
        rejected: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw: str | None = None
            try:
                raw = self.client.complete(SYSTEM_PROMPT, user, OUTPUT_SCHEMA)
                output = parse_output(raw, allowed, tickers)
            except (LlmCallError, InvalidDailyOutput) as error:
                rejected.append(LlmAttempt(attempt, type(error).__name__, str(error), user, raw))
                log.warning("daily LLM attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, i.job_id, error)
                continue
            cited = () if output.insufficient_evidence else tuple(
                dict.fromkeys(ref for p in output.points for ref in p.refs))
            weak = () if output.insufficient_evidence else tuple(p.shortfall for p in output.points if p.shortfall)
            return DailyWriteResult(DailyBody(llm_text(i, sections, output), GENERATED_BY_LLM,
                                              output.insufficient_evidence, cited, weak, output), tuple(rejected),
                                    AcceptedAttempt(attempt, user, raw))
        return DailyWriteResult(None, tuple(rejected))

    def rewrite_point(self, i: DailyInput, sections: tuple[Section, ...], current: Point, no: int,
                      new_refs: tuple[str, ...]) -> PointRewrite:
        """근거가 약한 문장 하나만 다시 쓴다. 나머지 문장은 건드리지 않는다. i와 sections에는 새로 찾은 기사까지 들어 있다."""
        data = build_llm_input(i, sections, self.settings)
        data["rewrite"] = {"sentenceNo": no, "sentence": {"statement": current.statement, "refs": list(current.refs)},
                           "shortfall": current.shortfall, "newRefs": list(new_refs)}
        user = json.dumps(data, ensure_ascii=False)
        allowed = frozenset(e.ref for s in sections for e in s.entries)
        tickers = frozenset(i.tickers()) | frozenset(r.ticker for r in i.close_rates or ()) \
            | frozenset(t for f in i.filings or () for t in f.tickers) | i.member_us_symbols()
        rejected: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw: str | None = None
            try:
                raw = self.client.complete(SYSTEM_PROMPT + REWRITE_ADDENDUM, user, OUTPUT_SCHEMA)
                output = parse_output(raw, allowed, tickers)
                if output.insufficient_evidence or len(output.points) != 1:
                    raise InvalidDailyOutput("rewrite must return exactly one point")
            except (LlmCallError, InvalidDailyOutput) as error:
                rejected.append(LlmAttempt(attempt, type(error).__name__, str(error), user, raw))
                log.warning("daily LLM sentence rewrite attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, i.job_id, error)
                continue
            return PointRewrite(output.points[0], tuple(rejected), AcceptedAttempt(attempt, user, raw))
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
