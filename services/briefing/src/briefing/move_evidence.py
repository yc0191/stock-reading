"""급변동 리포트의 근거 목록과 LLM 입력. Kafka를 모르며 모은 자료(ReportInput)를 인자로 받는다.

근거 번호(기사 번호, 공시 접수번호, 코드가 붙이는 머리글과 순번)와 근거 목록(evidence_catalog), 배경·비교 숫자,
확인하지 못한 항목, LLM에 넘기는 입력을 만든다. LLM 호출과 출력 해석은 report.py에 있다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from briefing.article_groups import RESEARCH_MARK, ArticleRow, GroupedArticles, group_article_lists
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.compose import KST, direction_of, headline, high_headline, window_headline
from briefing.events import format_timestamp
from briefing.jobs import ReportInput
from briefing.job_log import DETECTION_PREV_CLOSE_HIGH
from briefing.move_figures import (
    EXCHANGE_RATE_API,
    MARKET_SUMMARY_API,
    MoveFigures,
    compared_label,
    compares_today,
    display,
    high_today,
    report_figures,
    today_first,
    window_minutes,
)
from briefing.move_flow import flow_input
from briefing.sources import (
    BACKGROUND_LABEL,
    BACKGROUND_NAMES,
    BACKGROUND_OK,
    BACKGROUND_SOURCE,
    BACKGROUND_STATUS_MARKS,
    CLOSE_RATE_SOURCE,
    FILING_SOURCE,
    MARK_MISSING,
    MARK_NOT_APPLICABLE,
    MARK_PRESENT,
    MARK_TOO_LARGE,
    MARK_TRUNCATED,
    Article,
    Background,
    etf_info_input,
)

# 자료 종류. briefing.generated evidence의 kind 값과 같다. 배경 자료는 apiType을 그대로 쓴다.
KIND_ARTICLE = "article"
KIND_FILING = "filing"
KIND_CLOSE_RATE = "overseas-close-rate"

# 코드가 붙이는 근거 번호의 머리글. 기사는 articleId, 공시는 접수번호를 그대로 쓴다.
CLOSE_RATE_REF_PREFIX = "U"
BACKGROUND_REF_PREFIX = "B"
FIGURE_REF_PREFIX = "F"

# 배경 원문에서 꺼낸 숫자와 코드가 계산한 비교 숫자의 출처. 제공자 이름 대신 중립 표현을 쓴다.
FIGURE_SOURCE = "시세 자료(배경)"
COMPUTED_SOURCE = "시세 자료로 코드가 계산"
CLOSE_COMPUTED_SOURCE = "해외 시세 자료로 코드가 계산"

@dataclass(frozen=True)
class EvidenceItem:
    """근거 번호가 붙은 자료 하나. briefing.generated evidence 항목이 된다."""

    kind: str
    ref: str
    title: str
    source: str
    published_at: datetime
    url: str | None = None
    article_id: str | None = None


def move_headline(report_input: ReportInput, settings: CoreSettings) -> str:
    """머리말: 종목, 시각, 방향과 변동률, 몇 분 사이 움직였는지. 짧은 시간 변동은 오늘 등락을 먼저 쓴다."""
    i = report_input
    first = today_first(i, settings)
    if first is not None:
        return window_headline(i.symbol, i.previous_price, i.price, i.change_percent, first[0], first[1], i.as_of,
                               display_name(i), i.detected_at)
    high = high_today(i)
    if high is not None:
        return high_headline(i.symbol, i.previous_price, i.price, i.change_percent, high[0], high[1], i.as_of,
                             display_name(i))
    line = headline(i.symbol, i.previous_price, i.price, i.change_percent, i.as_of, display_name(i))
    if i.detection_type == "prevClose":
        return f"{line}, 전일 종가 대비"
    if i.detection_type == DETECTION_PREV_CLOSE_HIGH:
        # 상은 변동률이 전일 종가가 아니라 기준가(그날 처음엔 전일 종가, 상을 낸 뒤엔 그때 가격) 대비 값이다.
        return f"{line}, 기준가 대비"
    if i.detected_at is None or i.detection_type != "window":
        # 이전 형식은 움직인 구간을 알 수 없어 시간을 쓰지 않는다.
        return line
    minutes = window_minutes(i)
    if minutes > settings.window_move_max_minutes:
        # 오늘 등락을 먼저 쓰는 머리말과 같은 까닭으로, 구간 상한을 넘는 분은 거짓이라 쓰지 않는다.
        return f"{line}, 짧은 시간 사이"
    # window_minutes는 1분 미만도 1로 세므로, 1분이 안 된 변동만 따로 '1분 안에'로 쓴다.
    under_a_minute = i.detected_at - i.as_of < timedelta(minutes=1)
    return f"{line}, 1분 안에" if under_a_minute else f"{line}, {minutes}분 사이"


_UNCHECKED_STATUS = {MARK_MISSING: "기간 안에 받은 자료 없음", MARK_TRUNCATED: "상한 때문에 일부만 받음",
                     MARK_TOO_LARGE: "원문이 커서 빠짐"}
# 세계지수의 대상은 대륙 코드라 사람이 읽는 '확인하지 못한 것'에는 이름으로 적는다. 모르는 코드는 그대로 둔다.
WORLD_INDEX_CONTINENT_NAMES = {"C": "아메리카", "S": "아시아", "E": "유럽"}


def unchecked_items(report_input: ReportInput) -> list[str]:
    """받지 못하거나 잘린 자료를 코드가 상태 표시로 적는다. 종목코드와 티커는 쓰지 않는다."""
    i = report_input
    us = bool(i.tickers)
    items = []

    def add(name: str, status: str) -> None:
        if status in _UNCHECKED_STATUS:
            items.append(f"{name}: {_UNCHECKED_STATUS[status]}")

    add("시세 이력", _status(len(i.price_points), i.history_truncated))
    add("기사", _status(len(i.articles), i.articles_truncated))
    if not us:
        items.append("미국 공시·미국 종목 등락률: 변동 종목에 미국 종목이 없어 조회하지 않음")
    else:
        add("미국 공시", _status(len(i.filings), i.filings_truncated))
        if i.tickers_truncated:
            items.append("미국 종목 목록: 상한 때문에 일부만 받음")
        missing = len(set(i.tickers) - {r.ticker for r in i.close_rates})
        if missing:
            items.append(f"미국 종목 직전 정규장 등락률: {len(i.tickers)}개 가운데 {missing}개 받지 못함")
    for b in i.background:
        name = BACKGROUND_NAMES.get(b.api_type, b.api_type)
        if b.target and b.api_type == "world-index":
            name = f"{name} {WORLD_INDEX_CONTINENT_NAMES.get(b.target, b.target)}"
        add(name, BACKGROUND_STATUS_MARKS.get(b.status, MARK_MISSING))
    f = report_figures(i)
    present = {b.api_type for b in i.background if b.status == BACKGROUND_OK}
    if EXCHANGE_RATE_API in present and f.exchange_rate is None:
        items.append("원/달러 환율: 환율종합 원문에서 값을 찾지 못함")
    for label, figure in (("코스피종합", f.kospi), ("코스닥종합", f.kosdaq)):
        if MARKET_SUMMARY_API in present and figure is None:
            items.append(f"{label}: 시장종합 원문에서 값을 찾지 못함")
    return items


# ---- LLM ----

def _close_rate_refs(i: ReportInput) -> list[str]:
    return [f"{CLOSE_RATE_REF_PREFIX}{n}" for n in range(1, len(i.close_rates) + 1)]


def _background_refs(i: ReportInput) -> list[str | None]:
    """원문이 있는 배경 자료에만 번호를 붙인다. 없음·너무 큼은 인용할 내용이 없다."""
    refs: list[str | None] = []
    number = 0
    for item in i.background:
        if item.status == BACKGROUND_OK:
            number += 1
            refs.append(f"{BACKGROUND_REF_PREFIX}{number}")
        else:
            refs.append(None)
    return refs


def _background_title(item: Background) -> str:
    name = BACKGROUND_NAMES.get(item.api_type, item.api_type)
    # 국내 일봉의 대상은 변동 종목 자신이라 제목에 다시 쓰지 않는다.
    target = f" {item.target}" if item.target and item.api_type == "world-index" else ""
    return f"{name}{target} ({BACKGROUND_LABEL})"


def evidence_catalog(report_input: ReportInput, settings: CoreSettings = DEFAULT_SETTINGS) -> dict[str, EvidenceItem]:
    """LLM이 인용할 수 있는 근거 번호와 자료. LLM 입력과 출력 검증이 같은 목록을 쓴다."""
    i = report_input
    catalog: dict[str, EvidenceItem] = {}
    for a in i.articles + i.research_articles:
        catalog[a.article_id] = EvidenceItem(KIND_ARTICLE, a.article_id, a.title, a.publisher, a.published_at,
                                             a.url, a.article_id)
    # 본문이 회사를 티커로 말하므로 근거 목록에서도 어느 회사 자료인지 구분되게 제목에 티커를 넣는다.
    # 리포트는 받는 사람의 메일로만 가고, 로그에는 제목을 남기지 않는다.
    for f in i.filings:
        items = f" 항목 {', '.join(f.items)}" if f.items else ""
        catalog[f.accession_number] = EvidenceItem(
            KIND_FILING, f.accession_number, f"{', '.join(f.tickers)} {f.form} 공시{items}", FILING_SOURCE,
            f.accepted_at, f.url)
    for ref, r in zip(_close_rate_refs(i), i.close_rates):
        catalog[ref] = EvidenceItem(KIND_CLOSE_RATE, ref, f"{r.ticker} 직전 정규장 등락률 {r.rate}% ({r.date})",
                                    CLOSE_RATE_SOURCE, r.close_at)
    for ref, b in zip(_background_refs(i), i.background):
        if ref is not None:
            catalog[ref] = EvidenceItem(b.api_type, ref, _background_title(b), BACKGROUND_SOURCE, b.fetched_at)
    for figure in figure_entries(i, settings):
        if figure.ref is not None:
            catalog[figure.ref] = EvidenceItem(figure.kind, figure.ref, figure.title, figure.source, figure.at)
    return catalog


# ---- 배경 숫자와 비교 숫자 ----

@dataclass(frozen=True)
class FigureEntry:
    """LLM 입력의 숫자 항목 하나. 값이 하나도 없으면 ref가 없고 인용할 수 없다."""

    name: str
    kind: str
    ref: str | None
    title: str
    source: str
    at: datetime | None
    values: dict[str, str]


def _figure_candidates(f: MoveFigures, compared: str) -> list[tuple[str, str, str, datetime | None, dict[str, str]]]:
    """(이름, kind, 출처, 기준 시각, 값) 목록. 순서가 근거 번호 순서다. compared는 지수와 비교한 등락률의 이름이다."""
    rows: list[tuple[str, str, str, datetime | None, dict[str, str]]] = []
    fx = f.exchange_rate
    rows.append(("원/달러 환율", EXCHANGE_RATE_API, FIGURE_SOURCE, fx.fetched_at if fx else None, {
        "value": display(fx.value if fx else None),
        "change": display(fx.change if fx else None, sign=True),
        "ratePercent": display(fx.rate if fx else None, sign=True),
        "receivedTime": display(fx.received_time if fx else None),
    }))
    for label, index in (("코스피종합", f.kospi), ("코스닥종합", f.kosdaq)):
        rows.append((label, MARKET_SUMMARY_API, FIGURE_SOURCE, index.fetched_at if index else None, {
            "value": display(index.value if index else None),
            "change": display(index.change if index else None, sign=True),
            "ratePercent": display(index.rate if index else None, sign=True),
        }))
    for label, index, diff in (("코스피종합", f.kospi, f.vs_kospi), ("코스닥종합", f.kosdaq, f.vs_kosdaq)):
        values = {"differencePoint": display(diff, sign=True)}
        if diff is not None and f.stale_index_at is not None:
            values["indexReceivedAt"] = f"{f.stale_index_at.astimezone(KST):%H:%M}"
        rows.append((f"{compared} - {label} 등락률", MARKET_SUMMARY_API, COMPUTED_SOURCE,
                     index.fetched_at if index else None, values))
    us = f.us_close
    rows.append(("미국 종목 직전 정규장 등락 집계", KIND_CLOSE_RATE, CLOSE_COMPUTED_SOURCE,
                 us.latest_close_at if us else None, {
                     "up": display(us.up if us else None),
                     "down": display(us.down if us else None),
                     "flat": display(us.flat if us else None),
                     "averageRatePercent": display(us.average if us else None, sign=True),
                 }))
    return rows


_VALUE_LABELS = {"value": "값", "change": "전일대비", "ratePercent": "등락률(%)", "receivedTime": "최종 수신",
                 "differencePoint": "차이(%p)", "indexReceivedAt": "지수 받은 시각", "up": "오름", "down": "내림", "flat": "보합",
                 "averageRatePercent": "평균 등락률(%)"}


def figure_entries(report_input: ReportInput, settings: CoreSettings = DEFAULT_SETTINGS) -> list[FigureEntry]:
    """배경 원문에서 꺼낸 숫자와 비교 숫자. 값이 있는 항목에만 F1, F2 … 순번을 붙인다."""
    i = report_input
    entries = []
    number = 0
    for name, kind, source, at, values in _figure_candidates(report_figures(i, settings), compared_label(i)):
        present = at is not None and any(v != MARK_MISSING for v in values.values())
        ref = None
        if present:
            number += 1
            ref = f"{FIGURE_REF_PREFIX}{number}"
        title = f"{name} " + ", ".join(f"{_VALUE_LABELS[k]} {v}" for k, v in values.items())
        entries.append(FigureEntry(name, kind, ref, title, source, at, values))
    return entries


def _figure_input(entry: FigureEntry, as_of: datetime) -> dict[str, Any]:
    data: dict[str, Any] = {"name": entry.name, "status": MARK_PRESENT if entry.ref else MARK_MISSING}
    if entry.ref is None:
        return data
    data["ref"] = entry.ref
    data["source"] = entry.source
    data.update(entry.values)
    data["basedOn"] = _at(entry.at)
    data["minutesBeforeMove"] = int((as_of - entry.at).total_seconds() // 60)
    return data


def _status(count: int, truncated: bool, applicable: bool = True) -> str:
    if not applicable:
        return MARK_NOT_APPLICABLE
    if truncated:
        return MARK_TRUNCATED
    return MARK_PRESENT if count else MARK_MISSING


def _hours_before(at: datetime, as_of: datetime) -> float:
    return round((as_of - at) / timedelta(hours=1), 1)


def _at(value: datetime) -> str:
    return format_timestamp(value.astimezone(KST))


def build_llm_input(report_input: ReportInput, settings: CoreSettings = DEFAULT_SETTINGS, *,
                    group: bool = True) -> dict[str, Any]:
    """LLM에게 줄 공개 정보만 고른다. 보유 수량·금액 같은 개인 정보는 ReportInput에도 없고 여기서도 넣지 않는다."""
    i = report_input
    us = bool(i.tickers)
    rated = {r.ticker for r in i.close_rates}
    # 검색어 입력처럼 기사 칸을 쓰지 않는 곳은 group=False로 묶기를 건너뛴다.
    articles, research_articles = article_groups(i, settings).lists if group else (
        [_article_input(a, i.as_of) for a in i.articles], [_article_input(a, i.as_of) for a in i.research_articles])
    data: dict[str, Any] = {
        "symbol": i.symbol,
        "direction": direction_of(i.change_percent),
        "changePercent": round(i.change_percent, 2),
        "moveAt": _at(i.as_of),
        "priceHistoryStatus": _status(len(i.price_points), i.history_truncated),
        "priceHistory": [{"at": _at(p.as_of), "price": p.price} for p in i.price_points],
        "articlesStatus": _status(len(i.articles), i.articles_truncated),
        "articles": articles,
        "filingsStatus": _status(len(i.filings), i.filings_truncated, us),
        "filings": [
            {
                "ref": f.accession_number,
                "tickers": list(f.tickers),
                "form": f.form,
                "items": list(f.items),
                "acceptedAt": _at(f.accepted_at),
                "hoursBeforeMove": _hours_before(f.accepted_at, i.as_of),
            }
            for f in i.filings
        ],
        # 미국 종목 목록이 잘렸으면 등락률도 일부만 있다.
        "overseasCloseRatesStatus": _status(len(i.close_rates), i.tickers_truncated, us),
        "overseasCloseRates": [
            {
                "ref": ref,
                "ticker": r.ticker,
                "date": r.date,
                "rate": r.rate,
                "closeAt": _at(r.close_at),
                "hoursBeforeMove": _hours_before(r.close_at, i.as_of),
                "receivedAt": _at(r.fetched_at),
            }
            for ref, r in zip(_close_rate_refs(i), i.close_rates)
        ],
        "overseasCloseRatesMissing": [t for t in i.tickers if t not in rated],
        "background": [_background_input(ref, b, i.as_of) for ref, b in zip(_background_refs(i), i.background)],
        # 배경 원문에서 꺼낸 숫자와 코드가 계산한 비교 숫자. LLM은 계산하지 않고 ref로 인용만 한다.
        "figures": [_figure_input(e, i.as_of) for e in figure_entries(i, settings)],
    }
    # 오늘 등락률은 짧은 시간·기준가 대비 변동일 때만 넣는다. 다른 유형의 입력 모양은 바꾸지 않는다.
    if compares_today(i) and i.today_change_percent is not None:
        data["todayChangePercent"] = round(i.today_change_percent, 2)
    # 같은 날 앞선 급변동은 있을 때만 넣는다(없으면 입력 모양이 그대로다).
    if i.earlier_moves:
        data["todayFlow"] = flow_input(i.earlier_moves)
    # ETF 정보는 응답에 있을 때만 넣는다. 정보가 없는 이전 형식 응답의 입력 모양은 바꾸지 않는다.
    if i.etf is not None:
        data["etf"] = etf_info_input(i.etf, settings.max_etf_members)
    # 재검색 기사는 다시 쓸 때만 있다. 첫 작성의 입력 모양은 바꾸지 않는다.
    if i.research_articles:
        data["researchArticlesStatus"] = _status(len(i.research_articles), i.research_articles_truncated)
        data["researchArticles"] = research_articles
    return data


def article_groups(report_input: ReportInput, settings: CoreSettings = DEFAULT_SETTINGS) -> GroupedArticles:
    """처음 모은 기사와 재검색 기사를 함께 같은 사건끼리 묶는다. 둘 다 이미 시간 범위로 거른 기사다."""
    i = report_input
    return group_article_lists(
        [[ArticleRow(_article_input(a, i.as_of), a.title, a.published_at) for a in i.articles],
         [ArticleRow(_article_input(a, i.as_of), a.title, a.published_at) for a in i.research_articles]],
        settings, marks=({}, RESEARCH_MARK))


def _article_input(a: Article, as_of: datetime) -> dict[str, Any]:
    return {
        "ref": a.article_id,
        "articleId": a.article_id,
        "title": a.title,
        "summary": a.summary,
        "source": a.publisher,
        "publishedAt": _at(a.published_at),
        # 내림으로 계산해 음수는 언제나 변동 전에 나온 기사를 뜻하게 한다.
        "minutesFromMove": int((a.published_at - as_of).total_seconds() // 60),
    }


def _background_input(ref: str | None, item: Background, as_of: datetime) -> dict[str, Any]:
    data: dict[str, Any] = {
        "category": BACKGROUND_LABEL,
        "name": BACKGROUND_NAMES.get(item.api_type, item.api_type),
        "apiType": item.api_type,
        "target": item.target,
        "status": BACKGROUND_STATUS_MARKS.get(item.status, MARK_MISSING),
    }
    if ref is not None:
        data["ref"] = ref
    if item.fetched_at is not None:
        data["receivedAt"] = _at(item.fetched_at)
        data["minutesBeforeMove"] = int((as_of - item.fetched_at).total_seconds() // 60)
    if item.response is not None:
        data["response"] = item.response
    if item.response_bytes is not None:
        data["responseBytes"] = item.response_bytes
    if item.index_rates:
        data["indexRates"] = [
            {
                "code": r.code,
                "name": r.name,
                "businessDay": r.business_day,
                "close": r.close,
                "prevBusinessDay": r.prev_business_day if r.prev_business_day is not None else MARK_MISSING,
                "prevClose": r.prev_close if r.prev_close is not None else MARK_MISSING,
                "rate": r.rate if r.rate is not None else MARK_MISSING,
            }
            for r in item.index_rates
        ]
    return data


def display_name(report_input: ReportInput) -> str | None:
    """응답의 종목 이름을 먼저 쓰고, 없거나 비었으면 ETF 이름을 쓴다. 둘 다 없으면 None."""
    for name in (report_input.name, report_input.etf.name if report_input.etf else None):
        if name and name.strip():
            return name.strip()
    return None


_TIME_LABELS = {KIND_ARTICLE: "발행", KIND_FILING: "접수", KIND_CLOSE_RATE: "마감"}


def _describe(item: EvidenceItem, with_url: bool = True) -> str:
    label = _TIME_LABELS.get(item.kind, "받음")
    text = f"{item.title} — {item.source}, {label} {item.published_at.astimezone(KST):%m-%d %H:%M}"
    return f"{text} {item.url}" if item.url and with_url else text
