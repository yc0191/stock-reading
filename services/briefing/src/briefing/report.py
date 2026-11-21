"""리포트 입력(ReportInput)으로 briefing.generated를 만든다. Kafka는 모르고, 발행은 포트로 넘긴다.

코드가 LLM 입력을 정하고 출력을 검증한다. LLM은 원인마다 제목, 구체 문장, 뜻 문장과 코드가 붙인 근거 번호(기사·공시·
해외 등락률·배경 자료·비교 숫자)만 만들고, 머리말·오늘 흐름·근거 목록·면책 문구·변동률과 비교 숫자 계산은 코드가 맡는다.
메일은 짧게 쓴다: 원인은 3개까지, 원인마다 제목과 두 문장(구체 문장 하나, 뜻 문장 하나)이며 근거 목록은 맨 아래에 한 번만 둔다.
받지 못한 자료('확인하지 못한 것')는 메일에 싣지 않고 작업 기록(job_log)에만 남긴다.
"""
from __future__ import annotations

import json
import logging
import unicodedata
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, Protocol

from briefing.article_groups import RESEARCH_MARK, ArticleRow, GroupedArticles, group_article_lists
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.compose import DISCLAIMER, KST, NO_EVIDENCE, direction_of, headline, high_headline, window_headline
from briefing.events import BRIEFING_GENERATED_TOPIC, OutboundEvent, format_timestamp
from briefing.jobs import ReportInput
from briefing.job_log import (
    DETECTION_PREV_CLOSE_HIGH,
    REASON_LLM_ACCEPTED,
    REASON_LLM_CALL_FAILED,
    REASON_LLM_REJECTED,
    REASON_NO_EVIDENCE,
    REASON_PUBLISHED,
    STAGE_LLM,
    STAGE_PUBLISH,
    DiscardJobLog,
    EntryType,
    GeneratedBy,
    JobLogEntry,
    JobLogPort,
    JobResult,
    RunStatus,
    record_result,
)
from briefing.move_figures import (
    BASE_EARLIER_MOVE,
    BASE_PREV_CLOSE,
    EXCHANGE_RATE_API,
    MARKET_SUMMARY_API,
    MoveFigures,
    comparison_facts,
    compared_label,
    compares_today,
    display,
    high_today,
    report_figures,
    today_first,
)
from briefing.move_flow import flow_input
from briefing.move_prompts import OUTPUT_SCHEMA, REWRITE_ADDENDUM, SYSTEM_PROMPT, system_prompt  # noqa: F401 - 다른 모듈이 여기서 가져온다
from briefing.move_mail import build_move_mail
from briefing.move_view import MoveView, MoveViewCause, MoveViewEvidence, view_text
from briefing.llm_output import (
    InvalidLlmOutput,
    check_items_present,
    output_cited_refs,
    output_item,
    check_one_sentence,
    output_optional_sentence,
    output_sentence,
    output_weak_shortfall,
    check_no_ticker,
    parse_output_object,
    ticker_pattern,
)
from briefing.llm import (API_KEY_ENV, AcceptedAttempt, AnthropicClient, LlmCallError, LlmClient, LlmUsage,
                          attempt_usage,
                          llm_config, usage_record)
from briefing.ports import FailureReason, RequestPort
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

log = logging.getLogger("briefing.report")

GENERATED_BY_RULE = GeneratedBy.RULE_BASED.value
GENERATED_BY_LLM = GeneratedBy.LLM.value
LLM_MAX_ATTEMPTS = 3
RULE_BASED_LIMIT = 3

LLM_INSUFFICIENT = "변동 시점 전후에 공개된 자료만으로는 변동 원인을 추정하기 어렵습니다."
# 원인은 3개까지, 문장 칸(구체·뜻·시각 차이)은 한 문장씩이라 원인마다 두 문장 안팎으로 끝난다.
MAX_CAUSES = 3
RULE_BASED_NOTE = "원인은 추정하지 않고, 코드가 계산한 비교 사실과 변동 시점 전후의 자료를 나열합니다."
COMPARISON_TITLE = "비교 사실:"
LLM_DISCLAIMER = "※ 투자 조언이 아니며, 위 내용은 변동 시점 전후에 공개된 자료로 추정한 것일 뿐 원인을 단정하지 않습니다."

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


@dataclass(frozen=True)
class LlmAttempt:
    """실패한 LLM 시도 하나. 실패 기록에 그대로 담는다."""

    attempt: int
    llm_input: str
    # LLM의 원래 응답. 호출 자체가 실패했으면 None이다.
    response: str | None
    rejection: str
    error_type: str
    # 응답을 받지 못한 호출 실패는 None이다.
    usage: LlmUsage | None = None


@dataclass(frozen=True)
class Report:
    text: str
    generated_by: str
    evidence: tuple[EvidenceItem, ...]
    # 끝내 성공했더라도 그 전에 실패한 LLM 시도들
    failed_attempts: tuple[LlmAttempt, ...] = ()
    # 받아들인 LLM 시도(규칙 기반이면 None)
    accepted: AcceptedAttempt | None = None
    # LLM이 근거 부족(insufficientEvidence true)을 냈다. 재검색 여부를 정하는 데 쓴다.
    insufficient_evidence: bool = False
    # LLM이 근거가 약하다고 한 원인들의 부족한 점. 재검색 여부를 정하고 검색어 LLM 입력에 들어간다.
    weak_shortfalls: tuple[str, ...] = ()
    # 받아들인 LLM 출력. 문장 하나만 다시 쓸 때 나머지 원인을 그대로 두고 본문을 다시 만드는 데 쓴다(규칙 기반이면 None).
    output: LlmOutput | None = None
    # 메일 내용. 글자판(text)과 HTML이 같은 값에서 나온다.
    view: MoveView | None = None


class ReportError(Exception):
    """리포트를 끝내 만들지 못했다. 메시지에는 짧은 사유만 담는다."""

    def __init__(self, detail: str, attempts: int, failed_attempts: tuple[LlmAttempt, ...] = ()) -> None:
        super().__init__(detail)
        self.detail = detail
        self.attempts = attempts
        self.failed_attempts = failed_attempts


class ReportWriter(Protocol):
    def write(self, report_input: ReportInput) -> Report: ...


class InvalidOutput(InvalidLlmOutput):
    pass


@dataclass(frozen=True)
class Cause:
    """원인 하나의 묶음. detail은 refs를, meaning은 meaning_refs를 인용한다."""

    title: str
    detail: str
    refs: tuple[str, ...]
    meaning: str
    meaning_refs: tuple[str, ...]
    # 근거가 약하다고 한 원인의 부족한 점. 약하지 않으면 None.
    shortfall: str | None = None
    # 기사끼리 수치·해석이 엇갈릴 때의 한 문장('시각 차이' 줄)과 그 근거. 엇갈리지 않으면 None과 빈 목록.
    conflict: str | None = None
    conflict_refs: tuple[str, ...] = ()

    def cited(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(self.refs + self.meaning_refs + self.conflict_refs))


@dataclass(frozen=True)
class LlmOutput:
    causes: tuple[Cause, ...]
    insufficient_evidence: bool


# ---- 규칙 기반 ----

@dataclass(frozen=True)
class RuleBasedWriter:
    """원인을 추정하지 않고 비교 사실과 최근 기사·공시를 나열한다. LLM 키가 없을 때 쓰는 기본 구현이다."""

    limit: int = RULE_BASED_LIMIT
    settings: CoreSettings = DEFAULT_SETTINGS

    def write(self, report_input: ReportInput) -> Report:
        i = report_input
        catalog = evidence_catalog(i, self.settings)
        evidence = tuple(
            item
            for kind in (KIND_ARTICLE, KIND_FILING)
            for item in sorted((e for e in catalog.values() if e.kind == kind),
                               key=lambda e: e.published_at, reverse=True)[: self.limit]
        )
        notes = [RULE_BASED_NOTE]
        facts = comparison_facts(i, self.settings)
        if facts:
            notes.append(COMPARISON_TITLE)
            notes.extend(f"- {fact}" for fact in facts)
        if not evidence:
            notes.append(NO_EVIDENCE)
        view = MoveView(move_headline(i), tuple(m.line() for m in i.earlier_moves), tuple(notes),
                        evidence_title="근거 후보:",
                        evidence=tuple(_view_evidence(n, e) for n, e in enumerate(evidence, start=1)),
                        closing=DISCLAIMER)
        return Report(view_text(view), GENERATED_BY_RULE, evidence, view=view)


def move_headline(report_input: ReportInput) -> str:
    """머리말: 종목, 시각, 방향과 변동률, 몇 분 사이 움직였는지. 짧은 시간 변동은 오늘 등락을 먼저 쓴다."""
    i = report_input
    first = today_first(i)
    if first is not None:
        return window_headline(i.symbol, i.previous_price, i.price, i.change_percent, first[0], first[1], i.as_of,
                               display_name(i))
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
    minutes = int((i.detected_at - i.as_of).total_seconds() // 60)
    return f"{line}, {minutes}분 사이" if minutes >= 1 else f"{line}, 1분 안에"


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


def parse_output(text: str, allowed_ids: frozenset[str], forbidden_tickers: frozenset[str] = frozenset(),
                 settings: CoreSettings = DEFAULT_SETTINGS) -> LlmOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나, 빈 문장이 있거나, 문장이 근거를 인용하지 않거나,
    준 적 없는 근거 번호를 인용하거나, LLM 입력에 있던 미국 티커를 문장에 쓰거나, 원인 칸이 글자 수 상한을 넘으면 InvalidOutput.
    근거 목록의 제목에 붙는 티커(공시)는 코드가 쓰므로 검사하지 않는다."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "causes", InvalidOutput)
    causes = []
    for raw in data["causes"]:
        item = output_item(raw, "cause", InvalidOutput)
        cause = Cause(
            title=output_sentence(item, "title", "cause", InvalidOutput),
            detail=output_sentence(item, "detail", "cause", InvalidOutput),
            refs=output_cited_refs(item, "refs", "cause", allowed_ids, InvalidOutput),
            meaning=output_sentence(item, "meaning", "cause", InvalidOutput),
            meaning_refs=output_cited_refs(item, "meaningRefs", "cause", allowed_ids, InvalidOutput),
            shortfall=output_weak_shortfall(item, "cause", InvalidOutput),
            conflict=output_optional_sentence(item, "conflict", "cause", InvalidOutput),
        )
        if cause.conflict is not None:
            cause = replace(cause, conflict_refs=output_cited_refs(item, "conflictRefs", "cause", allowed_ids,
                                                                  InvalidOutput))
        elif item.get("conflictRefs"):
            raise InvalidOutput("cause has conflictRefs without conflict")
        for sentence in (cause.detail, cause.meaning, cause.conflict):
            if sentence is not None:
                check_one_sentence(sentence, "cause sentence", InvalidOutput)
                # 자모를 나눈 형태(NFD)로 와도 한 글자를 한 자로 세도록 합친 형태로 센다.
                if len(unicodedata.normalize("NFC", sentence)) > settings.max_cause_chars:
                    raise InvalidOutput(f"cause sentence is longer than {settings.max_cause_chars} characters")
        for sentence in (cause.title, cause.detail, cause.meaning, cause.conflict):
            if sentence is not None:
                check_no_ticker(sentence, tickers, "cause", InvalidOutput)
        if cause.shortfall is not None:
            check_no_ticker(cause.shortfall, tickers, "cause shortfall", InvalidOutput)
        causes.append(cause)
    if len(causes) > MAX_CAUSES:
        raise InvalidOutput(f"more than {MAX_CAUSES} causes")
    insufficient = check_items_present(data, "causes", "cause", InvalidOutput)
    return LlmOutput(tuple(causes), insufficient)


@dataclass(frozen=True)
class SentenceRewrite:
    """원인 하나를 다시 쓴 결과. cause가 None이면 끝내 실패했고 원래 원인을 그대로 둔다."""

    cause: Cause | None
    failed_attempts: tuple[LlmAttempt, ...]
    accepted: AcceptedAttempt | None = None


def _cause_input(cause: Cause) -> dict[str, Any]:
    return {"title": cause.title, "detail": cause.detail, "refs": list(cause.refs), "meaning": cause.meaning,
            "meaningRefs": list(cause.meaning_refs)}


@dataclass(frozen=True)
class LlmWriter:
    client: LlmClient
    max_attempts: int = LLM_MAX_ATTEMPTS
    settings: CoreSettings = DEFAULT_SETTINGS

    def write(self, report_input: ReportInput) -> Report:
        user = json.dumps(build_llm_input(report_input, self.settings), ensure_ascii=False)
        groups = article_groups(report_input, self.settings).record()
        allowed = frozenset(evidence_catalog(report_input, self.settings))
        tickers = us_symbols_in_input(report_input)
        reason = ""
        failed: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            response: str | None = None
            usage: LlmUsage | None = None
            try:
                reply = self.client.complete(system_prompt(self.settings), user, OUTPUT_SCHEMA)
                response, usage = reply.text, reply.usage
                output = parse_output(response, allowed, tickers, self.settings)
            except (LlmCallError, InvalidOutput) as error:
                reason = str(error)
                failed.append(LlmAttempt(attempt, user, response, reason, type(error).__name__,
                                         attempt_usage(usage, error)))
                log.warning("LLM report attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, report_input.job_id, reason)
                continue
            return replace(llm_report(report_input, output), failed_attempts=tuple(failed),
                           accepted=AcceptedAttempt(attempt, user, response, usage, groups))
        raise ReportError(f"LLM report failed after {self.max_attempts} attempts: {reason}", self.max_attempts,
                          tuple(failed))

    def rewrite_cause(self, report_input: ReportInput, current: Cause, no: int,
                      new_refs: tuple[str, ...]) -> SentenceRewrite:
        """근거가 약한 원인 하나만 다시 쓴다. 나머지 원인은 건드리지 않는다."""
        return _rewrite_cause(self, report_input, current, no, new_refs)


def _rewrite_cause(writer: LlmWriter, report_input: ReportInput, current: Cause, no: int,
                   new_refs: tuple[str, ...]) -> SentenceRewrite:
    data = build_llm_input(report_input, writer.settings)
    data["rewrite"] = {"sentenceNo": no, "sentence": _cause_input(current), "shortfall": current.shortfall,
                       "newRefs": list(new_refs)}
    user = json.dumps(data, ensure_ascii=False)
    groups = article_groups(report_input, writer.settings).record()
    allowed = frozenset(evidence_catalog(report_input, writer.settings))
    tickers = us_symbols_in_input(report_input)
    failed: list[LlmAttempt] = []
    for attempt in range(1, writer.max_attempts + 1):
        response: str | None = None
        usage: LlmUsage | None = None
        try:
            reply = writer.client.complete(system_prompt(writer.settings), user, OUTPUT_SCHEMA, REWRITE_ADDENDUM)
            response, usage = reply.text, reply.usage
            output = parse_output(response, allowed, tickers, writer.settings)
            if output.insufficient_evidence or len(output.causes) != 1:
                raise InvalidOutput("rewrite must return exactly one cause")
        except (LlmCallError, InvalidOutput) as error:
            failed.append(LlmAttempt(attempt, user, response, str(error), type(error).__name__,
                                     attempt_usage(usage, error)))
            log.warning("LLM sentence rewrite attempt %d/%d failed for job %s: %s",
                        attempt, writer.max_attempts, report_input.job_id, error)
            continue
        return SentenceRewrite(output.causes[0], tuple(failed),
                               AcceptedAttempt(attempt, user, response, usage, groups))
    return SentenceRewrite(None, tuple(failed))


def us_symbols_in_input(i: ReportInput) -> frozenset[str]:
    """LLM 입력에 있던 미국 티커. 문장에 쓰면 거부한다."""
    return frozenset(i.tickers) | frozenset(r.ticker for r in i.close_rates) \
        | frozenset(t for f in i.filings for t in f.tickers) | (i.etf.us_tickers() if i.etf else frozenset())


def llm_report(report_input: ReportInput, output: LlmOutput) -> Report:
    i = report_input
    by_id = evidence_catalog(i)
    flow = tuple(m.line() for m in i.earlier_moves)
    if output.insufficient_evidence:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않는다. 억지 원인이 섞이지 않게 한다.
        view = MoveView(move_headline(i), flow, (LLM_INSUFFICIENT,), closing=LLM_DISCLAIMER)
        return Report(view_text(view), GENERATED_BY_LLM, (), insufficient_evidence=True, output=output, view=view)

    cited: list[str] = []
    for cause in output.causes:
        for ref in cause.cited():
            if ref not in cited:
                cited.append(ref)
    # 근거 번호는 리포트 전체에서 하나로 매기고, 근거 목록은 맨 아래에 한 번만 보여 준다.
    number = {ref: index for index, ref in enumerate(cited, start=1)}
    causes = tuple(MoveViewCause(
        c.title, c.detail, _numbers(c.refs, number), c.meaning, _numbers(c.meaning_refs, number),
        c.conflict, _numbers(c.conflict_refs, number)) for c in output.causes)
    view = MoveView(move_headline(i), flow, (), causes,
                    evidence=tuple(_view_evidence(number[ref], by_id[ref]) for ref in cited), closing=LLM_DISCLAIMER)
    return Report(view_text(view), GENERATED_BY_LLM, tuple(by_id[x] for x in cited),
                  weak_shortfalls=tuple(c.shortfall for c in output.causes if c.shortfall), output=output, view=view)


def _numbers(refs: tuple[str, ...], number: dict[str, int]) -> tuple[int, ...]:
    return tuple(number[x] for x in refs)


def _view_evidence(no: int, item: EvidenceItem) -> MoveViewEvidence:
    return MoveViewEvidence(no, _describe(item, with_url=False), item.url)


def build_report_writer(env: Mapping[str, str],
                        client_factory: Callable[[str, str], LlmClient] = AnthropicClient,
                        settings: CoreSettings = DEFAULT_SETTINGS) -> ReportWriter:
    """시작할 때 한 번 부른다. 키가 없어도 시작을 막지 않고 규칙 기반으로 바꾼다."""
    config = llm_config(env)
    if config is None:
        log.warning("%s is not set; briefing reports will be rule-based", API_KEY_ENV)
        return RuleBasedWriter(settings=settings)
    log.info("briefing reports will be written by the LLM (model=%s)", config.model)
    return LlmWriter(client_factory(config.api_key, config.model), settings=settings)


# ---- 발행 ----

def briefing_event(report_input: ReportInput, report: Report, now: datetime, html: str | None = None,
                   images: list[dict[str, str]] | None = None) -> OutboundEvent:
    """html·images는 선택 칸이라 있을 때만 싣는다(데일리 판과 같은 모양). 글자판(text)은 언제나 함께 싣는다."""
    i = report_input
    as_of = format_timestamp(i.as_of)
    return OutboundEvent(
        topic=BRIEFING_GENERATED_TOPIC,
        key=i.symbol,
        value={
            # 결정적 ID: 같은 변동을 다시 처리해도 같은 브리핑 ID가 나와 소비자가 중복을 걸러낼 수 있다.
            "eventId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"briefing:{i.job_id}")),
            "eventType": "briefing.generated",
            "schemaVersion": 1,
            "occurredAt": as_of,
            "producedAt": format_timestamp(now),
            "correlationId": i.correlation_id,
            "payload": {
                "symbol": i.symbol,
                "changePercent": i.change_percent,
                "asOf": as_of,
                "text": report.text,
                "generatedBy": report.generated_by,
                "evidence": [_evidence_entry(e) for e in report.evidence],
                **_name_entry(i),
                **_today_first_entry(i),
                **({"html": html} if html else {}),
                **({"images": images} if html and images else {}),
            },
        },
    )


def display_name(report_input: ReportInput) -> str | None:
    """응답의 종목 이름을 먼저 쓰고, 없거나 비었으면 ETF 이름을 쓴다. 둘 다 없으면 None."""
    for name in (report_input.name, report_input.etf.name if report_input.etf else None):
        if name and name.strip():
            return name.strip()
    return None


def _today_first_entry(report_input: ReportInput) -> dict[str, float | int | str]:
    """메일 제목도 머리말처럼 오늘 등락을 먼저 쓰도록 같은 값을 넘긴다. 상은 baseKind로 기준가 종류(전일 종가·앞선 상)도 알린다. 머리말이 이전 모양이면 칸을 빼고,
    분을 쓰지 않는 머리말('짧은 시간')이면 moveMinutes만 뺀다."""
    high = high_today(report_input)
    if high is not None:
        return {"todayChangePercent": high[0], "baseKind": BASE_PREV_CLOSE if high[1] else BASE_EARLIER_MOVE}
    first = today_first(report_input)
    if first is None:
        return {}
    return {"todayChangePercent": first[0]} | ({} if first[1] is None else {"moveMinutes": first[1]})


def _name_entry(report_input: ReportInput) -> dict[str, str]:
    """이름을 알 때만 name 칸을 담는다. 모르거나 비었으면 칸을 뺀다."""
    name = display_name(report_input)
    return {"name": name} if name else {}


@dataclass
class ReportStage:
    """BriefingCore의 ReadyPort 구현. 리포트를 만들면 결과를 기록한 뒤 발행하고, 끝내 못 만들면 llm_error로 기록한다."""

    writer: ReportWriter
    publisher: RequestPort
    job_log: JobLogPort = field(default_factory=DiscardJobLog)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))
    settings: CoreSettings = DEFAULT_SETTINGS

    def on_ready(self, report_input: ReportInput) -> None:
        report = self.write(report_input)
        if report is not None:
            self.publish(report_input, report)

    def write(self, report_input: ReportInput, fail_job: bool = True) -> Report | None:
        """리포트를 만든다. 끝내 못 만들면 llm_error로 기록하고 None.

        fail_job이 False이면 작업을 실패로 닫지 않고 시도 기록만 남긴다(이미 낼 수 있는 보고서가 있을 때).
        """
        try:
            report = self.writer.write(report_input)
        except ReportError as error:
            log.warning("job %s failed: %s", report_input.job_id, FailureReason.LLM_ERROR.value)
            now = self.clock()
            self._record_attempts(report_input, error.failed_attempts, now)
            self._record(report_input, EntryType.STEP_FAILED, FailureReason.LLM_ERROR.value, "ReportError",
                         error.attempts, now, {"detail": error.detail,
                                               "failedAttempts": [_attempt_data(a) for a in error.failed_attempts]})
            if fail_job:
                record_result(self.job_log, JobResult(
                    report_input.job_id, RunStatus.FAILED, now, failure_reason=FailureReason.LLM_ERROR.value))
            return None
        now = self.clock()
        self._record_attempts(report_input, report.failed_attempts, now)
        if report.accepted is not None:
            self._record_accepted(report_input, report.accepted, now)
        return report

    def rewrite_sentence(self, report_input: ReportInput, report: Report, sentence_no: int, round: int,
                         new_refs: tuple[str, ...]) -> Report | None:
        """근거가 약한 원인 하나만 다시 쓰고 본문을 다시 만든다. 끝내 못 쓰면 None(원래 보고서를 그대로 둔다).

        report_input에는 이 원인을 위해 새로 찾은 기사까지 이미 들어 있다.
        """
        rewrite = getattr(self.writer, "rewrite_cause", None)
        if rewrite is None or report.output is None:
            return None
        current = report.output.causes[sentence_no - 1]
        result = rewrite(report_input, current, sentence_no, new_refs)
        now = self.clock()
        self._record_attempts(report_input, result.failed_attempts, now, sentence_no, round)
        if result.cause is None:
            return None
        self._record_accepted(report_input, result.accepted, now, sentence_no, round)
        causes = list(report.output.causes)
        causes[sentence_no - 1] = result.cause
        return llm_report(report_input, LlmOutput(tuple(causes), False))

    def publish(self, report_input: ReportInput, report: Report, research_count: int = 0) -> None:
        """최종 결과를 job_run에 적고, 이 작업의 줄이 모두 저장됐을 때만 발행한다."""
        now = self.clock()
        if not report.evidence:
            self._skip_without_evidence(report_input, report, research_count, now)
            return
        i = report_input
        event, mail_notes = self._event(i, report, now)
        # 받지 못한 자료와 차트가 빠진 사유는 메일 본문에 싣지 않고 작업 기록에만 남긴다.
        missing = [*unchecked_items(i), *mail_notes]
        if missing:
            self._record(i, EntryType.MISSING_ITEMS, REASON_PUBLISHED, None, 0, now,
                         {"evidence": event.value["payload"]["evidence"], "weakItems": list(report.weak_shortfalls),
                          "missingItems": missing},
                         stage=STAGE_PUBLISH)
        result = JobResult(
            i.job_id, RunStatus.SUCCEEDED, now, generated_by=GeneratedBy(report.generated_by),
            final_text=report.text, weak_count=len(report.weak_shortfalls), research_count=research_count,
            result_event_id=event.value["eventId"])
        if record_result(self.job_log, result):
            self.publisher.send(event)

    def _event(self, i: ReportInput, report: Report, now: datetime) -> tuple[OutboundEvent, tuple[str, ...]]:
        """글자판과 같은 내용의 HTML·차트를 담은 이벤트. 내용(view)이 없는 보고서는 글자판만 싣는다."""
        if report.view is None:
            return briefing_event(i, report, now), ()
        mail = build_move_mail(i, report.view, self.settings,
                               lambda html, images: briefing_event(i, report, now, html, images))
        return briefing_event(i, report, now, mail.html, list(mail.images)), mail.notes

    def _skip_without_evidence(self, i: ReportInput, report: Report, research_count: int, now: datetime) -> None:
        """본문에 인용한 근거가 하나도 없으면 메일을 보내지 않는다. 결과와 이유는 작업 기록에 남긴다."""
        log.info("job %s has no cited evidence; result event not published", i.job_id)
        self._record(i, EntryType.MISSING_ITEMS, REASON_NO_EVIDENCE, None, 0, now, {
            "receivedCounts": {"articles": len(i.articles), "researchArticles": len(i.research_articles),
                               "filings": len(i.filings), "closeRates": len(i.close_rates),
                               "priceHistory": len(i.price_points)},
            "finalText": report.text}, stage=STAGE_PUBLISH)
        record_result(self.job_log, JobResult(
            i.job_id, RunStatus.SUCCEEDED, now, failure_reason=REASON_NO_EVIDENCE,
            generated_by=GeneratedBy(report.generated_by), final_text=report.text,
            weak_count=len(report.weak_shortfalls), research_count=research_count))

    def _record_attempts(self, report_input: ReportInput, attempts: tuple[LlmAttempt, ...], now: datetime,
                         sentence_no: int | None = None, round: int | None = None) -> None:
        for a in attempts:
            reason = REASON_LLM_CALL_FAILED if a.response is None else REASON_LLM_REJECTED
            self._record(report_input, EntryType.LLM_REJECTED, reason, a.error_type, a.attempt, now,
                         _attempt_data(a), sentence_no=sentence_no, round=round)

    def _record_accepted(self, report_input: ReportInput, a: AcceptedAttempt, now: datetime,
                         sentence_no: int | None = None, round: int | None = None) -> None:
        self._record(report_input, EntryType.REPORT_WRITTEN, REASON_LLM_ACCEPTED, None, a.attempt, now,
                     {"attempt": a.attempt, "llmInput": a.llm_input, "llmResponse": a.response, "rejection": None,
                      "usage": usage_record(a.usage), "articleGroups": a.article_groups},
                     sentence_no=sentence_no, round=round)

    def _record(self, report_input: ReportInput, entry_type: EntryType, reason: str, error_type: str | None,
                attempts: int, now: datetime, extra: dict[str, Any], stage: str = STAGE_LLM,
                sentence_no: int | None = None, round: int | None = None) -> None:
        data: dict[str, Any] = {"missingItems": unchecked_items(report_input)}
        data.update(extra)
        self.job_log.record(JobLogEntry(
            job_id=report_input.job_id, entry_type=entry_type, stage=stage, reason=reason, occurred_at=now,
            sentence_no=sentence_no, round=round, error_type=error_type, attempts=attempts, data=data))


def _attempt_data(a: LlmAttempt) -> dict[str, Any]:
    # 호출 자체가 실패해 응답이 없으면 그 사실을 값으로 남긴다(빈 값으로 두지 않는다).
    return {"attempt": a.attempt, "llmInput": a.llm_input,
            "llmResponse": a.response if a.response is not None else "(응답 없음: 호출 실패)",
            "rejection": a.rejection, "usage": usage_record(a.usage)}


def _evidence_entry(item: EvidenceItem) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "title": item.title,
        "source": item.source,
        "publishedAt": format_timestamp(item.published_at),
        "kind": item.kind,
        "ref": item.ref,
    }
    if item.article_id is not None:
        entry["articleId"] = item.article_id
    if item.url is not None:
        entry["url"] = item.url
    return entry


_TIME_LABELS = {KIND_ARTICLE: "발행", KIND_FILING: "접수", KIND_CLOSE_RATE: "마감"}


def _describe(item: EvidenceItem, with_url: bool = True) -> str:
    label = _TIME_LABELS.get(item.kind, "받음")
    text = f"{item.title} — {item.source}, {label} {item.published_at.astimezone(KST):%m-%d %H:%M}"
    return f"{text} {item.url}" if item.url and with_url else text
