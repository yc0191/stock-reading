"""리포트 입력(ReportInput)으로 briefing.generated를 만든다. Kafka는 모르고, 발행은 포트로 넘긴다.

코드가 LLM 입력을 정하고 출력을 검증한다. LLM은 원인마다 제목, 구체 문장, 뜻 문장과 코드가 붙인 근거 번호(기사·공시·
해외 등락률·배경 자료·비교 숫자)만 만들고, 머리말·확인하지 못한 것·면책 문구·변동률과 비교 숫자 계산은 코드가 맡는다.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, Protocol

from briefing.article_groups import RESEARCH_MARK, ArticleRow, GroupedArticles, group_article_lists
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.compose import DISCLAIMER, KST, NO_EVIDENCE, direction_of, headline
from briefing.events import BRIEFING_GENERATED_TOPIC, OutboundEvent, format_timestamp
from briefing.jobs import ReportInput
from briefing.job_log import (
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
    EXCHANGE_RATE_API,
    MARKET_SUMMARY_API,
    MoveFigures,
    display,
    move_figures,
    signed,
)
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
UNCHECKED_TITLE = "확인하지 못한 것:"
UNCHECKED_NONE = "- 없음"
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

SYSTEM_PROMPT = """당신은 ETF 가격 변동과 같은 시기에 공개된 자료를 연결해 원인 후보를 설명하는 도우미입니다.
입력은 JSON이며 종목 코드, 변동 방향과 변동률, 변동 시각, 시세 이력, 기사, 미국 공시, 미국 종목 직전 정규장 등락률, 배경 자료, 숫자 항목(figures)을 담고 있습니다.
근거로 쓸 수 있는 자료에는 ref(근거 번호)가 붙어 있습니다.
- researchArticles는 근거가 부족해 다시 검색해 찾은 기사이며(있을 때만 담깁니다) 기사와 같은 방식으로 읽고 인용합니다.
- articles·researchArticles의 한 칸이 sameEventArticles이면 제목 낱말과 발행 시각이 가까워 코드가 같은 사건을 다룬 기사로 묶은 것입니다. 묶음 안의 기사도 저마다 ref가 있어 그 ref로 인용하며, 묶음 자체에는 근거 번호가 없습니다. 묶음 안에서 research가 true인 기사는 다시 검색해 찾은 기사입니다. 묶이지 않은 기사는 한 칸에 하나씩 들어 있습니다.
- 기사의 minutesFromMove는 기사 발행 시각과 변동 시각의 차이(분)이며, 음수는 변동 전, 양수는 변동 후에 나온 기사입니다.
- 공시의 hoursBeforeMove는 공시 접수 시각이 변동보다 몇 시간 전인지입니다. 공시는 형식과 항목 번호만 있고 본문은 없습니다.
- 해외 등락률은 변동 종목에 담긴 미국 종목의 직전 정규장 등락률(%)이며 hoursBeforeMove는 그 장 마감이 변동보다 몇 시간 전인지입니다.
- 배경 자료(category가 "배경")는 환율·시장종합·세계지수·국내 일봉의 응답 원문(response)을 그대로 준 것이며, receivedAt은 받은 시각, minutesBeforeMove는 변동보다 몇 분 전에 받은 값인지입니다.
- 세계지수 배경 자료의 indexRates는 코드가 지수마다 직전 영업일 종가와 비교해 계산한 등락률입니다. 원문의 전일대비·등락률은 장이 끝난 지수에서 0.00으로 오므로 0.00을 "변동 없음"으로 읽지 말고, 등락률은 indexRates의 계산된 rate를 씁니다. rate가 "없음"이면 그 지수의 등락률은 모르는 것입니다.
- figures는 코드가 배경 원문에서 꺼낸 숫자(원/달러 환율, 코스피종합, 코스닥종합)와 코드가 계산한 비교 숫자입니다. "변동률 - 코스피종합 등락률"의 differencePoint는 변동 종목의 변동률에서 지수 등락률을 뺀 값(%p)으로, 음수면 시장 전체보다 더 내렸거나 덜 올랐다는 뜻이고 양수면 더 올랐거나 덜 내렸다는 뜻입니다. "미국 종목 직전 정규장 등락 집계"는 담긴 미국 종목 가운데 오른 수·내린 수·보합 수와 평균 등락률(%)입니다. status가 "없음"인 항목에는 ref가 없어 인용할 수 없습니다.
- etf(변동 종목이 ETF이고 정보가 있을 때만 담깁니다)는 ETF의 이름(name)·추종 대상(tracks)·업종(sector)과 비중 순위(rank, 1이 가장 큼) 순의 구성 종목(members)입니다. ETF가 무엇을 따라가고 무엇이 담겨 있는지 이해하는 맥락으로만 쓰고, 이것을 변동의 원인으로 단정하지 않습니다. ref가 없어 인용할 수 없으며, 근거는 ref가 있는 자료에서 찾습니다.
- 상태 표시: "잘림"은 상한 때문에 일부만 받았다는 뜻, "없음"은 기간 안에 자료가 없다는 뜻, "너무 큼"은 원문이 커서 빠졌다는 뜻, "해당 없음"은 변동 종목에 미국 종목이 없어 공시·해외 등락률을 조회하지 않았다는 뜻입니다.

쓰는 법:
- 원인 후보마다 묶음 하나를 씁니다. title은 원인을 짧게 이름 붙인 제목입니다(예: "미국 반도체 종목 실적 부진").
- detail은 무엇이 어땠는지 구체적으로 쓴 문장입니다. 기사 제목·요약과 공시(형식·항목)에 실제로 있는 내용만 씁니다. 어느 회사·어느 부분인지 같은 세부가 자료에 없으면 지어내지 않고 그 부분은 "자료에 없음"이라고 씁니다.
- meaning은 그 원인과 관련된 숫자가 무슨 뜻인지 쓴 문장입니다. figures의 비교 숫자나 해외 등락률·세계지수 등락률을 ref로 인용해, 예를 들어 "변동률이 코스피종합 등락률보다 1.05%p 낮아 시장 전체보다 더 빠졌다"처럼 씁니다. 숫자는 입력에 있는 값을 그대로 옮기고, 맞는 숫자가 없으면 그 원인의 근거 자료가 무엇을 보여 주는지 씁니다.
- 같은 사건 묶음은 한 기사만 보지 말고 묶음 안 기사들의 제목·요약을 함께 읽어, 여러 기사에 나뉘어 나온 사실(누가, 무엇을, 수치, 발언)을 모아 한 원인으로 씁니다. 내용을 가져온 기사의 ref는 모두 refs에 넣습니다.
- refs에는 detail의 근거가 된 자료의 ref를, meaningRefs에는 meaning의 근거가 된 자료의 ref를 하나 이상씩 넣습니다. 입력에 없는 ref는 쓰지 않습니다.

규칙:
- 원인을 단정하지 않습니다. "~때문에 올랐다"가 아니라 "~가 영향을 줬을 수 있다"처럼 씁니다.
- 기사끼리 수치나 해석이 다르면 어느 쪽이 맞다고 단정하지 않고 양쪽을 모두 씁니다. 수치는 "보도에 따라 2천억~3천억 원으로 엇갈립니다"처럼 범위로 쓰고, 해석이 갈리면 "...를 이유로 드는 보도와 ...를 우려하는 보도가 함께 나왔습니다"처럼 씁니다. 양쪽 기사의 ref를 모두 refs(또는 meaningRefs)에 넣어 코드가 문장 끝에 두 근거 번호를 함께 보이게 합니다.
- 문장 안에 근거 번호나 ref·articleId 값을 직접 쓰지 않습니다. 근거 번호는 코드가 refs로 문장 끝에 붙입니다.
- 배경 자료는 변동 당시의 상태를 보여 줄 뿐입니다. 배경 자료를 변동의 원인으로 단정하지 않고, 기사·공시와 함께 상황을 설명하는 데에만 씁니다.
- 원인마다 weak(근거 약함 여부)와 shortfall(부족한 점)을 냅니다. 그 원인을 뒷받침하는 자료가 약하면(시각이 맞지 않거나, 간접 정보뿐이거나, 세부가 자료에 없으면) weak를 true로 하고 shortfall에 어떤 자료가 더 있어야 하는지 한 문장으로 씁니다. 근거가 충분하면 weak를 false로, shortfall을 빈 문자열로 둡니다. shortfall은 찾아야 할 자료를 말할 뿐 원인을 단정하지 않습니다.
- 제목·문장·부족한 점에 미국 종목의 티커(ticker, tickers, members의 symbol 중 미국 종목의 값)를 쓰지 않습니다. 개별 종목을 말해야 하면 회사 이름이나 "미국 종목"이라고 씁니다.
- 근거가 부족하면 insufficientEvidence를 true로 하고 causes는 빈 목록으로 둡니다. 억지로 원인을 만들지 않습니다.
- 투자 조언(매수·매도·보유 권유, 가격 전망)을 하지 않습니다.
- 숫자를 새로 계산하지 않습니다. 더하기·빼기·평균도 하지 않고 입력에 있는 값만 씁니다.
- 문장은 한국어로 씁니다.

출력은 다음 JSON 하나만 냅니다:
{"causes":[{"title":"...","detail":"...","refs":["..."],"meaning":"...","meaningRefs":["..."],"weak":false,"shortfall":""}],"insufficientEvidence":false}"""

OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "causes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "detail": {"type": "string"},
                    "refs": {"type": "array", "items": {"type": "string"}},
                    "meaning": {"type": "string"},
                    "meaningRefs": {"type": "array", "items": {"type": "string"}},
                    "weak": {"type": "boolean"},
                    "shortfall": {"type": "string"},
                },
                "required": ["title", "detail", "refs", "meaning", "meaningRefs", "weak", "shortfall"],
                "additionalProperties": False,
            },
        },
        "insufficientEvidence": {"type": "boolean"},
    },
    "required": ["causes", "insufficientEvidence"],
    "additionalProperties": False,
}


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

    def cited(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(self.refs + self.meaning_refs))


@dataclass(frozen=True)
class LlmOutput:
    causes: tuple[Cause, ...]
    insufficient_evidence: bool


# ---- 규칙 기반 ----

@dataclass(frozen=True)
class RuleBasedWriter:
    """원인을 추정하지 않고 비교 사실과 최근 기사·공시를 나열한다. LLM 키가 없을 때 쓰는 기본 구현이다."""

    limit: int = RULE_BASED_LIMIT

    def write(self, report_input: ReportInput) -> Report:
        i = report_input
        catalog = evidence_catalog(i)
        evidence = tuple(
            item
            for kind in (KIND_ARTICLE, KIND_FILING)
            for item in sorted((e for e in catalog.values() if e.kind == kind),
                               key=lambda e: e.published_at, reverse=True)[: self.limit]
        )
        lines = [move_headline(i), "", RULE_BASED_NOTE]
        facts = comparison_facts(i)
        if facts:
            lines.append(COMPARISON_TITLE)
            lines.extend(f"- {fact}" for fact in facts)
        if evidence:
            lines.append("근거 후보:")
            for index, item in enumerate(evidence, start=1):
                lines.append(f"  {index}. {_describe(item)}")
        else:
            lines.append(NO_EVIDENCE)
        lines.append("")
        lines.extend(unchecked_lines(i))
        lines.append(DISCLAIMER)
        return Report("\n".join(lines), GENERATED_BY_RULE, evidence)


def comparison_facts(report_input: ReportInput) -> list[str]:
    """코드가 계산한 비교 숫자를 문장으로 바꾼다. 값이 없는 비교는 쓰지 않는다."""
    i = report_input
    f = move_figures(i.change_percent, i.background, i.close_rates)
    facts = []
    for label, diff in (("코스피종합", f.vs_kospi), ("코스닥종합", f.vs_kosdaq)):
        if diff is None:
            continue
        if diff == 0:
            facts.append(f"변동률이 {label} 등락률과 같습니다 (차이 0.00%p, 시세 자료로 코드가 계산).")
        else:
            side = "높습니다" if diff > 0 else "낮습니다"
            facts.append(f"변동률이 {label} 등락률보다 {abs(diff)}%p {side} (차이 {signed(diff)}%p, 시세 자료로 코드가 계산).")
    us = f.us_close
    if us is not None:
        facts.append(f"담긴 미국 종목은 직전 정규장에서 {us.up}개 오르고 {us.down}개 내렸으며 {us.flat}개는 보합이었고, "
                     f"평균 등락률은 {signed(us.average)}%입니다 (해외 시세 자료로 코드가 계산).")
    return facts


def move_headline(report_input: ReportInput) -> str:
    """머리말: 종목, 시각, 방향과 변동률, 몇 분 사이 움직였는지."""
    i = report_input
    line = headline(i.symbol, i.previous_price, i.price, i.change_percent, i.as_of, display_name(i))
    if i.detection_type == "prevClose":
        return f"{line}, 전일 종가 대비"
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
    f = move_figures(i.change_percent, i.background, i.close_rates)
    present = {b.api_type for b in i.background if b.status == BACKGROUND_OK}
    if EXCHANGE_RATE_API in present and f.exchange_rate is None:
        items.append("원/달러 환율: 환율종합 원문에서 값을 찾지 못함")
    for label, figure in (("코스피종합", f.kospi), ("코스닥종합", f.kosdaq)):
        if MARKET_SUMMARY_API in present and figure is None:
            items.append(f"{label}: 시장종합 원문에서 값을 찾지 못함")
    return items


def unchecked_lines(report_input: ReportInput) -> list[str]:
    items = unchecked_items(report_input)
    return [UNCHECKED_TITLE] + ([f"- {item}" for item in items] if items else [UNCHECKED_NONE])


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


def evidence_catalog(report_input: ReportInput) -> dict[str, EvidenceItem]:
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
    for figure in figure_entries(i):
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


def _figure_candidates(f: MoveFigures) -> list[tuple[str, str, str, datetime | None, dict[str, str]]]:
    """(이름, kind, 출처, 기준 시각, 값) 목록. 순서가 근거 번호 순서다."""
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
    for label, index, diff in (("변동률 - 코스피종합 등락률", f.kospi, f.vs_kospi),
                               ("변동률 - 코스닥종합 등락률", f.kosdaq, f.vs_kosdaq)):
        rows.append((label, MARKET_SUMMARY_API, COMPUTED_SOURCE, index.fetched_at if index else None,
                     {"differencePoint": display(diff, sign=True)}))
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
                 "differencePoint": "차이(%p)", "up": "오름", "down": "내림", "flat": "보합",
                 "averageRatePercent": "평균 등락률(%)"}


def figure_entries(report_input: ReportInput) -> list[FigureEntry]:
    """배경 원문에서 꺼낸 숫자와 비교 숫자. 값이 있는 항목에만 F1, F2 … 순번을 붙인다."""
    i = report_input
    f = move_figures(i.change_percent, i.background, i.close_rates)
    entries = []
    number = 0
    for name, kind, source, at, values in _figure_candidates(f):
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
        "figures": [_figure_input(e, i.as_of) for e in figure_entries(i)],
    }
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


def parse_output(text: str, allowed_ids: frozenset[str], forbidden_tickers: frozenset[str] = frozenset()) -> LlmOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나, 빈 문장이 있거나, 문장이 근거를 인용하지 않거나,
    준 적 없는 근거 번호를 인용하거나, LLM 입력에 있던 미국 티커를 문장에 쓰면 InvalidOutput.
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
        )
        for sentence in (cause.title, cause.detail, cause.meaning):
            check_no_ticker(sentence, tickers, "cause", InvalidOutput)
        if cause.shortfall is not None:
            check_no_ticker(cause.shortfall, tickers, "cause shortfall", InvalidOutput)
        causes.append(cause)
    insufficient = check_items_present(data, "causes", "cause", InvalidOutput)
    return LlmOutput(tuple(causes), insufficient)


REWRITE_ADDENDUM = """

다시 쓰기 요청:
- 입력의 rewrite 칸은 근거가 약하다고 한 원인 하나(sentence)와 그 부족한 점(shortfall)입니다. newRefs는 그 부족한 점을 채우려고 새로 찾아 researchArticles에 담은 기사의 ref입니다.
- 그 원인 하나만 다시 씁니다. causes에는 다시 쓴 원인 하나만 담고 다른 원인은 쓰지 않으며, insufficientEvidence는 false로 둡니다.
- 새 기사로 부족한 점이 채워지면 weak를 false로, 그래도 약하면 weak를 true로 하고 shortfall에 아직 부족한 점을 씁니다. 새 기사에 없는 내용은 지어내지 않습니다.
- 새 기사가 같은 사건 묶음(sameEventArticles)에 들었으면 묶음의 다른 기사와 함께 읽어 종합하고, 기사끼리 수치·해석이 다르면 위의 규칙대로 양쪽을 쓰고 단정하지 않습니다.
- 근거 번호·단정 금지·티커 금지 같은 위의 규칙은 그대로 지킵니다."""


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
        allowed = frozenset(evidence_catalog(report_input))
        tickers = us_symbols_in_input(report_input)
        reason = ""
        failed: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            response: str | None = None
            usage: LlmUsage | None = None
            try:
                reply = self.client.complete(SYSTEM_PROMPT, user, OUTPUT_SCHEMA)
                response, usage = reply.text, reply.usage
                output = parse_output(response, allowed, tickers)
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
    allowed = frozenset(evidence_catalog(report_input))
    tickers = us_symbols_in_input(report_input)
    failed: list[LlmAttempt] = []
    for attempt in range(1, writer.max_attempts + 1):
        response: str | None = None
        usage: LlmUsage | None = None
        try:
            reply = writer.client.complete(SYSTEM_PROMPT, user, OUTPUT_SCHEMA, REWRITE_ADDENDUM)
            response, usage = reply.text, reply.usage
            output = parse_output(response, allowed, tickers)
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
    lines = [move_headline(i), ""]
    if output.insufficient_evidence:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않는다. 억지 원인이 섞이지 않게 한다.
        lines.append(LLM_INSUFFICIENT)
        lines.append("")
        lines.extend(unchecked_lines(i))
        lines.append(LLM_DISCLAIMER)
        return Report("\n".join(lines), GENERATED_BY_LLM, (), insufficient_evidence=True, output=output)

    cited: list[str] = []
    for cause in output.causes:
        for ref in cause.cited():
            if ref not in cited:
                cited.append(ref)
    # 근거 번호는 리포트 전체에서 하나로 매기고, 원인마다 그 원인이 인용한 자료를 다시 보여 준다.
    number = {ref: index for index, ref in enumerate(cited, start=1)}
    for index, cause in enumerate(output.causes, start=1):
        lines.append(f"{index}. {cause.title}")
        lines.append(f"- {cause.detail} [근거 {_numbers(cause.refs, number)}]")
        lines.append(f"- {cause.meaning} [근거 {_numbers(cause.meaning_refs, number)}]")
        if cause.shortfall:
            lines.append(weak_note_line(cause.shortfall, "  "))
        for ref in cause.cited():
            lines.append(f"  근거 {number[ref]}. {_describe(by_id[ref])}")
        lines.append("")
    lines.extend(unchecked_lines(i))
    lines.append(LLM_DISCLAIMER)
    return Report("\n".join(lines), GENERATED_BY_LLM, tuple(by_id[x] for x in cited),
                  weak_shortfalls=tuple(c.shortfall for c in output.causes if c.shortfall), output=output)


def _numbers(refs: tuple[str, ...], number: dict[str, int]) -> str:
    return ", ".join(str(number[x]) for x in refs)


def build_report_writer(env: Mapping[str, str],
                        client_factory: Callable[[str, str], LlmClient] = AnthropicClient,
                        settings: CoreSettings = DEFAULT_SETTINGS) -> ReportWriter:
    """시작할 때 한 번 부른다. 키가 없어도 시작을 막지 않고 규칙 기반으로 바꾼다."""
    config = llm_config(env)
    if config is None:
        log.warning("%s is not set; briefing reports will be rule-based", API_KEY_ENV)
        return RuleBasedWriter()
    log.info("briefing reports will be written by the LLM (model=%s)", config.model)
    return LlmWriter(client_factory(config.api_key, config.model), settings=settings)


# ---- 발행 ----

def briefing_event(report_input: ReportInput, report: Report, now: datetime) -> OutboundEvent:
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
            },
        },
    )


def display_name(report_input: ReportInput) -> str | None:
    """응답의 종목 이름을 먼저 쓰고, 없거나 비었으면 ETF 이름을 쓴다. 둘 다 없으면 None."""
    for name in (report_input.name, report_input.etf.name if report_input.etf else None):
        if name and name.strip():
            return name.strip()
    return None


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
        event = briefing_event(report_input, report, now)
        i = report_input
        missing = unchecked_items(i)
        if missing:
            self._record(i, EntryType.MISSING_ITEMS, REASON_PUBLISHED, None, 0, now,
                         {"evidence": event.value["payload"]["evidence"], "weakItems": list(report.weak_shortfalls)},
                         stage=STAGE_PUBLISH)
        result = JobResult(
            i.job_id, RunStatus.SUCCEEDED, now, generated_by=GeneratedBy(report.generated_by),
            final_text=report.text, weak_count=len(report.weak_shortfalls), research_count=research_count,
            result_event_id=event.value["eventId"])
        if record_result(self.job_log, result):
            self.publisher.send(event)

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


def _describe(item: EvidenceItem) -> str:
    label = _TIME_LABELS.get(item.kind, "받음")
    text = f"{item.title} — {item.source}, {label} {item.published_at.astimezone(KST):%m-%d %H:%M}"
    return f"{text} {item.url}" if item.url else text
