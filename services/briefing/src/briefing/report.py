"""리포트 입력(ReportInput)으로 briefing.generated를 만든다. Kafka는 모르고, 발행은 포트로 넘긴다.

코드가 LLM 입력을 정하고 출력을 검증한다. LLM은 코드가 붙인 근거 번호(기사·공시·해외 등락률·배경 자료)를 인용한 문장만 만들고,
머리말·면책 문구·변동률 계산은 코드가 맡는다.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, Protocol

from briefing.compose import DISCLAIMER, KST, NO_EVIDENCE, direction_of, headline
from briefing.events import BRIEFING_GENERATED_TOPIC, OutboundEvent, format_timestamp
from briefing.jobs import (
    BACKGROUND_MISSING,
    BACKGROUND_OK,
    BACKGROUND_TOO_LARGE,
    Background,
    Failure,
    FailurePort,
    FailureReason,
    ReportInput,
    RequestPort,
)

log = logging.getLogger("briefing.report")

GENERATED_BY_RULE = "rule-based"
GENERATED_BY_LLM = "llm"
DEFAULT_MODEL = "claude-sonnet-5-5"
MODEL_ENV = "BRIEFING_LLM_MODEL"
API_KEY_ENV = "ANTHROPIC_API_KEY"
KEY_PLACEHOLDERS = frozenset({"", "change-me"})
LLM_MAX_ATTEMPTS = 3
RULE_BASED_LIMIT = 3

LLM_INSUFFICIENT = "변동 시점 전후에 공개된 자료만으로는 변동 원인을 추정하기 어렵습니다."
LLM_DISCLAIMER = "※ 투자 조언이 아니며, 위 내용은 변동 시점 전후에 공개된 자료로 추정한 것일 뿐 원인을 단정하지 않습니다."

# 자료 종류. briefing.generated evidence의 kind 값과 같다. 배경 자료는 apiType을 그대로 쓴다.
KIND_ARTICLE = "article"
KIND_FILING = "filing"
KIND_CLOSE_RATE = "overseas-close-rate"

# LLM 입력에 담는 자료 상태 표시
MARK_PRESENT = "있음"
MARK_MISSING = "없음"
MARK_TRUNCATED = "잘림"
MARK_TOO_LARGE = "너무 큼"
MARK_NOT_APPLICABLE = "해당 없음"
BACKGROUND_LABEL = "배경"
BACKGROUND_STATUS_MARKS = {BACKGROUND_OK: MARK_PRESENT, BACKGROUND_MISSING: MARK_MISSING,
                           BACKGROUND_TOO_LARGE: MARK_TOO_LARGE}
BACKGROUND_NAMES = {"exchange-rate-summary": "환율종합", "market-summary": "시장종합",
                    "world-index": "세계지수", "daily-price": "국내 일봉"}
# 출처에는 자료를 준 회사 이름 대신 중립 표현을 쓴다.
FILING_SOURCE = "미국 공시"
CLOSE_RATE_SOURCE = "해외 시세 자료"
BACKGROUND_SOURCE = "시세 자료(배경)"
# 코드가 붙이는 근거 번호의 머리글. 기사는 articleId, 공시는 접수번호를 그대로 쓴다.
CLOSE_RATE_REF_PREFIX = "U"
BACKGROUND_REF_PREFIX = "B"

SYSTEM_PROMPT = """당신은 ETF 가격 변동과 같은 시기에 공개된 자료를 연결해 설명하는 도우미입니다.
입력은 JSON이며 종목 코드, 변동 방향과 변동률, 변동 시각, 시세 이력, 기사, 미국 공시, 미국 종목 직전 정규장 등락률, 배경 자료를 담고 있습니다.
근거로 쓸 수 있는 자료에는 ref(근거 번호)가 붙어 있습니다.
- 기사의 minutesFromMove는 기사 발행 시각과 변동 시각의 차이(분)이며, 음수는 변동 전, 양수는 변동 후에 나온 기사입니다.
- 공시의 hoursBeforeMove는 공시 접수 시각이 변동보다 몇 시간 전인지입니다.
- 해외 등락률은 변동 종목에 담긴 미국 종목의 직전 정규장 등락률(%)이며 hoursBeforeMove는 그 장 마감이 변동보다 몇 시간 전인지입니다.
- 배경 자료(category가 "배경")는 환율·시장종합·세계지수·국내 일봉의 응답 원문(response)을 그대로 준 것이며, receivedAt은 받은 시각, minutesBeforeMove는 변동보다 몇 분 전에 받은 값인지입니다.
- 세계지수 배경 자료의 indexRates는 코드가 지수마다 직전 영업일 종가와 비교해 계산한 등락률입니다. 원문의 전일대비·등락률은 장이 끝난 지수에서 0.00으로 오므로 0.00을 "변동 없음"으로 읽지 말고, 등락률은 indexRates의 계산된 rate를 씁니다. rate가 "없음"이면 그 지수의 등락률은 모르는 것입니다.
- 상태 표시: "잘림"은 상한 때문에 일부만 받았다는 뜻, "없음"은 기간 안에 자료가 없다는 뜻, "너무 큼"은 원문이 커서 빠졌다는 뜻, "해당 없음"은 변동 종목에 미국 종목이 없어 공시·해외 등락률을 조회하지 않았다는 뜻입니다.

규칙:
- 원인을 단정하지 않습니다. "~때문에 올랐다"가 아니라 "~가 영향을 줬을 수 있다"처럼 씁니다.
- 배경 자료는 변동 당시의 상태를 보여 줄 뿐입니다. 배경 자료를 변동의 원인으로 단정하지 않고, 기사·공시와 함께 상황을 설명하는 데에만 씁니다.
- 각 문장은 근거가 된 자료의 ref를 refs에 하나 이상 넣습니다. 입력에 없는 ref는 쓰지 않습니다.
- 근거가 부족하면 insufficientEvidence를 true로 하고 causes는 빈 목록으로 둡니다. 억지로 원인을 만들지 않습니다.
- 투자 조언(매수·매도·보유 권유, 가격 전망)을 하지 않습니다.
- 숫자를 새로 계산하지 않습니다. 입력에 있는 값만 씁니다.
- 문장은 한국어로 씁니다.

출력은 다음 JSON 하나만 냅니다:
{"causes":[{"statement":"...","refs":["..."]}],"insufficientEvidence":false}"""

OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "causes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "statement": {"type": "string"},
                    "refs": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["statement", "refs"],
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
class Report:
    text: str
    generated_by: str
    evidence: tuple[EvidenceItem, ...]


class ReportError(Exception):
    """리포트를 끝내 만들지 못했다. 메시지에는 짧은 사유만 담는다."""

    def __init__(self, detail: str, attempts: int) -> None:
        super().__init__(detail)
        self.detail = detail
        self.attempts = attempts


class ReportWriter(Protocol):
    def write(self, report_input: ReportInput) -> Report: ...


class LlmClient(Protocol):
    def complete(self, system: str, user: str) -> str: ...


class LlmCallError(Exception):
    """LLM 호출 실패. 키가 섞이지 않게 원래 예외의 종류 이름만 담는다."""


class InvalidOutput(ValueError):
    pass


@dataclass(frozen=True)
class Cause:
    statement: str
    refs: tuple[str, ...]


@dataclass(frozen=True)
class LlmOutput:
    causes: tuple[Cause, ...]
    insufficient_evidence: bool


# ---- 규칙 기반 ----

@dataclass(frozen=True)
class RuleBasedWriter:
    """기사와 공시를 종류마다 최근 순으로 나열한다. LLM 키가 없을 때 쓰는 기본 구현이다."""

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
        lines = [headline(i.symbol, i.previous_price, i.price, i.change_percent, i.as_of)]
        if evidence:
            lines.append("근거 후보:")
            for index, item in enumerate(evidence, start=1):
                lines.append(f"  {index}. {_describe(item)}")
        else:
            lines.append(NO_EVIDENCE)
        lines.append(DISCLAIMER)
        return Report("\n".join(lines), GENERATED_BY_RULE, evidence)


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
    for a in i.articles:
        catalog[a.article_id] = EvidenceItem(KIND_ARTICLE, a.article_id, a.title, a.publisher, a.published_at,
                                             a.url, a.article_id)
    # 본문이 회사를 티커로 말하므로 근거 목록에서도 어느 회사 자료인지 구분되게 제목에 티커를 넣는다.
    # 리포트는 작업자 메일로만 가고, 로그에는 제목을 남기지 않는다.
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
    return catalog


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


def build_llm_input(report_input: ReportInput) -> dict[str, Any]:
    """LLM에게 줄 공개 정보만 고른다. 보유 수량·금액 같은 개인 정보는 ReportInput에도 없고 여기서도 넣지 않는다."""
    i = report_input
    us = bool(i.tickers)
    rated = {r.ticker for r in i.close_rates}
    return {
        "symbol": i.symbol,
        "direction": direction_of(i.change_percent),
        "changePercent": round(i.change_percent, 2),
        "moveAt": _at(i.as_of),
        "priceHistoryStatus": _status(len(i.price_points), i.history_truncated),
        "priceHistory": [{"at": _at(p.as_of), "price": p.price} for p in i.price_points],
        "articlesStatus": _status(len(i.articles), i.articles_truncated),
        "articles": [
            {
                "ref": a.article_id,
                "articleId": a.article_id,
                "title": a.title,
                "summary": a.summary,
                "source": a.publisher,
                "publishedAt": _at(a.published_at),
                # 내림으로 계산해 음수는 언제나 변동 전에 나온 기사를 뜻하게 한다.
                "minutesFromMove": int((a.published_at - i.as_of).total_seconds() // 60),
            }
            for a in i.articles
        ],
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


def parse_output(text: str, allowed_ids: frozenset[str]) -> LlmOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나 준 적 없는 근거 번호를 인용하면 InvalidOutput."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise InvalidOutput("output is not valid JSON") from error
    if not isinstance(data, dict) or not isinstance(data.get("causes"), list) \
            or not isinstance(data.get("insufficientEvidence"), bool):
        raise InvalidOutput("output does not have causes and insufficientEvidence")
    causes = []
    for item in data["causes"]:
        if not isinstance(item, dict):
            raise InvalidOutput("cause is not an object")
        statement, ids = item.get("statement"), item.get("refs")
        if not isinstance(statement, str) or not statement.strip():
            raise InvalidOutput("cause has no statement")
        if not isinstance(ids, list) or not ids or not all(isinstance(x, str) for x in ids):
            raise InvalidOutput("cause cites no refs")
        unknown = [x for x in ids if x not in allowed_ids]
        if unknown:
            raise InvalidOutput(f"cause cites {len(unknown)} ref(s) that were not given")
        causes.append(Cause(statement.strip(), tuple(dict.fromkeys(ids))))
    if not data["insufficientEvidence"] and not causes:
        raise InvalidOutput("no causes although evidence was not marked insufficient")
    return LlmOutput(tuple(causes), data["insufficientEvidence"])


@dataclass(frozen=True)
class LlmWriter:
    client: LlmClient
    max_attempts: int = LLM_MAX_ATTEMPTS

    def write(self, report_input: ReportInput) -> Report:
        user = json.dumps(build_llm_input(report_input), ensure_ascii=False)
        allowed = frozenset(evidence_catalog(report_input))
        reason = ""
        for attempt in range(1, self.max_attempts + 1):
            try:
                output = parse_output(self.client.complete(SYSTEM_PROMPT, user), allowed)
            except (LlmCallError, InvalidOutput) as error:
                reason = str(error)
                log.warning("LLM report attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, report_input.job_id, reason)
                continue
            return _llm_report(report_input, output)
        raise ReportError(f"LLM report failed after {self.max_attempts} attempts: {reason}", self.max_attempts)


def _llm_report(report_input: ReportInput, output: LlmOutput) -> Report:
    i = report_input
    by_id = evidence_catalog(i)
    lines = [headline(i.symbol, i.previous_price, i.price, i.change_percent, i.as_of)]
    if output.insufficient_evidence:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않는다. 억지 원인이 섞이지 않게 한다.
        lines.append(LLM_INSUFFICIENT)
        lines.append(LLM_DISCLAIMER)
        return Report("\n".join(lines), GENERATED_BY_LLM, ())

    cited: list[str] = []
    for cause in output.causes:
        for ref in cause.refs:
            if ref not in cited:
                cited.append(ref)
    number = {ref: index for index, ref in enumerate(cited, start=1)}
    for cause in output.causes:
        refs = ", ".join(str(number[x]) for x in cause.refs)
        lines.append(f"- {cause.statement} [근거 {refs}]")
    lines.append("근거:")
    for ref in cited:
        lines.append(f"  {number[ref]}. {_describe(by_id[ref])}")
    lines.append(LLM_DISCLAIMER)
    return Report("\n".join(lines), GENERATED_BY_LLM, tuple(by_id[x] for x in cited))


class AnthropicClient:
    """공식 SDK로 Claude를 부른다. 키는 SDK 클라이언트 안에만 두고 이 객체의 필드나 repr에 남기지 않는다."""

    def __init__(self, api_key: str, model: str, max_tokens: int = 16000) -> None:
        import anthropic

        self._anthropic = anthropic
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def __repr__(self) -> str:
        return f"AnthropicClient(model={self.model!r})"

    def complete(self, system: str, user: str) -> str:
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
                output_config={"format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}},
            )
        except self._anthropic.APIError as error:
            # SDK 예외 문구에는 요청 정보가 섞일 수 있어 종류 이름만 남기고 원래 예외도 잇지 않는다.
            raise LlmCallError(f"LLM call failed: {type(error).__name__}") from None
        if response.stop_reason != "end_turn":
            raise LlmCallError(f"LLM stopped with {response.stop_reason}")
        texts = [block.text for block in response.content if block.type == "text"]
        if not texts:
            raise LlmCallError("LLM returned no text")
        return texts[0]


def build_report_writer(env: Mapping[str, str],
                        client_factory: Callable[[str, str], LlmClient] = AnthropicClient) -> ReportWriter:
    """시작할 때 한 번 부른다. 키가 없어도 시작을 막지 않고 규칙 기반으로 바꾼다."""
    api_key = env.get(API_KEY_ENV, "").strip()
    # .env.example 의 자리표시 값을 그대로 복사해도 키가 없는 것으로 본다. 그대로 쓰면 모든 변동이 llm_error 가 된다.
    if api_key in KEY_PLACEHOLDERS:
        log.warning("%s is not set; briefing reports will be rule-based", API_KEY_ENV)
        return RuleBasedWriter()
    model = env.get(MODEL_ENV, "").strip() or DEFAULT_MODEL
    log.info("briefing reports will be written by the LLM (model=%s)", model)
    return LlmWriter(client_factory(api_key, model))


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
            },
        },
    )


@dataclass
class ReportStage:
    """BriefingCore의 ReadyPort 구현. 리포트를 만들면 발행하고, 끝내 못 만들면 llm_error로 기록한다."""

    writer: ReportWriter
    publisher: RequestPort
    failures: FailurePort
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))

    def on_ready(self, report_input: ReportInput) -> None:
        try:
            report = self.writer.write(report_input)
        except ReportError as error:
            log.warning("job %s failed: %s", report_input.job_id, FailureReason.LLM_ERROR.value)
            self.failures.record(Failure(report_input.job_id, report_input.symbol, FailureReason.LLM_ERROR,
                                         error.detail, error.attempts))
            return
        self.publisher.send(briefing_event(report_input, report, self.clock()))


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
    return f"{item.title} — {item.source}, {label} {item.published_at.astimezone(KST):%m-%d %H:%M}"
