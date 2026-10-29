"""리포트 입력(ReportInput)으로 briefing.generated를 만든다. Kafka는 모르고, 발행은 포트로 넘긴다.

코드가 LLM 입력을 정하고 출력을 검증한다. LLM은 근거 기사를 인용한 문장만 만들고, 머리말·면책 문구·변동률 계산은 코드가 맡는다.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping, Protocol

from briefing.compose import DISCLAIMER, KST, NO_EVIDENCE, direction_of, headline
from briefing.events import BRIEFING_GENERATED_TOPIC, OutboundEvent, format_timestamp
from briefing.jobs import Article, Failure, FailurePort, FailureReason, ReportInput, RequestPort

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

SYSTEM_PROMPT = """당신은 ETF 가격 변동과 같은 시기에 공개된 기사를 연결해 설명하는 도우미입니다.
입력은 JSON이며 종목 코드, 변동 방향과 변동률, 변동 시각, 시세 이력, 기사 목록을 담고 있습니다.
기사의 minutesFromMove는 기사 발행 시각과 변동 시각의 차이(분)이며, 음수는 변동 전, 양수는 변동 후에 나온 기사입니다.

규칙:
- 원인을 단정하지 않습니다. "~때문에 올랐다"가 아니라 "~가 영향을 줬을 수 있다"처럼 씁니다.
- 각 문장은 근거가 된 기사의 articleId를 articleIds에 하나 이상 넣습니다. 입력에 없는 articleId는 쓰지 않습니다.
- 근거가 부족하면 insufficientEvidence를 true로 하고 causes는 빈 목록으로 둡니다. 억지로 원인을 만들지 않습니다.
- 투자 조언(매수·매도·보유 권유, 가격 전망)을 하지 않습니다.
- 숫자를 새로 계산하지 않습니다. 입력에 있는 값만 씁니다.
- 문장은 한국어로 씁니다.

출력은 다음 JSON 하나만 냅니다:
{"causes":[{"statement":"...","articleIds":["..."]}],"insufficientEvidence":false}"""

OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "causes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "statement": {"type": "string"},
                    "articleIds": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["statement", "articleIds"],
                "additionalProperties": False,
            },
        },
        "insufficientEvidence": {"type": "boolean"},
    },
    "required": ["causes", "insufficientEvidence"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class Report:
    text: str
    generated_by: str
    evidence: tuple[Article, ...]


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
    article_ids: tuple[str, ...]


@dataclass(frozen=True)
class LlmOutput:
    causes: tuple[Cause, ...]
    insufficient_evidence: bool


# ---- 규칙 기반 ----

@dataclass(frozen=True)
class RuleBasedWriter:
    """기사를 최근 순으로 나열한다. LLM 키가 없을 때 쓰는 기본 구현이다."""

    limit: int = RULE_BASED_LIMIT

    def write(self, report_input: ReportInput) -> Report:
        i = report_input
        evidence = tuple(sorted(i.articles, key=lambda a: a.published_at, reverse=True)[: self.limit])
        lines = [headline(i.symbol, i.previous_price, i.price, i.change_percent, i.as_of)]
        if evidence:
            lines.append("근거 후보:")
            for index, article in enumerate(evidence, start=1):
                lines.append(f"  {index}. {_describe(article)}")
        else:
            lines.append(NO_EVIDENCE)
        lines.append(DISCLAIMER)
        return Report("\n".join(lines), GENERATED_BY_RULE, evidence)


# ---- LLM ----

def build_llm_input(report_input: ReportInput) -> dict[str, Any]:
    """LLM에게 줄 공개 정보만 고른다. 보유 수량·금액 같은 개인 정보는 ReportInput에도 없고 여기서도 넣지 않는다."""
    i = report_input
    return {
        "symbol": i.symbol,
        "direction": direction_of(i.change_percent),
        "changePercent": round(i.change_percent, 2),
        "moveAt": format_timestamp(i.as_of.astimezone(KST)),
        "priceHistory": [
            {"at": format_timestamp(p.as_of.astimezone(KST)), "price": p.price} for p in i.price_points
        ],
        "articles": [
            {
                "articleId": a.article_id,
                "title": a.title,
                "summary": a.summary,
                "source": a.publisher,
                "publishedAt": format_timestamp(a.published_at.astimezone(KST)),
                # 내림으로 계산해 음수는 언제나 변동 전에 나온 기사를 뜻하게 한다.
                "minutesFromMove": int((a.published_at - i.as_of).total_seconds() // 60),
            }
            for a in i.articles
        ],
    }


def parse_output(text: str, allowed_ids: frozenset[str]) -> LlmOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나 준 적 없는 articleId를 인용하면 InvalidOutput."""
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
        statement, ids = item.get("statement"), item.get("articleIds")
        if not isinstance(statement, str) or not statement.strip():
            raise InvalidOutput("cause has no statement")
        if not isinstance(ids, list) or not ids or not all(isinstance(x, str) for x in ids):
            raise InvalidOutput("cause cites no articleIds")
        unknown = [x for x in ids if x not in allowed_ids]
        if unknown:
            raise InvalidOutput(f"cause cites {len(unknown)} articleId(s) that were not given")
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
        allowed = frozenset(a.article_id for a in report_input.articles)
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
    by_id = {a.article_id: a for a in i.articles}
    lines = [headline(i.symbol, i.previous_price, i.price, i.change_percent, i.as_of)]
    if output.insufficient_evidence:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않는다. 억지 원인이 섞이지 않게 한다.
        lines.append(LLM_INSUFFICIENT)
        lines.append(LLM_DISCLAIMER)
        return Report("\n".join(lines), GENERATED_BY_LLM, ())

    cited: list[str] = []
    for cause in output.causes:
        for article_id in cause.article_ids:
            if article_id not in cited:
                cited.append(article_id)
    number = {article_id: index for index, article_id in enumerate(cited, start=1)}
    for cause in output.causes:
        refs = ", ".join(str(number[x]) for x in cause.article_ids)
        lines.append(f"- {cause.statement} [근거 {refs}]")
    lines.append("근거:")
    for article_id in cited:
        lines.append(f"  {number[article_id]}. {_describe(by_id[article_id])}")
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
                "evidence": [
                    {
                        "title": a.title,
                        "source": a.publisher,
                        "publishedAt": format_timestamp(a.published_at),
                        "articleId": a.article_id,
                        "url": a.url,
                    }
                    for a in report.evidence
                ],
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


def _describe(article: Article) -> str:
    return f"{article.title} — {article.publisher}, 발행 {article.published_at.astimezone(KST):%m-%d %H:%M}"
