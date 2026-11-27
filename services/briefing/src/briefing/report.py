"""리포트 입력(ReportInput)으로 briefing.generated를 만든다. Kafka는 모르고, 발행은 포트로 넘긴다.

코드가 LLM 입력을 정하고 출력을 검증한다. LLM은 원인마다 제목, 구체 문장, 뜻 문장과 코드가 붙인 근거 번호(기사·공시·
해외 등락률·배경 자료·비교 숫자)만 만들고, 머리말·오늘 흐름·근거 목록·면책 문구·변동률과 비교 숫자 계산은 코드가 맡는다.
메일은 짧게 쓴다: 원인은 3개까지, 원인마다 제목과 두 문장(구체 문장 하나, 뜻 문장 하나)이며 근거 목록은 맨 아래에 한 번만 둔다.
받지 못한 자료('확인하지 못한 것')는 메일에 싣지 않고 작업 기록(job_log)에만 남긴다.
이 파일은 작성기(규칙 기반, LLM)와 출력 해석을 맡는다. 근거 목록과 LLM 입력은 move_evidence.py,
작업 단계와 발행은 report_stage.py에 있다.
"""
from __future__ import annotations

import json
import logging
import unicodedata
from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping, Protocol

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.compose import DISCLAIMER, NO_EVIDENCE
from briefing.jobs import ReportInput
from briefing.job_log import GeneratedBy
from briefing.move_figures import comparison_facts
from briefing.move_prompts import OUTPUT_SCHEMA, REWRITE_ADDENDUM, system_prompt
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
from briefing.llm import (
    API_KEY_ENV,
    AcceptedAttempt,
    AnthropicClient,
    LlmCallError,
    LlmClient,
    LlmUsage,
    attempt_usage,
    llm_config,
)
from briefing.move_evidence import (
    EvidenceItem,
    KIND_ARTICLE,
    KIND_FILING,
    _describe,
    article_groups,
    build_llm_input,
    evidence_catalog,
    move_headline,
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
        view = MoveView(move_headline(i, self.settings), tuple(m.line() for m in i.earlier_moves), tuple(notes),
                        evidence_title="근거 후보:",
                        evidence=tuple(_view_evidence(n, e) for n, e in enumerate(evidence, start=1)),
                        closing=DISCLAIMER)
        return Report(view_text(view), GENERATED_BY_RULE, evidence, view=view)


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
            return replace(llm_report(report_input, output, self.settings), failed_attempts=tuple(failed),
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


def llm_report(report_input: ReportInput, output: LlmOutput, settings: CoreSettings) -> Report:
    i = report_input
    by_id = evidence_catalog(i, settings)
    flow = tuple(m.line() for m in i.earlier_moves)
    if output.insufficient_evidence:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않는다. 억지 원인이 섞이지 않게 한다.
        view = MoveView(move_headline(i, settings), flow, (LLM_INSUFFICIENT,), closing=LLM_DISCLAIMER)
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
    view = MoveView(move_headline(i, settings), flow, (), causes,
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
