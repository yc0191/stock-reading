"""발행 직전 마지막 검사. 급변동과 데일리 판이 함께 쓴다. Kafka를 모르며 본문(draft)은 어댑터를 거쳐서만 다룬다.

흐름: 코드 검사(항상) -> 다시 쓴 문장이 있거나 코드 검사가 고칠 수 있는 문제를 찾았으면 LLM이 메일 전체를 검토해 문제 문장
번호와 이유를 낸다 -> 문제 문장만 한 번 고친다(근거 번호 규칙 그대로) -> 고친 본문을 코드 검사와 LLM 검토로 한 번 더 본다 ->
코드 검사를 통과한 본문 중 문제가 적은 쪽을 낸다(같으면 고치기 전 본문). 검토 호출이 끝내 실패하면 그때의 본문으로 낸다.
검토·고침에는 따로 시간 상한이 없고 LLM 호출마다 시도 횟수 상한만 쓴다.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping, Protocol

from briefing.job_log import (
    REASON_LLM_CALL_FAILED,
    REASON_LLM_REJECTED,
    DiscardJobLog,
    EntryType,
    JobLogEntry,
    JobLogPort,
)
from briefing.llm import LlmCallError, LlmClient, LlmUsage, attempt_usage, usage_record
from briefing.llm_output import InvalidLlmOutput, output_item, output_sentence

log = logging.getLogger("briefing.final_check")

STAGE_FINAL_CHECK = "final-check"

# final-check 줄의 사유
REASON_CODE_CHECKED = "code-checked"
REASON_REVIEWED = "reviewed"
REASON_REVIEW_FAILED = "review-failed"
REASON_FIX_FAILED = "fix-failed"
REASON_CHOSEN = "body-chosen"
# sentence-fixed 줄의 사유
REASON_FIXED = "fixed-after-review"

# 코드 검사가 찾는 문제
PROBLEM_UNKNOWN_REF = "ref-not-given"
PROBLEM_SHARED_ARTICLE = "article-cited-by-two-sentences"
PROBLEM_WEAK_LEFT = "weak-mark-left"
PROBLEM_OUT_OF_RANGE = "article-out-of-range"
# LLM 검토가 찾은 문제
PROBLEM_REVIEW = "review"
# 근거 약함 표시는 자료가 모자라다는 뜻이라 고쳐 쓸 수 없다. 기록만 하고 검토를 부르지 않는다.
UNFIXABLE_PROBLEMS = frozenset({PROBLEM_WEAK_LEFT})
# 근거 규칙(목록 밖 번호, 시간 범위)을 어긴 문제. 이것이 남은 본문은 코드 검사를 통과하지 못한다.
RULE_PROBLEMS = frozenset({PROBLEM_UNKNOWN_REF, PROBLEM_OUT_OF_RANGE})

# 최종으로 고른 본문
CHOSE_BEFORE = "before-fix"
CHOSE_AFTER = "after-fix"


@dataclass(frozen=True)
class MailSentence:
    """메일에 실린 LLM 문장 하나. 번호는 메일 안 순서(1부터)이며 고쳐도 바뀌지 않는다."""

    no: int
    kind: str
    text: str
    refs: tuple[str, ...]
    weak: bool = False
    # 문장 단위 재검색으로 다시 쓴 문장인가
    rewritten: bool = False
    # 정해진 문구처럼 고쳐 쓰면 안 되는 문장. 검토에는 보여 주지만 문제 문장으로 고를 수 없다.
    fixable: bool = True
    # 문장이 다루는 대상(ETF 이름 등). 문장만으로는 대상이 안 보이는 ETF별 문장에만 있다.
    about: str | None = None

    def as_input(self) -> dict[str, Any]:
        data = {"no": self.no, "kind": self.kind, "text": self.text, "refs": list(self.refs), "weak": self.weak,
                "rewritten": self.rewritten, "fixable": self.fixable}
        if self.about:
            data["about"] = self.about
        return data


@dataclass(frozen=True)
class Source:
    """인용할 수 있는 자료 하나에서 코드 검사가 보는 것. 기사만 발행 시각을 범위와 대 본다."""

    article: bool
    published_at: datetime | None = None


@dataclass(frozen=True)
class Problem:
    sentence_no: int
    kind: str
    detail: str

    def as_record(self) -> dict[str, Any]:
        return {"sentenceNo": self.sentence_no, "kind": self.kind, "detail": self.detail}


def code_check(sentences: tuple[MailSentence, ...], sources: Mapping[str, Source], since: datetime,
               until: datetime) -> tuple[Problem, ...]:
    """인용 번호가 목록 안인지, 같은 기사를 두 문장이 이유로 썼는지, 근거 약함 표시가 남았는지, 인용한 기사가 범위 안인지 본다."""
    problems: list[Problem] = []
    cited_by: dict[str, list[int]] = {}
    for s in sentences:
        unknown = [ref for ref in s.refs if ref not in sources]
        if unknown:
            problems.append(Problem(s.no, PROBLEM_UNKNOWN_REF, f"준 목록에 없는 근거 번호 {len(unknown)}개를 인용함"))
        for ref in dict.fromkeys(s.refs):
            source = sources.get(ref)
            if source is None or not source.article:
                continue
            cited_by.setdefault(ref, []).append(s.no)
            if source.published_at is not None and not since <= source.published_at <= until:
                problems.append(Problem(s.no, PROBLEM_OUT_OF_RANGE, f"범위 밖에 발행된 기사({ref})를 인용함"))
        if s.weak:
            problems.append(Problem(s.no, PROBLEM_WEAK_LEFT, "근거 약함 표시가 남음"))
    for ref, nos in cited_by.items():
        if len(nos) < 2:
            continue
        for no in nos:
            others = ", ".join(str(n) for n in nos if n != no)
            problems.append(Problem(no, PROBLEM_SHARED_ARTICLE, f"{others}번 문장과 같은 기사({ref})를 이유로 씀"))
    return tuple(sorted(problems, key=lambda p: (p.sentence_no, p.kind)))


def fixable(problems: tuple[Problem, ...]) -> tuple[Problem, ...]:
    return tuple(p for p in problems if p.kind not in UNFIXABLE_PROBLEMS)


def passes(problems: tuple[Problem, ...]) -> bool:
    """코드 검사를 통과했는가. 근거 규칙을 어긴 문제가 없어야 한다."""
    return not any(p.kind in RULE_PROBLEMS for p in problems)


def quality_count(code: tuple[Problem, ...], review: tuple[Problem, ...]) -> int:
    """본문을 고를 때 세는 문제 수: 검토가 찾은 문제와, 근거 규칙 밖의 고칠 수 있는 코드 검사 문제."""
    return len(review) + sum(1 for p in fixable(code) if p.kind not in RULE_PROBLEMS)


# ---- LLM 검토 ----

REVIEW_SYSTEM_PROMPT = """당신은 나가기 직전의 브리핑 메일을 검토하는 도우미입니다. 문장을 고치지 않고 문제가 있는 문장의 번호와 이유만 냅니다.
입력은 JSON입니다.
- mail은 받는 사람이 읽을 메일 글 전체입니다.
- firstLine(있을 때만)은 코드가 자료로 쓴 첫 요약 줄입니다. 고칠 수 없고 맞는 값으로 봅니다.
- sentences는 LLM이 쓴 문장 목록입니다. no는 문장 번호, kind는 문장 종류(summary는 첫 요약 줄 구실을 하는 요약 문장), refs는 그 문장이 인용한 근거 번호, about(있을 때만)은 그 문장이 다루는 ETF, weak는 근거 약함 표시, rewritten은 근거를 다시 찾아 다시 쓴 문장, fixable이 false이면 정해진 문구라 고칠 수 없는 문장입니다.
- codeCheck는 코드가 먼저 찾은 문제입니다. kind가 article-cited-by-two-sentences이면 두 문장이 같은 기사를 이유로 쓴 것이고, weak-mark-left는 근거가 모자라 남긴 표시라 문제로 내지 않습니다.

찾을 문제:
- 문장끼리 모순: 두 문장이 같은 대상의 방향·수치·원인을 서로 어긋나게 말합니다(예: 한 문장은 올랐다고, 다른 문장은 내렸다고 함).
- 같은 내용 겹침: 두 문장이 같은 사실이나 같은 기사 내용을 되풀이합니다. 다른 대상에 같은 정해진 문구를 쓴 것은 겹침이 아닙니다.
- 첫 요약 줄과 본문 불일치: firstLine이나 summary 문장이 말하는 방향·대상·결론이 본문 문장과 맞지 않습니다.
- codeCheck에 있는 문제 가운데 weak-mark-left가 아닌 것도 그 문장의 문제로 냅니다.

규칙:
- 문제가 있는 문장만 냅니다. 문제가 없으면 problems를 빈 목록으로 둡니다.
- sentenceNo는 sentences에 있고 fixable이 true인 문장 번호만 씁니다. 두 문장이 서로 어긋나면 고쳐야 할 쪽 하나를 고르고, 첫 요약 줄과 본문이 어긋나면 틀린 쪽 문장을 고릅니다.
- reason은 무엇이 어떻게 문제인지 한국어 한 문장으로 씁니다. 근거 자료를 새로 판단하거나 투자 조언을 하지 않습니다.

출력은 다음 JSON 하나만 냅니다:
{"problems":[{"sentenceNo":1,"reason":"..."}]}"""

REVIEW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "problems": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"sentenceNo": {"type": "integer"}, "reason": {"type": "string"}},
                "required": ["sentenceNo", "reason"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["problems"],
    "additionalProperties": False,
}


class InvalidReview(InvalidLlmOutput):
    pass


def parse_review(text: str, allowed_nos: frozenset[int]) -> tuple[Problem, ...]:
    """검토 응답을 검증한다. 형식이 틀리거나 고칠 수 있는 문장 목록 밖 번호를 고르면 InvalidReview.
    같은 문장을 여러 번 고르면 이유를 이어 붙여 하나로 둔다."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as cause:
        raise InvalidReview("review is not valid JSON") from cause
    if not isinstance(data, dict) or not isinstance(data.get("problems"), list):
        raise InvalidReview("review does not have problems")
    reasons: dict[int, list[str]] = {}
    for raw in data["problems"]:
        item = output_item(raw, "problem", InvalidReview)
        no = item.get("sentenceNo")
        if not isinstance(no, int) or isinstance(no, bool) or no not in allowed_nos:
            raise InvalidReview("problem names a sentence that was not given as fixable")
        reasons.setdefault(no, []).append(output_sentence(item, "reason", "problem", InvalidReview))
    return tuple(Problem(no, PROBLEM_REVIEW, " / ".join(r)) for no, r in sorted(reasons.items()))


# ---- LLM 호출 ----

@dataclass(frozen=True)
class CallAttempt:
    attempt: int
    llm_input: str
    response: str | None
    rejection: str | None = None
    error_type: str | None = None
    usage: LlmUsage | None = None

    def as_record(self) -> dict[str, Any]:
        # 호출 자체가 실패해 응답이 없으면 그 사실을 값으로 남긴다(빈 값으로 두지 않는다).
        return {"attempt": self.attempt, "llmInput": self.llm_input,
                "llmResponse": self.response if self.response is not None else "(응답 없음: 호출 실패)",
                "rejection": self.rejection, "usage": usage_record(self.usage)}


@dataclass(frozen=True)
class CallResult:
    value: Any
    rejected: tuple[CallAttempt, ...]
    # 받아들인 시도(끝내 실패했으면 None)
    accepted: CallAttempt | None


def call_llm(client: LlmClient, max_attempts: int, system: str, addendum: str, user: str,
             schema: dict[str, Any], parse: Callable[[str], Any], what: str, job_id: str) -> CallResult:
    """시도 횟수 상한까지 부르고 검증한다. 호출 실패와 검증 거부를 같은 시도로 센다."""
    rejected: list[CallAttempt] = []
    for attempt in range(1, max_attempts + 1):
        response: str | None = None
        usage: LlmUsage | None = None
        try:
            reply = client.complete(system, user, schema, addendum)
            response, usage = reply.text, reply.usage
            value = parse(response)
        except (LlmCallError, InvalidLlmOutput) as error:
            rejected.append(CallAttempt(attempt, user, response, str(error), type(error).__name__,
                                        attempt_usage(usage, error)))
            log.warning("final check %s attempt %d/%d failed for job %s: %s", what, attempt, max_attempts, job_id, error)
            continue
        return CallResult(value, tuple(rejected), CallAttempt(attempt, user, response, usage=usage))
    return CallResult(None, tuple(rejected), None)


# ---- 어댑터와 흐름 ----

@dataclass(frozen=True)
class FixCall:
    """문장 하나를 고치는 호출. llm_input은 처음 작성과 같은 근거 자료 입력이고, 흐름이 메일 문장과 고칠 문장을 더한다."""

    system: str
    addendum: str
    schema: dict[str, Any]
    llm_input: dict[str, Any]


class FinalCheckAdapter(Protocol):
    """본문(draft)을 다루는 쪽. 급변동과 데일리 판이 각자 구현한다."""

    def job_id(self, draft: Any) -> str: ...

    def sentences(self, draft: Any) -> tuple[MailSentence, ...]: ...

    def sources(self, draft: Any) -> Mapping[str, Source]: ...

    def window(self, draft: Any) -> tuple[datetime, datetime]:
        """인용한 기사의 발행 시각이 들어야 하는 범위(양끝 포함)."""

    def mail(self, draft: Any) -> tuple[str | None, str]:
        """(코드가 쓴 첫 요약 줄 또는 None, 메일 글 전체)."""

    def fix_call(self, draft: Any, sentence: MailSentence) -> FixCall: ...

    def parse_fix(self, draft: Any, sentence: MailSentence, text: str) -> Any:
        """고친 문장 응답을 근거 번호 규칙대로 검증한다. 어기면 InvalidLlmOutput."""

    def apply(self, draft: Any, fixed: Mapping[int, Any]) -> Any:
        """고친 문장(문장 번호 -> parse_fix 결과)만 바꾼 새 draft. 나머지 문장은 그대로다."""


@dataclass(frozen=True)
class _Inspection:
    sentences: tuple[MailSentence, ...]
    code: tuple[Problem, ...]


@dataclass
class FinalCheck:
    adapter: FinalCheckAdapter
    client: LlmClient
    max_attempts: int
    job_log: JobLogPort = field(default_factory=DiscardJobLog)

    def run(self, draft: Any, now: datetime) -> Any:
        """마지막 검사를 거쳐 낼 본문을 돌려준다. 무엇을 왜 골랐는지는 작업 기록에 남긴다."""
        job_id = self.adapter.job_id(draft)
        before = self._inspect(job_id, draft, now, 1)
        rewritten = [s.no for s in before.sentences if s.rewritten]
        if not rewritten and not fixable(before.code):
            return self._choose(job_id, draft, now, CHOSE_BEFORE, "다시 쓴 문장과 고칠 수 있는 문제가 없어 검토하지 않음")
        first = self._review(job_id, draft, before, now, 1)
        if first is None:
            return self._choose(job_id, draft, now, CHOSE_BEFORE, "검토 호출이 끝내 실패해 지금 본문을 냄")
        targets = self._targets(before, first)
        if not targets:
            return self._choose(job_id, draft, now, CHOSE_BEFORE, "검토에서 문제를 찾지 못함")
        fixed = self._fix(job_id, draft, before, targets, now)
        if fixed is None:
            return self._choose(job_id, draft, now, CHOSE_BEFORE, "문제 문장을 하나도 고치지 못함")
        after = self._inspect(job_id, fixed, now, 2)
        second = self._review(job_id, fixed, after, now, 2)
        counts = {"beforeFix": quality_count(before.code, first),
                  "afterFix": None if second is None else quality_count(after.code, second)}
        if second is None:
            # 다시 검토가 끝내 실패하면 그때의 본문(고친 본문)을 낸다. 코드 검사를 통과하지 못했으면 고치기 전 본문이다.
            if passes(after.code):
                return self._choose(job_id, fixed, now, CHOSE_AFTER, "다시 검토 호출이 끝내 실패해 지금(고친) 본문을 냄", counts)
            return self._choose(job_id, draft, now, CHOSE_BEFORE, "다시 검토가 실패했고 고친 본문이 코드 검사를 통과하지 못함", counts)
        if passes(after.code) and (not passes(before.code) or counts["afterFix"] < counts["beforeFix"]):
            why = "고친 본문의 문제가 더 적음" if counts["afterFix"] else "고친 뒤 문제가 없음"
            if not passes(before.code):
                why = "고치기 전 본문이 코드 검사를 통과하지 못함"
            return self._choose(job_id, fixed, now, CHOSE_AFTER, why, counts)
        why = "고친 본문이 코드 검사를 통과하지 못함" if not passes(after.code) else "고친 본문의 문제가 적지 않아 고치기 전 본문을 냄"
        return self._choose(job_id, draft, now, CHOSE_BEFORE, why, counts)

    def _inspect(self, job_id: str, draft: Any, now: datetime, round_no: int) -> _Inspection:
        sentences = self.adapter.sentences(draft)
        since, until = self.adapter.window(draft)
        problems = code_check(sentences, self.adapter.sources(draft), since, until)
        self._record(job_id, EntryType.FINAL_CHECK, REASON_CODE_CHECKED, now, round=round_no, data={
            "problems": [p.as_record() for p in problems], "fixable": len(fixable(problems)),
            "passed": passes(problems), "rewrittenSentences": [s.no for s in sentences if s.rewritten]})
        return _Inspection(sentences, problems)

    def _review(self, job_id: str, draft: Any, inspection: _Inspection, now: datetime,
                round_no: int) -> tuple[Problem, ...] | None:
        """메일 전체를 LLM에 보여 문제 문장을 찾는다. 끝내 실패하면 None."""
        first_line, mail = self.adapter.mail(draft)
        data: dict[str, Any] = {"mail": mail, "sentences": [s.as_input() for s in inspection.sentences],
                                "codeCheck": [p.as_record() for p in inspection.code]}
        if first_line:
            data["firstLine"] = first_line
        allowed = frozenset(s.no for s in inspection.sentences if s.fixable)
        result = call_llm(self.client, self.max_attempts, REVIEW_SYSTEM_PROMPT, "",
                          json.dumps(data, ensure_ascii=False), REVIEW_SCHEMA, lambda text: parse_review(text, allowed),
                          "review", job_id)
        self._record_rejected(job_id, result.rejected, now, round_no=round_no)
        if result.accepted is None:
            self._record(job_id, EntryType.FINAL_CHECK, REASON_REVIEW_FAILED, now, round=round_no,
                         attempts=len(result.rejected), error_type=result.rejected[-1].error_type if result.rejected else None,
                         data={"failedAttempts": [a.as_record() for a in result.rejected]})
            return None
        problems: tuple[Problem, ...] = result.value
        self._record(job_id, EntryType.FINAL_CHECK, REASON_REVIEWED, now, round=round_no,
                     attempts=result.accepted.attempt,
                     data={"problems": [p.as_record() for p in problems], **result.accepted.as_record()})
        return problems

    @staticmethod
    def _targets(inspection: _Inspection, review: tuple[Problem, ...]) -> dict[int, list[str]]:
        """고칠 문장과 그 이유. 검토가 고른 문장과, 코드 검사가 고칠 수 있는 문제를 찾은 문장이다(정해진 문구는 빼고)."""
        allowed = {s.no for s in inspection.sentences if s.fixable}
        targets: dict[int, list[str]] = {}
        for p in (*review, *fixable(inspection.code)):
            if p.sentence_no in allowed:
                targets.setdefault(p.sentence_no, []).append(p.detail)
        return dict(sorted(targets.items()))

    def _fix(self, job_id: str, draft: Any, inspection: _Inspection, targets: Mapping[int, list[str]],
             now: datetime) -> Any | None:
        """문제 문장만 하나씩 고친다. 하나도 못 고치면 None."""
        by_no = {s.no: s for s in inspection.sentences}
        fixed: dict[int, Any] = {}
        accepted: dict[int, tuple[CallAttempt, list[str]]] = {}
        first_line, _ = self.adapter.mail(draft)
        mail = {"firstLine": first_line, "sentences": [s.as_input() for s in inspection.sentences]}
        for no, reasons in targets.items():
            sentence = by_no[no]
            call = self.adapter.fix_call(draft, sentence)
            data = dict(call.llm_input)
            data["mail"] = mail
            data["fix"] = {"sentenceNo": no, "kind": sentence.kind, "sentence": sentence.text,
                           "refs": list(sentence.refs), "problems": reasons}
            result = call_llm(self.client, self.max_attempts, call.system, call.addendum,
                              json.dumps(data, ensure_ascii=False), call.schema,
                              lambda text, s=sentence: self.adapter.parse_fix(draft, s, text), "fix", job_id)
            self._record_rejected(job_id, result.rejected, now, sentence_no=no)
            if result.accepted is None:
                self._record(job_id, EntryType.FINAL_CHECK, REASON_FIX_FAILED, now, sentence_no=no,
                             attempts=len(result.rejected), data={"problems": reasons})
                continue
            fixed[no] = result.value
            accepted[no] = (result.accepted, reasons)
        if not fixed:
            return None
        new = self.adapter.apply(draft, fixed)
        after = {s.no: s for s in self.adapter.sentences(new)}
        for no, (attempt, reasons) in accepted.items():
            self.job_log.record(JobLogEntry(
                job_id=job_id, entry_type=EntryType.SENTENCE_FIXED, stage=STAGE_FINAL_CHECK, reason=REASON_FIXED,
                occurred_at=now, sentence_no=no, before=by_no[no].text, after=after[no].text,
                attempts=attempt.attempt, data={"problems": reasons, **attempt.as_record()}))
        return new

    def _choose(self, job_id: str, draft: Any, now: datetime, chosen: str, why: str,
                counts: Mapping[str, int | None] | None = None) -> Any:
        _, mail = self.adapter.mail(draft)
        log.info("final check job=%s chose=%s", job_id, chosen)
        self._record(job_id, EntryType.FINAL_CHECK, REASON_CHOSEN, now,
                     data={"chosen": chosen, "why": why, "problemCounts": dict(counts or {}), "finalText": mail})
        return draft

    def _record_rejected(self, job_id: str, attempts: tuple[CallAttempt, ...], now: datetime, *,
                         round_no: int | None = None, sentence_no: int | None = None) -> None:
        for a in attempts:
            reason = REASON_LLM_CALL_FAILED if a.response is None else REASON_LLM_REJECTED
            self._record(job_id, EntryType.LLM_REJECTED, reason, now, round=round_no, sentence_no=sentence_no,
                         attempts=a.attempt, error_type=a.error_type, data=a.as_record())

    def _record(self, job_id: str, entry_type: EntryType, reason: str, now: datetime, *, data: dict[str, Any],
                round: int | None = None, sentence_no: int | None = None, attempts: int = 0,
                error_type: str | None = None) -> None:
        self.job_log.record(JobLogEntry(
            job_id=job_id, entry_type=entry_type, stage=STAGE_FINAL_CHECK, reason=reason, occurred_at=now,
            sentence_no=sentence_no, round=round, error_type=error_type, attempts=attempts, data=data))
