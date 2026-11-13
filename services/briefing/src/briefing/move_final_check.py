"""급변동 보고서의 마지막 검사 어댑터. 원인 하나를 메일 문장 하나로 보고, 고칠 때는 그 원인 하나만 다시 받는다."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Mapping

from briefing.final_check import FinalCheck, FixCall, MailSentence, Source
from briefing.job_log import JobLogPort
from briefing.report import (
    KIND_ARTICLE,
    OUTPUT_SCHEMA,
    SYSTEM_PROMPT,
    Cause,
    InvalidOutput,
    LlmOutput,
    LlmWriter,
    ReportWriter,
    build_llm_input,
    evidence_catalog,
    llm_report,
    move_headline,
    parse_output,
    us_symbols_in_input,
)
from briefing.sentence_research import WHOLE_REPORT, MoveDraft, cause_text

KIND_CAUSE = "cause"

FIX_ADDENDUM = """

마지막 검토 뒤 고치기 요청:
- 입력의 mail은 나가기 직전 메일의 원인 문장 목록(sentences, firstLine은 코드가 쓴 첫 줄)이고, fix 칸은 검토에서 문제가 나온 원인 하나(sentenceNo, sentence, refs)와 그 문제(problems)입니다.
- 그 원인 하나만 problems가 풀리도록 고쳐 씁니다. 다른 원인과 모순되거나 같은 내용을 되풀이하지 않게 하고, 첫 줄의 방향·대상과 맞게 씁니다. 다른 원인과 같은 기사를 이유로 썼다는 문제이면 그 기사 대신 다른 자료를 인용하거나, 그 기사가 이 원인에만 맞으면 그 내용을 이 원인에 모읍니다.
- causes에는 고친 원인 하나만 담고 insufficientEvidence는 false로 둡니다. weak와 shortfall은 원래 원인 그대로 냅니다(근거 약함 표시는 바꾸지 않습니다).
- 자료에 없는 내용을 지어내지 않고, 근거 번호·단정 금지·티커 금지 같은 위의 규칙은 그대로 지킵니다."""


@dataclass(frozen=True)
class MoveFinalCheck:
    writer: LlmWriter

    def job_id(self, draft: MoveDraft) -> str:
        return draft.input.job_id

    def sentences(self, draft: MoveDraft) -> tuple[MailSentence, ...]:
        output = draft.report.output
        if output is None or output.insufficient_evidence:
            return ()
        whole = WHOLE_REPORT in draft.rewritten
        return tuple(MailSentence(n, KIND_CAUSE, cause_text(c), c.cited(), c.shortfall is not None,
                                  whole or n in draft.rewritten)
                     for n, c in enumerate(output.causes, start=1))

    def sources(self, draft: MoveDraft) -> Mapping[str, Source]:
        return {ref: Source(item.kind == KIND_ARTICLE, item.published_at)
                for ref, item in evidence_catalog(draft.input).items()}

    def window(self, draft: MoveDraft) -> tuple[datetime, datetime]:
        s, as_of = self.writer.settings, draft.input.as_of
        return as_of - s.evidence_window, as_of + s.delta

    def mail(self, draft: MoveDraft) -> tuple[str | None, str]:
        return move_headline(draft.input), draft.report.text

    def fix_call(self, draft: MoveDraft, sentence: MailSentence) -> FixCall:
        # 처음 작성과 같은 지시문 블록을 써서 그 캐시를 읽고, 고치기 요청은 덧붙임 블록으로 보낸다.
        return FixCall(SYSTEM_PROMPT, FIX_ADDENDUM, OUTPUT_SCHEMA, build_llm_input(draft.input, self.writer.settings))

    def parse_fix(self, draft: MoveDraft, sentence: MailSentence, text: str) -> Cause:
        i = draft.input
        output = parse_output(text, frozenset(evidence_catalog(i)), us_symbols_in_input(i))
        if output.insufficient_evidence or len(output.causes) != 1:
            raise InvalidOutput("fix must return exactly one cause")
        current = draft.report.output.causes[sentence.no - 1]
        return replace(output.causes[0], shortfall=current.shortfall)

    def apply(self, draft: MoveDraft, fixed: Mapping[int, Any]) -> MoveDraft:
        causes = list(draft.report.output.causes)
        for no, cause in fixed.items():
            causes[no - 1] = cause
        return replace(draft, report=llm_report(draft.input, LlmOutput(tuple(causes), False)))


def move_final_check(writer: ReportWriter, job_log: JobLogPort) -> FinalCheck | None:
    """LLM 작성기일 때만 마지막 검사를 만든다. 검토·고침은 같은 Claude 클라이언트와 시도 횟수 상한을 쓴다."""
    if not isinstance(writer, LlmWriter):
        return None
    return FinalCheck(MoveFinalCheck(writer), writer.client, writer.max_attempts, job_log)
