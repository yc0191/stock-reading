"""데일리 판(아침 판, 마감 후 판)의 마지막 검사 어댑터. Claude가 쓴 문장(요약, ETF 이유, 눈에 띄는 점, 환율·금리 이슈,
그 밖의 이슈, ETF별 예상, 아침 예상 대조 이유)을 메일 문장으로 보고, 고칠 때는 문제 문장들만 한 호출로 다시 받는다."""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Mapping

from briefing.daily_sentences import DailyDraft
from briefing.daily_prompts import NO_REASON_STATEMENT, SYSTEM_PROMPT, SYSTEM_PROMPT_POST_CLOSE
from briefing.daily_sections import KIND_NEWS
from briefing.daily_output import POINT_ETF, POINT_NOTABLE, DailyOutput, Point, member_repeat_reason, sentence_slots
from briefing.daily_report import DailyLlmWriter, conclusion, llm_text, llm_user, summary_banned_names
from briefing.events import EDITION_POST_CLOSE
from briefing.final_check import BatchFixCall, FinalCheck, MailSentence, Source
from briefing.job_log import JobLogPort
from briefing.llm_output import (
    InvalidLlmOutput,
    check_no_ticker,
    output_cited_refs,
    output_sentence,
    ticker_pattern,
)
from briefing.sentence_research import WHOLE_REPORT

# 마지막 검사가 문장으로 보는 DailyOutput 칸(요약 줄 다음, 메일에 나오는 차례)
OUTPUT_FIELDS = ("points", "issues", "others", "outlooks", "reviews")
# 아침 판에서 멤버 모두에게 같은 글로 나가는 칸. 고친 문장에도 ETF 이름을 쓰지 않는다.
MORNING_SHARED_FIELDS = frozenset({"points", "issues", "others"})

# 메일 문장의 종류. ETF 이유·눈에 띄는 점 외에는 DailyOutput 칸 이름을 그대로 쓴다.
KIND_SUMMARY = "summary"
POINT_KINDS = {POINT_ETF: "etf-reason", POINT_NOTABLE: "notable"}

FIX_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"statement": {"type": "string"}, "refs": {"type": "array", "items": {"type": "string"}}},
    "required": ["statement", "refs"],
    "additionalProperties": False,
}

FIX_ADDENDUM = """

마지막 검토 뒤 고치기 요청:
- 입력의 mail은 나가기 직전 메일의 문장 목록(sentences, firstLine은 코드가 쓴 결론 줄)이고, fixes 칸은 검토에서 문제가 나온 문장들(sentenceNo, kind, sentence, refs)과 그 문제(problems)입니다.
- fixes의 문장마다 그 문장만 problems가 풀리도록 고쳐 씁니다. 다른 문장과 모순되거나 같은 내용을 되풀이하지 않게 하고(함께 고치는 문장끼리도), 결론 줄과 본문에 맞게 씁니다. 먼저 나온 문장이 이유로 쓴 기사를 이유로 썼다는 문제이면 그 기사 대신 다른 자료를 인용하거나 그 문장에 맞는 내용만 남깁니다.
- 이번에는 위의 출력 형식 대신 다음 JSON 하나만 냅니다. fixes에는 고친 문장마다 한 칸을 fixes 입력의 차례대로 담고, sentenceNo는 그 문장 번호, statement는 고친 문장, refs는 그 문장의 근거 번호입니다.
{"fixes":[{"sentenceNo":1,"statement":"...","refs":["..."]}]}
- 원래 refs가 빈 목록인 문장(요약 줄, 이유를 찾지 못한 문장)은 refs를 빈 목록으로 둡니다. 원래 refs가 있으면 입력에 있는 근거 번호를 하나 이상 넣습니다.
- 요약 줄(kind가 summary)과 아침 판의 환율·금리 이슈(kind가 fx, rate)·그 밖의 이슈(kind가 other)·전 거래일 한국 증시(kind가 krPrev)에는 ETF 이름이나 종목코드를 쓰지 않습니다. 근거 약함 표시는 바꾸지 않습니다.
- 자료에 없는 내용을 지어내지 않고, 근거 번호·단정 금지·티커 금지·구성 종목 이슈 되풀이 금지 같은 위의 규칙은 그대로 지킵니다."""


class InvalidFix(InvalidLlmOutput):
    pass


@dataclass(frozen=True)
class _Unit:
    """DailyOutput 안의 문장 자리. field는 DailyOutput 칸 이름, index는 그 칸 안의 순서다."""

    field: str
    index: int
    kind: str
    statement: str
    point: Point | None


def _units(output: DailyOutput | None) -> list[_Unit]:
    """메일에 나오는 차례: 요약 줄 -> 요약 문장·ETF 이유·눈에 띄는 점 -> 환율·금리 이슈 -> 그 밖의 이슈 -> ETF별 예상 -> 대조 이유."""
    if output is None or output.insufficient_evidence:
        return []
    units = [_Unit("summary", n, KIND_SUMMARY, line, None) for n, line in enumerate(output.summary)]
    for name in OUTPUT_FIELDS:
        for n, p in enumerate(getattr(output, name)):
            kind = POINT_KINDS.get(p.kind, KIND_SUMMARY) if name == "points" else p.kind or name
            units.append(_Unit(name, n, kind, p.statement, p))
    return units


@dataclass(frozen=True)
class DailyFinalCheck:
    writer: DailyLlmWriter

    def job_id(self, draft: DailyDraft) -> str:
        return draft.job.job_id

    def sentences(self, draft: DailyDraft) -> tuple[MailSentence, ...]:
        rewritten = draft.draft.rewritten
        names = {e.symbol: e.label() for e in draft.input.etfs or ()}
        # 다시 쓴 문장 번호는 재검색의 문장 자리(sentence_slots) 번호다.
        slots = {slot: n for n, slot in enumerate(sentence_slots(draft.draft.output), start=1)} \
            if draft.draft.output is not None else {}
        out = []
        for no, u in enumerate(_units(draft.draft.output), start=1):
            p = u.point
            again = WHOLE_REPORT in rewritten or slots.get((u.field, u.index)) in rewritten
            out.append(MailSentence(
                no, u.kind, u.statement, p.refs if p else (), bool(p and p.shortfall), again,
                # 이유를 찾지 못한 ETF의 정해진 문장은 고쳐 쓰지 않는다(결론 줄이 이 문장으로 ETF 수를 센다).
                fixable=u.statement.strip() != NO_REASON_STATEMENT, about=names.get(p.symbol) if p and p.symbol else None))
        return tuple(out)

    def sources(self, draft: DailyDraft) -> Mapping[str, Source]:
        return {e.ref: Source(e.kind == KIND_NEWS, e.published_at) for s in draft.sections for e in s.entries}

    def window(self, draft: DailyDraft) -> tuple[datetime, datetime]:
        return draft.input.range_start, draft.input.as_of

    def mail(self, draft: DailyDraft) -> tuple[str | None, str]:
        i, output = draft.input, draft.draft.output
        first = conclusion(i, output) if i.edition == EDITION_POST_CLOSE else None
        return first, draft.draft.text

    def batch_fix_call(self, draft: DailyDraft) -> BatchFixCall:
        # 처음 작성·다시 쓰기와 같은 지시문과 근거 입력을 앞에 두어 그 캐시를 읽고, 고치기 요청은 근거 뒤에 붙인다.
        i, w = draft.input, self.writer
        system = SYSTEM_PROMPT_POST_CLOSE if i.edition == EDITION_POST_CLOSE else SYSTEM_PROMPT
        return BatchFixCall(system, FIX_ADDENDUM, FIX_SCHEMA,
                            lambda tail: llm_user(i, draft.sections, w.settings, w.core, tail))

    def parse_fix(self, draft: DailyDraft, sentence: MailSentence, text: str) -> tuple[str, tuple[str, ...]]:
        """고친 문장과 근거 번호. 원래 근거 번호가 있던 문장은 목록 안 번호를 하나 이상, 없던 문장은 빈 목록이어야 한다."""
        i = draft.input
        try:
            data = json.loads(text)
        except json.JSONDecodeError as cause:
            raise InvalidFix("fix is not valid JSON") from cause
        if not isinstance(data, dict):
            raise InvalidFix("fix is not an object")
        statement = output_sentence(data, "statement", "fix", InvalidFix)
        post_close = i.edition == EDITION_POST_CLOSE
        # 미국 티커 금지는 처음 작성과 같이 마감 후 판에만 건다.
        if post_close:
            check_no_ticker(statement, ticker_pattern(self.writer.forbidden_tickers(i)), "fix", InvalidFix)
        if sentence.refs:
            allowed = frozenset(e.ref for s in draft.sections for e in s.entries)
            refs = output_cited_refs(data, "refs", "fix", allowed, InvalidFix)
        elif data.get("refs") == []:
            refs = ()
        else:
            raise InvalidFix("fix must keep a sentence without refs without refs")
        unit = _units(draft.draft.output)[sentence.no - 1]
        # 마감 후 판 요약 줄은 멤버 모두에게 같은 글이라 ETF 이름·종목코드를 쓰지 않는다(처음 작성의 검사와 같다).
        if unit.field == "summary" and any(name in statement for name in summary_banned_names(i)):
            raise InvalidFix("summary mentions an etf name or code")
        # 아침 판 밤사이 이슈·환율·금리·그 밖의 이슈도 멤버 모두에게 같은 글이다(처음 작성의 검사와 같다).
        if not post_close and unit.field in MORNING_SHARED_FIELDS \
                and any(name in statement for name in summary_banned_names(i)):
            raise InvalidFix(f"{unit.kind} mentions an etf name or code")
        if unit.point is not None and unit.point.kind == POINT_ETF:
            issues = tuple(x for x in i.member_issues if x.etf == unit.point.symbol)
            if member_repeat_reason(statement, issues) is not None:
                raise InvalidFix("etf reason repeats the member issue line")
        return statement, refs

    def apply(self, draft: DailyDraft, fixed: Mapping[int, Any]) -> DailyDraft:
        output = draft.draft.output
        units = _units(output)
        fields: dict[str, list[Any]] = {name: list(getattr(output, name))
                                        for name in ("summary", *OUTPUT_FIELDS)}
        for no, (statement, refs) in fixed.items():
            u = units[no - 1]
            fields[u.field][u.index] = statement if u.point is None else replace(u.point, statement=statement, refs=refs)
        new = replace(output, **{name: tuple(values) for name, values in fields.items()})
        body = replace(draft.draft, text=llm_text(draft.input, draft.sections, new), cited=new.cited_refs(),
                       weak_shortfalls=new.weak_shortfalls(), output=new)
        return replace(draft, draft=body)


def daily_final_check(writer: DailyLlmWriter | None, job_log: JobLogPort) -> FinalCheck | None:
    """LLM 작성기가 있을 때만 마지막 검사를 만든다. 검토·고침은 같은 Claude 클라이언트와 시도 횟수 상한을 쓴다."""
    if writer is None:
        return None
    return FinalCheck(DailyFinalCheck(writer), writer.client, writer.max_attempts, job_log)
