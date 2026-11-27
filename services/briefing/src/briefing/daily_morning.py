"""아침 판 본문의 내용과 글자판. HTML(daily_html)도 같은 값(MorningView)을 같은 순서로 쓴다. Kafka를 모른다.

Claude 본문이 있으면: 제목 → 전 거래일 한국 증시 → 밤사이 이슈 → 환율·금리 → 그 밖의 이슈 → 내 ETF(개장 흐름과 예상) → 근거 약함 모음 → 면책 →
서버 상태 → 근거 목록. 항목별 숫자 목록은 본문에 싣지 않고, 숫자는 문장과 근거 목록에만 나온다.
Claude 본문이 없으면(규칙 기반): 제목 → 항목별 숫자 → 환율 이슈 → 금리 이슈 → 면책 → 서버 상태.
근거 번호는 본문에 나온 순서(전 거래일 한국 증시 → 밤사이 이슈 → 환율·금리 → 그 밖의 이슈 → ETF 예상)로 붙는다.
"""
from __future__ import annotations

from dataclasses import dataclass

from briefing.compose import KST
from briefing.daily_post_close import IssueRow, issue_rows
from briefing.daily_prompts import DIRECTION_UNSURE, DIRECTIONS
from briefing.daily_sections import (
    DAILY_DISCLAIMER,
    DAILY_INSUFFICIENT,
    ITEM_KR_PREV,
    NO_DATA,
    DailyInput,
    EtfDay,
    Section,
    etf_groups,
    etf_name,
)
from briefing.daily_output import POINT_KR_PREV, POINT_OUTLOOK, DailyOutput
from briefing.daily_report import headline, section_lines
from briefing.open_flow import signed

KR_PREV_TITLE = ITEM_KR_PREV
OVERNIGHT_TITLE = "밤사이 이슈"
FX_RATE_TITLE = "환율·금리"
OTHERS_TITLE = "그 밖의 이슈"
OUTLOOK_TITLE = "내 ETF: 개장 흐름과 예상"
# 근거가 약한 문장은 그 줄 끝에 표시만 하고, 부족한 점은 면책 위 한곳에 모은다.
WEAK_MARK = "(근거 약함)"
WEAK_TITLE = "※ 근거 약함"
NO_OPEN_FLOW = "개장 흐름 자료 없음"
# 멤버마다 거르고 나니 밤사이 이슈 문장이 하나도 남지 않은 판
NO_OVERNIGHT = "받은 자료로 정리한 밤사이 이슈가 없습니다."
# 예상은 받은 자료로 짐작한 것이라 맞지 않을 수 있음을 면책 줄 아래에 따로 밝힌다.
OUTLOOK_NOTE = "※ ETF별 예상은 받은 자료로 짐작한 것이며 실제 등락과 다를 수 있습니다."
# 본문에 보이는 방향 문구. 키는 출력 형식의 방향 값이다.
DIRECTION_TEXT = {
    DIRECTIONS[0]: "상승 예상",
    DIRECTIONS[1]: "하락 예상",
    DIRECTIONS[2]: "보합 예상",
    DIRECTION_UNSURE: "예상 어려움(근거 부족)",
}


@dataclass(frozen=True)
class Sentence:
    """번호가 붙는 문장 하나(밤사이 이슈, 그 밖의 이슈). numbers는 근거 번호, shortfall은 근거가 약하면 부족한 점이다."""

    statement: str
    numbers: tuple[int, ...]
    shortfall: str | None


@dataclass(frozen=True)
class OutlookRow:
    """ETF 한 줄(이름, 개장 흐름, 방향)과 그 아래 이유. numbers가 비어 있으면 근거 번호 없이 쓴 이유(예상 어려움)다."""

    symbol: str
    name: str
    flow: str
    direction: str
    reason: str
    numbers: tuple[int, ...]
    shortfall: str | None

    @property
    def line(self) -> str:
        return f"{self.name}  {self.flow}  {DIRECTION_TEXT[self.direction]}"


@dataclass(frozen=True)
class MorningView:
    """아침 판 본문의 내용. has_output가 False면 LLM 없이 코드가 쓴 본문이다.
    weak는 근거가 약한 문장마다 (어느 문장인지, 부족한 점)이다."""

    has_output: bool
    insufficient: bool
    # 전 거래일 한국 증시: 코드가 쓴 마감 등락률 줄과 그 아래 Claude가 쓴 이유 문장(없으면 None).
    kr_prev_line: str
    kr_prev: Sentence | None
    points: tuple[Sentence, ...]
    issues: tuple[IssueRow, ...]
    others: tuple[Sentence, ...]
    groups: tuple[tuple[str, tuple[OutlookRow, ...]], ...]
    weak: tuple[tuple[str, str], ...]
    evidence: tuple[tuple[int, str], ...]


def open_flow_text(etf: EtfDay) -> str:
    """'개장 +1.8% → 09:10 +2.3%'. 시가 등락률과 마지막 시세의 전일 종가 대비 등락률이다. 자료가 없으면 안내 문구."""
    flow = etf.open_flow
    if flow is None or not flow.present or flow.open_rate is None:
        return NO_OPEN_FLOW
    if flow.end_at is None or flow.rate_vs_prev_close is None:
        return f"개장 {signed(flow.open_rate)}"
    return f"개장 {signed(flow.open_rate)} → {flow.end_at.astimezone(KST):%H:%M} {signed(flow.rate_vs_prev_close)}"


def issue_label(row: IssueRow) -> str:
    """환율·금리 칸 안의 줄 머리말('환율', '금리')."""
    return row.title.removesuffix(" 이슈")


def morning_view(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None) -> MorningView:
    by_ref = {e.ref: e for s in sections for e in s.entries}
    usable = output is not None and not output.insufficient_evidence
    numbers: dict[str, int] = {}

    def number(refs: tuple[str, ...]) -> tuple[int, ...]:
        return tuple(numbers.setdefault(ref, len(numbers) + 1) for ref in refs)

    kr_point = next((p for p in output.issues if p.kind == POINT_KR_PREV), None) if usable else None
    kr_prev = Sentence(kr_point.statement, number(kr_point.refs), kr_point.shortfall) if kr_point else None
    points = tuple(Sentence(p.statement, number(p.refs), p.shortfall) for p in output.points) if usable else ()
    issues = issue_rows(i, output if usable else None, number)
    others = tuple(Sentence(p.statement, number(p.refs), p.shortfall) for p in output.others) if usable else ()
    outlooks = {p.symbol: p for p in output.outlooks} if usable else {}
    groups = []
    for title, members in etf_groups(i.etfs or ()):
        rows = tuple(OutlookRow(e.symbol, etf_name(e), open_flow_text(e), p.direction, p.statement,
                                number(p.refs), p.shortfall)
                     for e in members for p in [outlooks.get(e.symbol)] if p is not None)
        if rows:
            groups.append((title, rows))
    weak = [*([(KR_PREV_TITLE, kr_prev.shortfall)] if kr_prev is not None and kr_prev.shortfall else []),
            *((f"{OVERNIGHT_TITLE} {n}", s.shortfall) for n, s in enumerate(points, start=1) if s.shortfall),
            *((issue_label(r), r.shortfall) for r in issues if r.shortfall),
            *((OTHERS_TITLE, s.shortfall) for s in others if s.shortfall),
            *((row.name, row.shortfall) for _, rows in groups for row in rows if row.shortfall)]
    evidence: tuple[tuple[int, str], ...] = tuple((n, by_ref[ref].describe) for ref, n in numbers.items())
    return MorningView(output is not None, bool(output is not None and output.insufficient_evidence),
                       kr_prev_text(sections), kr_prev, points, issues, others, tuple(groups), tuple(weak), evidence)


def kr_prev_text(sections: tuple[Section, ...]) -> str:
    """전 거래일 한국 증시 칸의 숫자 줄. 항목의 줄(마감 등락률 또는 자료 없음과 그 이유)을 그대로 쓴다."""
    section = next((s for s in sections if s.name == ITEM_KR_PREV), None)
    return section.lines[0] if section is not None and section.lines else NO_DATA


def cite(numbers: tuple[int, ...]) -> str:
    return f"[근거 {', '.join(str(n) for n in numbers)}]"


def _with_cite(text: str, numbers: tuple[int, ...], shortfall: str | None) -> str:
    return f"{text}{f' {cite(numbers)}' if numbers else ''}{f' {WEAK_MARK}' if shortfall else ''}"


def _rule_based_lines(i: DailyInput, sections: tuple[Section, ...], v: MorningView) -> list[str]:
    """LLM 없이 코드가 쓴 본문: 항목별 숫자와 이슈 칸 대체 줄(받은 숫자, 안내 줄)."""
    lines = [*headline(i), *section_lines(sections)]
    if v.has_output:
        # Claude가 근거 부족이라고 답한 판. 숫자 목록 아래에 그 사실만 쓴다.
        lines.extend(["", f"{OVERNIGHT_TITLE}:", f"  {DAILY_INSUFFICIENT}"])
    for issue in v.issues:
        lines.extend(["", f"{issue.title}:", f"  - {issue.statement}"])
    return lines


def morning_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None,
                 status_text: str | None = None) -> str:
    """아침 판 글자판. output이 None이거나 근거 부족이면 숫자·목록과 이슈 칸 대체 줄만 있는 규칙 기반 본문이다."""
    v = morning_view(i, sections, output)
    if not v.has_output or v.insufficient:
        lines = _rule_based_lines(i, sections, v)
    else:
        lines = [*headline(i), "", f"■ {KR_PREV_TITLE}", f"  {v.kr_prev_line}"]
        if v.kr_prev is not None:
            lines.append(f"  {_with_cite(v.kr_prev.statement, v.kr_prev.numbers, v.kr_prev.shortfall)}")
        lines.extend(["", f"■ {OVERNIGHT_TITLE}"])
        lines.extend(f"  {n}. {_with_cite(s.statement, s.numbers, s.shortfall)}"
                     for n, s in enumerate(v.points, start=1))
        if not v.points:
            lines.append(f"  {NO_OVERNIGHT}")
        lines.extend(["", f"■ {FX_RATE_TITLE}"])
        lines.extend(f"  - {issue_label(r)}: {_with_cite(r.statement, r.numbers, r.shortfall)}" for r in v.issues)
        if v.others:
            lines.extend(["", f"■ {OTHERS_TITLE}"])
            lines.extend(f"  - {_with_cite(s.statement, s.numbers, s.shortfall)}" for s in v.others)
        if v.groups:
            lines.extend(["", f"■ {OUTLOOK_TITLE}"])
            for title, rows in v.groups:
                if title:
                    lines.append(f"  [{title}]")
                for row in rows:
                    lines.append(f"  - {row.line}{f' {WEAK_MARK}' if row.shortfall else ''}")
                    # 방향을 정하지 못한 ETF는 근거 번호 없이 쓴다.
                    lines.append(f"    {row.reason}{f' {cite(row.numbers)}' if row.numbers else ''}")
        if v.weak:
            lines.extend(["", f"{WEAK_TITLE} {len(v.weak)}건"])
            lines.extend(f"  - {label}: {shortfall}" for label, shortfall in v.weak)
    lines.extend(["", DAILY_DISCLAIMER])
    if v.groups:
        lines.append(OUTLOOK_NOTE)
    if status_text:
        lines.extend(["", status_text])
    if v.evidence:
        lines.extend(["", "근거:"])
        lines.extend(f"  {n}. {describe}" for n, describe in v.evidence)
    return "\n".join(lines)


def outlook_records(output: DailyOutput | None) -> list[dict[str, object]]:
    """작업 기록에 남기는 ETF별 예상. 마감 후 판이 같은 날 예상과 실제 등락을 대조할 때 다시 읽는 형태다."""
    if output is None or output.insufficient_evidence:
        return []
    return [{"symbol": p.symbol, "direction": p.direction, "reason": p.statement, "refs": list(p.refs),
             "weak": p.shortfall is not None, "shortfall": p.shortfall}
            for p in output.outlooks if p.kind == POINT_OUTLOOK]
