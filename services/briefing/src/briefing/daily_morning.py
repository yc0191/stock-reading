"""아침 판 본문의 내용과 글자판. HTML(daily_html)도 같은 값(MorningView)을 같은 순서로 쓴다. Kafka를 모른다.

순서: 제목 → 항목별 숫자 → 요약 → 환율 이슈 → 금리 이슈 → ETF별 예상 → 면책 → 서버 상태 → 근거 목록.
근거 번호는 본문에 나온 순서(요약 → 이슈 칸 → ETF 예상)로 붙는다.
"""
from __future__ import annotations

from dataclasses import dataclass

from briefing.daily_post_close import IssueRow, issue_rows
from briefing.daily_prompts import DIRECTION_UNSURE, DIRECTIONS
from briefing.daily_report import (
    DAILY_DISCLAIMER,
    DAILY_INSUFFICIENT,
    POINT_OUTLOOK,
    DailyInput,
    DailyOutput,
    Entry,
    Section,
    etf_groups,
    etf_name,
    headline,
    section_lines,
)
from briefing.llm_output import weak_note_line

OUTLOOK_TITLE = "ETF별 예상"
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
class OutlookRow:
    """ETF 한 줄(이름, 방향)과 그 아래 이유. numbers가 비어 있으면 근거 번호 없이 쓴 이유(예상 어려움)다."""

    symbol: str
    line: str
    direction: str
    reason: str
    numbers: tuple[int, ...]
    shortfall: str | None


@dataclass(frozen=True)
class MorningView:
    """아침 판 본문의 내용. has_output가 False면 LLM 없이 코드가 쓴 본문이다."""

    has_output: bool
    insufficient: bool
    points: tuple[tuple[str, tuple[int, ...], str | None], ...]
    issues: tuple[IssueRow, ...]
    groups: tuple[tuple[str, tuple[OutlookRow, ...]], ...]
    evidence: tuple[tuple[int, str], ...]


def morning_view(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None) -> MorningView:
    by_ref = {e.ref: e for s in sections for e in s.entries}
    usable = output is not None and not output.insufficient_evidence
    numbers: dict[str, int] = {}

    def number(refs: tuple[str, ...]) -> tuple[int, ...]:
        return tuple(numbers.setdefault(ref, len(numbers) + 1) for ref in refs)

    points = tuple((p.statement, number(p.refs), p.shortfall) for p in output.points) if usable else ()
    issues = issue_rows(i, output if usable else None, number)
    outlooks = {p.symbol: p for p in output.outlooks} if usable else {}
    groups = []
    for title, members in etf_groups(i.etfs or ()):
        rows = tuple(OutlookRow(e.symbol, f"{etf_name(e)} {DIRECTION_TEXT[p.direction]}", p.direction, p.statement,
                                number(p.refs), p.shortfall)
                     for e in members for p in [outlooks.get(e.symbol)] if p is not None)
        if rows:
            groups.append((title, rows))
    evidence: tuple[tuple[int, str], ...] = tuple((n, by_ref[ref].describe) for ref, n in numbers.items())
    return MorningView(output is not None, bool(output is not None and output.insufficient_evidence), points, issues,
                       tuple(groups), evidence)


def cite(numbers: tuple[int, ...]) -> str:
    return f"[근거 {', '.join(str(n) for n in numbers)}]"


def morning_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None,
                 status_text: str | None = None) -> str:
    """아침 판 글자판. output이 None이면 요약·예상 없이 코드가 쓴 숫자·목록과 이슈 칸 대체 줄만 있는 규칙 기반 본문이다."""
    v = morning_view(i, sections, output)
    lines = [*headline(i), *section_lines(sections)]
    if v.has_output:
        lines.extend(["", "요약:"])
        if v.insufficient:
            lines.append(f"  {DAILY_INSUFFICIENT}")
        for statement, nums, shortfall in v.points:
            lines.append(f"  - {statement} {cite(nums)}")
            if shortfall:
                lines.append(weak_note_line(shortfall, "    "))
    for issue in v.issues:
        lines.extend(["", f"{issue.title}:"])
        lines.append(f"  - {issue.statement}{f' {cite(issue.numbers)}' if issue.numbers else ''}")
        if issue.shortfall:
            lines.append(weak_note_line(issue.shortfall, "    "))
    if v.groups:
        lines.extend(["", f"■ {OUTLOOK_TITLE}"])
        for title, rows in v.groups:
            if title:
                lines.append(f"  [{title}]")
            for row in rows:
                lines.append(f"  - {row.line}")
                # 방향을 정하지 못한 ETF는 근거 번호 없이 쓴다.
                lines.append(f"    이유: {row.reason}{f' {cite(row.numbers)}' if row.numbers else ''}")
                if row.shortfall:
                    lines.append(weak_note_line(row.shortfall, "      "))
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
