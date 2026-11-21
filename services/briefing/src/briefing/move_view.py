"""레벨 상 메일 한 통의 내용(MoveView)과 글자판. 글자판과 HTML이 같은 값에서 나오도록 내용을 먼저 만들고 모양은 따로 입힌다.

순서: 머리말 → 오늘 흐름 → 원인(제목, 구체 문장, 뜻 문장, 시각 차이) → 근거 목록(맨 아래 한 번) → 면책.
'확인하지 못한 것'은 메일에 싣지 않고 작업 기록(job_log)에만 남긴다. Kafka를 모른다.
"""
from __future__ import annotations

from dataclasses import dataclass

from briefing.move_flow import FLOW_TITLE

CONFLICT_LABEL = "시각 차이"
EVIDENCE_TITLE = "근거:"


@dataclass(frozen=True)
class MoveViewCause:
    title: str
    detail: str
    detail_numbers: tuple[int, ...]
    meaning: str
    meaning_numbers: tuple[int, ...]
    # 기사끼리 수치·해석이 엇갈릴 때 LLM이 낸 한 문장과 그 근거 번호
    conflict: str | None = None
    conflict_numbers: tuple[int, ...] = ()


@dataclass(frozen=True)
class MoveViewEvidence:
    number: int
    text: str
    url: str | None = None


@dataclass(frozen=True)
class MoveView:
    headline: str
    flow: tuple[str, ...] = ()
    # 원인 대신 쓰는 줄(근거 부족 문구, 규칙 기반의 비교 사실 등)
    notes: tuple[str, ...] = ()
    causes: tuple[MoveViewCause, ...] = ()
    evidence_title: str = EVIDENCE_TITLE
    evidence: tuple[MoveViewEvidence, ...] = ()
    closing: str = ""


def cite(numbers: tuple[int, ...]) -> str:
    return f"[근거 {', '.join(str(n) for n in numbers)}]"


def view_text(v: MoveView) -> str:
    lines = [v.headline, ""]
    if v.flow:
        lines.append(f"{FLOW_TITLE}:")
        lines.extend(f"- {line}" for line in v.flow)
        lines.append("")
    if v.notes:
        lines.extend(v.notes)
        lines.append("")
    for index, c in enumerate(v.causes, start=1):
        lines.append(f"{index}. {c.title}")
        lines.append(f"- {c.detail} {cite(c.detail_numbers)}")
        lines.append(f"- {c.meaning} {cite(c.meaning_numbers)}")
        if c.conflict:
            lines.append(f"- {CONFLICT_LABEL}: {c.conflict} {cite(c.conflict_numbers)}")
        lines.append("")
    if v.evidence:
        lines.append(v.evidence_title)
        for e in v.evidence:
            lines.append(f"  {e.number}. {e.text}" + (f" {e.url}" if e.url else ""))
    if v.closing:
        lines.append(v.closing)
    return "\n".join(lines)

