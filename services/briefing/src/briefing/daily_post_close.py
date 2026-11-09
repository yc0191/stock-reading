"""마감 후 판 본문의 내용과 글자판. HTML(daily_html)도 같은 값(PostCloseView)을 같은 순서로 쓴다. Kafka를 모른다."""
from __future__ import annotations

from dataclasses import dataclass

from briefing.daily_report import (
    DAILY_DISCLAIMER,
    DAILY_INSUFFICIENT,
    ITEM_ETFS,
    ITEM_KR_TODAY,
    MINUS,
    NO_DATA,
    POINT_ETF,
    POINT_NOTABLE,
    DailyInput,
    DailyOutput,
    Entry,
    Section,
    etf_line,
    conclusion,
    etf_groups,
    headline,
    member_line,
    section_title,
)
from briefing.llm_output import weak_note_line
from briefing.move_figures import KOSDAQ_NAME, KOSPI_NAME, index_figure, round_cent
from briefing.sources import MARK_TRUNCATED


@dataclass(frozen=True)
class EtfRow:
    """ETF 한 줄과 그 아래 이유, '↳' 구성 종목 줄. reason이 None이면 이유를 쓰지 않은 줄(자료 없음 ETF, 규칙 기반 본문)이다."""

    line: str
    reason: str | None
    numbers: tuple[int, ...]
    shortfall: str | None
    members: tuple[str, ...]


@dataclass(frozen=True)
class PostCloseView:
    """마감 후 판 본문의 내용. 글자판과 HTML이 같은 값을 같은 순서로 보여 준다.

    출처 번호는 본문에 나온 순서(눈에 띄는 점 → ETF 줄)로 붙는다. has_output가 False면 LLM 없이 코드가 쓴 본문이다."""

    has_output: bool
    insufficient: bool
    market: str
    summary: tuple[str, ...]
    notables: tuple[tuple[str, tuple[int, ...], str | None], ...]
    etf_title: str
    groups: tuple[tuple[str, tuple[EtfRow, ...]], ...]
    extra: tuple[str, ...]
    sources: tuple[tuple[int, Entry], ...]


def market_line(i: DailyInput, sections: tuple[Section, ...]) -> str:
    """그날 한국 증시 줄. 시장종합 응답에서 꺼낼 수 있는 지수만 숫자로 쓰고, 못 꺼내면 그 지수는 자료 없음이다.
    범위 밖이거나 응답이 없어 항목이 비면 두 지수 모두 자료 없음이다."""
    section = next((s for s in sections if s.name == ITEM_KR_TODAY), None)
    usable = section is not None and not section.missing

    def part(label: str, name: str) -> str:
        figure = index_figure(tuple(i.background or ()), name) if usable else None
        if figure is None or figure.rate is None:
            return f"{label} {NO_DATA}"
        rate = round_cent(figure.rate)
        return f"{label} {'+' if rate >= 0 else MINUS}{abs(rate)}%"

    return f"{ITEM_KR_TODAY}: {part('코스피', KOSPI_NAME)} · {part('코스닥', KOSDAQ_NAME)}"


def post_close_view(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None) -> PostCloseView:
    by_ref = {e.ref: e for s in sections for e in s.entries}
    etf_section = next(s for s in sections if s.name == ITEM_ETFS)
    usable = output is not None and not output.insufficient_evidence
    reasons = {p.symbol: p for p in output.points if p.kind == POINT_ETF} if usable else {}
    notable_points = [p for p in output.points if p.kind == POINT_NOTABLE] if usable else []
    numbers: dict[str, int] = {}

    def number(refs: tuple[str, ...]) -> tuple[int, ...]:
        return tuple(numbers.setdefault(ref, len(numbers) + 1) for ref in refs)

    notables = tuple((p.statement, number(p.refs), p.shortfall) for p in notable_points)
    members: dict[str, list[str]] = {}
    for issue in i.member_issues:
        members.setdefault(issue.etf, []).append(member_line(issue))
    groups = []
    if i.etfs:
        for title, group in etf_groups(i.etfs):
            rows = []
            for e in group:
                p = reasons.get(e.symbol)
                # 하루 등락이 없는 ETF는 숫자 없이 자료 없음 줄만 쓰므로 구성 종목 줄도 붙이지 않는다.
                rows.append(EtfRow(etf_line(e), p.statement if p else None, number(p.refs) if p else (),
                                   p.shortfall if p else None, tuple(members.get(e.symbol, ())) if e.has_day() else ()))
            groups.append((title, tuple(rows)))
    else:
        groups.append(("", tuple(EtfRow(line, None, (), None, ()) for line in etf_section.lines)))
    extra = (f"({MARK_TRUNCATED}: 상한까지만 받음)",) if i.etfs and i.etfs_truncated else ()
    return PostCloseView(
        output is not None, bool(output is not None and output.insufficient_evidence), market_line(i, sections),
        output.summary if usable else (), notables, section_title(etf_section), tuple(groups), extra,
        tuple((n, by_ref[ref]) for ref, n in numbers.items()))


def source_plain(entry: Entry) -> str:
    """출처 설명에서 원문 링크를 뺀 글. 링크는 글자판이 뒤에 붙이고 HTML이 링크로 바꾼다."""
    return entry.describe.removesuffix(f" {entry.url}") if entry.url else entry.describe


def _numbers(numbers: tuple[int, ...]) -> str:
    return f"[출처 {', '.join(str(n) for n in numbers)}]"


def post_close_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None,
                    status_text: str | None = None) -> str:
    """마감 후 판 글자판. 순서: 제목 → 결론 → 그날 한국 증시 → 요약 2줄 → 눈에 띄는 점 → ETF 줄(이유, ↳) → 면책 → 서버 상태 → 출처.
    output이 None이면 요약·이유 없이 코드가 쓴 숫자·목록만 있는 규칙 기반 본문이다."""
    v = post_close_view(i, sections, output)
    lines = [*headline(i), "", conclusion(i), v.market]
    if v.has_output:
        lines.extend(["", "요약:"])
        if v.insufficient:
            lines.append(f"  {DAILY_INSUFFICIENT}")
        lines.extend(f"  - {line}" for line in v.summary)
    if v.notables:
        lines.extend(["", "눈에 띄는 점:"])
        for statement, nums, shortfall in v.notables:
            lines.append(f"  - {statement} {_numbers(nums)}")
            if shortfall:
                lines.append(weak_note_line(shortfall, "    "))
    lines.extend(["", f"■ {v.etf_title}"])
    for title, rows in v.groups:
        if title:
            lines.append(f"  [{title}]")
        for row in rows:
            lines.append(f"  - {row.line}")
            if row.reason is not None:
                lines.append(f"    이유: {row.reason} {_numbers(row.numbers)}")
                if row.shortfall:
                    lines.append(weak_note_line(row.shortfall, "      "))
            lines.extend(f"    {m}" for m in row.members)
    lines.extend(f"  - {line}" for line in v.extra)
    lines.extend(["", DAILY_DISCLAIMER])
    if status_text:
        lines.extend(["", status_text])
    if v.sources:
        lines.extend(["", "출처:"])
        for n, entry in v.sources:
            link = f" {entry.url}" if entry.url else ""
            lines.append(f"  {n}. {entry.kind} · {source_plain(entry)}{link}")
    return "\n".join(lines)
