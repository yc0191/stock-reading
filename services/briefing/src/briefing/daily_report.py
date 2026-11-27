"""데일리 판(아침 판, 마감 후 판) 본문 만들기. Kafka를 모르며 모은 자료(DailyInput)를 인자로 받는다.

항목별 숫자·목록과 '자료 없음' 표시, 머리말, 면책 문구는 코드가 쓴다. LLM은 코드가 붙인 근거 번호를 인용한
요약 문장만 만들고, LLM이 끝내 실패하면 요약 없이 코드가 쓴 숫자·목록 본문(규칙 기반)으로 나간다.
입력과 항목별 섹션은 daily_sections.py, LLM 출력 검사는 daily_output.py에 있다.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping

from briefing.compose import KST
from briefing.article_groups import RESEARCH_MARK, ArticleRow, GroupedArticles, group_article_lists, same_event
from briefing.config import DEFAULT_DAILY_SETTINGS, DEFAULT_SETTINGS, CoreSettings, DailySettings
from briefing.daily_inputs import PRE_OPEN_NOTICE, MemberIssue, no_reason_count
from briefing.daily_prompts import (
    DIRECTION_UNSURE,
    DIRECTIONS,
    NO_REASON_STATEMENT,
    OUTPUT_SCHEMA,
    OUTPUT_SCHEMA_MORNING,
    OUTPUT_SCHEMA_OUTLOOK_REWRITE,
    OUTPUT_SCHEMA_POST_CLOSE,
    REWRITE_ADDENDUM,
    REWRITE_ADDENDUM_OUTLOOK,
    REWRITE_ADDENDUM_POST_CLOSE,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_POST_CLOSE,
)
from briefing.events import EDITION_POST_CLOSE
from briefing.job_log import GeneratedBy
from briefing.llm import (API_KEY_ENV, AcceptedAttempt, AnthropicClient, CachedUser, LlmCallError, LlmClient,
                          LlmUsage, attempt_usage,
                          llm_config)
from briefing.sources import etf_info_input
from briefing.daily_sections import (
    CONCLUSION_NO_DATA,
    CONCLUSION_PREFIX,
    DailyInput,
    EDITION_NAMES,
    EtfDay,
    ITEM_ETFS,
    ITEM_ETFS_SPLIT_TITLE,
    ITEM_ETF_ARTICLES,
    ITEM_RESEARCH,
    KIND_NEWS,
    MINUS,
    NO_REASON_LABEL,
    NO_RELATED_LABEL,
    Section,
    WEEKDAYS,
    _at,
    _rate,
    _short,
    _shown,
    etf_groups,
    fallback_note,
)
from briefing.daily_output import (
    DailyOutput,
    InvalidDailyOutput,
    POINT_ETF,
    POINT_FX,
    POINT_KR_PREV,
    POINT_OUTLOOK,
    POINT_RATE,
    Point,
    _repeat_error,
    member_repeat_reason,
    mentions_any,
    parse_morning_output,
    parse_output,
    parse_post_close_output,
)

log = logging.getLogger("briefing.daily_report")

GENERATED_BY_LLM = GeneratedBy.LLM.value
GENERATED_BY_RULE = GeneratedBy.RULE_BASED.value


def _llm_item(s: Section, entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {"name": s.name, "status": s.status,
                            "entries": entries if entries is not None else [e.llm for e in s.entries]}
    # 값이 빈 대상은 있을 때만 싣는다. 빈 것이 없는 항목의 입력 모양은 바꾸지 않는다.
    if s.gaps_llm:
        item["missing"] = list(s.gaps_llm)
    return item


# 같은 사건으로 묶는 기사 항목. 앞 항목이 처음 모은 기사, 뒤 항목이 재검색 기사다(묶음은 앞 항목 자리에 놓인다).
ARTICLE_ITEMS = (ITEM_ETF_ARTICLES, ITEM_RESEARCH)


def _article_sections(sections: tuple[Section, ...]) -> list[Section]:
    return [s for name in ARTICLE_ITEMS for s in sections if s.name == name]


def article_groups(sections: tuple[Section, ...], core: CoreSettings = DEFAULT_SETTINGS) -> GroupedArticles:
    """ETF별 기사와 재검색 기사를 함께 같은 사건끼리 묶는다. 둘 다 판 범위로 거른 기사다."""
    found = _article_sections(sections)
    return group_article_lists(
        [[ArticleRow(e.llm, e.llm["title"], e.published_at) for e in s.entries]
         for s in found],
        core, marks=[RESEARCH_MARK if s.name == ITEM_RESEARCH else {} for s in found])


def build_llm_input(i: DailyInput, sections: tuple[Section, ...],
                    settings: DailySettings = DEFAULT_DAILY_SETTINGS,
                    core: CoreSettings = DEFAULT_SETTINGS, *, group: bool = True,
                    with_research: bool = True) -> dict[str, Any]:
    # 검색어 입력처럼 기사 칸을 쓰지 않는 곳은 group=False로 묶기를 건너뛴다.
    # 본문 작성 입력(llm_user)은 재검색 기사를 items 밖 뒤쪽에 따로 붙이므로 with_research=False로 뺀다.
    if not with_research:
        sections = tuple(s for s in sections if s.name != ITEM_RESEARCH)
    grouped = dict(zip((s.name for s in _article_sections(sections)), article_groups(sections, core).lists)) \
        if group else {}
    data: dict[str, Any] = {
        "edition": EDITION_NAMES[i.edition],
        "rangeStart": _at(i.range_start),
        "asOf": _at(i.as_of),
        "items": [_llm_item(s, grouped.get(s.name)) for s in sections],
    }
    # ETF 정보는 응답에 있을 때만 넣는다. 정보가 없는 이전 형식 응답의 입력 모양은 바꾸지 않는다.
    etf_info = [{"symbol": e.symbol, **etf_info_input(e.etf, settings.max_etf_members)}
                for e in i.etfs or () if e.etf is not None]
    if etf_info:
        data["etfInfo"] = etf_info
    if i.edition == EDITION_POST_CLOSE:
        data["notableCandidates"] = _candidates_input(i, sections)
        # 대조할 ETF가 있을 때만 싣는다. 없으면 입력 모양은 그대로다.
        reviews = _reviews_input(i)
        if reviews:
            data["reviewEtfs"] = reviews
    else:
        data["outlookEtfs"] = _outlook_etfs_input(i)
    return data


# 재검색 기사 칸의 키. 근거 입력 뒤에 찾은 순서대로 덧붙인다.
RESEARCH_KEY = "researchArticles"
# 앞에 놓인 기사 중 같은 사건으로 보이는 기사의 ref 목록. 묶음 안에 넣으면 앞 칸이 바뀌어 캐시를 읽지 못하므로 가리키기만 한다.
SAME_EVENT_AS_KEY = "sameEventAs"
RESEARCH_TRUNCATED_KEY = "researchTruncated"


def research_llm_entries(i: DailyInput, sections: tuple[Section, ...],
                         core: CoreSettings = DEFAULT_SETTINGS) -> list[dict[str, Any]]:
    """재검색 기사를 찾은 순서대로 낸다. 앞 기사(ETF별 기사, 먼저 찾은 재검색 기사)와 같은 사건이면 그 ref를 붙인다.

    다음 호출은 기사가 뒤에 더 붙을 뿐 앞 칸이 그대로라 앞 호출이 쓴 캐시를 읽는다.
    """
    research = next((s for s in sections if s.name == ITEM_RESEARCH), None)
    if research is None:
        return []
    by_ref = {e.ref: e for e in research.entries}
    earlier = [(e.ref, ArticleRow(e.llm, e.llm["title"], e.published_at))
               for s in sections if s.name == ITEM_ETF_ARTICLES for e in s.entries]
    out: list[dict[str, Any]] = []
    for a in dict.fromkeys(a.article_id for a in i.research_articles):
        e = by_ref.get(a)
        if e is None:
            continue
        row = ArticleRow(e.llm, e.llm["title"], e.published_at)
        same = [ref for ref, other in earlier if same_event(row, other, core)]
        out.append({**e.llm, SAME_EVENT_AS_KEY: same} if same else dict(e.llm))
        earlier.append((e.ref, row))
    return out


def llm_user(i: DailyInput, sections: tuple[Section, ...], settings: DailySettings = DEFAULT_DAILY_SETTINGS,
             core: CoreSettings = DEFAULT_SETTINGS, tail: Mapping[str, Any] | None = None) -> CachedUser:
    """본문 작성·문장 다시 쓰기·고치기가 보내는 입력(JSON 글 하나).

    처음 작성의 근거 입력 -> 찾은 순서대로의 재검색 기사 -> 호출마다 다른 칸(tail) 차례로 놓아, 같은 작업의 호출이
    앞부분을 똑같이 보내게 한다. 블록 경계(breaks)는 근거 입력 끝과 재검색 기사마다의 끝이다.
    """
    base = json.dumps(build_llm_input(i, sections, settings, core, with_research=False), ensure_ascii=False)
    parts = [base[:-1]]
    research = research_llm_entries(i, sections, core)
    for n, entry in enumerate(research):
        lead = f', "{RESEARCH_KEY}": [' if n == 0 else ", "
        parts.append(lead + json.dumps(entry, ensure_ascii=False))
    rest: dict[str, Any] = {}
    if research and i.research_articles_truncated:
        rest[RESEARCH_TRUNCATED_KEY] = True
    rest.update(tail or {})
    end = ("]" if research else "") + (", " + json.dumps(rest, ensure_ascii=False)[1:-1] if rest else "") + "}"
    breaks, at = [], 0
    for part in parts:
        at += len(part)
        breaks.append(at)
    return CachedUser("".join(parts) + end, tuple(breaks))


def _outlook_etfs_input(i: DailyInput) -> list[dict[str, Any]]:
    """아침 판이 방향을 예상할 ETF. 일봉이 비어 있어도 기사로 예상하므로 빼지 않는다. 보유·관심 구분은 멤버마다 달라 LLM에 주지 않는다."""
    return [{"symbol": e.symbol, "name": e.label()} for e in i.etfs or ()]


def _reviews_input(i: DailyInput) -> list[dict[str, Any]]:
    """코드가 정한 맞음·빗나감을 Claude에 준다. Claude는 판정을 바꾸지 않고 이유만 쓴다.
    실제 등락이 장중 시세 대체값이면 그 기준(actualBasis)을 함께 준다."""
    etfs = {e.symbol: e for e in i.etfs or ()}
    rows = []
    for r in i.reviews():
        if r.verdict is None:
            continue
        row = {"symbol": r.symbol, "morningDirection": r.call.direction, "morningReason": r.call.reason,
               "actualRate": str(r.rate), "verdict": r.verdict}
        if r.symbol in etfs and etfs[r.symbol].is_fallback():
            row["actualBasis"] = fallback_note(etfs[r.symbol])
        rows.append(row)
    return rows


def _candidates_input(i: DailyInput, sections: tuple[Section, ...]) -> list[dict[str, Any]]:
    """눈에 띄는 점 후보. 후보에 얽힌 ETF의 등락 근거 번호를 함께 준다."""
    day_refs = {e.llm["symbol"]: e.ref for s in sections if s.name == ITEM_ETFS for e in s.entries}
    return [{"kind": c.kind, "symbols": list(c.symbols), "refs": [day_refs[sym] for sym in c.symbols if sym in day_refs]}
            for c in i.notables]


# ---- 본문 ----

def headline(i: DailyInput) -> list[str]:
    local = i.as_of.astimezone(KST)
    return [f"[{EDITION_NAMES[i.edition]}] {local:%Y-%m-%d}({WEEKDAYS[local.weekday()]}) {local:%H:%M} 데일리 브리핑",
            f"다룬 범위: {_short(i.range_start)} ~ {_short(i.as_of)}"]


def section_title(s: Section) -> str:
    """글자판·HTML에 보이는 절 제목."""
    return ITEM_ETFS_SPLIT_TITLE if s.name == ITEM_ETFS and s.groups else s.name


def section_lines(sections: tuple[Section, ...]) -> list[str]:
    """항목마다 제목과 숫자·목록 줄. 재검색 기사와 숨김 항목은 싣지 않는다."""
    lines: list[str] = []
    for s in sections:
        if s.name == ITEM_RESEARCH or s.hidden:
            # 재검색 기사는 요약이 인용할 때만 근거 목록에 나온다. 항목으로 나열하지 않는다.
            continue
        lines.append("")
        lines.append(f"■ {section_title(s)}")
        if s.groups:
            grouped = {line for _, members in s.groups for line in members}
            for title, members in s.groups:
                lines.append(f"  [{title}]")
                lines.extend(f"  - {line}" for line in members)
            # 묶음에 들지 않은 줄(잘림 표시)은 맨 끝에 둔다.
            lines.extend(f"  - {line}" for line in s.lines if line not in grouped)
        else:
            lines.extend(f"  - {line}" for line in s.lines)
    return lines


def _signed(rate: float) -> str:
    """결론에 쓰는 등락률. 소수 한 자리에 부호를 붙이고 내림은 빼기 기호(−)로 쓴다."""
    shown = _shown(rate)
    return f"{'+' if shown >= 0 else MINUS}{abs(shown):.1f}%"


def _count_text(label: str, group: tuple[EtfDay, ...]) -> str:
    # 개수는 ETF 줄에 보이는 원래 등락률의 부호로 센다. 반올림하면 +0.04%가 보합이 되어 목록과 어긋난다.
    up = sum(1 for e in group if e.has_day() and (_rate(e.rate) or 0.0) > 0)
    nodata = sum(1 for e in group if not e.has_day() or _rate(e.rate) is None)
    text = f"{label} {len(group)}개 중 {up}개 상승"
    return f"{text}(자료 없음 {nodata})" if nodata else text


def claude_no_reason_count(output: DailyOutput) -> int:
    """Claude가 이유를 '받은 자료에서 이유를 찾지 못했습니다'로 쓴 ETF 수."""
    target = NO_REASON_STATEMENT.rstrip(".")
    return sum(1 for p in output.points if p.kind == POINT_ETF and p.statement.strip().rstrip(".") == target)


def conclusion(i: DailyInput, output: DailyOutput | None = None) -> str:
    """마감 후 판의 한 줄 결론. 개수와 최대 등락은 코드가 센다. 자료 없는 ETF도 개수에 넣는다.
    이유를 찾지 못한 ETF 수는 Claude 본문이 있으면 본문에 그렇게 쓴 ETF 수이고, 규칙 기반 본문이면 코드가 자료로 센다."""
    etfs = i.etfs or ()
    if i.before_open():
        return CONCLUSION_PREFIX + PRE_OPEN_NOTICE
    rated = [(e, v) for e in etfs if e.has_day() for v in [_rate(e.rate)] if v is not None]
    if not rated:
        return CONCLUSION_PREFIX + CONCLUSION_NO_DATA
    parts = [_count_text(title or "ETF", members) for title, members in etf_groups(etfs)]
    best = max(rated, key=lambda r: r[1])
    worst = min(rated, key=lambda r: r[1])
    if _shown(best[1]) > 0:
        parts.append(f"가장 크게 오름 {best[0].label()} {_signed(best[1])}")
    if _shown(worst[1]) < 0:
        parts.append(f"가장 크게 내림 {worst[0].label()} {_signed(worst[1])}")
    if output is not None and not output.insufficient_evidence:
        unexplained, label = claude_no_reason_count(output), NO_REASON_LABEL
    else:
        unexplained = no_reason_count(etfs, i.member_issues, i.etf_articles or (), i.filings or ())
        label = NO_RELATED_LABEL
    if unexplained:
        parts.append(f"{label} {unexplained}개")
    return CONCLUSION_PREFIX + " · ".join(parts)


def daily_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput | None,
               status_text: str | None = None) -> str:
    """글자판 본문. output이 None이면 숫자·목록만으로 된 규칙 기반 본문이다. 판마다 배치가 달라
    아침 판은 daily_morning, 마감 후 판은 daily_post_close가 쓴다."""
    # 두 모듈은 이 모듈의 자료 모양을 쓰므로 순환을 피하려고 여기서 가져온다.
    if i.edition == EDITION_POST_CLOSE:
        from briefing.daily_post_close import post_close_text
        return post_close_text(i, sections, output, status_text)
    from briefing.daily_morning import morning_text
    return morning_text(i, sections, output, status_text)


def rule_based_text(i: DailyInput, sections: tuple[Section, ...]) -> str:
    """숫자·목록만으로 된 본문. LLM 키가 없거나 LLM이 끝내 실패했을 때 쓴다."""
    return daily_text(i, sections, None)


def llm_text(i: DailyInput, sections: tuple[Section, ...], output: DailyOutput) -> str:
    return daily_text(i, sections, output)


# ---- LLM 작성기 ----

@dataclass(frozen=True)
class DailyBody:
    text: str
    generated_by: str
    insufficient_evidence: bool = False
    # 본문이 인용한 근거 번호(규칙 기반이면 없음)
    cited: tuple[str, ...] = ()
    # 근거가 약하다고 한 문장들의 부족한 점. 재검색 여부를 정하고 검색어 LLM 입력에 들어간다.
    weak_shortfalls: tuple[str, ...] = ()
    # 받아들인 LLM 출력. 문장 하나만 다시 쓸 때 나머지 문장을 그대로 두고 본문을 다시 만드는 데 쓴다(규칙 기반이면 None).
    output: DailyOutput | None = None


@dataclass(frozen=True)
class LlmAttempt:
    """거부되거나 실패한 LLM 시도 하나. 작업 기록에 그대로 넘긴다."""

    attempt: int
    error_type: str
    reason: str
    llm_input: str
    raw_output: str | None
    # 응답을 받지 못한 호출 실패는 None이다.
    usage: LlmUsage | None = None


@dataclass(frozen=True)
class DailyWriteResult:
    body: DailyBody | None
    rejected: tuple[LlmAttempt, ...]
    # 받아들인 시도(끝내 실패했으면 None)
    accepted: AcceptedAttempt | None = None


@dataclass(frozen=True)
class PointRewrite:
    """문장 하나를 다시 쓴 결과. point가 None이면 끝내 실패했고 원래 문장을 그대로 둔다."""

    point: Point | None
    rejected: tuple[LlmAttempt, ...]
    accepted: AcceptedAttempt | None = None


def _etfs_with_day(i: DailyInput) -> frozenset[str]:
    """이유를 써야 하는 ETF. 하루 등락이 없는 ETF는 코드가 자료 없음 줄로 쓴다."""
    return frozenset(e.symbol for e in i.etfs or () if e.has_day())


def _etfs_to_forecast(i: DailyInput) -> frozenset[str]:
    """아침 판이 방향을 예상해야 하는 ETF. 보유·관심 모두이고 일봉이 비어 있어도 뺀다고 정하지 않는다."""
    return frozenset(e.symbol for e in i.etfs or ())


def _reviews_to_explain(i: DailyInput) -> frozenset[str]:
    """Claude가 맞음·빗나감의 이유를 써야 하는 ETF. 예상 어려움은 대조하지 않으므로 뺀다."""
    return frozenset(r.symbol for r in i.reviews() if r.verdict is not None)


def _issues_by_etf(i: DailyInput) -> dict[str, tuple[MemberIssue, ...]]:
    grouped: dict[str, list[MemberIssue]] = {}
    for issue in i.member_issues:
        grouped.setdefault(issue.etf, []).append(issue)
    return {etf: tuple(issues) for etf, issues in grouped.items()}


def summary_banned_names(i: DailyInput) -> frozenset[str]:
    """요약에 쓰면 안 되는 ETF 이름과 종목코드. 한 글자 이름은 일반 낱말과 겹쳐 뺀다."""
    return frozenset(n for etf in i.etfs or () for n in (etf.symbol, etf.label()) if len(n) >= 2)


@dataclass(frozen=True)
class DailyLlmWriter:
    client: LlmClient
    max_attempts: int = DEFAULT_DAILY_SETTINGS.llm_max_attempts
    settings: DailySettings = DEFAULT_DAILY_SETTINGS
    # 같은 사건 기사 묶기 기준값은 급변동과 같은 CoreSettings를 쓴다.
    core: CoreSettings = DEFAULT_SETTINGS

    @staticmethod
    def forbidden_tickers(i: DailyInput) -> frozenset[str]:
        return frozenset(i.tickers()) | frozenset(r.ticker for r in i.close_rates or ()) \
            | frozenset(t for f in i.filings or () for t in f.tickers) | i.member_us_symbols()

    def write(self, i: DailyInput, sections: tuple[Section, ...]) -> DailyWriteResult:
        user = llm_user(i, sections, self.settings, self.core)
        groups = article_groups(sections, self.core).record()
        allowed = frozenset(e.ref for s in sections for e in s.entries)
        post_close = i.edition == EDITION_POST_CLOSE
        # 아침 판은 미국 종목 이름·티커를 문장에 쓸 수 있다. 멤버마다 거른 근거만 인용하므로 남의 보유 종목이 드러나지 않는다.
        tickers = self.forbidden_tickers(i) if post_close else frozenset()
        prompt, schema = (SYSTEM_PROMPT_POST_CLOSE, OUTPUT_SCHEMA_POST_CLOSE) if post_close \
            else (SYSTEM_PROMPT, OUTPUT_SCHEMA_MORNING)
        rejected: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw: str | None = None
            usage: LlmUsage | None = None
            try:
                reply = self.client.complete(prompt, user, schema)
                raw, usage = reply.text, reply.usage
                output = parse_post_close_output(raw, allowed, tickers, _etfs_with_day(i), _issues_by_etf(i),
                                                 bool(i.notables), _reviews_to_explain(i),
                                                 summary_banned_names(i)) \
                    if post_close else parse_morning_output(raw, allowed, tickers, _etfs_to_forecast(i),
                                                            summary_banned_names(i))
            except (LlmCallError, InvalidDailyOutput) as error:
                rejected.append(LlmAttempt(attempt, type(error).__name__, str(error), user, raw,
                                            attempt_usage(usage, error)))
                log.warning("daily LLM attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, i.job_id, error)
                continue
            return DailyWriteResult(DailyBody(llm_text(i, sections, output), GENERATED_BY_LLM,
                                              output.insufficient_evidence, output.cited_refs(),
                                              output.weak_shortfalls(), output), tuple(rejected),
                                    AcceptedAttempt(attempt, user, raw, usage, groups))
        return DailyWriteResult(None, tuple(rejected))

    def rewrite_point(self, i: DailyInput, sections: tuple[Section, ...], current: Point, no: int,
                      new_refs: tuple[str, ...], other_refs: tuple[str, ...] = ()) -> PointRewrite:
        """근거가 약한 문장 하나만 다시 쓴다. 나머지 문장은 건드리지 않는다. i와 sections에는 새로 찾은 기사까지 들어 있다.
        other_refs는 다른 문장이 인용한 근거 번호이며, 그중 기사 번호만 이 문장의 이유로 다시 쓰지 말라고 알린다."""
        groups = article_groups(sections, self.core).record()
        rewrite: dict[str, Any] = {"sentenceNo": no,
                                   "sentence": {"statement": current.statement, "refs": list(current.refs)},
                                   "shortfall": current.shortfall, "newRefs": list(new_refs)}
        if current.symbol is not None:
            rewrite["symbol"] = current.symbol
        # 밤사이 이슈·ETF 이유의 입력 모양은 그대로 두고, 환율·금리·전 거래일 칸과 ETF별 예상에만 종류(와 방향)를 알린다.
        if current.kind in (POINT_FX, POINT_RATE, POINT_KR_PREV, POINT_OUTLOOK):
            rewrite["kind"] = current.kind
        if current.direction is not None:
            rewrite["direction"] = current.direction
        # 마지막 검사는 같은 기사를 두 문장이 이유로 쓰면 고치게 하므로, 다른 문장이 이미 쓴 기사를 미리 알린다.
        articles = {e.ref for s in sections for e in s.entries if e.kind == KIND_NEWS}
        taken = [ref for ref in dict.fromkeys(other_refs) if ref in articles]
        if taken:
            rewrite["otherSentenceRefs"] = taken
        user = llm_user(i, sections, self.settings, self.core, {"rewrite": rewrite})
        allowed = frozenset(e.ref for s in sections for e in s.entries)
        post_close = i.edition == EDITION_POST_CLOSE
        tickers = self.forbidden_tickers(i) if post_close else frozenset()
        outlook = current.kind == POINT_OUTLOOK
        system = SYSTEM_PROMPT_POST_CLOSE if post_close else SYSTEM_PROMPT
        if outlook:
            addendum, schema = REWRITE_ADDENDUM_OUTLOOK, OUTPUT_SCHEMA_OUTLOOK_REWRITE
        elif post_close and current.kind == POINT_ETF:
            addendum, schema = REWRITE_ADDENDUM_POST_CLOSE, OUTPUT_SCHEMA
        else:
            addendum, schema = REWRITE_ADDENDUM, OUTPUT_SCHEMA
        # 이유를 찾지 못한 ETF 이유, 금리 기사가 없다는 칸, 전 거래일 이유를 찾지 못한 칸, 예상 어려움만 근거 번호 없이 둘 수 있다
        # (근거 약함이어야 함).
        no_refs_ok = current.kind in (POINT_ETF, POINT_FX, POINT_RATE, POINT_KR_PREV, POINT_OUTLOOK)
        shared = not post_close and current.kind in ("", POINT_FX, POINT_RATE, POINT_KR_PREV)
        rejected: list[LlmAttempt] = []
        for attempt in range(1, self.max_attempts + 1):
            raw: str | None = None
            usage: LlmUsage | None = None
            try:
                reply = self.client.complete(system, user, schema, addendum)
                raw, usage = reply.text, reply.usage
                output = parse_output(raw, allowed, tickers, weak_without_refs=no_refs_ok)
                if output.insufficient_evidence or len(output.points) != 1:
                    raise InvalidDailyOutput("rewrite must return exactly one point")
                direction = _rewritten_direction(raw, output.points[0]) if outlook else None
                repeated = member_repeat_reason(
                    output.points[0].statement, _issues_by_etf(i).get(current.symbol or "", ())) \
                    if current.kind == POINT_ETF else None
                if repeated is not None:
                    raise _repeat_error(repeated)
                # 아침 판 밤사이 이슈·환율·금리는 멤버 모두에게 같은 글이라 다시 쓴 문장도 ETF 이름을 쓰면 안 된다.
                rewritten = output.points[0]
                if shared and (mentions_any(rewritten.statement, summary_banned_names(i))
                               or mentions_any(rewritten.shortfall or "", summary_banned_names(i))):
                    raise InvalidDailyOutput("point mentions an etf name or code")
            except (LlmCallError, InvalidDailyOutput) as error:
                rejected.append(LlmAttempt(attempt, type(error).__name__, str(error), user, raw,
                                            attempt_usage(usage, error)))
                log.warning("daily LLM sentence rewrite attempt %d/%d failed for job %s: %s",
                            attempt, self.max_attempts, i.job_id, error)
                continue
            # 다시 쓴 문장은 원래 문장의 종류와 ETF를 이어받는다. ETF별 예상은 새로 정한 방향을 쓴다.
            point = replace(output.points[0], kind=current.kind, symbol=current.symbol,
                            direction=direction if outlook else current.direction)
            return PointRewrite(point, tuple(rejected), AcceptedAttempt(attempt, user, raw, usage, groups))
        return PointRewrite(None, tuple(rejected))


def refs_cited_by_others(output: DailyOutput, current: Point) -> tuple[str, ...]:
    """current가 아닌 문장들이 인용한 근거 번호(메일 차례, 겹침 없음)."""
    others = (p for p in (*output.points, *output.issues, *output.others, *output.outlooks, *output.reviews)
              if p is not current)
    return tuple(dict.fromkeys(ref for p in others for ref in p.refs))


def _rewritten_direction(raw: str, point: Point) -> str:
    """다시 쓴 ETF별 예상의 방향. 처음 작성의 검사와 같다: 목록 안 값이어야 하고, 방향을 정했으면 근거 번호가 있어야 하며,
    '예상 어려움'은 근거 약함이어야 한다."""
    direction = json.loads(raw)["points"][0].get("direction")
    if direction not in DIRECTIONS:
        raise InvalidDailyOutput("etf direction is not one of the allowed values")
    if direction == DIRECTION_UNSURE and point.shortfall is None:
        raise InvalidDailyOutput("etf direction is unsure but is not marked weak")
    if direction != DIRECTION_UNSURE and not point.refs:
        raise InvalidDailyOutput("etf has a direction but cites no refs")
    return direction


def build_daily_writer(env: Mapping[str, str],
                       client_factory: Callable[[str, str], LlmClient] = AnthropicClient,
                       settings: DailySettings = DEFAULT_DAILY_SETTINGS,
                       core: CoreSettings = DEFAULT_SETTINGS) -> DailyLlmWriter | None:
    """시작할 때 한 번 부른다. 키가 없으면 None이고 데일리 판은 규칙 기반 본문으로 나간다."""
    config = llm_config(env)
    if config is None:
        log.warning("%s is not set; daily briefings will be rule-based", API_KEY_ENV)
        return None
    return DailyLlmWriter(client_factory(config.api_key, config.model), settings.llm_max_attempts, settings, core)
