"""데일리 판 LLM 출력 검사와 해석. 코드가 붙여 준 근거 번호만 인용했는지, 본문에 쓰면 안 되는 티커·이름을 썼는지 확인하고
어긴 출력은 InvalidDailyOutput으로 거부한다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Any, Mapping

from briefing.daily_inputs import MemberIssue, decimal_rate
from briefing.daily_prompts import DIRECTION_UNSURE, DIRECTIONS
from briefing.llm_output import (
    InvalidLlmOutput,
    check_items_present,
    output_cited_refs,
    output_item,
    output_sentence,
    output_weak_shortfall,
    check_no_ticker,
    parse_output_object,
    ticker_pattern,
)

# 마감 후 판 출력의 문장 종류. 재검색은 아침 판과 같은 번호 체계로 문장을 가리키므로 두 종류를 한 목록에 담는다.
POINT_ETF = "etf"
POINT_NOTABLE = "notable"
# 아침 판 출력의 ETF별 방향 예상 문장 종류. 이유 문장(POINT_ETF)과 달리 방향(direction)을 함께 든다.
POINT_OUTLOOK = "outlook"
# 마감 후 판 출력의 아침 예상 대조 이유 문장 종류. 맞음·빗나감은 코드가 정하고 문장은 그 이유만 쓴다.
POINT_REVIEW = "review"
# 환율 이슈·금리 이슈 칸의 문장 종류와 Claude 출력 칸 이름. 이 문장은 points가 아니라 issues에 담는다(다시 쓰기 대상이 아님).
POINT_FX = "fx"
POINT_RATE = "rate"
ISSUE_KEYS = (("fxIssue", POINT_FX), ("rateIssue", POINT_RATE))
# 아침 판만의 전 거래일 한국 증시 칸. 숫자는 코드가 붙이고 Claude는 그 이유 한 문장을 쓴다. 다른 이슈 칸처럼 issues에 담는다.
POINT_KR_PREV = "krPrev"
MORNING_ISSUE_KEYS = (("krPrevIssue", POINT_KR_PREV), *ISSUE_KEYS)
# 아침 판 '그 밖의 이슈' 문장 종류와 Claude 출력 칸 이름(해외 시세·투자자별 순매수·구성 종목 공시 등). 문장 수 상한이 있다.
POINT_OTHER = "other"
OTHERS_KEY = "otherIssues"
MAX_OTHER_ISSUES = 3
SUMMARY_LINES = 2


# ---- 출력 검증 ----

class InvalidDailyOutput(InvalidLlmOutput):
    pass


@dataclass(frozen=True)
class Point:
    statement: str
    refs: tuple[str, ...]
    # 근거가 약하다고 한 문장의 부족한 점. 약하지 않으면 None.
    shortfall: str | None = None
    # 마감 후 판의 문장 종류(POINT_ETF, POINT_NOTABLE)와 ETF 이유의 종목코드. 아침 판은 둘 다 비어 있다.
    kind: str = ""
    symbol: str | None = None
    # 아침 판 ETF별 예상의 방향(DIRECTIONS 중 하나). 그 밖의 문장은 None.
    direction: str | None = None


@dataclass(frozen=True)
class DailyOutput:
    points: tuple[Point, ...]
    insufficient_evidence: bool
    # 마감 후 판의 요약 2줄. 아침 판은 비어 있다.
    summary: tuple[str, ...] = ()
    # 환율 이슈·금리 이슈 문장(kind가 POINT_FX, POINT_RATE)과 아침 판의 전 거래일 한국 증시 문장(POINT_KR_PREV). Claude가 내지 않았으면 없다.
    issues: tuple[Point, ...] = ()
    # 아침 판의 ETF별 방향 예상(kind가 POINT_OUTLOOK). 문장 단위 재검색의 대상이 아니라 points와 따로 둔다.
    outlooks: tuple[Point, ...] = ()
    # 마감 후 판의 아침 예상 대조 이유 문장(kind가 POINT_REVIEW). 대조할 ETF가 없으면 비어 있다.
    reviews: tuple[Point, ...] = ()
    # 아침 판의 그 밖의 이슈 문장(kind가 POINT_OTHER). 쓸 것이 없으면 비어 있다.
    others: tuple[Point, ...] = ()

    def cited_refs(self) -> tuple[str, ...]:
        """본문이 인용한 근거 번호. 근거 부족 출력은 문장을 쓰지 않으므로 없다."""
        if self.insufficient_evidence:
            return ()
        return tuple(dict.fromkeys(ref for p in (*self.points, *self.issues, *self.others, *self.outlooks, *self.reviews)
                                   for ref in p.refs))

    def weak_shortfalls(self) -> tuple[str, ...]:
        """근거가 약하다고 한 문장들의 부족한 점. 메일에 근거 약함으로 보이는 문장 모두이며 작업 기록의 개수가 된다.
        문장 단위 재검색 대상은 이것이 아니라 points만 본다(DailySentences.weak_sentences)."""
        if self.insufficient_evidence:
            return ()
        return tuple(p.shortfall for p in (*self.points, *self.issues, *self.others, *self.outlooks) if p.shortfall)


# 문장 단위 재검색이 가리키는 문장 자리. 번호는 이 칸 차례로 1부터 붙는다(밤사이 이슈·ETF 이유 → 환율·금리 → ETF별 예상).
RESEARCH_FIELDS = ("points", "issues", "outlooks")


def sentence_slots(output: DailyOutput) -> list[tuple[str, int]]:
    """(칸 이름, 칸 안 순서) 목록. n번째 자리가 문장 번호 n이다. 다시 써도 칸 길이는 그대로라 번호가 바뀌지 않는다."""
    return [(name, n) for name in RESEARCH_FIELDS for n in range(len(getattr(output, name)))]


def slot_point(output: DailyOutput | None, no: int) -> Point | None:
    if output is None or not 0 < no <= len(slots := sentence_slots(output)):
        return None
    name, index = slots[no - 1]
    return getattr(output, name)[index]


def with_slot_point(output: DailyOutput, no: int, point: Point) -> DailyOutput:
    name, index = sentence_slots(output)[no - 1]
    values = list(getattr(output, name))
    values[index] = point
    return replace(output, **{name: tuple(values)})


def parse_output(text: str, allowed_refs: frozenset[str],
                 forbidden_tickers: frozenset[str] = frozenset(),
                 weak_without_refs: bool = False) -> DailyOutput:
    """LLM 응답을 검증한다. 형식이 틀리거나 준 적 없는 근거 번호를 인용하거나
    LLM 입력에 있던 미국 티커를 문장에 쓰면 InvalidDailyOutput.
    weak_without_refs이면 근거 약함인 문장만 근거 번호 없이 둘 수 있다(마감 후 판 ETF 이유 다시 쓰기)."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "points", InvalidDailyOutput)
    points = _parse_points(data, allowed_refs, tickers, weak_without_refs)
    insufficient = check_items_present(data, "points", "point", InvalidDailyOutput)
    return DailyOutput(points, insufficient)


def _parse_points(data: dict[str, Any], allowed_refs: frozenset[str], tickers: re.Pattern[str] | None,
                  weak_without_refs: bool) -> tuple[Point, ...]:
    points = []
    for raw in data["points"]:
        item = output_item(raw, "point", InvalidDailyOutput)
        statement = output_sentence(item, "statement", "point", InvalidDailyOutput)
        no_refs = weak_without_refs and item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "point", allowed_refs, InvalidDailyOutput)
        check_no_ticker(statement, tickers, "point", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "point", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "point shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("point cites no refs but is not marked weak")
        points.append(Point(statement, refs, shortfall))
    return tuple(points)


def parse_morning_output(text: str, allowed_refs: frozenset[str], forbidden_tickers: frozenset[str],
                         required_etfs: frozenset[str], etf_names: frozenset[str] = frozenset()) -> DailyOutput:
    """아침 판 LLM 응답을 검증한다. 밤사이 이슈 문장(points)은 parse_output과 같은 검사이고, 더해 ETF별 예상(etfs)이 빠지거나
    겹치거나 모르는 ETF이거나, 방향이 목록 밖이거나, 방향을 정하고도 근거 번호가 없거나(근거 약한 '예상 어려움'만 번호 없이 둘 수 있다),
    근거 번호 없는 예상이 근거 약함이 아니면 InvalidDailyOutput. 근거 부족이면 문장을 쓰지 않으므로 이슈 칸과 etfs는 보지 않는다.
    밤사이 이슈·환율·금리·그 밖의 이슈는 멤버 모두에게 같은 글이라 ETF 이름·종목코드(etf_names)가 들어 있으면 거부한다."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "points", InvalidDailyOutput)
    points = _parse_points(data, allowed_refs, tickers, False)
    insufficient = check_items_present(data, "points", "point", InvalidDailyOutput)
    if insufficient:
        return DailyOutput((), True)
    if not isinstance(data.get("etfs"), list):
        raise InvalidDailyOutput("output does not have etfs")
    outlooks, seen = [], set()
    for raw in data["etfs"]:
        item = output_item(raw, "etf", InvalidDailyOutput)
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or symbol not in required_etfs:
            raise InvalidDailyOutput("etf is not one that was given for an outlook")
        if symbol in seen:
            raise InvalidDailyOutput("etf appears more than once")
        seen.add(symbol)
        direction = item.get("direction")
        if direction not in DIRECTIONS:
            raise InvalidDailyOutput("etf direction is not one of the allowed values")
        reason = output_sentence(item, "reason", "etf", InvalidDailyOutput)
        no_refs = item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "etf", allowed_refs, InvalidDailyOutput)
        check_no_ticker(reason, tickers, "etf", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "etf", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "etf shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("etf cites no refs but is not marked weak")
        elif direction == DIRECTION_UNSURE:
            # 방향을 못 정했다는 것은 근거가 모자라다는 뜻이라 근거 약함 표시가 함께 있어야 한다.
            raise InvalidDailyOutput("etf direction is unsure but is not marked weak")
        if no_refs and direction != DIRECTION_UNSURE:
            raise InvalidDailyOutput("etf has a direction but cites no refs")
        outlooks.append(Point(reason, refs, shortfall, POINT_OUTLOOK, symbol, direction))
    if required_etfs - seen:
        raise InvalidDailyOutput(f"{len(required_etfs - seen)} etf(s) have no outlook")
    issues = [_parse_issue(data[key], key, kind, allowed_refs, tickers)
              for key, kind in MORNING_ISSUE_KEYS if data.get(key) is not None]
    others = _parse_others(data.get(OTHERS_KEY), allowed_refs, tickers)
    for p in (*points, *issues, *others):
        # 부족한 점도 근거 약함 모음으로 모두에게 나가므로 같이 본다.
        if mentions_any(p.statement, etf_names) or mentions_any(p.shortfall or "", etf_names):
            raise InvalidDailyOutput(f"{p.kind or 'point'} mentions an etf name or code")
    return DailyOutput(points, False, (), tuple(issues), tuple(outlooks), others=others)


def _parse_others(raw: Any, allowed_refs: frozenset[str], tickers: re.Pattern[str] | None) -> tuple[Point, ...]:
    """그 밖의 이슈. 칸이 없으면 쓸 것이 없는 것이고, 상한을 넘으면 앞에서부터 상한까지만 쓴다(거부하면 멀쩡한 본문까지 버린다).
    근거 없이 쓸 이유가 없는 칸이라 문장마다 근거 번호가 있어야 한다."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise InvalidDailyOutput(f"{OTHERS_KEY} is not a list")
    others = []
    for item_raw in raw[:MAX_OTHER_ISSUES]:
        item = output_item(item_raw, OTHERS_KEY, InvalidDailyOutput)
        statement = output_sentence(item, "statement", OTHERS_KEY, InvalidDailyOutput)
        refs = output_cited_refs(item, "refs", OTHERS_KEY, allowed_refs, InvalidDailyOutput)
        check_no_ticker(statement, tickers, OTHERS_KEY, InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, OTHERS_KEY, InvalidDailyOutput)
        others.append(Point(statement, refs, shortfall, POINT_OTHER))
    return tuple(others)


MEMBER_REPEAT_MIN_NAME = 2


def member_repeat_reason(text: str, issues: tuple[MemberIssue, ...]) -> str | None:
    """이유 문장이 '↳' 줄에 이미 있는 내용(종목 이름, 직전 장 등락률 숫자)을 되풀이하면 그 내용을 돌려준다. 티커와 이름은 적지 않는다."""
    lowered = text.lower()
    for issue in issues:
        if issue.name and len(issue.name) >= MEMBER_REPEAT_MIN_NAME and issue.name.lower() in lowered:
            return "종목 이름"
        value = decimal_rate(issue.rate)
        if value is None:
            continue
        forms = {str(abs(value)), format(abs(value).normalize(), "f")}
        for form in sorted(forms):
            if re.search(rf"(?<![\d.]){re.escape(form)}\s*%", text):
                return f"등락률 숫자 {form}%"
    return None


def repeats_member_line(text: str, issues: tuple[MemberIssue, ...]) -> bool:
    return member_repeat_reason(text, issues) is not None


def _repeat_error(reason: str) -> InvalidDailyOutput:
    return InvalidDailyOutput(f"etf reason repeats the member issue line ({reason})")


def parse_post_close_output(text: str, allowed_refs: frozenset[str], forbidden_tickers: frozenset[str],
                            required_etfs: frozenset[str],
                            member_issues: Mapping[str, tuple[MemberIssue, ...]],
                            notables_given: bool = True,
                            required_reviews: frozenset[str] = frozenset(),
                            etf_names: frozenset[str] = frozenset()) -> DailyOutput:
    """마감 후 판 LLM 응답을 검증한다. 아침 판 검사(형식, 목록 밖 근거 번호, 티커)에 더해 요약이 2줄이 아니거나,
    등락 자료가 있는 ETF가 빠지거나 겹치거나 모르는 ETF가 있거나, 이유가 '↳' 줄을 되풀이하면 InvalidDailyOutput.
    눈에 띄는 점 후보가 없는데(notables_given=False) 쓴 눈에 띄는 점은 버린다.
    아침 예상을 대조할 ETF(required_reviews)는 대조 이유(reviews)가 빠지거나 겹치면 거부하고, 대조할 ETF가 아닌 항목은 버린다.
    요약은 멤버 모두에게 같은 글이라 ETF 이름·종목코드(etf_names)가 들어 있으면 거부한다(다른 멤버가 가진 ETF가 드러나지 않게)."""
    tickers = ticker_pattern(forbidden_tickers)
    data = parse_output_object(text, "etfs", InvalidDailyOutput)
    summary_raw, notables_raw = data.get("summary"), data.get("notables")
    if not isinstance(summary_raw, list) or not isinstance(notables_raw, list):
        raise InvalidDailyOutput("output does not have summary and notables")
    if not notables_given:
        # 거부하면 멀쩡한 요약과 ETF 이유까지 버리고, 세 번 모두 그러면 판이 규칙 기반으로 나간다.
        notables_raw = []
    insufficient = data["insufficientEvidence"]
    summary = []
    for raw in summary_raw:
        line = output_sentence({"summary": raw}, "summary", "summary", InvalidDailyOutput)
        check_no_ticker(line, tickers, "summary", InvalidDailyOutput)
        if any(name in line for name in etf_names):
            raise InvalidDailyOutput("summary mentions an etf name or code")
        summary.append(line)
    etf_points, seen = [], set()
    for raw in data["etfs"]:
        item = output_item(raw, "etf", InvalidDailyOutput)
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or symbol not in required_etfs:
            raise InvalidDailyOutput("etf is not one that was given with a daily move")
        if symbol in seen:
            raise InvalidDailyOutput("etf appears more than once")
        seen.add(symbol)
        reason = output_sentence(item, "reason", "etf", InvalidDailyOutput)
        # 이유를 찾지 못한 ETF는 근거 없이 둔다. 자기 일봉을 붙이면 근거가 있는 것처럼 보인다. 대신 근거 약함이어야 한다.
        no_refs = item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "etf", allowed_refs, InvalidDailyOutput)
        check_no_ticker(reason, tickers, "etf", InvalidDailyOutput)
        repeated = member_repeat_reason(reason, member_issues.get(symbol, ()))
        if repeated is not None:
            raise _repeat_error(repeated)
        shortfall = output_weak_shortfall(item, "etf", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "etf shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("etf cites no refs but is not marked weak")
        etf_points.append(Point(reason, refs, shortfall, POINT_ETF, symbol))
    notable_points = []
    for raw in notables_raw:
        item = output_item(raw, "notable", InvalidDailyOutput)
        statement = output_sentence(item, "statement", "notable", InvalidDailyOutput)
        refs = output_cited_refs(item, "refs", "notable", allowed_refs, InvalidDailyOutput)
        check_no_ticker(statement, tickers, "notable", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "notable", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "notable shortfall", InvalidDailyOutput)
        notable_points.append(Point(statement, refs, shortfall, POINT_NOTABLE))
    if insufficient:
        # 근거 부족이면 LLM이 함께 낸 문장도 쓰지 않으므로 이슈 칸이 비었거나 깨져 있어도 거부하지 않는다.
        return DailyOutput((), True)
    issue_points = [_parse_issue(data[key], key, kind, allowed_refs, tickers)
                    for key, kind in ISSUE_KEYS if data.get(key) is not None]
    if required_etfs - seen:
        raise InvalidDailyOutput(f"{len(required_etfs - seen)} etf(s) with a daily move have no reason")
    if len(summary) != SUMMARY_LINES:
        raise InvalidDailyOutput(f"summary must have {SUMMARY_LINES} lines")
    reviews = _parse_reviews(data.get("reviews"), allowed_refs, tickers, required_reviews)
    return DailyOutput(tuple(etf_points) + tuple(notable_points), False, tuple(summary), tuple(issue_points),
                       reviews=reviews)


def _parse_reviews(raw_reviews: Any, allowed_refs: frozenset[str], tickers: re.Pattern[str] | None,
                   required: frozenset[str]) -> tuple[Point, ...]:
    """아침 예상 대조 이유. 대조할 ETF마다 정확히 하나이고, 이유를 찾지 못한 문장만 근거 번호 없이 둔다(근거 약함 표시 필요)."""
    if not required:
        # 대조할 ETF가 없는데 쓴 문장을 거부하면 멀쩡한 본문까지 버리게 되므로 버리기만 한다.
        return ()
    if not isinstance(raw_reviews, list):
        raise InvalidDailyOutput("output does not have reviews")
    points, seen = [], set()
    for raw in raw_reviews:
        item = output_item(raw, "review", InvalidDailyOutput)
        symbol = item.get("symbol")
        if symbol not in required:
            continue
        if symbol in seen:
            raise InvalidDailyOutput("review appears more than once")
        seen.add(symbol)
        reason = output_sentence(item, "reason", "review", InvalidDailyOutput)
        no_refs = item.get("refs") == []
        refs = () if no_refs else output_cited_refs(item, "refs", "review", allowed_refs, InvalidDailyOutput)
        check_no_ticker(reason, tickers, "review", InvalidDailyOutput)
        shortfall = output_weak_shortfall(item, "review", InvalidDailyOutput)
        if shortfall is not None:
            check_no_ticker(shortfall, tickers, "review shortfall", InvalidDailyOutput)
        elif no_refs:
            raise InvalidDailyOutput("review cites no refs but is not marked weak")
        points.append(Point(reason, refs, shortfall, POINT_REVIEW, symbol))
    if required - seen:
        raise InvalidDailyOutput(f"{len(required - seen)} review(s) have no reason")
    return tuple(points)


def _parse_issue(raw: Any, key: str, kind: str, allowed_refs: frozenset[str],
                 tickers: re.Pattern[str] | None) -> Point:
    """환율 이슈·금리 이슈 문장 하나. 근거가 없다고 쓴 문장(금리 기사 없음 등)은 근거 약함일 때만 근거 번호 없이 둔다."""
    item = output_item(raw, key, InvalidDailyOutput)
    statement = output_sentence(item, "statement", key, InvalidDailyOutput)
    no_refs = item.get("refs") == []
    refs = () if no_refs else output_cited_refs(item, "refs", key, allowed_refs, InvalidDailyOutput)
    check_no_ticker(statement, tickers, key, InvalidDailyOutput)
    shortfall = output_weak_shortfall(item, key, InvalidDailyOutput)
    if shortfall is not None:
        check_no_ticker(shortfall, tickers, f"{key} shortfall", InvalidDailyOutput)
    elif no_refs:
        raise InvalidDailyOutput(f"{key} cites no refs but is not marked weak")
    return Point(statement, refs, shortfall, kind)


def mentions_any(text: str, names: frozenset[str]) -> bool:
    return any(name in text for name in names)
