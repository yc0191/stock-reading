"""가격 변동과 근거를 브리핑 문장으로 바꾼다. 서식만 다루며 LLM도 계산도 쓰지 않는다."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from briefing.evidence import NewsItem

KST = ZoneInfo("Asia/Seoul")

NO_EVIDENCE = "변동 시점 전후에 공개된 관련 근거를 찾지 못했습니다."
DISCLAIMER = "※ 투자 조언이 아니며, 위 내용은 변동 시점 전후에 공개된 자료의 나열일 뿐 원인을 단정하지 않습니다."


def direction_of(change_percent: float) -> str:
    return "상승" if change_percent > 0 else "하락"


def label_of(symbol: str, name: str | None) -> str:
    """이름을 알면 '이름(코드)', 모르거나 비었으면 코드만."""
    stripped = name.strip() if name else ""
    return f"{stripped}({symbol})" if stripped else symbol


def headline(symbol: str, previous_price: int, price: int, change_percent: float, as_of: datetime,
             name: str | None = None) -> str:
    return (f"[{label_of(symbol, name)}] {as_of.astimezone(KST):%H:%M} 기준 {direction_of(change_percent)} {change_percent:+.2f}% "
            f"({previous_price:,} → {price:,})")


def window_direction(change_percent: float, today_change_percent: float) -> str:
    """짧은 시간 변동의 방향 단어. 오늘 등락과 방향이 반대면 반등·반락, 같거나 오늘 등락이 0.00이면 상승·하락."""
    today = round(today_change_percent, 2)
    if today != 0 and (today > 0) != (change_percent > 0):
        return "반등" if change_percent > 0 else "반락"
    return direction_of(change_percent)


def window_headline(symbol: str, previous_price: int, price: int, change_percent: float,
                    today_change_percent: float, minutes: int | None, as_of: datetime, name: str | None = None) -> str:
    """짧은 시간 변동의 머리말. 몇 분 사이의 변동률보다 그날 흐름(오늘 등락)을 먼저 보여 준다.

    분이 None이면 구간을 정확히 알 수 없는 것이라(다시 보낸 변동) '짧은 시간'으로 쓴다.
    """
    span = "짧은 시간" if minutes is None else f"{minutes}분 새"
    return (f"[{label_of(symbol, name)}] {as_of.astimezone(KST):%H:%M} 기준 오늘 {today_change_percent:+.2f}% · "
            f"{span} {change_percent:+.2f}% {window_direction(change_percent, today_change_percent)} "
            f"({previous_price:,} → {price:,})")


def compose(
    symbol: str,
    previous_price: int,
    price: int,
    change_percent: float,
    as_of: datetime,
    evidence: list[NewsItem],
) -> str:
    lines = [headline(symbol, previous_price, price, change_percent, as_of)]
    if evidence:
        lines.append("근거 후보:")
        for index, item in enumerate(evidence, start=1):
            published = item.published_at.astimezone(KST)
            lines.append(f"  {index}. {item.title} — {item.source}, 발행 {published:%m-%d %H:%M}")
    else:
        lines.append(NO_EVIDENCE)
    lines.append(DISCLAIMER)
    return "\n".join(lines)
