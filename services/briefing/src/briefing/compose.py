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


def headline(symbol: str, previous_price: int, price: int, change_percent: float, as_of: datetime) -> str:
    return (f"[{symbol}] {as_of.astimezone(KST):%H:%M} 기준 {direction_of(change_percent)} {change_percent:+.2f}% "
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
