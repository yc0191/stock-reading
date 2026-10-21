"""근거 선택: 어떤 뉴스를 가격 변동의 원인 후보로 내놓을 수 있는가."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

DEFAULT_WINDOW = timedelta(hours=24)
DEFAULT_LIMIT = 3
MARKET_WIDE = "*"


@dataclass(frozen=True)
class NewsItem:
    id: str
    symbols: tuple[str, ...]
    title: str
    source: str
    published_at: datetime


def load_sample_news(path: Path, anchor: datetime) -> list[NewsItem]:
    """샘플 뉴스는 `hoursAgo`로 저장해서 어느 날에 실행해도 데모가 유효하다.

    음수는 "기준 시각보다 나중에 발행"을 뜻하며, 시간 인과 가드가 그런 항목을 근거에서
    빼는지 증명하기 위해 존재한다.
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [
        NewsItem(
            id=item["id"],
            symbols=tuple(item["symbols"]),
            title=item["title"],
            source=item["source"],
            published_at=anchor - timedelta(hours=item["hoursAgo"]),
        )
        for item in raw
    ]


def select_evidence(
    news: list[NewsItem],
    symbol: str,
    as_of: datetime,
    window: timedelta = DEFAULT_WINDOW,
    limit: int = DEFAULT_LIMIT,
) -> list[NewsItem]:
    """변동에 앞섰을 수 있는 가장 최근 뉴스를 돌려준다.

    규칙:
    - 종목과 관련 있거나 시장 전체에 대한 뉴스
    - 변동 시점 이전(같은 시각 포함)에 발행됨 (시간 인과 가드: 변동 이후 뉴스는 절대 인용하지 않음)
    - 변동 이전 `window` 안에 발행됨
    """
    eligible = [
        item
        for item in news
        if (symbol in item.symbols or MARKET_WIDE in item.symbols)
        and item.published_at <= as_of
        and as_of - item.published_at <= window
    ]
    return sorted(eligible, key=lambda item: item.published_at, reverse=True)[:limit]
