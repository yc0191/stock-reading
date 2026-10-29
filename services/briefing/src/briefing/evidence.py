"""근거 선택: 어떤 뉴스를 가격 변동의 원인 후보로 내놓을 수 있는가."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from briefing.config import DEFAULT_SETTINGS

DEFAULT_WINDOW = DEFAULT_SETTINGS.evidence_window
DEFAULT_DELTA = DEFAULT_SETTINGS.delta
log = logging.getLogger("briefing.evidence")

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

    음수는 "기준 시각보다 나중에 발행"을 뜻하며, Δ를 넘겨 늦게 나온 항목을 근거 범위가
    걸러내는지 증명하기 위해 존재한다.
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


def in_evidence_range(
    published_at: datetime,
    as_of: datetime,
    window: timedelta = DEFAULT_WINDOW,
    delta: timedelta = DEFAULT_DELTA,
) -> bool:
    """발행 시각이 [T - window, T + Δ] 안인가(양끝 포함)."""
    return as_of - window <= published_at <= as_of + delta


def select_evidence(
    news: list[NewsItem],
    symbol: str,
    as_of: datetime,
    window: timedelta = DEFAULT_WINDOW,
    limit: int = DEFAULT_LIMIT,
    delta: timedelta = DEFAULT_DELTA,
) -> list[NewsItem]:
    """변동 시각 T 근처에 발행된 가장 최근 뉴스를 돌려준다.

    규칙:
    - 종목과 관련 있거나 시장 전체에 대한 뉴스
    - 발행 시각이 T - window 이상, T + Δ 이하 (변동 직후 Δ 이내 기사도 원인 후보로 똑같이 다룬다)
    - 범위 밖 뉴스는 버린다. 조회 응답이 이미 범위를 지켰어야 하므로 경고 로그를 남긴다.
    """
    eligible = []
    for item in news:
        if symbol not in item.symbols and MARKET_WIDE not in item.symbols:
            continue
        if not in_evidence_range(item.published_at, as_of, window, delta):
            log.warning("dropping article %s outside the evidence range (publishedAt=%s, asOf=%s)",
                        item.id, item.published_at.isoformat(), as_of.isoformat())
            continue
        eligible.append(item)
    return sorted(eligible, key=lambda item: item.published_at, reverse=True)[:limit]
