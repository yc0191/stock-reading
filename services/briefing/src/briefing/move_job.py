"""급변동 근거 조회 작업의 값과 상태: 리포트 입력(ReportInput), 변동 메시지 읽기(parse_move), 작업 하나의 진행 상태,
이른 재검색 검색어. 작업 흐름(조회 보내기, 응답 받기, 완성)은 jobs.py의 BriefingCore에 있다.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from briefing.config import CoreSettings
from briefing.events import parse_timestamp
from briefing.evidence import MARKET_WIDE
from briefing.job_log import LEVEL_MID, level_of
from briefing.move_flow import FlowMove
from briefing.research import MAX_QUERY_LENGTH
from briefing.sources import Article, Background, CloseRate, EtfInfo, Exchange, Filing, PricePoint

# jobs.py에서 나눈 코드라 운영 로그와 로그를 읽는 테스트가 보는 로거 이름을 그대로 둔다.
log = logging.getLogger("briefing.jobs")

COLLECT_NEWS = "collect-news"
# 조회 결과가 왔지만 해석하지 못해 받지 못한 것으로 본 경우의 실패 종류
FAILURE_MALFORMED = "MalformedResponse"


@dataclass(frozen=True)
class ReportInput:
    job_id: str
    symbol: str
    correlation_id: str
    as_of: datetime
    previous_price: int
    price: int
    change_percent: float
    price_points: tuple[PricePoint, ...]
    articles: tuple[Article, ...]
    history_truncated: bool = False
    articles_truncated: bool = False
    # 변동 종목에 담긴 미국 종목. 비어 있으면 공시·해외 등락률은 해당 없음이다.
    tickers: tuple[str, ...] = ()
    tickers_truncated: bool = False
    close_rates: tuple[CloseRate, ...] = ()
    filings: tuple[Filing, ...] = ()
    filings_truncated: bool = False
    background: tuple[Background, ...] = ()
    # 변동 종목이 ETF일 때 응답에 담긴 ETF 정보. 이전 형식 응답에는 없다.
    etf: EtfInfo | None = None
    # 응답에 담긴 변동 종목 이름(ETF 또는 주식). 이전 형식 응답에는 없다.
    name: str | None = None
    # market.move를 감지한 시각(봉투의 occurredAt)과 감지 유형. 머리말의 "몇 분 사이"에 쓴다. 이전 형식에는 없을 수 있다.
    detected_at: datetime | None = None
    detection_type: str | None = None
    # 감지한 시세의 전일 종가 대비 오늘 등락률(%). 짧은 시간 변동을 시장의 하루 등락과 비교하고 첫 줄에 쓴다. 이전 형식에는 없다.
    today_change_percent: float | None = None
    # 근거 부족으로 다시 검색해 받은 기사. 기존 기사와 따로 두고, 근거 번호·링크 규칙은 기사와 같다.
    research_articles: tuple[Article, ...] = ()
    research_articles_truncated: bool = False
    # 같은 날 같은 종목의 앞선 급변동(작업 기록). '오늘 흐름'과 차트 표시, LLM 입력에 쓴다.
    earlier_moves: tuple[FlowMove, ...] = ()
    # 장중 차트용 1분 간격 시세(그날 chart_start부터 변동 delta 뒤까지). 받지 못했으면 비어 있고 차트만 빠진다.
    chart_points: tuple[PricePoint, ...] = ()


class ReadyPort(Protocol):
    def on_ready(self, report_input: ReportInput) -> None: ...


@dataclass(frozen=True)
class Completion:
    started_at: datetime
    job_type: str
    incomplete_symbols: frozenset[str]
    # 대기 한도 안에 저장되지 못한 기사 수. 그 기사는 조회에 없을 뿐 근거 조회는 진행한다(근거가 약하면 재검색이 채운다).
    unprocessed: int = 0

    def covers(self, symbol: str, as_of: datetime, settings: CoreSettings) -> bool:
        return (
            self.job_type == COLLECT_NEWS
            and self.started_at >= as_of + settings.delta
            and symbol not in self.incomplete_symbols
            and MARKET_WIDE not in self.incomplete_symbols
        )


class _Phase(Enum):
    WAITING_COLLECTION = "waiting_collection"
    FETCHING = "fetching"
    DONE = "done"


class _Early(Enum):
    """정기 수집을 기다리는 동안 하는 T+5분 재검색의 진행."""

    PENDING = "pending"            # 아직 시각이 안 됐다
    CONTEXT_SENT = "context_sent"  # ETF 이름을 알려고 배경 자료를 먼저 조회함
    REQUESTED = "requested"        # 재검색 요청을 보냄. 저장 끝 알림을 기다린다
    SKIPPED = "skipped"            # 이름을 모르거나 배경 자료가 오지 않아 정기 수집을 기다린다


@dataclass(frozen=True)
class _Context:
    tickers: tuple[str, ...]
    tickers_truncated: bool
    close_rates: tuple[CloseRate, ...]
    background: tuple[Background, ...]
    etf: EtfInfo | None = None
    name: str | None = None


@dataclass
class _Job:
    job_id: str
    symbol: str
    correlation_id: str
    as_of: datetime
    previous_price: int
    price: int
    change_percent: float
    created_at: datetime
    detected_at: datetime | None = None
    detection_type: str | None = None
    today_change_percent: float | None = None
    # 상이면 리포트를 만들고, 중이면 대표 기사만 고른다.
    level: str = LEVEL_MID
    phase: _Phase = _Phase.WAITING_COLLECTION
    early: _Early = _Early.PENDING
    # 시세·배경 자료·공시·기사: 맡긴 조회의 순번과 시각, 받았는지
    market: Exchange = field(default_factory=Exchange)
    market_points: tuple[PricePoint, ...] | None = None
    market_truncated: bool = False
    # 상의 장중 차트용 시세. 못 받아도 차트만 빠지고 리포트는 나간다.
    chart: Exchange = field(default_factory=Exchange)
    chart_points: tuple[PricePoint, ...] = ()
    context: Exchange = field(default_factory=Exchange)
    context_data: _Context | None = None
    filings: Exchange = field(default_factory=Exchange)
    filings_data: tuple[Filing, ...] = ()
    filings_truncated: bool = False
    news: Exchange = field(default_factory=Exchange)
    news_articles: tuple[Article, ...] | None = None
    news_truncated: bool = False
    # 실패 기록용: 조회마다 맡긴 순번·시각, 조회별 마지막 요청 범위
    request_log: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    request_ranges: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 실패 기록용: 조회별로 받은 응답 본문 원문 그대로
    responses: dict[str, dict[str, Any]] = field(default_factory=dict)


def parse_move(message: str | bytes, now: datetime) -> _Job:
    """market.move 원문을 작업으로 바꾼다. 형식이 잘못되면 ValueError."""
    try:
        move = json.loads(message)
        if move["eventType"] != "market.move":
            raise ValueError(f"unexpected eventType: {move['eventType']}")
        payload = move["payload"]
        return _Job(
            job_id=move["eventId"],
            symbol=payload["symbol"],
            correlation_id=move["correlationId"],
            # 변동 시각 T는 payload.asOf 하나로만 읽는다(detectionType·moveStartedAt 유무와 무관).
            as_of=parse_timestamp(payload["asOf"]),
            previous_price=int(payload["previousPrice"]),
            price=int(payload["price"]),
            change_percent=float(payload["changePercent"]),
            created_at=now,
            detected_at=parse_timestamp(move["occurredAt"]) if move.get("occurredAt") else None,
            detection_type=payload.get("detectionType"),
            today_change_percent=_optional_float(payload.get("todayChangePercent"), "todayChangePercent", move["eventId"]),
            level=level_of(payload.get("detectionType")),
        )
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"malformed market.move event: {error!r}") from error


def _optional_float(value: Any, field_name: str, event_id: str) -> float | None:
    """선택 칸의 숫자. 칸이 없거나 숫자가 아니면 None이다.

    숫자가 아닌 값은 계약 위반이지만 선택 칸 하나 때문에 급변동 리포트 전체를 버리지 않고, 그 칸만 없는 것으로 읽는다.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        log.warning("market.move %s: %s is not a number (%s); reading the event without it",
                    event_id, field_name, type(value).__name__)
        return None
    return float(value)


def early_research_id(job_id: str) -> str:
    """T+5분 재검색의 재검색 작업 id. 요청 계약이 uuid를 요구하므로 급변동 작업 id에서 정한다."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"research-early:{job_id}"))


def early_queries(etf: EtfInfo | None, limit: int, stock_name: str | None = None) -> tuple[str, ...]:
    """ETF 이름과 비중 순위가 높은 구성 종목 이름으로 만든 검색어. ETF 정보가 없는 주식이면 그 주식 이름 하나.
    둘 다 모르면 빈 튜플(재검색하지 않는다)."""
    name = (etf.name or "").strip() if etf is not None else ""
    if not name:
        # 주식 급변동은 맥락 응답에 ETF 정보가 없고 name만 있다. 이 이름 하나로 찾는다.
        stock = (stock_name or "").strip() if etf is None else ""
        return (stock,) if stock and len(stock) <= MAX_QUERY_LENGTH else ()
    names = [name] + [m.name.strip() for m in sorted(etf.members, key=lambda m: m.rank) if m.name and m.name.strip()]
    unique = dict.fromkeys(n for n in names if len(n) <= MAX_QUERY_LENGTH)
    return tuple(unique)[:limit]
