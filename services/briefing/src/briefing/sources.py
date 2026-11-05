"""근거 자료의 형식과 응답 해석, 요청 한 번의 순번 기록, LLM 입력에 붙이는 상태 표시. Kafka를 모르며 메시지를 인자로 받는다."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from briefing.events import parse_timestamp


@dataclass(frozen=True)
class PricePoint:
    as_of: datetime
    price: int


@dataclass(frozen=True)
class Article:
    article_id: str
    title: str
    summary: str
    published_at: datetime
    publisher: str
    url: str


@dataclass(frozen=True)
class Filing:
    accession_number: str
    tickers: tuple[str, ...]
    form: str
    items: tuple[str, ...]
    accepted_at: datetime
    url: str


@dataclass(frozen=True)
class CloseRate:
    ticker: str
    date: str
    close_at: datetime
    rate: str
    fetched_at: datetime


@dataclass(frozen=True)
class IndexRate:
    code: str
    name: str
    business_day: str
    close: str
    prev_business_day: str | None
    prev_close: str | None
    rate: str | None


BACKGROUND_OK = "ok"
BACKGROUND_MISSING = "missing"
BACKGROUND_TOO_LARGE = "too-large"


@dataclass(frozen=True)
class Background:
    """배경 자료 한 종류(와 대상)의 최신 1건. 원문은 해석하지 않고 그대로 둔다."""

    api_type: str
    target: str | None
    status: str
    response: str | None = None
    fetched_at: datetime | None = None
    response_bytes: int | None = None
    index_rates: tuple[IndexRate, ...] = ()


# LLM 입력에 담는 자료 상태 표시
MARK_PRESENT = "있음"
MARK_MISSING = "없음"
MARK_TRUNCATED = "잘림"
MARK_TOO_LARGE = "너무 큼"
MARK_NOT_APPLICABLE = "해당 없음"
BACKGROUND_LABEL = "배경"
BACKGROUND_STATUS_MARKS = {BACKGROUND_OK: MARK_PRESENT, BACKGROUND_MISSING: MARK_MISSING,
                           BACKGROUND_TOO_LARGE: MARK_TOO_LARGE}
BACKGROUND_NAMES = {"exchange-rate-summary": "환율종합", "market-summary": "시장종합",
                    "world-index": "세계지수", "daily-price": "국내 일봉"}
# 출처에는 자료를 준 회사 이름 대신 중립 표현을 쓴다.
FILING_SOURCE = "미국 공시"
CLOSE_RATE_SOURCE = "해외 시세 자료"
BACKGROUND_SOURCE = "시세 자료(배경)"


@dataclass
class Exchange:
    """응답 한 번으로 끝나는 요청(시세 이력, 배경 자료, 공시)의 순번과 보낸 시각."""

    seq: int = 0
    sent_at: datetime | None = None
    received: bool = False

    def awaiting(self) -> bool:
        return self.sent_at is not None and not self.received


def next_seq(exchange: Exchange, now: datetime) -> int:
    exchange.seq += 1
    exchange.sent_at = now
    return exchange.seq


def parse_background(item: dict) -> Background:
    fetched_at = item.get("fetchedAt")
    return Background(
        api_type=item["apiType"],
        target=item["target"],
        status=item["status"],
        response=item.get("response"),
        fetched_at=parse_timestamp(fetched_at) if fetched_at is not None else None,
        response_bytes=item.get("responseBytes"),
        # 선택 필드라 이전 형식 응답에는 없다.
        index_rates=tuple(
            IndexRate(r["code"], r["name"], r["businessDay"], r["close"], r.get("prevBusinessDay"),
                      r.get("prevClose"), r.get("rate"))
            for r in item.get("indexRates") or ()
        ),
    )


def response_payload(message: str | bytes, event_type: str) -> dict:
    try:
        event = json.loads(message)
        if event["eventType"] != event_type:
            raise ValueError(f"unexpected eventType: {event['eventType']}")
        payload = event["payload"]
        payload["jobId"], payload["seq"]  # noqa: B018 - 필수 필드 확인
        return payload
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"malformed {event_type} event: {error!r}") from error


def parse_close_rates(items: list[dict]) -> tuple[CloseRate, ...]:
    """해외 등락률 응답의 항목들을 값 객체로 바꾼다. 칸이 없으면 KeyError."""
    return tuple(
        CloseRate(r["ticker"], r["date"], parse_timestamp(r["closeAt"]), r["rate"], parse_timestamp(r["fetchedAt"]))
        for r in items
    )


def parse_filings(items: list[dict]) -> tuple[Filing, ...]:
    """공시 응답의 항목들을 값 객체로 바꾼다. 칸이 없으면 KeyError."""
    return tuple(
        Filing(f["accessionNumber"], tuple(f["tickers"]), f["form"], tuple(f["items"]),
               parse_timestamp(f["acceptedAt"]), f["url"])
        for f in items
    )
