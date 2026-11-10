"""근거 자료의 형식과 응답 해석, 요청 한 번의 순번 기록, LLM 입력에 붙이는 상태 표시. Kafka를 모르며 메시지를 인자로 받는다."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime

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


@dataclass(frozen=True)
class EtfMember:
    market: str
    symbol: str
    rank: int
    name: str | None = None
    weight: str | None = None


@dataclass(frozen=True)
class MovedMember:
    """상위 구성 안에서 순위만 바뀐 종목. 순위는 1이 가장 위다."""

    market: str
    symbol: str
    rank_before: int
    rank_after: int
    name: str | None = None


@dataclass(frozen=True)
class MemberChanges:
    """ETF 상위 구성 종목의 변경. 한 종목은 removed·added·moved 중 한 곳에만 있다.

    since는 바뀌기 전 구성을 마지막으로 확인한 날, as_of는 바뀐 구성을 처음 받은 날이다.
    removed·added의 순위는 각각 빠지기 전, 들어온 뒤의 순위다.
    """

    since: date
    as_of: date
    removed: tuple[EtfMember, ...]
    added: tuple[EtfMember, ...]
    moved: tuple[MovedMember, ...]


@dataclass(frozen=True)
class EtfInfo:
    """ETF의 이름·추종 대상·업종과 구성 종목. 응답의 선택 칸이라 모르는 값은 None이다."""

    members: tuple[EtfMember, ...]
    members_truncated: bool
    name: str | None = None
    tracks: str | None = None
    sector: str | None = None
    member_changes: MemberChanges | None = None

    def us_tickers(self) -> frozenset[str]:
        return frozenset(m.symbol for m in self.members if m.market == US_MARKET)


US_MARKET = "US"

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


def parse_etf_info(item: dict | None) -> EtfInfo | None:
    """응답의 ETF 정보(선택 칸)를 값 객체로 바꾼다. 이전 형식 응답에는 없어 None이다. 필수 칸이 없으면 KeyError."""
    if item is None:
        return None
    return EtfInfo(
        members=tuple(EtfMember(m["market"], m["symbol"], m["rank"], m.get("name"), m.get("weight"))
                      for m in item["members"]),
        members_truncated=bool(item["membersTruncated"]),
        name=item.get("name"), tracks=item.get("tracks"), sector=item.get("sector"),
        member_changes=parse_member_changes(item.get("memberChanges")),
    )


def parse_member_changes(item: dict | None) -> MemberChanges | None:
    """응답의 구성 변경(선택 칸)을 값 객체로 바꾼다. 변경이 없으면 칸이 없어 None이다. 필수 칸이 없으면 KeyError."""
    if item is None:
        return None
    return MemberChanges(
        since=date.fromisoformat(item["since"]), as_of=date.fromisoformat(item["asOf"]),
        removed=tuple(EtfMember(m["market"], m["symbol"], m["rank"], m.get("name")) for m in item["removed"]),
        added=tuple(EtfMember(m["market"], m["symbol"], m["rank"], m.get("name")) for m in item["added"]),
        moved=tuple(MovedMember(m["market"], m["symbol"], m["rankBefore"], m["rankAfter"], m.get("name"))
                    for m in item["moved"]),
    )


def etf_info_input(info: EtfInfo, max_members: int) -> dict:
    """LLM 입력에 넣을 ETF 정보. 구성 종목은 비중 순위(1이 가장 큼) 순으로 max_members개까지 담는다."""
    ranked = sorted(info.members, key=lambda m: m.rank)
    data: dict = {key: value for key, value in (("name", info.name), ("tracks", info.tracks),
                                                 ("sector", info.sector)) if value}
    data["members"] = [
        {"rank": m.rank, "market": m.market, "symbol": m.symbol,
         **({"name": m.name} if m.name else {}), **({"weightPercent": m.weight} if m.weight else {})}
        for m in ranked[:max_members]
    ]
    data["membersTruncated"] = info.members_truncated or len(ranked) > max_members
    return data
