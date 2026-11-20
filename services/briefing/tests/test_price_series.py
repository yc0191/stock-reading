"""데일리 판 그래프용 가격 자료(장중 흐름, 기간 종가): 읽을 범위, 요청과 응답, 범위 재검사, LLM 입력에서 빠짐. 값은 모두 가짜다."""
import json
import logging
from datetime import datetime, timedelta, timezone

from briefing.config import CoreSettings
from briefing.price_series import (
    DailyClose,
    DailyCloses,
    IntradayPoint,
    IntradaySeries,
    closes_in_range,
    intraday_in_range,
    intraday_series_start,
    parse_daily_closes,
    parse_intraday_series,
)
from briefing.queries import QueryKind
from contracts import validate_payload
from fake_news import answer_news
from test_daily_jobs import (  # noqa: F401  core는 테스트가 쓰는 fixture
    KST,
    MORNING_AT,
    OPEN_AT,
    OPEN_FLOW,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    TICK_ID,
    FakeLlm,
    core,
    daily_response,
    etf,
    filings_response,
    good_output,
    tick,
)


def at(hour, minute, second=0, day=20):
    return datetime(2026, 10, day, hour, minute, second, tzinfo=KST)


SERIES = {"status": "present", "prevClose": "10000", "truncated": False, "points": [
    {"at": "2026-10-20T09:00:59+09:00", "price": "10050", "rateVsPrevClose": "0.50"},
    {"at": "2026-10-20T09:01:58+09:00", "price": "9990", "rateVsPrevClose": "-0.10"}]}
NO_SERIES = {"status": "no-snapshot", "prevClose": None, "truncated": False, "points": []}
CLOSES = {"status": "present", "fetchedAt": "2026-10-19T16:30:02+09:00", "truncated": False, "points": [
    {"date": "20260930", "close": "9950", "rate": "0.51"},
    {"date": "20261019", "close": "10000", "rate": None}]}
NO_CLOSES = {"status": "no-daily-price", "fetchedAt": None, "truncated": False, "points": []}


def charted(symbol="TEST01", series=SERIES, closes=CLOSES, **kwargs):
    item = etf(symbol, **kwargs)
    if series is not None:
        item["intradaySeries"] = series
    if closes is not None:
        item["dailyCloses"] = closes
    return item


# ---- 범위 ----

def test_the_intraday_range_starts_at_the_market_open_and_covers_the_whole_day_for_the_post_close_edition():
    assert intraday_series_start(OPEN_AT) == at(9, 0)
    assert intraday_series_start(POST_CLOSE_AT) == at(9, 0)
    assert intraday_series_start(at(17, 30)) == at(9, 0)


def test_the_intraday_range_never_reaches_back_further_than_the_window():
    assert intraday_series_start(at(19, 0)) == at(10, 0)
    assert intraday_series_start(at(19, 0), CoreSettings(intraday_series_max_window=timedelta(hours=12))) == at(9, 0)


def test_there_is_no_intraday_range_before_the_market_opens_and_it_is_decided_in_korean_time():
    assert intraday_series_start(MORNING_AT) is None
    assert intraday_series_start(at(8, 59, 59)) is None
    assert intraday_series_start(OPEN_AT.astimezone(timezone.utc)) == at(9, 0)


def test_intraday_points_must_be_inside_the_range_in_time_order_and_under_the_limit():
    start, end = at(9, 0), at(9, 10)

    def series(*times, present=True):
        return IntradaySeries(present, "10000", False, tuple(IntradayPoint(t, "10000", "0.00") for t in times))

    assert intraday_in_range(series(start, end), start, end, 2)
    assert not intraday_in_range(series(start - timedelta(seconds=1), end), start, end, 2)
    assert not intraday_in_range(series(start, end + timedelta(seconds=1)), start, end, 2)
    assert not intraday_in_range(series(end, start), start, end, 2)
    assert not intraday_in_range(series(start, at(9, 5), end), start, end, 2)
    assert not intraday_in_range(series(), start, end, 2)
    # 범위 안 시세가 없다는 응답은 버릴 값이 없다. 없다면서 점이 있으면 버린다.
    assert intraday_in_range(IntradaySeries(False), start, end, 2)
    assert not intraday_in_range(series(start, present=False), start, end, 2)


def test_closes_must_be_received_inside_the_edition_range_and_dated_up_to_the_edition_date():
    start, end = at(17, 0, day=19), at(17, 0)

    def closes(*dates, fetched=at(16, 30)):
        return DailyCloses(True, fetched, False, tuple(DailyClose(d, "10000", None) for d in dates))

    assert closes_in_range(closes("20261016", "20261020"), start, end, 2)
    assert closes_in_range(closes("20261016", fetched=start), start, end, 2)
    assert closes_in_range(closes("20261016", fetched=end), start, end, 2)
    assert not closes_in_range(closes("20261016", fetched=end + timedelta(seconds=1)), start, end, 2)
    assert not closes_in_range(closes("20261016", fetched=start - timedelta(seconds=1)), start, end, 2)
    assert not closes_in_range(closes("20261016", "20261021"), start, end, 2)
    assert not closes_in_range(closes("20261019", "20261016"), start, end, 2)
    assert not closes_in_range(closes("20261016", "20261016"), start, end, 2)
    assert not closes_in_range(closes("20261015", "20261016", "20261019"), start, end, 2)
    assert not closes_in_range(DailyCloses(True, None, False, (DailyClose("20261016", "1", None),)), start, end, 2)
    assert closes_in_range(DailyCloses(False), start, end, 2)


def test_parse_reads_both_cells_and_a_missing_cell_is_none():
    assert parse_intraday_series(None) is None and parse_daily_closes(None) is None
    series = parse_intraday_series(SERIES)
    assert (series.present, series.prev_close, series.truncated) == (True, "10000", False)
    assert series.points[1] == IntradayPoint(at(9, 1, 58), "9990", "-0.10")
    assert parse_intraday_series(NO_SERIES) == IntradaySeries(False)
    closes = parse_daily_closes(CLOSES)
    assert closes.fetched_at == at(16, 30, 2, day=19)
    assert closes.points == (DailyClose("20260930", "9950", "0.51"), DailyClose("20261019", "10000", None))
    assert parse_daily_closes(NO_CLOSES) == DailyCloses(False)


# ---- 요청과 응답 ----

def test_both_editions_ask_for_the_closes_and_the_intraday_series_from_the_market_open(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, "9d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68"), POST_CLOSE_AT)

    morning, post_close = requests.api.of(QueryKind.DAILY)
    for request in (morning, post_close):
        assert validate_payload("market.daily.requested.v1.schema.json", request.payload) == []
        assert request.payload["intradaySince"] == "2026-10-20T09:00:00+09:00"
        assert request.payload["maxIntradayPoints"] == 420
        assert request.payload["maxDailyCloses"] == 130


def test_no_intraday_series_is_asked_for_before_the_market_opens_but_the_closes_are(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)

    [request] = requests.api.of(QueryKind.DAILY)
    assert "intradaySince" not in request.payload and "maxIntradayPoints" not in request.payload
    assert request.payload["maxDailyCloses"] == 130


def test_the_cells_in_range_are_kept_on_the_etf(core):
    c, _, _, details = core()
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    c.api.reply(daily_response(etfs=[charted(), charted("TEST02", NO_SERIES, NO_CLOSES), charted("TEST03", None, None)]),
                OPEN_AT)

    first, second, third = c._jobs[TICK_ID].etfs
    assert first.intraday_series.points[0] == IntradayPoint(at(9, 0, 59), "10050", "0.50")
    assert first.daily_closes.points[-1] == DailyClose("20261019", "10000", None)
    assert second.intraday_series == IntradaySeries(False) and second.daily_closes == DailyCloses(False)
    assert third.intraday_series is None and third.daily_closes is None


def test_cells_outside_the_range_are_dropped_with_a_warning_that_names_no_symbol(core, caplog):
    late_series = {**SERIES, "points": [*SERIES["points"],
                                        {"at": "2026-10-20T09:10:01+09:00", "price": "1", "rateVsPrevClose": None}]}
    stale_closes = {**CLOSES, "fetchedAt": "2026-10-19T09:09:59+09:00"}
    c, _, _, _ = core()
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    with caplog.at_level(logging.WARNING):
        c.api.reply(daily_response(etfs=[charted(series=late_series, closes=stale_closes)]), OPEN_AT)

    [etf_day] = c._jobs[TICK_ID].etfs
    assert etf_day.intraday_series is None and etf_day.daily_closes is None
    assert "dropped 1 intradaySeries" in caplog.text and "dropped 1 dailyCloses" in caplog.text
    assert "TEST01" not in caplog.text
    assert c._jobs[TICK_ID].excluded == {"intradaySeries": 1, "dailyCloses": 1}


def test_a_malformed_chart_cell_drops_only_that_cell_and_keeps_the_daily_data(core, caplog):
    broken = {**SERIES, "points": [{"at": "not a time", "price": "10050", "rateVsPrevClose": "0.50"}]}
    c, _, _, _ = core()
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    with caplog.at_level(logging.WARNING):
        c.api.reply(daily_response(etfs=[charted(series=broken)]), OPEN_AT)

    # 그래프 칸 하나가 깨져도 일봉·등락 같은 본문 자료는 받은 것으로 남는다.
    [etf_day] = c._jobs[TICK_ID].etfs
    assert etf_day.intraday_series is None and etf_day.daily_closes is not None and etf_day.rate is not None
    assert "malformed chart field" in caplog.text and "TEST01" not in caplog.text


def test_an_intraday_series_the_edition_did_not_ask_for_is_dropped(core, caplog):
    c, _, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    with caplog.at_level(logging.WARNING):
        c.api.reply(daily_response(etfs=[charted()]), MORNING_AT)

    [etf_day] = c._jobs[TICK_ID].etfs
    assert etf_day.intraday_series is None
    assert etf_day.daily_closes is not None
    assert "dropped 1 intradaySeries" in caplog.text


def test_the_series_and_closes_stay_out_of_the_llm_input(core):
    llm = FakeLlm(good_output("O1"))
    c, requests, _, _ = core(llm)
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    c.api.reply(daily_response(etfs=[charted(open_flow=OPEN_FLOW)]), OPEN_AT)
    c.api.reply(filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)

    [call] = llm.calls
    text = json.dumps(json.loads(call[1]), ensure_ascii=False)
    for marker in ("09:00:59", "09:01:58", "9990", "20260930", "9950", "intraday", "dailyCloses"):
        assert marker not in text, marker
