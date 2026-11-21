"""데일리 판 그래프용 가격 자료(장중 흐름, 기간 종가): 읽을 범위, 요청과 응답, 범위 재검사, LLM 입력에서 빠짐. 값은 모두 가짜다."""
import json
import logging
from datetime import datetime, timedelta, timezone

from dataclasses import replace

from briefing.config import CoreSettings
from briefing.daily_chart import chart_bars, intraday_lines, line_charts, period_lines
from briefing.daily_report import DailyInput, EtfDay
from briefing.sources import EtfInfo
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


# ---- 그래프 ----

def series_of(*rates, start=at(9, 0, 58)):
    return IntradaySeries(True, "10000", False, tuple(IntradayPoint(start + timedelta(minutes=n), "1", r)
                                                     for n, r in enumerate(rates)))


def closes_of(first, last, step_days=1):
    days, day = [], first
    while day <= last:
        if day.weekday() < 5:
            days.append(DailyClose(day.strftime("%Y%m%d"), str(10000 + len(days)), None))
        day += timedelta(days=step_days)
    return DailyCloses(True, at(16, 30, 2), False, tuple(days))


def chart_etf(symbol, relation="HELD", rate="1.00", series=None, closes=None, name=None):
    return EtfDay(symbol, "20261020", "10100", "10000", rate, at(16, 30, 2), (), False,
                  EtfInfo((), False, name or symbol), relation=relation, intraday_series=series, daily_closes=closes)


def board(*etfs):
    return DailyInput("job", "post-close", at(17, 0, day=19), at(17, 0), etfs, False, (), (), (), False)


def test_the_day_bar_spans_the_intraday_low_to_high_including_the_close_and_has_no_range_without_a_series():
    bars = chart_bars(board(chart_etf("TEST01", rate="0.80", series=series_of("-0.40", "1.20", "0.30")),
                            chart_etf("TEST02", rate="-2.00", series=series_of("-0.50", "0.10")),
                            chart_etf("TEST03", rate="0.10")))
    by_label = {b.label: (b.rate, b.low, b.high) for b in bars}
    assert by_label["TEST01"] == (0.8, -0.4, 1.2)
    # 종가가 장중 점보다 낮으면 범위가 종가까지 내려간다.
    assert by_label["TEST02"] == (-2.0, -2.0, 0.1)
    assert by_label["TEST03"] == (0.1, None, None)


def test_intraday_lines_keep_held_first_mark_watched_and_skip_series_with_fewer_than_two_points():
    lines = intraday_lines(board(chart_etf("TEST01", "WATCH", series=series_of("0.10", "0.20")),
                                 chart_etf("TEST02", series=series_of("-0.10", "-0.30")),
                                 chart_etf("TEST03", series=series_of("0.50"))))
    assert [(line.label, line.held, [y for _, y in line.points]) for line in lines] == [
        ("TEST02", True, [-0.1, -0.3]), ("TEST01", False, [0.1, 0.2])]


def test_a_period_line_starts_at_zero_and_an_etf_with_data_shorter_than_the_period_gets_no_line():
    from datetime import date
    long = closes_of(date(2026, 4, 20), date(2026, 10, 20))
    short = closes_of(date(2026, 9, 1), date(2026, 10, 20))
    month = period_lines(board(chart_etf("TEST01", closes=long), chart_etf("TEST02", closes=short)), 1)
    assert [line.label for line in month] == ["TEST01", "TEST02"]
    assert month[0].points[0][1] == 0.0 and month[0].points[-1][1] > 0
    six = period_lines(board(chart_etf("TEST01", closes=long), chart_etf("TEST02", closes=short)), 6)
    assert [line.label for line in six] == ["TEST01"]
    assert period_lines(board(chart_etf("TEST02", closes=short)), 6) == ()


def test_each_edition_gets_its_own_line_charts_and_a_chart_without_lines_is_left_out():
    from datetime import date
    full = chart_etf("TEST01", series=series_of("0.10", "0.20"), closes=closes_of(date(2026, 4, 20), date(2026, 10, 20)))
    morning = replace(board(full), edition="morning")
    assert [c.content_id for c in line_charts(morning, post_close=False)] == ["chart-open"]
    charts = line_charts(board(full), post_close=True)
    assert [c.content_id for c in charts] == ["chart-intraday", "chart-1m", "chart-6m"]
    assert all(c.png.startswith(b"\x89PNG") for c in charts)
    assert line_charts(board(chart_etf("TEST02")), post_close=True) == ()


def test_a_long_holiday_at_the_start_of_the_period_still_draws_the_line():
    from datetime import date
    # 기준일 2025-11-03의 한 달 전은 10-03이지만 연휴로 첫 거래일이 10-10(7일 뒤)이다.
    closes = closes_of(date(2025, 10, 10), date(2025, 11, 3))
    assert [line.label for line in period_lines(board(chart_etf("TEST01", closes=closes)), 1)] == ["TEST01"]
    # 한 달보다 훨씬 짧은 자료는 그대로 뺀다.
    assert period_lines(board(chart_etf("TEST01", closes=closes_of(date(2025, 10, 20), date(2025, 11, 3)))), 1) == ()

