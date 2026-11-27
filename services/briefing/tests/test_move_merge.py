"""장 시작~아침 판 시작 사이 급변동을 버리는 단계(MarketOpenDrop)를 확인한다. 시각은 인자로 넣으므로 경계값을 그대로 본다.

단위: 구간 경계, 구간 안 변동(ETF·개별 주식)의 버림과 작업 기록, 같은 eventId. 가상: BriefingRunner의 수신부터 끝까지
(구간 안은 리포트 없음, 구간 밖은 바로 리포트, 아침 판 결과가 없거나 재시작해도 리포트 없음).
"""
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from briefing import main
from briefing.config import DailySettings
from briefing.job_log import REASON_DROPPED_AT_MARKET_OPEN, REASON_MERGED_INTO_MORNING, RunStatus
from briefing.move_merge import MarketOpenDrop
from fake_api import FakeApi
from fake_job_log import FakeJobLog
from test_move_group import move
from test_runner import Clock, FakeConsumer, FakeMessage, FakeProducer

KST = timezone(timedelta(hours=9))
DAY = datetime(2026, 10, 20, tzinfo=KST)  # 화요일


def at(hour, minute, second=0):
    return DAY.replace(hour=hour, minute=minute, second=second)


@pytest.fixture
def parts():
    job_log = FakeJobLog()
    return MarketOpenDrop(job_log), job_log


def dropped(job_log):
    return [(f.job_id, f.status, f.failure_reason) for f in job_log.finishes]


# ---- 구간 ----

def test_the_window_starts_at_the_market_open_and_ends_before_the_edition_time(parts):
    drop, _ = parts
    assert not drop.in_window(at(8, 59, 59))
    assert drop.in_window(at(9, 0))
    assert drop.in_window(at(9, 9, 59))
    assert not drop.in_window(at(9, 10))


def test_a_weekend_move_is_never_dropped(parts):
    drop, _ = parts
    saturday = at(9, 5) + timedelta(days=4)
    assert saturday.weekday() == 5
    assert not drop.in_window(saturday)


def test_the_default_window_end_is_the_collector_morning_cron_time():
    """수집기의 아침 판 시작 시각 기본값과 브리핑의 구간 끝이 같아야 한다(다르면 구간 밖 변동이 버려지거나 구간 안 변동이 나간다)."""
    yml = (Path(__file__).resolve().parents[3] / "services/collector/src/main/resources/application.yml").read_text(
        encoding="utf-8")
    cron = re.search(r"morning-cron: \$\{COLLECTOR_DAILY_BRIEFING_MORNING_CRON:([^}]+)\}", yml).group(1)
    _, minute, hour, *_ = cron.split()
    end = DailySettings().morning_merge_end
    assert (end.hour, end.minute) == (int(hour), int(minute))


# ---- 버림 ----

@pytest.mark.parametrize("symbol", ["TEST01", "STOCK1"])
def test_a_window_move_of_an_etf_or_a_stock_is_dropped_and_its_job_ends_without_a_report(parts, symbol):
    """종류와 상관없이 구간 안 변동은 리포트 흐름으로 가지 않고, 받는 즉시 작업 기록을 버림으로 끝낸다(#108)."""
    drop, job_log = parts
    assert drop.on_move(move("m1", symbol=symbol, as_of=at(9, 3)), at(9, 3, 5)) is True

    [start] = job_log.starts
    assert start.job_id == "m1" and start.job_kind.value == "move"
    assert dropped(job_log) == [("m1", RunStatus.SUCCEEDED, REASON_DROPPED_AT_MARKET_OPEN)]


def test_moves_outside_the_window_go_straight_to_the_report_flow(parts):
    drop, job_log = parts
    assert drop.on_move(move("early", as_of=at(8, 59, 59)), at(9, 0)) is False
    assert drop.on_move(move("late", as_of=at(9, 10)), at(9, 10, 1)) is False
    assert job_log.starts == [] and job_log.finishes == []


def test_a_window_move_that_arrives_late_is_still_dropped(parts):
    """아침 판이 끝난 뒤에 받아도 보내지 않는다. 기준은 받은 시각이 아니라 변동 시각이다."""
    drop, job_log = parts
    assert drop.on_move(move("old", as_of=at(9, 5)), at(10, 0)) is True
    assert dropped(job_log) == [("old", RunStatus.SUCCEEDED, REASON_DROPPED_AT_MARKET_OPEN)]


def test_the_same_event_id_is_dropped_once(parts):
    drop, job_log = parts
    drop.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))
    assert drop.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 6)) is True
    assert len(job_log.starts) == 1 and len(job_log.finishes) == 1


def test_the_remembered_event_ids_stop_at_the_limit():
    job_log = FakeJobLog()
    drop = MarketOpenDrop(job_log, DailySettings(remembered_move_ids=2))
    for i in range(3):
        drop.on_move(move(f"m{i}", as_of=at(9, 3)), at(9, 3, 5))

    # 가장 오래된 m0는 잊었으므로 다시 오면 기록을 또 쓴다. 그래도 구간 안이라 리포트로는 가지 않는다.
    assert drop.on_move(move("m0", as_of=at(9, 3)), at(9, 13)) is True
    assert len(job_log.finishes) == 4
    assert drop.on_move(move("m2", as_of=at(9, 3)), at(9, 13)) is True
    assert len(job_log.finishes) == 4


# ---- 수신부터 끝까지 ----

def make_runner(**kwargs):
    clock, store, created = Clock(at(9, 0)), FakeJobLog(), []

    def consumer_factory(config):
        created.append(FakeConsumer(config))
        return created[-1]

    runner = main.BriefingRunner("localhost:9092", None, store, clock=clock, consumer_factory=consumer_factory,
                                 producer_factory=FakeProducer, declare_topics=lambda _bootstrap: None,
                                 api_factory=FakeApi, **kwargs)
    runner.open()
    return runner, created[-1], clock, store


@pytest.fixture
def runner_parts():
    return make_runner(daily_settings=DailySettings(morning_merge_enabled=True))


def feed(runner, consumer, clock, now, event_id, as_of, symbol="TEST01"):
    clock.now = now
    consumer.queue.append(FakeMessage("market.move", move(event_id, symbol=symbol, as_of=as_of)))
    runner.run_once()


def grouped(store):
    """급변동 리포트 흐름(묶음)으로 넘어간 작업."""
    return {e.job_id for e in store.entries if e.entry_type.value == "moves-grouped"}


def test_the_runner_drops_window_moves_of_etfs_and_stocks_and_reports_a_move_after_it(runner_parts):
    runner, consumer, clock, store = runner_parts
    feed(runner, consumer, clock, at(9, 0, 5), "etf", at(9, 0))
    feed(runner, consumer, clock, at(9, 3, 5), "stock", at(9, 3), symbol="STOCK1")
    assert grouped(store) == set()
    assert dropped(store) == [("etf", RunStatus.SUCCEEDED, REASON_DROPPED_AT_MARKET_OPEN),
                              ("stock", RunStatus.SUCCEEDED, REASON_DROPPED_AT_MARKET_OPEN)]
    # 모아 두지 않으므로 깨어날 기한이 없다.
    assert runner._next_deadline() is None

    feed(runner, consumer, clock, at(9, 12), "after", at(9, 11))
    # 구간 밖의 변동은 지금처럼 리포트 흐름으로 바로 간다.
    assert grouped(store) == {"after"}


def test_the_runner_reports_nothing_for_a_dropped_move_when_no_morning_edition_result_ever_comes(runner_parts):
    """아침 판이 실패하거나 결과가 없어도 버린 변동은 나중에 리포트로 넘어가지 않는다."""
    runner, consumer, clock, store = runner_parts
    feed(runner, consumer, clock, at(9, 3, 5), "in-window", at(9, 3))

    for now in (at(9, 40), at(10, 30), at(15, 0)):
        clock.now = now
        runner.run_once()

    assert grouped(store) == set() and [s.job_id for s in store.starts] == ["in-window"]
    assert len(store.finishes) == 1


def test_the_runner_shutdown_leaves_a_dropped_move_as_dropped(runner_parts):
    runner, consumer, clock, store = runner_parts
    feed(runner, consumer, clock, at(9, 3, 5), "in-window", at(9, 3))

    runner._abandon_pending_moves()

    assert dropped(store) == [("in-window", RunStatus.SUCCEEDED, REASON_DROPPED_AT_MARKET_OPEN)]


def test_with_the_morning_edition_switched_off_a_window_move_goes_straight_to_the_report_flow():
    runner, consumer, clock, store = make_runner()
    feed(runner, consumer, clock, at(9, 3, 5), "in-window", at(9, 3))

    assert grouped(store) == {"in-window"}
    assert [f for f in store.finishes if f.failure_reason == REASON_DROPPED_AT_MARKET_OPEN] == []


@pytest.mark.parametrize("value, enabled", [("true", True), (" TRUE ", True), ("false", False), ("", False)])
def test_the_drop_is_switched_on_by_the_same_variable_as_the_collector_morning_tick(monkeypatch, value, enabled):
    monkeypatch.setattr(main, "PostgresJobLog", lambda connect: FakeJobLog())
    monkeypatch.setattr(main, "PostgresStatusStore", lambda connect: None)
    monkeypatch.setattr(main, "PostgresMemberStore", lambda connect: None)
    monkeypatch.setattr(main, "psycopg_connector", lambda conninfo: None)
    monkeypatch.setattr(main, "db_conninfo", lambda env: "")
    runner = main.build_runner({"COLLECTOR_DAILY_BRIEFING_ENABLED": value})
    assert runner.daily_settings.morning_merge_enabled is enabled


# ---- 기록을 읽는 쪽(마감 후 판의 그날 급변동, 서버 상태의 브리핑 줄). 예전 '아침 판에 합침' 기록 ----

def test_the_post_close_move_line_says_an_old_record_was_merged_into_the_morning_edition():
    from briefing.daily_post_close import move_line
    from briefing.job_log import MoveRun

    merged = MoveRun("m1", "TEST01", at(9, 3), 3.2, "succeeded", None, REASON_MERGED_INTO_MORNING, 3.2, "high")
    reported = MoveRun("m2", "TEST01", at(10, 3), 3.2, "succeeded", "llm", None, 3.2, "high")
    # 같은 성공이라도 사유가 합침이 아니면 메일을 보낸 것으로 보인다.
    failed = MoveRun("m3", "TEST01", at(11, 3), 3.2, "failed", None, "llm_error", 3.2, "high")

    assert move_line(merged, {"TEST01": "테스트ETF"}) == "09:03 테스트ETF 오늘 +3.20% · 아침 판에 합침"
    assert move_line(reported, {"TEST01": "테스트ETF"}).endswith("메일 보냄")
    assert move_line(failed, {"TEST01": "테스트ETF"}).endswith("리포트 실패")


def test_the_server_status_counts_old_merged_moves_apart_from_reported_ones():
    from briefing.status import JobRunRow, _briefing_line

    started = at(9, 3)
    runs = [JobRunRow("a", "move", "succeeded", REASON_MERGED_INTO_MORNING, None, started),
            JobRunRow("b", "move", "succeeded", None, "llm", started),
            JobRunRow("c", "morning", "succeeded", None, "llm", started)]

    line = _briefing_line(runs)

    assert "급변동 2건(상 0(메일) · 중 1(기록) · " in line and "아침 판에 합침 1" in line
    assert "아침 판에 합침" not in _briefing_line(runs[1:])


# ---- 기록을 읽는 쪽. 장 시작 구간이라 버린 기록 ----

def test_the_post_close_move_line_says_a_dropped_move_was_not_sent():
    from briefing.daily_post_close import move_line
    from briefing.job_log import MoveRun

    # 레벨이 상이어도 버린 변동은 '메일 보냄'으로 보이지 않는다.
    dropped_run = MoveRun("m1", "STOCK1", at(9, 3), 3.2, "succeeded", None, REASON_DROPPED_AT_MARKET_OPEN, 3.2, "high")

    assert move_line(dropped_run, {"STOCK1": "테스트주식"}) == "09:03 테스트주식 오늘 +3.20% · 장 시작 구간이라 보내지 않음"


def test_the_server_status_counts_dropped_moves_apart_from_reported_ones():
    from briefing.status import JobRunRow, _briefing_line

    started = at(9, 3)
    runs = [JobRunRow("a", "move", "succeeded", REASON_DROPPED_AT_MARKET_OPEN, None, started),
            JobRunRow("b", "move", "succeeded", REASON_DROPPED_AT_MARKET_OPEN, None, started),
            JobRunRow("c", "move", "succeeded", None, "llm", started)]

    line = _briefing_line(runs)

    # 버린 변동은 중(기록)으로 세지 않는다.
    assert "급변동 3건(상 0(메일) · 중 1(기록) · " in line and "장 시작 구간이라 보내지 않음 2" in line
    assert "장 시작 구간이라 보내지 않음" not in _briefing_line(runs[2:])
