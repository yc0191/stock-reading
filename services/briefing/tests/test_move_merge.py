"""장 시작~아침 판 시작 사이 급변동을 아침 판에 합치는 단계(MorningMerge)를 확인한다. 시각은 인자로 넣으므로 경계값을 그대로 본다.

단위: 합침 구간 경계, 결과별 마무리(합침/리포트로 넘김), 기한. 가상: 아침 판 작업(DailyCore)의 결과 통지와 BriefingRunner의 수신부터 끝까지.
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import test_daily_jobs as jobs
from briefing import main
from briefing.config import DailySettings
from briefing.daily_post_close import MOVE_MERGED_TEXT
from briefing.daily_report import DailyLlmWriter
from briefing.job_log import REASON_INTERRUPTED, REASON_MERGED_INTO_MORNING, RunStatus
from briefing.move_merge import MorningMerge
from fake_api import FakeApi
from fake_job_log import FakeJobLog
from fake_members import FakeMembers
from fake_news import answer_news
from test_move_group import move
from test_runner import Clock, FakeConsumer, FakeMessage, FakeProducer

KST = timezone(timedelta(hours=9))
DAY = datetime(2026, 10, 20, tzinfo=KST)  # 화요일


def at(hour, minute, second=0):
    return DAY.replace(hour=hour, minute=minute, second=second)


class Sink:
    def __init__(self):
        self.calls = []

    def __call__(self, message, now):
        self.calls.append((json.loads(message)["eventId"], now))


@pytest.fixture
def parts():
    sink, job_log = Sink(), FakeJobLog()
    return MorningMerge(sink, job_log), sink, job_log


# ---- 합침 구간 ----

def test_the_window_starts_at_the_market_open_and_ends_before_the_edition_time(parts):
    merge, _, _ = parts
    assert not merge.in_window(at(8, 59, 59))
    assert merge.in_window(at(9, 0))
    assert merge.in_window(at(9, 9, 59))
    assert not merge.in_window(at(9, 10))


def test_a_weekend_move_is_never_held(parts):
    merge, _, _ = parts
    saturday = at(9, 5) + timedelta(days=4)
    assert saturday.weekday() == 5
    assert not merge.in_window(saturday)


def test_the_default_window_end_is_the_collector_morning_cron_time():
    """수집기의 아침 판 시작 시각 기본값과 브리핑의 합침 구간 끝이 같아야 한다(다르면 구간 밖 변동이 합쳐지거나 빠진다)."""
    yml = (Path(__file__).resolve().parents[3] / "services/collector/src/main/resources/application.yml").read_text(
        encoding="utf-8")
    cron = re.search(r"morning-cron: \$\{COLLECTOR_DAILY_BRIEFING_MORNING_CRON:([^}]+)\}", yml).group(1)
    _, minute, hour, *_ = cron.split()
    end = DailySettings().morning_merge_end
    assert (end.hour, end.minute) == (int(hour), int(minute))


def test_a_move_inside_the_window_is_held_and_starts_its_job_without_a_report(parts):
    merge, sink, job_log = parts
    assert merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5)) is True

    assert sink.calls == []
    [start] = job_log.starts
    assert start.job_id == "m1" and start.job_kind.value == "move"
    assert job_log.finishes == []
    assert merge.next_deadline() == at(9, 40)


def test_moves_outside_the_window_go_straight_to_the_report_flow(parts):
    merge, sink, job_log = parts
    assert merge.on_move(move("early", as_of=at(8, 59, 59)), at(9, 0)) is False
    assert merge.on_move(move("late", as_of=at(9, 10)), at(9, 10, 1)) is False
    assert job_log.starts == [] and merge.next_deadline() is None


def test_the_same_event_id_is_held_once(parts):
    merge, _, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))
    assert merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 6)) is True
    assert len(job_log.starts) == 1


# ---- 아침 판 결과 ----

def test_a_sent_morning_edition_closes_the_held_moves_as_merged_and_makes_no_report(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))
    merge.on_move(move("m2", symbol="TEST02", as_of=at(9, 8)), at(9, 8, 5))

    merge.on_morning_result(at(9, 10), True, at(9, 12), frozenset({"TEST01", "TEST02"}), at(9, 0))

    assert sink.calls == []
    assert [(f.job_id, f.status, f.failure_reason) for f in job_log.finishes] == [
        ("m1", RunStatus.SUCCEEDED, REASON_MERGED_INTO_MORNING), ("m2", RunStatus.SUCCEEDED, REASON_MERGED_INTO_MORNING)]
    assert merge.next_deadline() is None


def test_only_moves_whose_etf_open_flow_made_it_into_the_edition_are_merged(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))
    merge.on_move(move("m2", symbol="TEST02", as_of=at(9, 8)), at(9, 8, 5))

    # TEST02의 개장 흐름은 판에 없다(범위 안 시세 없음, 조회 실패, 범위 밖이라 버려짐 등).
    merge.on_morning_result(at(9, 10), True, at(9, 12), frozenset({"TEST01"}), at(9, 0))

    assert [f.job_id for f in job_log.finishes] == ["m1"]
    assert sink.calls == [("m2", at(9, 12))]


def test_a_move_before_the_flow_range_the_edition_read_is_not_merged(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))

    # 늦게 시작한 판은 읽은 구간이 09:30부터다. 09:03 변동은 그 구간 밖이라 판에 담기지 않았다.
    merge.on_morning_result(at(10, 0), True, at(10, 2), frozenset({"TEST01"}), at(9, 30))

    assert job_log.finishes == [] and sink.calls == [("m1", at(10, 2))]


def test_a_move_seen_again_after_it_was_merged_is_ignored_and_not_reported(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))
    merge.on_morning_result(at(9, 10), True, at(9, 12), frozenset({"TEST01"}), at(9, 0))

    assert merge.on_move(move("m1", as_of=at(9, 3)), at(9, 13)) is True
    assert sink.calls == [] and len(job_log.starts) == 1


def test_the_remembered_event_ids_stop_at_the_limit(parts):
    _, sink, job_log = parts
    merge = MorningMerge(sink, job_log, DailySettings(remembered_move_ids=2))
    for i in range(3):
        merge.on_move(move(f"m{i}", as_of=at(9, 3)), at(9, 3, 5))

    # 가장 오래된 m0는 잊었다. 판 결과 뒤라 합칠 곳이 없으므로 리포트 흐름(False)으로 간다.
    merge.on_morning_result(at(9, 10), True, at(9, 12), frozenset({"TEST01"}), at(9, 0))
    assert merge.on_move(move("m0", as_of=at(9, 3)), at(9, 13)) is False
    assert merge.on_move(move("m2", as_of=at(9, 3)), at(9, 13)) is True


def test_a_failed_morning_edition_sends_every_held_move_to_the_report_flow(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))
    merge.on_move(move("m2", symbol="TEST02", as_of=at(9, 8)), at(9, 8, 5))

    merge.on_morning_result(at(9, 10), False, at(9, 12))

    assert sink.calls == [("m1", at(9, 12)), ("m2", at(9, 12))]
    # 작업 기록은 리포트 흐름이 이어 쓰므로 합침으로 끝내지 않는다.
    assert job_log.finishes == []
    assert merge.next_deadline() is None


def test_after_the_morning_result_a_late_move_of_the_window_is_not_held(parts):
    merge, _, job_log = parts
    merge.on_morning_result(at(9, 10), True, at(9, 12))

    assert merge.on_move(move("late", as_of=at(9, 9)), at(9, 13)) is False
    assert job_log.starts == []


def test_a_morning_result_of_another_day_does_not_touch_todays_held_moves(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))

    merge.on_morning_result(at(9, 10) - timedelta(days=1), False, at(9, 12))

    assert sink.calls == [] and job_log.finishes == []
    assert merge.next_deadline() == at(9, 40)


def test_without_a_morning_result_the_held_moves_go_to_the_report_flow_after_the_grace(parts):
    merge, sink, _ = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))

    merge.on_tick(at(9, 40) - timedelta(seconds=1))
    assert sink.calls == []
    merge.on_tick(at(9, 40))  # 경계: 합침 구간이 끝나고 정확히 30분이 되면 넘긴다
    assert sink.calls == [("m1", at(9, 40))]
    assert merge.next_deadline() is None


def test_a_morning_edition_still_running_after_the_grace_keeps_the_moves_waiting(parts):
    _, sink, job_log = parts
    running = {"on": True}
    merge = MorningMerge(sink, job_log, morning_in_progress=lambda day: running["on"])
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))

    merge.on_tick(at(9, 45))
    assert sink.calls == [] and merge.next_deadline() is None  # 기다리는 동안 기한으로 깨어나지 않는다

    # 판이 끝나면 결과대로 처리한다(여기서는 성공해 합침).
    running["on"] = False
    merge.on_morning_result(at(9, 10), True, at(9, 46), frozenset({"TEST01"}), at(9, 0))
    assert sink.calls == [] and [f.job_id for f in job_log.finishes] == ["m1"]


def test_a_morning_edition_that_ended_without_a_result_releases_the_moves_after_the_grace(parts):
    _, sink, job_log = parts
    merge = MorningMerge(sink, job_log, morning_in_progress=lambda day: False)
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))

    merge.on_tick(at(9, 40))

    assert sink.calls == [("m1", at(9, 40))]


def test_a_window_move_that_arrives_after_the_grace_is_not_held(parts):
    merge, _, _ = parts
    assert merge.on_move(move("old", as_of=at(9, 5)), at(9, 40)) is False


def test_shutdown_ends_held_moves_as_interrupted_without_a_report(parts):
    merge, sink, job_log = parts
    merge.on_move(move("m1", as_of=at(9, 3)), at(9, 3, 5))

    assert merge.abandon_pending(at(9, 6)) == ["m1"]

    assert sink.calls == []
    [finish] = job_log.finishes
    assert (finish.job_id, finish.status, finish.failure_reason) == ("m1", RunStatus.FAILED, REASON_INTERRUPTED)
    assert merge.next_deadline() is None


# ---- 아침 판 작업의 결과 통지 ----

OPEN_AT = jobs.OPEN_AT


def morning_core(members=None, llm=None):
    """결과 통지를 모으는 DailyCore. 통지 목록을 함께 돌려준다."""
    notified = []
    requests, details = jobs.Requests(), jobs.Details()
    writer = DailyLlmWriter(llm, 3) if llm is not None else None
    c = jobs.DailyCore(requests, requests.api, writer, details, DailySettings(), members=members or FakeMembers(),
                       on_morning_result=lambda scheduled_for, ok, now, flowed, since:
                       notified.append((scheduled_for, ok, now, flowed, since)))
    return c, requests, notified


def run_morning(c, requests):
    c.on_collection_ready(jobs.tick(at=OPEN_AT), OPEN_AT)
    c.api.reply(jobs.daily_response(etfs=[jobs.etf(open_flow=jobs.OPEN_FLOW)]), OPEN_AT)
    c.api.reply(jobs.filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)


def test_a_sent_morning_edition_reports_success():
    c, requests, notified = morning_core()
    run_morning(c, requests)

    # 개장 흐름이 present로 담긴 종목과 읽은 구간 시작(판 시각 09:10에서 09:00)을 함께 알린다.
    assert notified == [(OPEN_AT, True, OPEN_AT, frozenset({"TEST01"}), at(9, 0))]


def test_an_etf_without_its_open_flow_in_the_edition_is_not_reported_as_flowed():
    c, requests, notified = morning_core()
    c.on_collection_ready(jobs.tick(at=OPEN_AT), OPEN_AT)
    c.api.reply(jobs.daily_response(etfs=[
        jobs.etf(open_flow=jobs.OPEN_FLOW), jobs.etf("TEST02", tickers=("TESTC",)),
        jobs.etf("TEST03", tickers=("TESTD",), open_flow={"status": "no-snapshot"})]), OPEN_AT)
    c.api.reply(jobs.filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)

    assert [n[3] for n in notified] == [frozenset({"TEST01"})]


def test_the_morning_edition_is_in_progress_from_its_tick_until_it_finishes():
    c, requests, _ = morning_core()
    day = OPEN_AT.date()
    assert c.morning_in_progress(day) is False
    c.on_collection_ready(jobs.tick(at=OPEN_AT), OPEN_AT)
    assert c.morning_in_progress(day) is True
    assert c.morning_in_progress(day + timedelta(days=1)) is False
    c.api.reply(jobs.daily_response(etfs=[jobs.etf(open_flow=jobs.OPEN_FLOW)]), OPEN_AT)
    c.api.reply(jobs.filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)
    assert c.morning_in_progress(day) is False


def test_a_morning_edition_that_could_not_read_its_members_reports_failure():
    c, requests, notified = morning_core(members=FakeMembers(None))
    run_morning(c, requests)

    assert [n[:3] for n in notified] == [(OPEN_AT, False, OPEN_AT)]
    assert requests.topic("briefing.daily.generated") == []


def test_the_post_close_edition_does_not_report_a_morning_result():
    c, requests, notified = morning_core()
    c.on_collection_ready(jobs.tick(jobs.POST_CLOSE_JOB, jobs.POST_CLOSE_AT), jobs.POST_CLOSE_AT)
    c.api.reply(jobs.daily_response(etfs=[jobs.etf(fetched="2026-10-20T16:30:00+09:00")]),
                              jobs.POST_CLOSE_AT)
    c.api.reply(jobs.filings_response(), jobs.POST_CLOSE_AT)
    answer_news(c, requests, jobs.POST_CLOSE_AT)

    assert notified == []


# ---- 수신부터 끝까지 ----

@pytest.fixture
def runner_parts():
    clock, store = Clock(at(9, 0)), FakeJobLog()
    created = []

    def consumer_factory(config):
        created.append(FakeConsumer(config))
        return created[-1]

    runner = main.BriefingRunner("localhost:9092", None, store, clock=clock, consumer_factory=consumer_factory,
                                 producer_factory=FakeProducer, declare_topics=lambda _bootstrap: None,
                                 daily_settings=DailySettings(morning_merge_enabled=True), api_factory=FakeApi)
    runner.open()
    return runner, created[-1], clock, store


def feed(runner, consumer, clock, now, event_id, as_of):
    clock.now = now
    consumer.queue.append(FakeMessage("market.move", move(event_id, as_of=as_of)))
    runner.run_once()


def test_the_runner_holds_a_window_move_and_a_move_after_it_starts_its_report_job(runner_parts):
    runner, consumer, clock, store = runner_parts
    feed(runner, consumer, clock, at(9, 3, 5), "in-window", at(9, 3))
    assert [s.job_id for s in store.starts] == ["in-window"]
    assert runner.grouper.next_deadline() is None  # 급변동 묶음 대기열에 들어가지 않았다

    feed(runner, consumer, clock, at(9, 12), "after", at(9, 11))
    # 합침 구간 밖의 변동은 지금처럼 작업이 바로 시작된다(묶음 조용한 시간 0).
    assert [s.job_id for s in store.starts] == ["in-window", "after"]


def test_the_runner_releases_a_held_move_to_the_report_flow_when_the_morning_edition_never_comes(runner_parts):
    runner, consumer, clock, store = runner_parts
    feed(runner, consumer, clock, at(9, 3, 5), "in-window", at(9, 3))
    assert runner._next_deadline() == at(9, 40)

    clock.now = at(9, 40)
    runner.run_once()

    # 리포트 흐름으로 넘어가 근거 조회가 시작됐고(요청 출구에 시세 이력 요청이 쌓임) 합쳤다는 기록은 없다.
    assert [f for f in store.finishes if f.failure_reason == REASON_MERGED_INTO_MORNING] == []
    assert any(e.entry_type.value == "moves-grouped" and e.job_id == "in-window" for e in store.entries)


def test_with_the_merge_switched_off_a_window_move_goes_straight_to_the_report_flow():
    clock, store, created = Clock(at(9, 0)), FakeJobLog(), []

    def consumer_factory(config):
        created.append(FakeConsumer(config))
        return created[-1]

    runner = main.BriefingRunner("localhost:9092", None, store, clock=clock, consumer_factory=consumer_factory,
                                 producer_factory=FakeProducer, declare_topics=lambda _bootstrap: None, api_factory=FakeApi)
    runner.open()
    feed(runner, created[-1], clock, at(9, 3, 5), "in-window", at(9, 3))

    assert any(e.entry_type.value == "moves-grouped" and e.job_id == "in-window" for e in store.entries)
    assert runner.merge.next_deadline() is None


@pytest.mark.parametrize("value, enabled", [("true", True), (" TRUE ", True), ("false", False), ("", False)])
def test_the_merge_is_switched_on_by_the_same_variable_as_the_collector_morning_tick(monkeypatch, value, enabled):
    monkeypatch.setattr(main, "PostgresJobLog", lambda connect: FakeJobLog())
    monkeypatch.setattr(main, "PostgresStatusStore", lambda connect: None)
    monkeypatch.setattr(main, "PostgresMemberStore", lambda connect: None)
    monkeypatch.setattr(main, "psycopg_connector", lambda conninfo: None)
    monkeypatch.setattr(main, "db_conninfo", lambda env: "")
    runner = main.build_runner({"COLLECTOR_DAILY_BRIEFING_ENABLED": value})
    assert runner.daily_settings.morning_merge_enabled is enabled


def test_the_runner_closes_a_held_move_as_merged_when_the_morning_edition_reports_success(runner_parts):
    runner, consumer, clock, store = runner_parts
    feed(runner, consumer, clock, at(9, 3, 5), "in-window", at(9, 3))

    runner.merge.on_morning_result(at(9, 10), True, at(9, 12), frozenset({"TEST01"}), at(9, 0))

    [finish] = store.finishes
    assert (finish.job_id, finish.failure_reason) == ("in-window", REASON_MERGED_INTO_MORNING)
    assert MOVE_MERGED_TEXT == "아침 판에 합침"


# ---- 기록을 읽는 쪽(마감 후 판의 그날 급변동, 서버 상태의 브리핑 줄) ----

def test_the_post_close_move_line_says_the_move_was_merged_into_the_morning_edition():
    from briefing.daily_post_close import move_line
    from briefing.job_log import MoveRun

    merged = MoveRun("m1", "TEST01", at(9, 3), 3.2, "succeeded", None, REASON_MERGED_INTO_MORNING)
    reported = MoveRun("m2", "TEST01", at(10, 3), 3.2, "succeeded", "llm", None)
    # 같은 성공이라도 사유가 합침이 아니면 리포트 성공으로 보인다.
    failed = MoveRun("m3", "TEST01", at(11, 3), 3.2, "failed", None, "llm_error")

    assert move_line(merged, {"TEST01": "테스트ETF"}) == "09:03 테스트ETF +3.20% · 아침 판에 합침"
    assert move_line(reported, {"TEST01": "테스트ETF"}).endswith("리포트 성공")
    assert move_line(failed, {"TEST01": "테스트ETF"}).endswith("리포트 실패")


def test_the_server_status_counts_merged_moves_apart_from_reported_ones():
    from briefing.status import JobRunRow, _briefing_line

    started = at(9, 3)
    runs = [JobRunRow("a", "move", "succeeded", REASON_MERGED_INTO_MORNING, None, started),
            JobRunRow("b", "move", "succeeded", None, "llm", started),
            JobRunRow("c", "morning", "succeeded", None, "llm", started)]

    line = _briefing_line(runs)

    assert "급변동 2건(성공 1 · " in line and "아침 판에 합침 1" in line
    assert "아침 판에 합침" not in _briefing_line(runs[1:])
