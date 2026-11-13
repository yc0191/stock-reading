"""같은 종목의 market.move를 모아 작업 하나로 만드는 단계(MoveGrouper)를 확인한다. 시각은 인자로 넣으므로 경계값을 그대로 본다.

단위: 묶음 규칙(조용한 시간, 최대 대기, 종목 분리, 같은 eventId). 가상: 가짜 Kafka를 단 BriefingRunner로 수신부터 작업 시작까지.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing import main
from briefing.config import CoreSettings
from briefing.job_log import EntryType
from briefing.jobs import BriefingCore
from briefing.move_group import MoveGrouper
from contracts import sample
from fake_api import FakeApi
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T0 = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
LONG_WAIT = timedelta(minutes=10)  # 조용한 시간을 길게 둔 묶음 규칙 시험용 값(기본값은 0분)
MAX_WAIT = timedelta(minutes=30)


def move(event_id, symbol="TEST01", price=10000, as_of=T0, change=1.5):
    event = sample("market.move.sample.json")
    event["eventId"] = event_id
    event["payload"].update(symbol=symbol, asOf=as_of.isoformat(), price=price, changePercent=change)
    return json.dumps(event)


class Sink:
    def __init__(self):
        self.calls = []

    def __call__(self, message, now):
        self.calls.append((json.loads(message)["eventId"], now))


@pytest.fixture
def parts():
    sink, job_log = Sink(), FakeJobLog()
    grouper = MoveGrouper(sink, job_log, CoreSettings(move_quiet_period=LONG_WAIT))
    return grouper, sink, job_log


def test_defaults_are_zero_quiet_minutes_and_thirty_minutes_at_most():
    settings = CoreSettings()
    assert settings.move_quiet_period == timedelta(0) and settings.move_max_wait == MAX_WAIT


def test_a_move_does_not_start_a_job_until_the_quiet_time_has_passed(parts):
    grouper, sink, _ = parts
    grouper.on_move(move("m1"), T0)

    grouper.on_tick(T0 + LONG_WAIT - timedelta(seconds=1))
    assert sink.calls == []
    assert grouper.next_deadline() == T0 + LONG_WAIT

    grouper.on_tick(T0 + LONG_WAIT)  # 경계: 정확히 10분이 되면 내보낸다
    assert sink.calls == [("m1", T0 + LONG_WAIT)]
    assert grouper.next_deadline() is None


def test_each_new_move_of_the_same_symbol_restarts_the_quiet_time_and_one_job_is_made_from_the_latest(parts):
    grouper, sink, job_log = parts
    grouper.on_move(move("m1", price=10000), T0)
    grouper.on_move(move("m2", price=10100), T0 + timedelta(minutes=9))
    grouper.on_move(move("m3", price=10200), T0 + timedelta(minutes=18))

    grouper.on_tick(T0 + timedelta(minutes=27, seconds=59))
    assert sink.calls == []
    grouper.on_tick(T0 + timedelta(minutes=28))

    assert sink.calls == [("m3", T0 + timedelta(minutes=28))]  # 가장 늦은 변동으로 작업 하나
    [grouped] = job_log.of_type(EntryType.MOVES_GROUPED)
    assert grouped.job_id == "m3" and grouped.attempts == 3
    # 모인 변동 전부가 순서대로, 각 eventId와 값과 함께 작업 기록에 남는다.
    assert [m["eventId"] for m in grouped.data["moves"]] == ["m1", "m2", "m3"]
    assert [m["price"] for m in grouped.data["moves"]] == [10000, 10100, 10200]
    assert grouped.data["quietSeconds"] == 600 and grouped.data["maxWaitSeconds"] == 1800


def test_moves_that_keep_coming_are_cut_at_thirty_minutes_from_the_first_and_later_ones_start_a_new_group(parts):
    grouper, sink, job_log = parts
    for minute in range(0, 30, 5):
        grouper.on_move(move(f"m{minute}"), T0 + timedelta(minutes=minute))
    assert sink.calls == []
    assert grouper.next_deadline() == T0 + MAX_WAIT  # 마지막 변동 뒤 10분(35분)보다 이른 30분이 상한이다

    grouper.on_tick(T0 + MAX_WAIT - timedelta(seconds=1))
    assert sink.calls == []
    grouper.on_move(move("m30"), T0 + MAX_WAIT)  # 상한 시각에 온 변동은 먼저 묶음을 내보내고 새 묶음이 된다

    assert [eid for eid, _ in sink.calls] == ["m25"]
    [first] = job_log.of_type(EntryType.MOVES_GROUPED)
    assert [m["eventId"] for m in first.data["moves"]] == ["m0", "m5", "m10", "m15", "m20", "m25"]
    assert grouper.next_deadline() == T0 + MAX_WAIT + LONG_WAIT
    grouper.on_tick(T0 + MAX_WAIT + LONG_WAIT)
    assert [eid for eid, _ in sink.calls] == ["m25", "m30"]


def test_a_move_arriving_after_the_quiet_time_started_a_new_group_even_if_no_tick_ran(parts):
    grouper, sink, _ = parts
    grouper.on_move(move("m1"), T0)
    grouper.on_move(move("m2"), T0 + LONG_WAIT)

    assert [eid for eid, _ in sink.calls] == ["m1"]
    grouper.on_tick(T0 + 2 * LONG_WAIT)
    assert [eid for eid, _ in sink.calls] == ["m1", "m2"]


def test_other_symbols_are_grouped_separately(parts):
    grouper, sink, job_log = parts
    grouper.on_move(move("a1", symbol="TEST01"), T0)
    grouper.on_move(move("b1", symbol="TEST02"), T0 + timedelta(minutes=6))
    grouper.on_move(move("a2", symbol="TEST01"), T0 + timedelta(minutes=8))

    grouper.on_tick(T0 + timedelta(minutes=15, seconds=59))
    assert sink.calls == []
    grouper.on_tick(T0 + timedelta(minutes=16))  # TEST02는 마지막 변동 뒤 10분, TEST01(18분)은 아직 아님
    assert [eid for eid, _ in sink.calls] == ["b1"]
    grouper.on_tick(T0 + timedelta(minutes=18))  # TEST01은 두 변동 중 늦은 것으로, 처음 변동 시각에서 끊기지 않는다
    assert [eid for eid, _ in sink.calls] == ["b1", "a2"]
    assert {e.job_id: len(e.data["moves"]) for e in job_log.of_type(EntryType.MOVES_GROUPED)} == {"a2": 2, "b1": 1}


def test_the_same_event_id_is_counted_once_even_after_its_group_was_sent(parts):
    grouper, sink, job_log = parts
    grouper.on_move(move("m1"), T0)
    grouper.on_move(move("m1"), T0 + timedelta(minutes=1))
    grouper.on_move(move("m2"), T0 + timedelta(minutes=2))
    grouper.on_tick(T0 + timedelta(minutes=12))
    grouper.on_move(move("m1"), T0 + timedelta(minutes=13))  # 이미 내보낸 묶음의 변동이 다시 와도 새 작업을 만들지 않는다
    grouper.on_tick(T0 + timedelta(minutes=30))

    assert [eid for eid, _ in sink.calls] == ["m2"]
    [grouped] = job_log.of_type(EntryType.MOVES_GROUPED)
    assert [m["eventId"] for m in grouped.data["moves"]] == ["m1", "m2"]


def test_a_malformed_move_is_rejected_and_does_not_join_a_group(parts):
    grouper, sink, _ = parts
    with pytest.raises(ValueError):
        grouper.on_move('{"eventType": "market.move", "eventId": "bad"}', T0)

    assert grouper.next_deadline() is None and sink.calls == []


def test_a_zero_quiet_time_starts_a_job_for_every_move_at_once():
    sink, job_log = Sink(), FakeJobLog()
    grouper = MoveGrouper(sink, job_log, CoreSettings(move_quiet_period=timedelta(0)))
    grouper.on_move(move("m1"), T0)
    grouper.on_move(move("m2"), T0)

    assert [eid for eid, _ in sink.calls] == ["m1", "m2"]


def test_the_memory_of_seen_event_ids_is_bounded():
    grouper = MoveGrouper(Sink(), FakeJobLog(), CoreSettings(remembered_jobs=3, move_quiet_period=timedelta(0)))
    for n in range(10):
        grouper.on_move(move(f"m{n}"), T0)

    assert len(grouper._seen) == 3


# ---- 가상: 가짜 Kafka를 단 실행부로 수신부터 작업 시작까지 ----

def test_runner_waits_for_the_quiet_time_and_then_starts_one_move_job_with_the_grouped_moves_in_the_record():
    from test_runner import Clock, FakeConsumer, FakeMessage, FakeProducer, FakeStore

    clock = Clock(T0)
    store = FakeStore()
    created = {}

    def consumer_factory(config):
        created.setdefault("consumers", []).append(FakeConsumer(config))
        return created["consumers"][-1]

    runner = main.BriefingRunner("localhost:9092", None, store, settings=CoreSettings(move_quiet_period=LONG_WAIT), clock=clock, consumer_factory=consumer_factory,
                                 producer_factory=FakeProducer, declare_topics=lambda _bootstrap: None, api_factory=FakeApi)
    runner.open()
    consumer = created["consumers"][-1]  # 마지막이 market.move를 받는 소비자다

    for n, minute in enumerate((0, 4), start=1):
        clock.now = T0 + timedelta(minutes=minute)
        consumer.queue.append(FakeMessage("market.move", move(f"it-m{n}", price=10000 + n)))
        runner.run_once()
    assert store.starts == []  # 아직 작업이 없다

    clock.now = T0 + timedelta(minutes=14)  # 마지막 변동(4분) 뒤 10분
    runner.run_once()

    [start] = store.starts
    assert start.job_id == "it-m2" and start.input == move("it-m2", price=10002)
    [grouped] = store.of_type(EntryType.MOVES_GROUPED)
    assert grouped.job_id == "it-m2" and [m["eventId"] for m in grouped.data["moves"]] == ["it-m1", "it-m2"]


def test_core_receives_the_latest_move_of_the_group_as_its_job():
    from test_jobs import Recorder

    rec = Recorder()
    core = BriefingCore(rec, rec.api, rec, job_log=rec.log)
    grouper = MoveGrouper(core.on_move, rec.log, CoreSettings(move_quiet_period=LONG_WAIT))
    grouper.on_move(move("m1", price=10000), T0)
    grouper.on_move(move("m2", price=10300, as_of=T0 + timedelta(minutes=3)), T0 + timedelta(minutes=3))
    grouper.on_tick(T0 + timedelta(minutes=13))

    [start] = rec.log.starts
    assert start.job_id == "m2" and start.target["moveAt"] == (T0 + timedelta(minutes=3)).isoformat()


# ---- 정상 종료: 대기 묶음을 실패 작업으로 남긴다 ----

def test_abandoning_two_pending_groups_fails_two_jobs_as_interrupted_without_starting_reports(parts):
    grouper, sink, job_log = parts
    grouper.on_move(move("a1", symbol="TEST01", price=10000), T0)
    grouper.on_move(move("a2", symbol="TEST01", price=10100, as_of=T0 + timedelta(minutes=2)), T0 + timedelta(minutes=2))
    grouper.on_move(move("b1", symbol="TEST02"), T0 + timedelta(minutes=3))
    stop_at = T0 + timedelta(minutes=5)

    assert sorted(grouper.abandon_pending(stop_at)) == ["a2", "b1"]

    assert sink.calls == []  # 작업 시작(근거 요청·LLM·발행)으로 넘기지 않는다
    assert {s.job_id: s.target["moveAt"] for s in job_log.starts} == {
        "a2": (T0 + timedelta(minutes=2)).isoformat(), "b1": T0.isoformat()}
    assert {f.job_id: (f.status.value, f.failure_reason, f.finished_at) for f in job_log.finishes} == {
        "a2": ("failed", "interrupted_by_restart", stop_at), "b1": ("failed", "interrupted_by_restart", stop_at)}
    assert {e.job_id: len(e.data["moves"]) for e in job_log.of_type(EntryType.MOVES_GROUPED)} == {"a2": 2, "b1": 1}
    assert grouper.next_deadline() is None  # 다시 비워도 같은 줄을 또 남기지 않는다
    assert grouper.abandon_pending(stop_at) == []
    assert len(job_log.starts) == 2


def test_abandoning_with_no_pending_group_records_nothing(parts):
    grouper, _, job_log = parts

    assert grouper.abandon_pending(T0) == []
    assert job_log.starts == [] and job_log.entries == [] and job_log.finishes == []


def test_a_group_already_sent_as_a_job_is_not_recorded_again_at_shutdown(parts):
    grouper, sink, job_log = parts
    grouper.on_move(move("m1"), T0)
    grouper.on_tick(T0 + LONG_WAIT)

    assert grouper.abandon_pending(T0 + LONG_WAIT) == []
    assert [eid for eid, _ in sink.calls] == ["m1"]
    assert len(job_log.of_type(EntryType.MOVES_GROUPED)) == 1 and job_log.finishes == []


def test_runner_shutdown_records_pending_groups_before_closing_and_a_record_failure_does_not_block_it():
    from test_runner import Clock, FakeConsumer, FakeMessage, FakeProducer, FakeStore

    clock, store, order = Clock(T0), FakeStore(), []

    class Closing(FakeConsumer):
        def close(self):
            order.append(("close", len(store.finishes)))  # 닫을 때 이미 실패 기록이 끝나 있어야 한다

    runner = main.BriefingRunner("localhost:9092", None, store, settings=CoreSettings(move_quiet_period=LONG_WAIT), clock=clock, consumer_factory=Closing,
                                 producer_factory=FakeProducer, declare_topics=lambda _bootstrap: None, api_factory=FakeApi)
    runner.open()
    runner._consumer.queue.append(FakeMessage("market.move", move("s1", symbol="TEST01")))
    runner.run_once()
    runner._consumer.queue.append(FakeMessage("market.move", move("s2", symbol="TEST02")))
    runner.run_once()
    assert store.starts == []

    runner._running.clear()
    runner._loop()  # 멈춤 요청 뒤 루프가 끝날 때의 정리만 돈다(SIGTERM 경로)

    assert sorted(f.job_id for f in store.finishes) == ["s1", "s2"]
    assert all(f.failure_reason == "interrupted_by_restart" for f in store.finishes)
    assert order and all(count == 2 for _, count in order)

    # 기록 저장이 끝내 실패해도 종료는 끝까지 간다
    broken = FakeStore()
    broken.start = lambda run: (_ for _ in ()).throw(RuntimeError("db down"))
    runner = main.BriefingRunner("localhost:9092", None, broken, settings=CoreSettings(move_quiet_period=LONG_WAIT), clock=clock, consumer_factory=Closing,
                                 producer_factory=FakeProducer, declare_topics=lambda _bootstrap: None, api_factory=FakeApi)
    runner.open()
    runner._consumer.queue.append(FakeMessage("market.move", move("s3")))
    runner.run_once()
    runner._running.clear()
    runner._loop()
    assert runner._consumer is None  # close까지 갔다
