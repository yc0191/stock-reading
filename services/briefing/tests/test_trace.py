"""추적 ID: 보내는 메시지 헤더와 처리 중 로그 줄에 이벤트 본문의 correlationId가 같은 값으로 붙는지 본다."""
import io
import json
import logging
from datetime import timedelta

import pytest

from briefing import trace
from briefing.config import CoreSettings
from briefing.move_merge import MorningMerge
from briefing.trace import CORRELATION_ID_KEY, CorrelationIdFilter, correlation_context
from fake_job_log import FakeJobLog
from test_move_group import move
from test_move_merge import at
from test_runner import (  # noqa: F401 - setup은 픽스처
    MOVE,
    FakeMessage,
    articles_message,
    completed_message,
    context_message,
    feed,
    history_message,
    move_message,
    setup,
)
from test_weak_research import EDITIONS, MOVE_TWO_WEAK, DailyFlow, Flow, queries, two_weak


@pytest.fixture
def lines(caplog):
    """briefing 로거의 줄을 (correlationId, 메시지)로 모은다. 실제 실행처럼 핸들러에 필터를 건다."""
    caplog.handler.addFilter(CorrelationIdFilter())
    caplog.set_level(logging.DEBUG, logger="briefing")

    def collect():
        return [(getattr(r, CORRELATION_ID_KEY), r.getMessage()) for r in caplog.records if r.name.startswith("briefing")]

    return collect


def test_names_are_the_agreed_ones():
    assert trace.CORRELATION_ID_KEY == "correlationId"
    assert trace.HTTP_HEADER == "X-Correlation-Id"
    assert "%(correlationId)s" in trace.LOG_FORMAT


def test_reading_the_id_from_a_body_and_from_headers():
    assert trace.from_event(json.dumps({"correlationId": "c-1", "payload": {}})) == "c-1"
    assert trace.from_event(b'{"correlationId": "c-1"}') == "c-1"
    for broken in (None, b"", "not json", "[]", '{"correlationId": ""}', '{"correlationId": 1}'):
        assert trace.from_event(broken) is None
    assert trace.from_headers([("other", b"x"), ("correlationId", b"c-2")]) == "c-2"
    assert trace.from_headers(None) is None
    assert trace.from_headers([("correlationId", None)]) is None


def test_outgoing_headers_carry_the_body_value():
    assert trace.kafka_headers({"correlationId": "c-1"}) == [("correlationId", b"c-1")]
    assert trace.kafka_headers({"eventId": "e"}) == []


def test_the_log_format_prints_the_id_inside_a_context_and_blank_outside():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("[%(correlationId)s] %(message)s"))
    handler.addFilter(CorrelationIdFilter())
    logger = logging.getLogger("briefing.trace-test")
    logger.addHandler(handler)
    try:
        with correlation_context("c-1"):
            logger.warning("inside")
            # 값이 없는 문맥은 바깥 값을 지운다. 앞 메시지의 값이 다른 메시지 줄에 찍히면 안 된다.
            with correlation_context(None):
                logger.warning("no id")
        logger.warning("outside")
    finally:
        logger.removeHandler(handler)

    assert stream.getvalue().splitlines() == ["[c-1] inside", "[] no id", "[] outside"]


def test_configure_logging_puts_the_filter_on_the_handlers(monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    trace.configure_logging()
    try:
        assert root.handlers and all(any(isinstance(f, CorrelationIdFilter) for f in h.filters) for h in root.handlers)
        assert "%(correlationId)s" in root.handlers[0].formatter._fmt
    finally:
        for handler in root.handlers:
            handler.close()


def test_every_produced_message_has_the_body_correlation_id_in_its_header(setup):
    make, *_ = setup
    runner, consumer, producer = make()

    feed(runner, consumer, move_message(), completed_message(), history_message(1), context_message(1),
         articles_message(1))

    assert [topic for topic, _, _ in producer.sent] == ["briefing.generated"]
    for (topic, _, body), headers in zip(producer.sent, producer.headers):
        assert headers == [("correlationId", body["correlationId"].encode())], topic


def test_log_lines_while_handling_a_message_carry_its_header_id(setup, lines):
    make, *_ = setup
    runner, consumer, _ = make()

    feed(runner, consumer, FakeMessage("unknown.topic", "{}", headers=[("correlationId", b"from-header")]))

    assert ("from-header", "message from unexpected topic unknown.topic ignored") in lines()


def test_a_message_without_the_header_uses_the_body_id(setup, lines):
    make, *_ = setup
    runner, consumer, _ = make()

    feed(runner, consumer, move_message(), completed_message())

    handled = lines()
    assert handled
    assert {cid for cid, _ in handled} == {MOVE["correlationId"]}


def test_timer_driven_work_logs_with_the_job_id(setup, lines, caplog):
    make, clock, store, _ = setup
    runner, consumer, _ = make()
    feed(runner, consumer, move_message(), completed_message(started_at="2026-10-20T16:00:00+09:00"))
    caplog.clear()

    # 수집 기다림 한도가 지나 실패하는 일은 메시지 처리 밖(제한 시각 처리)에서 일어난다.
    clock.advance(minutes=30)
    runner.run_once()

    assert store.failures
    timed_out = lines()
    assert timed_out
    assert {cid for cid, _ in timed_out} == {MOVE["correlationId"]}



# ---- 메시지 처리 밖(제한 시각 처리)에서 찍는 줄 ----

LIMIT = timedelta(minutes=2)


def test_the_move_total_time_limit_logs_with_the_job_id(lines, caplog):
    flow = Flow([MOVE_TWO_WEAK], [queries("검색어 하나"), queries("검색어 둘")], CoreSettings(research_total_limit=LIMIT))
    flow.to_report()
    caplog.clear()

    # 전체 시간 상한으로 끝내는 일은 메시지 처리 밖에서 일어난다.
    flow.clock.now += LIMIT
    flow.sentences.on_tick(flow.clock.now)

    (briefing,) = flow.briefings()
    expired = lines()
    assert any("reached the total time limit" in message for _, message in expired)
    assert {cid for cid, _ in expired} == {briefing.value["correlationId"]}


@pytest.mark.parametrize("edition", sorted(EDITIONS))
def test_the_daily_total_time_limit_logs_with_the_job_id(lines, caplog, edition):
    flow = DailyFlow(edition, [], [], settings=CoreSettings(research_total_limit=LIMIT))
    flow.report_llm.outputs = [two_weak(flow)]
    flow.query_llm.outputs = [queries("검색어 하나"), queries("검색어 둘")]
    flow.to_first_report()
    caplog.clear()

    flow.now += LIMIT
    flow.sentences.on_tick(flow.now)

    (event,) = flow.published()
    expired = lines()
    assert any("reached the total time limit" in message for _, message in expired)
    assert {cid for cid, _ in expired} == {event.value["correlationId"]}


def test_releasing_a_held_move_runs_with_its_id():
    seen = []
    merge = MorningMerge(lambda message, now: seen.append((json.loads(message)["eventId"], trace.current())),
                         FakeJobLog())
    for event_id, as_of in (("m1", at(9, 3)), ("m2", at(9, 4))):
        event = json.loads(move(event_id, as_of=as_of))
        event["correlationId"] = f"c-{event_id}"
        merge.on_move(json.dumps(event), as_of + timedelta(seconds=5))

    # 합침 기한이 지나 보류한 변동을 넘기는 일은 메시지 처리 밖에서 일어난다.
    merge.on_tick(at(9, 40))

    assert seen == [("m1", "c-m1"), ("m2", "c-m2")]
    assert trace.current() is None


def test_log_lines_while_handling_a_query_result_carry_the_queries_trace_id(setup, lines):
    make, *_ = setup
    runner, consumer, _ = make()
    feed(runner, consumer, move_message(), completed_message(), history_message(1), context_message(1))

    feed(runner, consumer, articles_message(1))

    # 조회 결과로 리포트를 쓰고 발행하는 줄도 그 변동의 추적 ID로 찍힌다.
    published = [cid for cid, message in lines() if message.startswith("published briefing")]
    assert published == [MOVE["correlationId"]]
