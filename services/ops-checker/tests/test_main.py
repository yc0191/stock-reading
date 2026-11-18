"""Kafka 실행부의 알림 처리 루프(_handle) 시험. 가짜 checker 와 stop 이벤트만 쓰고 Kafka는 쓰지 않는다."""
import threading

from ops_checker import main
from ops_checker.flow import MalformedNotification


class FakeChecker:
    def __init__(self, *outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def handle(self, value):
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_success_is_done_after_one_call():
    checker = FakeChecker("ok")
    assert main._handle(checker, b"x", threading.Event(), 0) is True
    assert checker.calls == 1


def test_malformed_notification_is_skipped_without_retry():
    checker = FakeChecker(MalformedNotification("bad"))
    assert main._handle(checker, b"x", threading.Event(), 0) is True
    assert checker.calls == 1


def test_other_value_error_is_retried_not_skipped():
    checker = FakeChecker(ValueError("from processing"), "ok")
    assert main._handle(checker, b"x", threading.Event(), 0) is True
    assert checker.calls == 2


def test_gives_up_after_three_failures_and_still_commits():
    checker = FakeChecker(*[RuntimeError("db")] * 5)
    assert main._handle(checker, b"x", threading.Event(), 0) is True
    assert checker.calls == main.ATTEMPTS == 3


def test_stop_request_during_retry_wait_does_not_commit():
    stop = threading.Event()
    stop.set()
    checker = FakeChecker(RuntimeError("db"))
    assert main._handle(checker, b"x", stop, 0) is False
    assert checker.calls == 1
