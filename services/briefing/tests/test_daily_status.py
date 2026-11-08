"""마감 후 판 본문 끝의 서버 상태 절을 가짜 요청 출구·가짜 저장소·가짜 LLM으로 돌려 확인한다."""
import json
from datetime import timedelta

import pytest

from briefing.config import DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_report import DailyLlmWriter
from briefing.job_log import EntryType
from briefing.status import CollectionRecord, JobRunRow
from contracts import validate
from test_daily_jobs import (
    KST,
    MORNING_AT,
    MORNING_JOB,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    TICK_ID,
    Details,
    FakeLlm,
    Requests,
    EXCHANGE,
    MARKET,
    WORLD_C,
    daily_response,
    etf,
    filings_response,
    good_output,
    tick,
)

ZERO = {key: 0 for key in (
    "TOKEN_ISSUE_FAILED", "HTTP_ERROR", "RESULT_CODE_FAILED", "EMPTY_PRICE", "PRICE_PARSE_FAILED",
    "CONNECTION_FAILED", "TIMEOUT", "FIELD_MISMATCH", "TICK_SKIPPED_RATE_LIMITED", "TICK_SKIPPED_SUPERSEDED",
    "TICK_SKIPPED_OTHER")}
OPERATIONS = {
    "collections": [
        {"kind": "pre-open", "firstReceivedAt": "2026-10-20T08:30:02+09:00",
         "lastReceivedAt": "2026-10-20T08:30:09+09:00", "receivedCount": 4},
        {"kind": "quote-1s", "firstReceivedAt": "2026-10-20T09:00:01+09:00",
         "lastReceivedAt": "2026-10-20T15:30:00+09:00", "receivedCount": 70200},
        {"kind": "intraday", "firstReceivedAt": None, "lastReceivedAt": None, "receivedCount": 0},
        {"kind": "post-close", "firstReceivedAt": "2026-10-20T16:30:00+09:00",
         "lastReceivedAt": "2026-10-20T16:30:05+09:00", "receivedCount": 3},
    ],
    "quoteFailures": {**ZERO, "EMPTY_PRICE": 3, "TICK_SKIPPED_RATE_LIMITED": 5},
    "droppedMessages": [],
}


class FakeStatus:
    """StatusPort 가짜. None이면 조회 실패, 예외 객체면 그 예외를 던진다."""

    def __init__(self, collections=(), runs=()):
        self.collections = collections if collections is None or isinstance(collections, Exception) \
            else list(collections)
        self.runs = runs if runs is None or isinstance(runs, Exception) else list(runs)
        self.queries = []
        self.saved = []

    def save_collection(self, record):
        self.saved.append(record)
        return True

    def collections_between(self, since, until):
        self.queries.append(("collections", since, until))
        if isinstance(self.collections, Exception):
            raise self.collections
        return self.collections

    def job_runs_between(self, since, until):
        self.queries.append(("runs", since, until))
        if isinstance(self.runs, Exception):
            raise self.runs
        return self.runs


def completed(n, job_type="collect-news", incomplete=()):
    at = POST_CLOSE_AT - timedelta(hours=5, minutes=n)
    return CollectionRecord(f"e{n}", job_type, at, at, at, 4, tuple(incomplete), None, "{}")


def morning_run(status="succeeded", by="llm"):
    return JobRunRow("morning-job", "morning", status, None, by, MORNING_AT)


def with_operations(operations=OPERATIONS, **kwargs):
    kwargs.setdefault("etfs", [etf(fetched="2026-10-20T16:30:02+09:00")])
    kwargs.setdefault("background", [WORLD_C, EXCHANGE, {**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00"}])
    message = json.loads(daily_response(**kwargs))
    message["schemaVersion"] = 2
    if operations is not None:
        message["payload"]["operations"] = operations
    return json.dumps(message)


@pytest.fixture
def make():
    def build(status, llm=None, edition_job=POST_CLOSE_JOB, at=POST_CLOSE_AT, answer="default"):
        requests, details = Requests(), Details()
        settings = DailySettings()
        writer = DailyLlmWriter(llm, settings.llm_max_attempts) if llm is not None else None
        core = DailyCore(requests, writer, details, settings, status=status)
        edition = "post-close" if edition_job == POST_CLOSE_JOB else "morning"
        core.on_schedule_tick(tick(edition_job, at), at, edition)
        core.on_daily_data_responded(with_operations() if answer == "default" else answer, at)
        return core, requests, details
    return build


def only_event(requests):
    [event] = requests.topic("briefing.daily.generated")
    assert validate("briefing.daily.generated.v1.schema.json", event.value) == []
    return event.value["payload"]


def status_lines(text):
    return text.split("──────── 서버 상태")[1].split("\n")[1:]


# ---- 요청 ----

def test_the_post_close_request_asks_for_the_operations_but_the_morning_request_does_not(make):
    _, requests, _ = make(FakeStatus())
    [post] = requests.topic("market.daily.requested")
    assert post.value["payload"]["includeOperations"] is True
    assert validate("market.daily.requested.v1.schema.json", post.value) == []
    _, morning_requests, _ = make(FakeStatus(), edition_job=MORNING_JOB, at=MORNING_AT)
    [morning] = morning_requests.topic("market.daily.requested")
    assert "includeOperations" not in morning.value["payload"]


def test_without_a_status_store_nothing_is_asked_or_added(make):
    _, requests, _ = make(None)
    assert "includeOperations" not in requests.topic("market.daily.requested")[0].value["payload"]
    assert "서버 상태" not in only_event(requests)["text"]


# ---- 본문 끝 ----

def test_the_four_lines_come_after_the_disclaimer_at_the_very_end_of_a_rule_based_body(make):
    status = FakeStatus([completed(1), completed(2, incomplete=["TEST01"]), completed(3, "research-news")],
                        [morning_run()])
    _, requests, details = make(status)
    payload = only_event(requests)
    text = payload["text"]
    assert text.index("투자 조언") < text.index("──────── 서버 상태 (10-19 17:00 ~ 10-20 17:00)")
    assert status_lines(text) == [
        "- 수집: 장 시작 전 08:30 · 1초 현재가 09:00~15:30 · 장중 없음 · 장 마감 후 16:30",
        "- 시세: 받음 70,200 · 실패 8(0.01%) [빈 현재가 3 · 호출 상한 건너뜀 5]",
        "- 뉴스: 정기 수집 2회 · 일부 종목 빠짐 1회 · 기사 8건 · 재검색 1회",
        "- 브리핑: 급변동 0건(성공 0 · 실패 0 · 진행 중 0 · 규칙 기반 0) · 아침 판 성공",
        "- 주의: 호출 상한 건너뜀 5 · 마감 후 판 규칙 기반",
    ]
    assert payload["generatedBy"] == "rule-based"
    [result] = details.results()
    assert result.final_text == text  # 저장하는 최종 본문도 같다


def test_the_range_given_to_the_store_is_the_edition_range(make):
    status = FakeStatus()
    make(status)
    assert status.queries == [("collections", POST_CLOSE_AT - timedelta(days=1), POST_CLOSE_AT),
                              ("runs", POST_CLOSE_AT - timedelta(days=1), POST_CLOSE_AT)]


def test_the_lines_are_also_added_to_an_llm_body_and_the_attention_has_no_rule_based_item(make):
    llm = FakeLlm(good_output("B1"))
    status = FakeStatus([completed(1)], [morning_run()])
    _, requests, _ = make(status, llm=llm)
    payload = only_event(requests)
    assert payload["generatedBy"] == "llm" and "요약:" in payload["text"]
    assert payload["text"].index("투자 조언") < payload["text"].index("서버 상태")
    assert status_lines(payload["text"])[-1] == "- 주의: 호출 상한 건너뜀 5"
    # 숫자는 코드가 세므로 LLM 입력에 서버 상태가 들어가지 않는다.
    assert all("서버 상태" not in call[1] for call in llm.calls)


def test_the_current_post_close_job_is_not_counted_in_the_briefing_line(make):
    current = JobRunRow(TICK_ID, "post-close", "running", None, None, POST_CLOSE_AT)
    stuck = JobRunRow("old", "move", "running", None, "llm", POST_CLOSE_AT - timedelta(hours=1))
    _, requests, _ = make(FakeStatus([], [morning_run(), current, stuck]))
    lines = status_lines(only_event(requests)["text"])
    assert lines[3] == "- 브리핑: 급변동 1건(성공 0 · 실패 0 · 진행 중 1 · 규칙 기반 0) · 아침 판 성공"
    assert "오래 진행 중인 작업 1건" in lines[4]


# ---- 자료 없음 ----

def test_without_operations_the_first_two_lines_say_no_data_and_the_part_is_recorded_as_missing(make):
    _, requests, details = make(FakeStatus([], [morning_run()]), answer=with_operations(None))
    payload = only_event(requests)
    lines = status_lines(payload["text"])
    assert lines[0] == "- 수집: 자료 없음" and lines[1] == "- 시세: 자료 없음"
    assert lines[2].startswith("- 뉴스: 정기 수집 0회")  # 나머지 줄은 그대로 나간다
    assert "서버 상태: 수집·시세(시세처리기 운영 상태 없음)" in payload["missingItems"]
    [missing] = details.of_type(EntryType.MISSING_ITEMS)
    assert "서버 상태: 수집·시세(시세처리기 운영 상태 없음)" in missing.data["missingItems"]


def test_a_malformed_operations_field_is_treated_as_no_operations_and_the_edition_still_goes_out(make):
    _, requests, _ = make(FakeStatus([], [morning_run()]), answer=with_operations({"collections": "x"}))
    assert status_lines(only_event(requests)["text"])[0] == "- 수집: 자료 없음"


def test_when_the_data_response_never_comes_the_edition_still_goes_out_with_no_data_lines():
    requests, details = Requests(), Details()
    core = DailyCore(requests, None, details, DailySettings(), status=FakeStatus([], [morning_run()]))
    core.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    for n in (1, 2, 3):
        core.on_tick(POST_CLOSE_AT + timedelta(seconds=10 * n))
    payload = only_event(requests)
    assert status_lines(payload["text"])[0] == "- 수집: 자료 없음"
    assert "서버 상태: 수집·시세(시세처리기 운영 상태 없음)" in payload["missingItems"]


def test_unreadable_news_and_job_records_say_no_data_for_their_lines(make):
    _, requests, _ = make(FakeStatus(None, None))
    payload = only_event(requests)
    lines = status_lines(payload["text"])
    assert lines[2] == "- 뉴스: 자료 없음" and lines[3] == "- 브리핑: 자료 없음"
    assert "서버 상태: 뉴스(수집 완료 기록 조회 실패)" in payload["missingItems"]
    assert "서버 상태: 브리핑(작업 기록 조회 실패)" in payload["missingItems"]


def test_a_store_that_raises_does_not_stop_the_edition(make):
    _, requests, _ = make(FakeStatus(RuntimeError("password=hunter2"), []))
    payload = only_event(requests)
    assert "자료 없음" in payload["text"] and "hunter2" not in payload["text"]
    assert any("서버 상태" in item for item in payload["missingItems"])


# ---- 아침 판 ----

def test_the_morning_edition_is_unchanged_and_never_reads_the_store(make):
    status = FakeStatus()
    core, requests, _ = make(status, edition_job=MORNING_JOB, at=MORNING_AT, answer=daily_response())
    # 아침 판은 공시까지 받아야 나간다.
    core.on_filings_responded(filings_response(), MORNING_AT)
    assert "서버 상태" not in only_event(requests)["text"]
    assert status.queries == []
