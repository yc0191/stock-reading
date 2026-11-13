"""데일리 판을 언제 시작할지(판 시작 알림, 못 받은 자료 표시, 한도 시각, 휴장, 하루 한 번)를 DailyCore로 확인한다."""
import logging
from datetime import datetime, timedelta

from briefing.config import DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_start import STALE_DATA_ITEM
from briefing.job_log import EntryType
from briefing.queries import QueryKind
from fake_news import answer_news
from test_daily_jobs import (  # noqa: F401  core는 테스트가 쓰는 fixture
    KST,
    MORNING_AT,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    TICK_ID,
    Requests,
    core,
    daily_response,
    filings_response,
    published,
    tick,
)

OTHER_ID = "9d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68"
ON = DailySettings(deadline_enabled=True)
MORNING_DEADLINE = datetime(2026, 10, 20, 9, 20, tzinfo=KST)
POST_CLOSE_DEADLINE = datetime(2026, 10, 20, 17, 30, tzinfo=KST)


def finish_morning(c, requests, now):
    job_id = requests.api.of(QueryKind.DAILY)[0].payload["jobId"]
    c.api.reply(daily_response(job_id=job_id), now)
    c.api.reply(filings_response(job_id=job_id), now)
    answer_news(c, requests, now)


def starts(details):
    return [(s.job_kind.value, s.target["rangeEnd"]) for s in details.starts]


# ---- 알림으로 시작 ----

def test_the_sources_the_collector_missed_are_marked_first_in_the_edition(core):
    c, requests, _, details = core()
    c.on_collection_ready(tick(missing=("world-index", "overseas-daily-price", "new-kind")), MORNING_AT)
    finish_morning(c, requests, MORNING_AT)

    [event] = published(requests)
    assert event.value["payload"]["missingItems"][0] == "수집 못 받은 자료: 세계지수, 해외 일봉, new-kind"
    assert event.value["correlationId"] == f"daily-morning-{MORNING_AT.isoformat()}"
    [entry] = details.of_type(EntryType.MISSING_ITEMS)
    assert entry.data["missingItems"][0] == "수집 못 받은 자료: 세계지수, 해외 일봉, new-kind"


def test_a_notice_without_missing_sources_adds_no_item(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    finish_morning(c, requests, MORNING_AT)

    [event] = published(requests)
    assert not any(i.startswith("수집 못 받은 자료") for i in event.value["payload"]["missingItems"])
    assert STALE_DATA_ITEM not in event.value["payload"]["missingItems"]


def test_the_edition_reads_data_up_to_the_notices_time(core):
    c, requests, _, _ = core()
    at = datetime(2026, 10, 20, 16, 31, 5, tzinfo=KST)
    c.on_collection_ready(tick(POST_CLOSE_JOB, at), at + timedelta(seconds=1))

    [request] = requests.api.of(QueryKind.DAILY)
    assert request.payload["asOf"] == "2026-10-20T16:31:05+09:00"
    assert request.payload["since"] == "2026-10-19T16:31:05+09:00"


# ---- 하루 한 번 ----

def test_a_second_notice_for_the_same_day_and_edition_is_ignored_and_logged(core, caplog):
    c, requests, _, details = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    with caplog.at_level(logging.INFO, logger="briefing.daily_start"):
        # 수동 재수집이 낸 알림: eventId가 달라도 같은 날 같은 판이다.
        c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT + timedelta(minutes=20), OTHER_ID),
                              POST_CLOSE_AT + timedelta(minutes=20))

    assert len(requests.api.of(QueryKind.DAILY)) == 1
    assert starts(details) == [("post-close", POST_CLOSE_AT.isoformat())]
    assert "already started; this start is ignored" in caplog.text


def test_the_other_edition_and_the_next_day_still_start(core):
    c, requests, _, details = core()
    c.on_collection_ready(tick(at=MORNING_AT), MORNING_AT)
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, OTHER_ID), POST_CLOSE_AT)
    next_day = MORNING_AT + timedelta(days=1)
    c.on_collection_ready(tick(at=next_day, event_id="00000000-0000-4000-8000-0000000000aa"), next_day)

    assert [k for k, _ in starts(details)] == ["morning", "post-close", "morning"]


def test_an_edition_found_in_the_job_log_is_not_made_again(core, caplog):
    # 재시작한 브리핑: 메모리 기억은 없지만 작업 기록에 그날 그 판이 있다.
    c, requests, _, details = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    again = Requests()
    restarted = DailyCore(again, again.api, None, details)
    with caplog.at_level(logging.INFO, logger="briefing.daily_start"):
        restarted.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, OTHER_ID), POST_CLOSE_AT)

    assert again.sent == []
    assert "already in the job log" in caplog.text


def test_a_notice_for_an_earlier_day_is_not_made_late(core):
    c, requests, _, details = core()
    yesterday = POST_CLOSE_AT - timedelta(days=1)
    c.on_collection_ready(tick(POST_CLOSE_JOB, yesterday), MORNING_AT)

    assert requests.sent == [] and details.starts == []


# ---- 휴장 ----

def test_a_market_closed_notice_makes_no_edition_and_blocks_a_later_notice_that_day(core):
    c, requests, _, details = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, status="market-closed"), POST_CLOSE_AT)
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, OTHER_ID), POST_CLOSE_AT)

    assert requests.sent == [] and details.starts == []


def test_a_market_closed_day_recorded_in_the_db_stays_closed_after_a_restart(core):
    # 휴장 알림은 이미 읽은 것으로 확정돼 재시작 뒤 다시 오지 않는다. DB의 휴장 기록만 남는다.
    c, requests, _, details = core(settings=ON)
    closed_at = MORNING_AT.replace(hour=9, minute=10)
    c.on_collection_ready(tick(at=closed_at, status="market-closed"), closed_at)
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, OTHER_ID, status="market-closed"), POST_CLOSE_AT)
    again = Requests()
    restarted = DailyCore(again, again.api, None, details, ON)
    restarted.check_deadlines(MORNING_DEADLINE.replace(hour=10, minute=0))
    restarted.check_deadlines(POST_CLOSE_DEADLINE.replace(hour=18, minute=0))

    assert again.sent == [] and requests.sent == [] and details.starts == []


def test_a_market_closed_record_blocks_a_later_notice_after_a_restart(core):
    c, _, _, details = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, status="market-closed"), POST_CLOSE_AT)
    again = Requests()
    DailyCore(again, again.api, None, details).on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT, OTHER_ID), POST_CLOSE_AT)

    assert again.sent == [] and details.starts == []


def test_an_unreadable_market_closed_record_still_makes_the_edition(core, caplog):
    # 읽지 못하면 판을 빠뜨리는 것보다 만드는 편을 택한다(같은 날 같은 판 확인과 같은 규칙).
    c, requests, _, details = core(settings=ON)
    details.fail_reads = True
    with caplog.at_level(logging.WARNING, logger="briefing.daily_start"):
        c.check_deadlines(MORNING_DEADLINE)

    assert starts(details) == [("morning", MORNING_DEADLINE.isoformat())]
    assert "could not read whether the market was closed" in caplog.text


# ---- 한도 시각 ----

def test_the_deadline_starts_the_edition_with_the_stale_mark_only_once(core):
    c, requests, _, details = core(settings=ON)
    c.check_deadlines(MORNING_DEADLINE - timedelta(seconds=1))
    assert requests.sent == []

    c.check_deadlines(MORNING_DEADLINE)
    c.check_deadlines(MORNING_DEADLINE + timedelta(minutes=5))
    assert starts(details) == [("morning", MORNING_DEADLINE.isoformat())]
    finish_morning(c, requests, MORNING_DEADLINE)
    [event] = published(requests)
    assert event.value["payload"]["missingItems"][0] == STALE_DATA_ITEM
    assert event.value["correlationId"] == f"daily-morning-deadline-{MORNING_DEADLINE.isoformat()}"


def test_an_edition_started_by_its_notice_is_not_started_again_at_the_deadline(core):
    c, requests, _, details = core(settings=ON)
    c.check_deadlines(MORNING_DEADLINE - timedelta(minutes=10))
    c.on_collection_ready(tick(at=MORNING_DEADLINE - timedelta(minutes=10)), MORNING_DEADLINE - timedelta(minutes=10))
    c.check_deadlines(MORNING_DEADLINE)

    assert [k for k, _ in starts(details)] == ["morning"]


def test_a_market_closed_notice_turns_off_only_that_editions_deadline(core):
    c, requests, _, details = core(settings=ON)
    c.on_collection_ready(tick(at=MORNING_AT.replace(hour=9, minute=10), status="market-closed"),
                          MORNING_AT.replace(hour=9, minute=10))
    c.check_deadlines(POST_CLOSE_DEADLINE)

    assert starts(details) == [("post-close", POST_CLOSE_DEADLINE.isoformat())]


def test_the_deadline_is_off_by_default_and_on_weekends(core):
    off, off_requests, _, _ = core()
    off.check_deadlines(POST_CLOSE_DEADLINE)
    weekend, weekend_requests, _, _ = core(settings=ON)
    weekend.check_deadlines(POST_CLOSE_DEADLINE + timedelta(days=4))  # 토요일

    assert off_requests.sent == [] and weekend_requests.sent == []


def test_the_default_deadlines_come_after_the_collectors_notice_times():
    settings = DailySettings()
    assert (settings.morning_deadline.hour, settings.morning_deadline.minute) == (9, 20)
    assert (settings.post_close_deadline.hour, settings.post_close_deadline.minute) == (17, 30)
    assert settings.morning_deadline > settings.morning_merge_end
    assert settings.deadline_enabled is False


def test_the_notice_id_is_the_edition_job_id(core):
    # 알림의 eventId가 판 작업 id다(요청의 jobId와 결과 이벤트 id의 바탕).
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    [request] = requests.api.of(QueryKind.DAILY)
    assert request.payload["jobId"] == TICK_ID
