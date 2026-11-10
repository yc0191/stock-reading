"""서버 상태 절(status.py): 수집 완료 읽기, 운영 상태 읽기, 4줄과 주의 한 줄 만들기. 저장소와 Kafka는 부르지 않는다."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.status import (
    CollectionRecord,
    CollectionStat,
    JobRunRow,
    Operations,
    build_status_report,
    parse_collection,
    parse_operations,
)

KST = timezone(timedelta(hours=9))
SINCE = datetime(2026, 10, 19, 17, 0, tzinfo=KST)
AS_OF = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
CURRENT = "post-close-job"
ZERO_FAILURES = {key: 0 for key in (
    "TOKEN_ISSUE_FAILED", "HTTP_ERROR", "RESULT_CODE_FAILED", "EMPTY_PRICE", "PRICE_PARSE_FAILED",
    "CONNECTION_FAILED", "TIMEOUT", "FIELD_MISMATCH", "TICK_SKIPPED_RATE_LIMITED", "TICK_SKIPPED_SUPERSEDED",
    "TICK_SKIPPED_OTHER")}


def at(hour, minute=0, day=20):
    return datetime(2026, 10, day, hour, minute, tzinfo=KST)


def stat(kind, first=None, last=None, count=0):
    return CollectionStat(kind, first, last, count)


def full_collections():
    return (stat("pre-open", at(8, 30), at(8, 30), 4), stat("quote-1s", at(9, 0), at(15, 30), 1000),
            stat("intraday", at(9, 0), at(15, 30), 216), stat("post-close", at(16, 30), at(16, 30), 3))


def ops(failures=None, dropped=0, collections=None):
    return Operations(full_collections() if collections is None else collections,
                      {**ZERO_FAILURES, **(failures or {})}, dropped)


def completed(n, job_type="collect-news", incomplete=(), published=3, research=None, finished=None):
    finished = finished or at(10, n % 60)
    return CollectionRecord(f"event-{n}-{job_type}", job_type, finished, finished, finished, published,
                            tuple(incomplete), research, "{}")


def run(n, kind="move", status="succeeded", reason=None, by="llm", started=None):
    return JobRunRow(f"job-{n}", kind, status, reason, by, started or at(10, n % 60))


def report(operations="default", collections="default", runs="default", rule_based=False, current_research=0):
    return build_status_report(
        range_start=SINCE, as_of=AS_OF, current_job_id=CURRENT, current_rule_based=rule_based,
        operations=ops() if operations == "default" else operations,
        collections=[completed(1)] if collections == "default" else collections,
        runs=[run(0, "morning")] if runs == "default" else runs,
        stuck_after=timedelta(minutes=30), current_research_count=current_research)


def lines(r):
    return r.text.split("\n")


# ---- 수집 완료 읽기 ----

def collection_event(**payload):
    body = {"jobType": "collect-news", "scheduledFor": "2026-10-20T16:15:00+09:00",
            "startedAt": "2026-10-20T16:15:01+09:00", "finishedAt": "2026-10-20T16:15:42+09:00",
            "publishedCount": 12, "incompleteSymbols": ["TEST02"], **payload}
    return json.dumps({"eventId": "b3f1a6d2-84c7-4e0b-9a52-1d7c3e8f6a40", "eventType": "collection.completed",
                       "schemaVersion": 1, "occurredAt": "2026-10-20T16:15:42+09:00",
                       "producedAt": "2026-10-20T16:15:42+09:00", "correlationId": "c", "payload": body})


def test_a_regular_collection_is_read_with_its_raw_text_unchanged():
    raw = collection_event()
    record = parse_collection(raw.encode())
    assert record.event_id == "b3f1a6d2-84c7-4e0b-9a52-1d7c3e8f6a40"
    assert (record.job_type, record.published_count, record.incomplete_symbols) == ("collect-news", 12, ("TEST02",))
    assert record.finished_at == datetime(2026, 10, 20, 16, 15, 42, tzinfo=KST)
    assert record.raw == raw and not record.is_research()


def test_a_research_collection_is_told_apart_by_its_job_type_or_job_id():
    by_type = parse_collection(collection_event(jobType="research-news"))
    by_id = parse_collection(collection_event(researchJobId="9b1c0a44-3f5e-4c1d-8a7b-2d6e0f9a1c33"))
    assert by_type.is_research() and by_id.is_research()
    assert by_id.research_job_id == "9b1c0a44-3f5e-4c1d-8a7b-2d6e0f9a1c33"


@pytest.mark.parametrize("message", ["{not json", json.dumps({"eventType": "collection.completed"}),
                                     json.dumps({"eventType": "other", "eventId": "x", "payload": {}}),
                                     collection_event(publishedCount="many")])
def test_a_malformed_collection_is_rejected(message):
    with pytest.raises(ValueError):
        parse_collection(message)


# ---- 운영 상태 읽기 ----

def operations_payload():
    return {"collections": [{"kind": "quote-1s", "firstReceivedAt": "2026-10-20T09:00:01+09:00",
                             "lastReceivedAt": "2026-10-20T15:30:00+09:00", "receivedCount": 70200},
                            {"kind": "post-close", "firstReceivedAt": None, "lastReceivedAt": None,
                             "receivedCount": 0}],
            "quoteFailures": {"EMPTY_PRICE": 3}, "droppedMessages": [{"topic": "a", "count": 1},
                                                                    {"topic": "b", "count": 2}]}


def test_operations_are_read_and_dropped_messages_are_summed():
    result = parse_operations(operations_payload())
    assert result.collections[0].received_count == 70200 and result.collections[1].first_received_at is None
    assert result.quote_failures == {"EMPTY_PRICE": 3} and result.dropped_messages == 3


@pytest.mark.parametrize("value", [None, "text", {}, {"collections": [{"kind": "x"}], "quoteFailures": {},
                                                      "droppedMessages": []}])
def test_missing_or_malformed_operations_become_none(value):
    assert parse_operations(value) is None


# ---- 4줄 ----

def test_all_four_lines_and_a_clean_attention_line():
    r = report(operations=ops({"EMPTY_PRICE": 344, "FIELD_MISMATCH": 295}), runs=[
        run(1), run(2), run(3, status="failed", reason="news_no_response"), run(4, status="running", started=at(16, 50)),
        run(5, "morning"),
    ], collections=[completed(1), completed(2, incomplete=["TEST01"], published=5),
                    completed(3, "research-news", published=7)])
    assert r.missing == ()
    assert lines(r) == [
        "──────── 서버 상태 (10-19 17:00 ~ 10-20 17:00) ────────",
        "- 수집: 장 시작 전 08:30 · 1초 현재가 09:00~15:30 · 장중 09:00~15:30 · 장 마감 후 16:30",
        "- 시세: 받음 1,000 · 실패 639(38.99%) [빈 현재가 344 · 칸 불일치 295]",
        "- 뉴스: 정기 수집 2회 · 일부 종목 빠짐 1회 · 기사 8건 · 재검색 1회",
        "- 브리핑: 급변동 4건(성공 2 · 실패 1[기사 무응답 1] · 진행 중 1 · 규칙 기반 0) · 아침 판 성공",
        "- 주의: 급변동 리포트 실패 1[기사 무응답 1]",
    ]


def test_the_research_this_edition_sent_is_added_to_the_research_finished_in_the_range():
    r = report(collections=[completed(1), completed(2, "research-news")], current_research=2)
    assert lines(r)[3] == "- 뉴스: 정기 수집 1회 · 일부 종목 빠짐 0회 · 기사 3건 · 재검색 3회"


def test_a_collection_that_did_not_come_is_written_as_none():
    r = report(operations=ops(collections=(stat("pre-open"), stat("quote-1s", at(9), at(15, 30), 5),
                                           stat("intraday", None, None, 0))))
    assert lines(r)[1] == "- 수집: 장 시작 전 없음 · 1초 현재가 09:00~15:30 · 장중 없음 · 장 마감 후 없음"


def test_a_collection_over_two_days_shows_the_dates():
    r = report(operations=ops(collections=(stat("quote-1s", at(9, day=19), at(15, 30), 5),)))
    assert "1초 현재가 10-19 09:00~10-20 15:30" in lines(r)[1]


def test_the_quote_line_has_no_ratio_without_failures_and_orders_kinds_by_the_contract():
    assert lines(report(operations=ops()))[2] == "- 시세: 받음 1,000 · 실패 0"
    r = report(operations=ops({"TIMEOUT": 1, "TOKEN_ISSUE_FAILED": 2, "TICK_SKIPPED_RATE_LIMITED": 3,
                               "TICK_SKIPPED_SUPERSEDED": 4}))
    assert lines(r)[2].endswith("[토큰 발급 실패 2 · 시간 초과 1 · 호출 상한 건너뜀 3 · 새 tick에 밀림 4]")
    assert "(0.99%)" in lines(r)[2]


def test_an_unknown_failure_kind_is_shown_by_its_contract_name():
    r = report(operations=Operations(full_collections(), {**ZERO_FAILURES, "NEW_KIND": 2}, 0))
    assert lines(r)[2].endswith("[NEW_KIND 2]")


def test_no_quotes_and_no_failures_has_no_ratio():
    r = report(operations=ops(collections=(stat("quote-1s"),)))
    assert lines(r)[2] == "- 시세: 받음 0 · 실패 0"


def test_no_operations_writes_no_data_for_the_first_two_lines_and_names_the_missing_part():
    r = report(operations=None)
    assert lines(r)[1] == "- 수집: 자료 없음" and lines(r)[2] == "- 시세: 자료 없음"
    assert r.missing == ("서버 상태: 수집·시세(시세처리기 운영 상태 없음)",)
    assert "시세처리기" not in lines(r)[5]


def test_no_collections_is_zero_not_missing():
    r = report(collections=[])
    assert lines(r)[3] == "- 뉴스: 정기 수집 0회 · 일부 종목 빠짐 0회 · 기사 0건 · 재검색 0회" and r.missing == ()


def test_unreadable_stores_write_no_data_and_name_the_missing_parts():
    r = report(collections=None, runs=None)
    assert lines(r)[3] == "- 뉴스: 자료 없음" and lines(r)[4] == "- 브리핑: 자료 없음"
    assert r.missing == ("서버 상태: 뉴스(수집 완료 기록 조회 실패)", "서버 상태: 브리핑(작업 기록 조회 실패)")


def test_an_unreadable_store_is_also_raised_in_the_attention_line():
    assert lines(report(runs=None))[5] == "- 주의: 브리핑 기록 읽기 실패"
    assert lines(report(collections=None))[5] == "- 주의: 뉴스 수집 기록 읽기 실패"
    both = lines(report(collections=None, runs=None))[5]
    assert both == "- 주의: 뉴스 수집 기록 읽기 실패 · 브리핑 기록 읽기 실패"


def test_the_old_tick_skip_has_its_own_name_and_is_not_an_attention_item():
    r = report(operations=ops({"TICK_SKIPPED_OTHER": 3}))
    assert lines(r)[2].endswith("[오래된 tick 건너뜀 3]")
    assert lines(r)[5] == "- 주의: 없음"


def test_no_moves_and_the_current_job_itself_is_not_counted():
    r = report(runs=[run(0, "morning"), JobRunRow(CURRENT, "post-close", "running", None, None, at(17))])
    assert lines(r)[4] == "- 브리핑: 급변동 0건(성공 0 · 실패 0 · 진행 중 0 · 규칙 기반 0) · 아침 판 성공"
    assert lines(r)[5] == "- 주의: 없음"


def test_failure_reasons_are_counted_per_reason_and_unknown_reasons_stay_as_is():
    runs = [run(0, "morning"), run(1, status="failed", reason="llm_error"), run(2, status="failed", reason="llm_error"),
            run(3, status="failed", reason="weird"), run(4, status="failed", reason=None)]
    r = report(runs=runs)
    assert "실패 4[LLM 오류 2 · unknown 1 · weird 1]" in lines(r)[4]


def test_the_morning_edition_result_is_shown():
    assert lines(report(runs=[run(0, "morning", status="failed", reason="llm_error")]))[4].endswith("아침 판 실패")
    assert lines(report(runs=[run(0, "morning", status="running")]))[4].endswith("아침 판 진행 중")
    assert lines(report(runs=[run(0, "morning", by="rule-based")]))[4].endswith("아침 판 성공(규칙 기반)")
    assert lines(report(runs=[]))[4].endswith("아침 판 기록 없음")


def test_the_latest_morning_edition_wins():
    runs = [run(0, "morning", status="failed", reason="llm_error", started=at(8, 0)),
            run(1, "morning", status="succeeded", started=at(8, 45))]
    assert lines(report(runs=runs))[4].endswith("아침 판 성공")


# ---- 주의 ----

def attention(**kwargs):
    return lines(report(**kwargs))[5]


def test_each_attention_target_is_reported_alone():
    assert attention(operations=ops(dropped=3)) == "- 주의: 시세처리기 버린 메시지 3"
    assert attention(operations=ops({"RESULT_CODE_FAILED": 2})) == "- 주의: 결과 코드 실패 2"
    assert attention(operations=ops({"PRICE_PARSE_FAILED": 1})) == "- 주의: 가격 해석 실패 1"
    assert attention(operations=ops({"TICK_SKIPPED_RATE_LIMITED": 99})) == "- 주의: 호출 상한 건너뜀 99"
    assert attention(operations=ops({"TOKEN_ISSUE_FAILED": 1})) == "- 주의: 토큰 발급 실패 1"
    assert attention(rule_based=True) == "- 주의: 마감 후 판 규칙 기반"
    assert attention(runs=[run(0, "morning"), run(1, by="rule-based")]) == "- 주의: 규칙 기반 급변동 1건"
    assert attention(runs=[run(0, "morning", by="rule-based")]) == "- 주의: 아침 판 규칙 기반"
    assert attention(runs=[run(0, "morning"), run(1, status="failed", reason="llm_error")]) \
        == "- 주의: 급변동 리포트 실패 1[LLM 오류 1]"
    assert attention(runs=[run(0, "morning", status="failed", reason="llm_error")]) == "- 주의: 아침 판 실패"
    assert attention(runs=[]) == "- 주의: 아침 판 기록 없음"


def test_a_job_running_longer_than_the_limit_is_stuck_but_a_recent_one_is_not():
    old = run(1, status="running", started=AS_OF - timedelta(minutes=31))
    edge = run(2, status="running", started=AS_OF - timedelta(minutes=30))
    recent = run(3, status="running", started=AS_OF - timedelta(minutes=5))
    assert attention(runs=[run(0, "morning"), old, edge, recent]) == "- 주의: 오래 진행 중인 작업 1건"


def test_environment_problems_are_not_attention_targets():
    env = {"HTTP_ERROR": 5, "CONNECTION_FAILED": 4, "TIMEOUT": 3, "EMPTY_PRICE": 2, "FIELD_MISMATCH": 1,
           "TICK_SKIPPED_SUPERSEDED": 9, "TICK_SKIPPED_OTHER": 8}
    r = report(operations=ops(env), collections=[completed(1, incomplete=["TEST01"])])
    assert lines(r)[5] == "- 주의: 없음"


def test_several_attention_targets_are_joined_in_one_line():
    r = report(operations=ops({"TOKEN_ISSUE_FAILED": 1}, dropped=2), runs=[run(0, "morning")])
    assert lines(r)[5] == "- 주의: 시세처리기 버린 메시지 2 · 토큰 발급 실패 1"
