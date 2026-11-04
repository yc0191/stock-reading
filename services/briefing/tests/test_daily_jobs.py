"""데일리 판 작업(DailyCore)을 가짜 요청 출구·실패 기록·LLM으로 돌린다. 실제 Kafka·DB·LLM은 부르지 않는다."""
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import DailySettings
from briefing.daily_jobs import (
    REASON_LLM_REJECTED,
    REASON_MISSING_ITEMS,
    REASON_RESENT,
    STAGE_COMPOSE,
    STAGE_DAILY_DATA,
    STAGE_FILINGS,
    STAGE_LLM,
    DailyCore,
    DailyFailureRecord,
    DailyFailureRecordPort,
    DiscardDailyFailureRecords,
    range_start,
)
from briefing.daily_report import (
    GENERATED_BY_LLM,
    GENERATED_BY_RULE,
    ITEM_ETFS,
    ITEM_FILINGS,
    ITEM_KR_REVIEW,
    ITEM_KR_TODAY,
    ITEM_US_INDEX,
    NO_DATA,
    DailyLlmWriter,
)
from briefing.llm import LlmCallError
from briefing.ports import FailureReason
from contracts import validate

KST = timezone(timedelta(hours=9))
TICK_ID = "8d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68"
MORNING_AT = datetime(2026, 10, 20, 8, 45, tzinfo=KST)    # 화요일
POST_CLOSE_AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
MORNING_JOB = "daily-briefing-morning"
POST_CLOSE_JOB = "daily-briefing-post-close"


# ---- 가짜 ----

class Requests:
    def __init__(self):
        self.sent = []

    def send(self, event):
        self.sent.append(event)

    def topic(self, name):
        return [e for e in self.sent if e.topic == name]


class Failures:
    def __init__(self):
        self.records = []

    def record(self, failure):
        self.records.append(failure)


class Details:
    """DailyFailureRecordPort 의 가짜. 넘어온 기록 값을 그대로 모은다."""

    def __init__(self):
        self.records: list[DailyFailureRecord] = []

    def record(self, record):
        self.records.append(record)

    def by_reason(self, reason):
        return [r for r in self.records if r.reason == reason]


class FakeLlm:
    def __init__(self, *outputs):
        self.outputs = list(outputs)
        self.calls = []

    def complete(self, system, user, output_schema):
        self.calls.append((system, user, output_schema))
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return output


def tick(job_type=MORNING_JOB, at=MORNING_AT, event_id=TICK_ID):
    return json.dumps({
        "eventId": event_id, "eventType": "schedule.tick", "schemaVersion": 1,
        "occurredAt": at.isoformat(), "producedAt": at.isoformat(),
        "correlationId": f"{job_type}-{at.isoformat()}",
        "payload": {"jobType": job_type, "scheduledFor": at.isoformat()},
    })


def envelope(event_type, payload):
    return json.dumps({
        "eventId": "00000000-0000-4000-8000-000000000009", "eventType": event_type, "schemaVersion": 1,
        "occurredAt": "2026-10-20T08:45:01+09:00", "producedAt": "2026-10-20T08:45:01+09:00",
        "correlationId": "c", "payload": payload,
    })


def etf(symbol="TEST01", tickers=("TESTA", "FAKEB"), fetched="2026-10-19T16:30:02+09:00", rate="-1.50"):
    return {"symbol": symbol, "date": "20261019", "close": "9850", "prevClose": "10000", "rate": rate,
            "fetchedAt": fetched, "tickers": list(tickers), "tickersTruncated": False}


WORLD_C = {"apiType": "world-index", "target": "C", "status": "ok", "response": "{\"raw\": \"TEST\"}",
           "fetchedAt": "2026-10-20T08:30:02+09:00",
           "indexRates": [{"code": "TESTIX", "name": "TEST INDEX", "businessDay": "20261019", "close": "100.00",
                           "prevBusinessDay": "20261016", "prevClose": "98.00", "rate": "2.04"}]}
EXCHANGE = {"apiType": "exchange-rate-summary", "target": None, "status": "ok", "response": "{\"fx\": \"TEST\"}",
            "fetchedAt": "2026-10-20T08:30:03+09:00"}
MARKET = {"apiType": "market-summary", "target": None, "status": "ok", "response": "{\"kr\": \"TEST\"}",
          "fetchedAt": "2026-10-19T16:30:05+09:00"}


def daily_response(seq=1, etfs=None, close_rates=None, background=None, truncated=False, job_id=TICK_ID):
    return envelope("market.daily.responded", {
        "jobId": job_id, "seq": seq,
        "etfs": [etf()] if etfs is None else etfs,
        "closeRates": [{"ticker": "TESTA", "date": "20261019", "closeAt": "2026-10-20T05:00:00+09:00",
                        "rate": "-1.25", "fetchedAt": "2026-10-20T08:30:02+09:00"}] if close_rates is None
        else close_rates,
        "background": [WORLD_C, EXCHANGE, MARKET] if background is None else background,
        "truncated": truncated,
    })


def filings_response(seq=1, filings=None, job_id=TICK_ID):
    return envelope("news.filings.responded", {
        "jobId": job_id, "seq": seq, "symbol": "TEST01",
        "filings": [{"accessionNumber": "0000000001-26-000001", "tickers": ["TESTA"], "form": "8-K",
                     "items": ["2.02"], "acceptedAt": "2026-10-20T05:30:28+09:00",
                     "url": "https://www.sec.gov/Archives/edgar/data/1/test-8k.htm"}] if filings is None else filings,
        "truncated": False,
    })


def good_output(*refs):
    return json.dumps({"points": [{"statement": "미국 지수가 오른 가운데 공시가 나왔을 수 있습니다.",
                                   "refs": list(refs) or ["B1"]}], "insufficientEvidence": False})


@pytest.fixture
def core():
    def make(llm=None, settings=DailySettings()):
        requests, failures = Requests(), Failures()
        details: DailyFailureRecordPort = Details()
        writer = DailyLlmWriter(llm, settings.llm_max_attempts) if llm is not None else None
        return DailyCore(requests, failures, writer, details, settings), requests, failures, details
    return make


def published(requests):
    events = requests.topic("briefing.daily.generated")
    for event in events:
        assert validate("briefing.daily.generated.v1.schema.json", event.value) == []
    return events


# ---- 범위 계산 ----

def test_range_starts_at_the_same_time_on_the_previous_weekday():
    assert range_start(MORNING_AT) == datetime(2026, 10, 19, 8, 45, tzinfo=KST)
    assert range_start(POST_CLOSE_AT) == datetime(2026, 10, 19, 17, 0, tzinfo=KST)


def test_monday_range_goes_back_to_friday():
    monday = datetime(2026, 10, 19, 8, 45, tzinfo=KST)
    assert range_start(monday) == datetime(2026, 10, 16, 8, 45, tzinfo=KST)


def test_range_never_goes_back_further_than_the_limit():
    monday = datetime(2026, 10, 19, 8, 45, tzinfo=KST)
    assert range_start(monday, DailySettings(max_lookback=timedelta(days=2))) == monday - timedelta(days=2)
    assert DailySettings().max_lookback == timedelta(days=7)


def test_range_is_computed_from_the_scheduled_time_in_korean_time_and_is_repeatable():
    utc = MORNING_AT.astimezone(timezone.utc)
    assert range_start(utc) == range_start(MORNING_AT) == range_start(MORNING_AT)


# ---- 시작 신호 ----

def test_each_edition_only_handles_its_own_job_type(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "morning")
    c.on_schedule_tick(tick("collect"), MORNING_AT, "morning")
    assert requests.sent == []
    c.on_schedule_tick(tick(MORNING_JOB), MORNING_AT, "morning")
    assert len(requests.topic("market.daily.requested")) == 1


def test_the_same_tick_is_handled_once(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    c.on_schedule_tick(tick(), MORNING_AT + timedelta(seconds=5), "morning")
    assert len(requests.topic("market.daily.requested")) == 1
    assert len(published(requests)) == 1


def test_malformed_tick_is_rejected(core):
    c, _, _, _ = core()
    with pytest.raises(ValueError):
        c.on_schedule_tick("{not json", MORNING_AT, "morning")
    with pytest.raises(ValueError):
        c.on_schedule_tick(json.dumps({"eventType": "schedule.tick", "payload": {"jobType": MORNING_JOB}}),
                           MORNING_AT, "morning")


# ---- 자료 모으기 ----

def test_daily_data_request_matches_the_contract_and_is_keyed_by_job(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    [event] = requests.topic("market.daily.requested")
    assert validate("market.daily.requested.v1.schema.json", event.value) == []
    assert event.key == TICK_ID
    payload = event.value["payload"]
    assert payload["jobId"] == TICK_ID and payload["seq"] == 1
    assert payload["asOf"] == "2026-10-20T08:45:00+09:00"
    assert payload["since"] == "2026-10-19T08:45:00+09:00"


def test_morning_requests_filings_with_all_us_tickers_after_the_daily_data(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    assert requests.topic("news.filings.requested") == []
    c.on_daily_data_responded(daily_response(etfs=[
        etf("TEST02", ("FAKEB", "TESTC")), etf("TEST01", ("TESTA", "FAKEB")), etf("TEST03", ())]), MORNING_AT)
    [event] = requests.topic("news.filings.requested")
    assert validate("news.filings.requested.v1.schema.json", event.value) == []
    payload = event.value["payload"]
    assert payload["symbol"] == "TEST01"  # 미국 종목이 있는 첫 ETF(종목코드 순)
    assert payload["tickers"] == ["TESTA", "FAKEB", "TESTC"]
    assert payload["since"] == "2026-10-19T08:45:00+09:00" and payload["until"] == "2026-10-20T08:45:00+09:00"
    assert payload["maxFilings"] == DailySettings().max_filings
    assert published(requests) == []


def test_no_filings_request_when_no_etf_holds_us_stocks(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(etfs=[etf(tickers=())], close_rates=[]), MORNING_AT)
    assert requests.topic("news.filings.requested") == []
    [event] = published(requests)
    text = event.value["payload"]["text"]
    assert "해당 없음" in text
    assert ITEM_FILINGS not in event.value["payload"]["missingItems"]


def test_post_close_edition_never_requests_filings(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    c.on_daily_data_responded(daily_response(etfs=[etf(fetched="2026-10-20T16:30:02+09:00", rate="0.50")],
                                             background=[{**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00"}]),
                              POST_CLOSE_AT)
    assert requests.topic("news.filings.requested") == []
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["edition"] == "post-close" and payload["missingItems"] == []
    assert ITEM_KR_TODAY in payload["text"] and ITEM_ETFS in payload["text"]
    assert "TEST01 0.50%" in payload["text"]
    assert ITEM_US_INDEX not in payload["text"]


def test_morning_edition_uses_us_markets_filings_and_the_previous_korean_session(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["missingItems"] == [] and payload["generatedBy"] == GENERATED_BY_RULE
    text = payload["text"]
    for item in (ITEM_US_INDEX, "환율", "미국 종목 직전 장 등락", ITEM_FILINGS, ITEM_KR_REVIEW):
        assert item in text
    assert ITEM_ETFS not in text
    assert "TEST INDEX(TESTIX) 2.04%" in text
    assert "8-K 공시 항목 2.02" in text
    assert payload["rangeStart"] == "2026-10-19T08:45:00+09:00"
    assert payload["rangeEnd"] == "2026-10-20T08:45:00+09:00"


def test_data_received_after_the_scheduled_time_or_before_the_range_is_dropped_with_a_warning(core, caplog):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    with caplog.at_level(logging.WARNING, logger="briefing.daily_jobs"):
        c.on_daily_data_responded(daily_response(
            etfs=[etf(fetched="2026-10-20T17:00:01+09:00"), etf("TEST02", fetched="2026-10-19T16:59:59+09:00")],
            background=[{**MARKET, "fetchedAt": "2026-10-20T17:00:01+09:00"}]), POST_CLOSE_AT)
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [ITEM_KR_TODAY, ITEM_ETFS]
    assert "dropped 2 etfs" in caplog.text and "dropped 1 background" in caplog.text
    assert "TEST01" not in caplog.text and "TESTA" not in caplog.text


def test_data_received_exactly_at_both_ends_of_the_range_is_kept(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    c.on_daily_data_responded(daily_response(
        etfs=[etf(fetched="2026-10-20T17:00:00+09:00"), etf("TEST02", fetched="2026-10-19T17:00:00+09:00")],
        background=[{**MARKET, "fetchedAt": "2026-10-20T17:00:00+09:00"}]), POST_CLOSE_AT)
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == []


def test_stale_responses_are_ignored(core):
    c, requests, _, _ = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_tick(MORNING_AT + timedelta(seconds=10))  # seq 2로 다시 보냄
    c.on_daily_data_responded(daily_response(seq=1), MORNING_AT + timedelta(seconds=11))
    assert requests.topic("news.filings.requested") == []
    c.on_daily_data_responded(daily_response(seq=2), MORNING_AT + timedelta(seconds=12))
    assert len(requests.topic("news.filings.requested")) == 1


def test_filings_responses_of_other_jobs_are_ignored(core):
    c, requests, _, _ = core()
    c.on_filings_responded(filings_response(job_id="00000000-0000-4000-8000-0000000000aa"), MORNING_AT)
    assert requests.sent == []


# ---- 재요청과 무응답 ----

def test_daily_data_is_resent_with_a_new_seq_and_given_up_after_the_last_attempt(core):
    c, requests, failures, details = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    assert c.next_deadline() == MORNING_AT + timedelta(seconds=10)
    c.on_tick(MORNING_AT + timedelta(seconds=9))
    assert len(requests.topic("market.daily.requested")) == 1
    for n in (1, 2):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    seqs = [e.value["payload"]["seq"] for e in requests.topic("market.daily.requested")]
    assert seqs == [1, 2, 3]
    c.on_tick(MORNING_AT + timedelta(seconds=30))
    assert c.next_deadline() is None

    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_RULE
    assert payload["missingItems"] == [ITEM_US_INDEX, "환율", "미국 종목 직전 장 등락", ITEM_FILINGS, ITEM_KR_REVIEW]
    assert payload["text"].count(NO_DATA) >= 5
    assert requests.topic("news.filings.requested") == []

    [failure] = failures.records
    assert failure.reason is FailureReason.CONTEXT_NO_RESPONSE
    assert failure.symbol == "morning" and failure.job_id == TICK_ID and failure.attempts == 3
    assert "missing:" in failure.detail and ITEM_FILINGS in failure.detail

    resent = details.by_reason(REASON_RESENT)
    assert [r.attempts for r in resent] == [1, 2] and {r.stage for r in resent} == {STAGE_DAILY_DATA}
    [final] = details.by_reason("context_no_response")
    assert final.attempts == 3 and final.error_type == "Timeout"
    assert [r["seq"] for r in final.data["requests"]] == [1, 2, 3]
    assert final.data["notReceived"] == ["market.daily.responded"]
    assert final.data["requestRange"] == {"since": "2026-10-19T08:45:00+09:00", "asOf": "2026-10-20T08:45:00+09:00"}


def test_filings_no_response_still_sends_the_edition_and_records_one_failure(core):
    c, requests, failures, details = core()
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    for n in (1, 2, 3):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [ITEM_FILINGS]
    [failure] = failures.records
    assert failure.reason is FailureReason.FILINGS_NO_RESPONSE and failure.symbol == "morning"
    [final] = details.by_reason("filings_no_response")
    assert final.stage == STAGE_FILINGS
    # 받은 응답은 내용 전부를 담는다.
    assert final.data["received"]["market.daily.responded"]["background"][0]["response"] == WORLD_C["response"]
    assert final.data["notReceived"] == ["news.filings.responded"]
    assert len(details.by_reason(REASON_RESENT)) == 2


# ---- LLM ----

def test_llm_writes_the_summary_and_cites_only_given_refs(core):
    llm = FakeLlm(good_output("B1", "0000000001-26-000001"))
    c, requests, failures, details = core(llm)
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_LLM
    assert "요약:" in payload["text"] and "[근거 1, 2]" in payload["text"]
    assert "투자 조언" in payload["text"]
    assert failures.records == [] and details.records == []
    _, user, schema = llm.calls[0]
    assert "insufficientEvidence" in schema["required"]
    assert {i["name"] for i in json.loads(user)["items"]} >= {ITEM_US_INDEX, ITEM_FILINGS}


def test_rejected_llm_attempts_are_each_recorded_even_when_the_edition_succeeds(core):
    unknown = json.dumps({"points": [{"statement": "근거 밖", "refs": ["Z9"]}], "insufficientEvidence": False})
    llm = FakeLlm("not json", unknown, good_output())
    c, requests, failures, details = core(llm)
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    [event] = published(requests)
    assert event.value["payload"]["generatedBy"] == GENERATED_BY_LLM
    assert failures.records == []
    rejected = details.by_reason(REASON_LLM_REJECTED)
    assert [r.attempts for r in rejected] == [1, 2]
    assert all(r.stage == STAGE_LLM for r in rejected)
    assert rejected[0].data["llm"]["rawOutput"] == "not json"
    assert rejected[1].data["llm"]["rawOutput"] == unknown
    assert "were not given" in rejected[1].data["llm"]["rejectReason"]
    assert json.loads(rejected[0].data["llm"]["input"])["edition"] == "아침 판"


def test_llm_failing_every_time_falls_back_to_the_rule_based_body_and_records_llm_error(core):
    llm = FakeLlm(LlmCallError("LLM call failed: APIError"), "{}", "[]")
    c, requests, failures, details = core(llm)
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_RULE
    assert "요약:" not in payload["text"] and "TEST INDEX(TESTIX) 2.04%" in payload["text"]
    [failure] = failures.records
    assert failure.reason is FailureReason.LLM_ERROR and failure.attempts == 3 and failure.symbol == "morning"
    assert [r.attempts for r in details.by_reason(REASON_LLM_REJECTED)] == [1, 2, 3]
    assert details.by_reason(REASON_LLM_REJECTED)[0].error_type == "LlmCallError"
    assert details.by_reason(REASON_LLM_REJECTED)[0].data["llm"]["rawOutput"] is None
    [final] = details.by_reason("llm_error")
    assert final.attempts == 3 and final.data["llm"]["input"]


def test_insufficient_evidence_is_marked_in_the_body(core):
    llm = FakeLlm(json.dumps({"points": [], "insufficientEvidence": True}))
    c, requests, _, _ = core(llm)
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    [event] = published(requests)
    assert event.value["payload"]["generatedBy"] == GENERATED_BY_LLM
    assert "요약할 근거가 부족합니다" in event.value["payload"]["text"]


def test_llm_is_not_called_when_there_is_no_evidence_at_all(core):
    llm = FakeLlm()
    c, requests, _, _ = core(llm)
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    for n in (1, 2, 3):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    assert llm.calls == []
    [event] = published(requests)
    assert event.value["payload"]["generatedBy"] == GENERATED_BY_RULE


def test_first_failure_is_the_reason_and_the_edition_gets_one_line(core):
    llm = FakeLlm("x", "x", "x")
    c, requests, failures, _ = core(llm)
    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    for n in (1, 2, 3):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    [failure] = failures.records
    assert failure.reason is FailureReason.FILINGS_NO_RESPONSE
    assert len(published(requests)) == 1


# ---- 실패 상세 기록 ----

def test_missing_items_are_recorded_with_every_field_filled(core):
    c, requests, _, details = core()
    c.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    c.on_daily_data_responded(daily_response(etfs=[], background=[]), POST_CLOSE_AT)
    [record] = details.by_reason(REASON_MISSING_ITEMS)
    assert record.kind == "post-close" and record.job_id == TICK_ID and record.stage == STAGE_COMPOSE
    assert record.range_start == datetime(2026, 10, 19, 17, 0, tzinfo=KST) and record.range_end == POST_CLOSE_AT
    assert record.data["missingItems"] == [ITEM_KR_TODAY, ITEM_ETFS]
    assert record.data["received"]["market.daily.responded"]["etfs"] == []
    assert record.occurred_at == POST_CLOSE_AT


def test_records_and_events_carry_no_keys_or_personal_data(core, caplog, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-secret-value")
    llm = FakeLlm("bad", good_output())
    c, requests, failures, details = core(llm)
    with caplog.at_level(logging.DEBUG):
        c.on_schedule_tick(tick(), MORNING_AT, "morning")
        c.on_daily_data_responded(daily_response(), MORNING_AT)
        c.on_filings_responded(filings_response(), MORNING_AT)
    dumped = json.dumps([e.value for e in requests.sent], ensure_ascii=False) \
        + json.dumps([r.data for r in details.records], ensure_ascii=False) + caplog.text
    for forbidden in ("sk-test-secret-value", "@", "quantity", "amount", "수량", "금액"):
        assert forbidden not in dumped
    # 로그에는 종목코드와 티커를 쓰지 않는다.
    assert "TEST01" not in caplog.text and "TESTA" not in caplog.text


def test_default_detail_port_keeps_nothing_and_logs_nothing(caplog):
    record = DailyFailureRecord("morning", TICK_ID, MORNING_AT, MORNING_AT, STAGE_LLM, "x", None, 1,
                                {"llm": {"input": "TEST-INPUT"}}, MORNING_AT)
    with caplog.at_level(logging.DEBUG):
        assert DiscardDailyFailureRecords().record(record) is None
    assert caplog.text == ""
