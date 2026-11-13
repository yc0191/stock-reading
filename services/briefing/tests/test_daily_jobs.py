"""데일리 판 작업(DailyCore)을 가짜 요청 출구·실패 기록·LLM으로 돌린다. 실제 Kafka·DB·LLM은 부르지 않는다."""
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from fake_anthropic import as_reply
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
    range_start,
)
from briefing.daily_report import (
    GENERATED_BY_LLM,
    GENERATED_BY_RULE,
    ITEM_ETF_ARTICLES,
    ITEM_ETFS,
    ITEM_ETFS_SPLIT_TITLE,
    ITEM_EXCHANGE,
    ITEM_FILINGS,
    ITEM_KR_REVIEW,
    ITEM_KR_TODAY,
    ITEM_OPEN_FLOW,
    ITEM_US_INDEX,
    NO_DATA,
    DailyLlmWriter,
)
from briefing.job_log import DiscardJobLog, EntryType, GeneratedBy, JobKind, JobLogEntry, RunStatus
from briefing.llm import LlmCallError
from briefing.ports import FailureReason
from contracts import validate
from fake_job_log import FakeJobLog
from fake_news import answer_news

KST = timezone(timedelta(hours=9))
TICK_ID = "8d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68"
MORNING_AT = datetime(2026, 10, 20, 8, 45, tzinfo=KST)    # 화요일
OPEN_AT = datetime(2026, 10, 20, 9, 10, tzinfo=KST)        # 장 시작 뒤 아침 판 시각
POST_CLOSE_AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
# 판 시작 알림(daily.collection.completed)의 edition 값
MORNING_JOB = "morning"
POST_CLOSE_JOB = "post-close"


# ---- 가짜 ----

class Requests:
    def __init__(self):
        self.sent = []

    def send(self, event):
        self.sent.append(event)

    def topic(self, name):
        return [e for e in self.sent if e.topic == name]


class Details(FakeJobLog):
    """작업 기록 가짜. 넘어온 줄을 그대로 모은다."""

    @property
    def records(self):
        return self.entries

    def by_reason(self, reason):
        return self.of_reason(reason)


class Failures:
    """끝내 실패한 줄만 보여 주는 창(이전의 실패 테이블 한 줄에 해당)."""

    def __init__(self, details):
        self.details = details

    @property
    def records(self):
        return self.details.failed


class FakeLlm:
    def __init__(self, *outputs):
        self.outputs = list(outputs)
        self.calls = []

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
        self.calls.append((system, user, output_schema))
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return output


def tick(job_type=MORNING_JOB, at=MORNING_AT, event_id=TICK_ID, missing=(), status="collected"):
    """수집기의 판 시작 알림(daily.collection.completed). job_type은 판(edition)이다."""
    return json.dumps({
        "eventId": event_id, "eventType": "daily.collection.completed", "schemaVersion": 1,
        "occurredAt": at.isoformat(), "producedAt": at.isoformat(),
        "correlationId": f"daily-{job_type}-{at.isoformat()}",
        "payload": {"edition": job_type, "status": status, "scheduledFor": at.isoformat(),
                    "missingSources": list(missing)},
    })


def envelope(event_type, payload):
    return json.dumps({
        "eventId": "00000000-0000-4000-8000-000000000009", "eventType": event_type, "schemaVersion": 1,
        "occurredAt": "2026-10-20T08:45:01+09:00", "producedAt": "2026-10-20T08:45:01+09:00",
        "correlationId": "c", "payload": payload,
    })


# 시세처리기가 09:00~09:10 저장 시세로 계산해 주는 개장 흐름(가짜 값). 시가 등락률은 응답 값이 아니라 가격에서 계산한 값이다.
OPEN_FLOW = {"status": "present", "prevClose": "10000", "open": "10200", "openSource": "raw", "openRate": "2.00",
             "startAt": "2026-10-20T09:00:01+09:00", "startPrice": "10100", "endAt": "2026-10-20T09:09:58+09:00",
             "endPrice": "10300", "flowRate": "1.98", "rateVsPrevClose": "3.00"}


def etf(symbol="TEST01", tickers=("TESTA", "FAKEB"), fetched="2026-10-19T16:30:02+09:00", rate="-1.50",
        open_flow=None):
    item = {"symbol": symbol, "date": "20261019", "close": "9850", "prevClose": "10000", "rate": rate,
            "fetchedAt": fetched, "tickers": list(tickers), "tickersTruncated": False}
    if open_flow is not None:
        item["openFlow"] = open_flow
    return item


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


def morning_output(*refs, symbols=("TEST01",), directions=None, **extra):
    """아침 판 Claude 출력. 요약 한 줄, ETF마다 방향 예상 한 줄(근거는 refs, 기본 B1), 환율·금리 이슈 칸."""
    cited = list(refs) or ["B1"]
    etfs = [{"symbol": symbol, "direction": (directions or {}).get(symbol, "상승"),
             "reason": "미국 지수가 오른 영향을 줄 수 있습니다.", "refs": cited, "weak": False, "shortfall": ""}
            for symbol in symbols]
    issue = {"statement": "환율이 영향을 줄 수 있습니다.", "refs": cited, "weak": False, "shortfall": ""}
    return json.dumps({"points": [{"statement": "미국 지수가 오른 가운데 공시가 나왔을 수 있습니다.",
                                   "refs": cited}], "etfs": etfs, "fxIssue": issue,
                       "rateIssue": {**issue, "statement": "받은 자료에 금리 관련 기사가 없습니다.", "refs": [],
                                     "weak": True, "shortfall": "금리 기사가 더 있어야 합니다."},
                       "insufficientEvidence": False, **extra}, ensure_ascii=False)


good_output = morning_output


def post_close_output(symbols=("TEST01", "TEST02", "TEST03"), refs=None, weak=(), notables=(), summary=None,
                      reasons=None, insufficient=False):
    """마감 후 판 Claude 출력. ETF마다 이유 한 줄을 쓰고, 근거는 기본으로 그 ETF의 하루 등락 번호(E1부터)다."""
    refs = refs or {}
    etfs = [{"symbol": symbol, "reason": (reasons or {}).get(symbol, "시장 흐름과 함께 움직였을 수 있습니다."),
             "refs": refs.get(symbol, [f"E{n}"]), "weak": symbol in weak,
             "shortfall": "그 ETF를 직접 다룬 기사가 더 있어야 합니다." if symbol in weak else ""}
            for n, symbol in enumerate(symbols, start=1)]
    return json.dumps({"summary": ["국내 증시와 ETF가 함께 움직였습니다.", "일부 ETF는 근거가 약합니다."]
                       if summary is None else summary,
                       "etfs": [] if insufficient else etfs, "notables": list(notables),
                       "insufficientEvidence": insufficient}, ensure_ascii=False)


@pytest.fixture
def core():
    def make(llm=None, settings=DailySettings()):
        requests, details = Requests(), Details()
        writer = DailyLlmWriter(llm, settings.llm_max_attempts) if llm is not None else None
        return DailyCore(requests, writer, details, settings), requests, Failures(details), details
    return make


def published(requests):
    events = requests.topic("briefing.daily.generated")
    for event in events:
        assert validate("briefing.daily.generated.v2.schema.json", event.value) == []
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

def test_each_notice_starts_only_its_own_edition_and_an_unknown_edition_is_rejected(core):
    c, requests, _, details = core()
    with pytest.raises(ValueError):
        c.on_collection_ready(tick("collect"), MORNING_AT)
    assert requests.sent == []
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    assert [s.job_kind.value for s in details.starts] == ["post-close"]
    assert len(requests.topic("market.daily.requested")) == 1


def test_the_same_notice_is_handled_once(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    c.on_collection_ready(tick(), MORNING_AT + timedelta(seconds=5))
    assert len(requests.topic("market.daily.requested")) == 1
    assert len(published(requests)) == 1


def test_malformed_notice_is_rejected(core):
    c, _, _, _ = core()
    with pytest.raises(ValueError):
        c.on_collection_ready("{not json", MORNING_AT)
    with pytest.raises(ValueError):
        c.on_collection_ready(json.dumps({"eventType": "daily.collection.completed", "payload": {"edition": MORNING_JOB}}),
                              MORNING_AT)
    with pytest.raises(ValueError):
        # 더 이상 쓰지 않는 시작 신호는 받지 않는다.
        c.on_collection_ready(json.dumps({"eventId": TICK_ID, "eventType": "schedule.tick", "correlationId": "x",
                                          "payload": {"jobType": "daily-briefing-morning",
                                                      "scheduledFor": MORNING_AT.isoformat()}}), MORNING_AT)


# ---- 자료 모으기 ----

def test_daily_data_request_matches_the_contract_and_is_keyed_by_job(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    [event] = requests.topic("market.daily.requested")
    assert validate("market.daily.requested.v1.schema.json", event.value) == []
    assert event.key == TICK_ID
    payload = event.value["payload"]
    assert payload["jobId"] == TICK_ID and payload["seq"] == 1
    assert payload["asOf"] == "2026-10-20T08:45:00+09:00"
    assert payload["since"] == "2026-10-19T08:45:00+09:00"


def test_morning_requests_filings_with_all_us_tickers_after_the_daily_data(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
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
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(etfs=[etf(tickers=())], close_rates=[]), MORNING_AT)
    assert requests.topic("news.filings.requested") == []
    answer_news(c, requests, MORNING_AT)
    [event] = published(requests)
    text = event.value["payload"]["text"]
    assert "해당 없음" in text
    assert ITEM_FILINGS not in event.value["payload"]["missingItems"]


def test_post_close_edition_requests_the_filings_of_the_us_stocks_in_its_range(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    c.on_daily_data_responded(daily_response(etfs=[etf(fetched="2026-10-20T16:30:02+09:00", rate="0.50")],
                                             background=[WORLD_C, EXCHANGE, {**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00"}]),
                              POST_CLOSE_AT)
    [filings] = requests.topic("news.filings.requested")
    assert filings.value["payload"]["tickers"] == ["TESTA", "FAKEB"]
    assert filings.value["payload"]["since"] == "2026-10-19T17:00:00+09:00"
    assert filings.value["payload"]["until"] == "2026-10-20T17:00:00+09:00"
    # 응답이 오기 전에는 판이 나가지 않는다.
    assert published(requests) == []
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["edition"] == "post-close" and payload["missingItems"] == []
    # 기본 멤버는 이 ETF를 보유하므로 절 제목이 보유·관심으로 나뉜 제목이다.
    assert ITEM_ETFS_SPLIT_TITLE in payload["text"] and f"{ITEM_KR_TODAY}: 코스피 " in payload["text"]
    assert "TEST01 +0.50%" in payload["text"]
    assert ITEM_US_INDEX not in payload["text"]


def test_morning_edition_uses_us_markets_filings_and_the_previous_korean_session(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    # 장 시작(09:00) 전에 시작한 판은 읽을 개장 시세가 없어 그 항목만 빠진다.
    assert payload["missingItems"] == [ITEM_OPEN_FLOW] and payload["generatedBy"] == GENERATED_BY_RULE
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
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    with caplog.at_level(logging.WARNING, logger="briefing.daily_jobs"):
        c.on_daily_data_responded(daily_response(
            etfs=[etf(fetched="2026-10-20T17:00:01+09:00"), etf("TEST02", fetched="2026-10-19T16:59:59+09:00")],
            background=[WORLD_C, EXCHANGE, {**MARKET, "fetchedAt": "2026-10-20T17:00:01+09:00"}]), POST_CLOSE_AT)
        answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [ITEM_KR_TODAY, ITEM_ETFS]
    assert "dropped 2 etfs" in caplog.text and "dropped 1 background" in caplog.text
    assert "TEST01" not in caplog.text and "TESTA" not in caplog.text


def test_data_received_exactly_at_both_ends_of_the_range_is_kept(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    c.on_daily_data_responded(daily_response(
        etfs=[etf(fetched="2026-10-20T17:00:00+09:00"), etf("TEST02", fetched="2026-10-19T17:00:00+09:00")],
        background=[WORLD_C, EXCHANGE, {**MARKET, "fetchedAt": "2026-10-20T17:00:00+09:00"}]), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == []


def test_stale_responses_are_ignored(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
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
    c.on_collection_ready(tick(), MORNING_AT)
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
    assert payload["missingItems"] == [ITEM_US_INDEX, "환율", "미국 종목 직전 장 등락", ITEM_FILINGS, ITEM_KR_REVIEW,
                                         ITEM_OPEN_FLOW, ITEM_ETF_ARTICLES]
    assert payload["text"].count(NO_DATA) >= 5
    assert requests.topic("news.filings.requested") == []

    [failure] = failures.records
    assert failure.reason == FailureReason.CONTEXT_NO_RESPONSE
    assert failure.job_id == TICK_ID and failure.attempts == 3
    [run] = details.starts
    assert run.job_kind == JobKind.MORNING and run.job_id == TICK_ID
    [result] = details.results()
    assert result.status == RunStatus.SUCCEEDED and result.failure_reason == "context_no_response"
    [missing] = details.of_type(EntryType.MISSING_ITEMS)
    assert ITEM_FILINGS in missing.data["missingItems"]

    resent = details.by_reason(REASON_RESENT)
    assert [r.attempts for r in resent] == [1, 2] and {r.stage for r in resent} == {STAGE_DAILY_DATA}
    [final] = details.by_reason("context_no_response")
    assert final.attempts == 3 and final.error_type == "Timeout"
    assert [r["seq"] for r in final.data["requests"]] == [1, 2, 3]
    assert final.data["notReceived"] == ["market.daily.responded"]
    assert final.data["requestRange"] == {"since": "2026-10-19T08:45:00+09:00", "asOf": "2026-10-20T08:45:00+09:00"}


def test_filings_no_response_still_sends_the_edition_and_records_one_failure(core):
    c, requests, failures, details = core()
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT, filings_silent=True)
    for n in (1, 2, 3):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    [event] = published(requests)
    assert event.value["payload"]["missingItems"] == [ITEM_FILINGS, ITEM_OPEN_FLOW]
    [failure] = failures.records
    assert failure.reason == FailureReason.FILINGS_NO_RESPONSE and failure.job_id == TICK_ID
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
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    c.on_daily_data_responded(daily_response(etfs=[etf(open_flow=OPEN_FLOW)]), OPEN_AT)
    c.on_filings_responded(filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_LLM
    assert "요약:" in payload["text"] and "[근거 1, 2]" in payload["text"]
    assert "투자 조언" in payload["text"]
    assert failures.records == []
    assert [e.entry_type for e in details.records] == [EntryType.REPORT_WRITTEN, EntryType.ETF_OUTLOOK,
                                                       EntryType.MEMBER_BRIEFING]
    [result] = details.results()
    assert (result.status, result.generated_by, result.failure_reason) == (RunStatus.SUCCEEDED, GeneratedBy.LLM, None)
    _, user, schema = llm.calls[0]
    assert "insufficientEvidence" in schema["required"]
    assert {i["name"] for i in json.loads(user)["items"]} >= {ITEM_US_INDEX, ITEM_FILINGS}


def test_rejected_llm_attempts_are_each_recorded_even_when_the_edition_succeeds(core):
    unknown = json.dumps({"points": [{"statement": "근거 밖", "refs": ["Z9"]}], "insufficientEvidence": False})
    llm = FakeLlm("not json", unknown, good_output())
    c, requests, failures, details = core(llm)
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
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
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_RULE
    assert "요약:" not in payload["text"] and "TEST INDEX(TESTIX) 2.04%" in payload["text"]
    [failure] = failures.records
    assert failure.reason == FailureReason.LLM_ERROR and failure.attempts == 3 and failure.job_id == TICK_ID
    assert [r.attempts for r in details.by_reason(REASON_LLM_REJECTED)] == [1, 2, 3]
    assert details.by_reason(REASON_LLM_REJECTED)[0].error_type == "LlmCallError"
    assert details.by_reason(REASON_LLM_REJECTED)[0].data["llm"]["rawOutput"] is None
    [final] = details.by_reason("llm_error")
    assert final.attempts == 3 and final.data["llm"]["input"]


def test_insufficient_evidence_is_marked_in_the_body(core):
    llm = FakeLlm(json.dumps({"points": [], "insufficientEvidence": True}))
    c, requests, _, _ = core(llm)
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    [event] = published(requests)
    assert event.value["payload"]["generatedBy"] == GENERATED_BY_LLM
    assert "요약할 근거가 부족합니다" in event.value["payload"]["text"]


def test_llm_is_not_called_when_there_is_no_evidence_at_all(core):
    llm = FakeLlm()
    c, requests, _, _ = core(llm)
    c.on_collection_ready(tick(), MORNING_AT)
    for n in (1, 2, 3):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    assert llm.calls == []
    [event] = published(requests)
    assert event.value["payload"]["generatedBy"] == GENERATED_BY_RULE


def test_every_failure_is_logged_but_the_result_line_names_the_first_one(core):
    llm = FakeLlm("x", "x", "x")
    c, requests, failures, details = core(llm)
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT, filings_silent=True)
    for n in (1, 2, 3):
        c.on_tick(MORNING_AT + timedelta(seconds=10 * n))
    assert [f.reason for f in failures.records] == [FailureReason.FILINGS_NO_RESPONSE, FailureReason.LLM_ERROR]
    [result] = details.results()
    assert result.failure_reason == FailureReason.FILINGS_NO_RESPONSE.value
    assert len(published(requests)) == 1


# ---- 실패 상세 기록 ----

def test_missing_items_are_recorded_with_every_field_filled(core):
    c, requests, _, details = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    c.on_daily_data_responded(daily_response(etfs=[], background=[]), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [record] = details.by_reason(REASON_MISSING_ITEMS)
    assert record.job_id == TICK_ID and record.stage == STAGE_COMPOSE
    [run] = details.starts
    assert run.job_kind == JobKind.POST_CLOSE
    assert run.target == {"rangeStart": datetime(2026, 10, 19, 17, 0, tzinfo=KST).isoformat(),
                          "rangeEnd": POST_CLOSE_AT.isoformat()}
    assert record.data["missingItems"] == [ITEM_KR_TODAY, ITEM_US_INDEX, ITEM_EXCHANGE, ITEM_ETFS]
    assert record.data["received"]["market.daily.responded"]["etfs"] == []
    assert record.occurred_at == POST_CLOSE_AT


def test_records_and_events_carry_no_keys_or_personal_data(core, caplog, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-secret-value")
    llm = FakeLlm("bad", good_output())
    c, requests, failures, details = core(llm)
    with caplog.at_level(logging.DEBUG):
        c.on_collection_ready(tick(), MORNING_AT)
        c.on_daily_data_responded(daily_response(), MORNING_AT)
        c.on_filings_responded(filings_response(), MORNING_AT)
        answer_news(c, requests, MORNING_AT)
    dumped = json.dumps([e.value for e in requests.sent], ensure_ascii=False) \
        + json.dumps([r.data for r in details.records], ensure_ascii=False) + caplog.text
    for forbidden in ("sk-test-secret-value", "@", "quantity", "amount", "수량", "금액"):
        assert forbidden not in dumped
    # 로그에는 종목코드와 티커를 쓰지 않는다.
    assert "TEST01" not in caplog.text and "TESTA" not in caplog.text


def test_default_log_port_keeps_nothing_and_logs_nothing(caplog):
    entry = JobLogEntry(TICK_ID, EntryType.STEP_FAILED, STAGE_LLM, "x", MORNING_AT, attempts=1,
                        data={"llm": {"input": "TEST-INPUT"}})
    with caplog.at_level(logging.DEBUG):
        assert DiscardJobLog().record(entry) is True
        assert DiscardJobLog().has_failed(TICK_ID) is False
    assert caplog.text == ""


# ---- 결과 줄과 저장 실패 ----

def test_every_edition_leaves_one_result_line_with_body_cited_evidence_missing_items_and_method(core):
    llm = FakeLlm(good_output("B1", "0000000001-26-000001"))
    c, requests, _, details = core(llm)
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    c.on_daily_data_responded(daily_response(etfs=[etf(open_flow=OPEN_FLOW)]), OPEN_AT)
    c.on_filings_responded(filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)

    [event] = published(requests)
    [result] = details.results()
    # 작업 한 줄(job_run)은 morning으로 시작해 본문·작성 방식·메일의 브리핑 ID와 함께 끝난다.
    [run] = details.starts
    assert (run.job_kind, result.job_id, result.status) == (JobKind.MORNING, TICK_ID, RunStatus.SUCCEEDED)
    # 작업 기록의 본문은 멤버를 가르지 않은 판 전체 글이라 멤버 메일의 [보유] 묶음 표시만 다르다.
    assert result.final_text == event.value["payload"]["text"].replace("  [보유]\n", "")
    assert result.generated_by == GeneratedBy.LLM and result.result_event_id == event.value["eventId"]
    assert (result.weak_count, result.research_count, result.failure_reason) == (0, 0, None)
    # 빠진 항목이 없으면 missing-items 줄도 없고, 인용한 근거는 최종 본문의 근거 목록에 있다.
    assert details.of_type(EntryType.MISSING_ITEMS) == [] and event.value["payload"]["missingItems"] == []
    assert "근거:" in result.final_text and "[근거 1, 2]" in result.final_text


def test_the_accepted_llm_attempt_keeps_its_input_and_original_response(core):
    answer = good_output("B1")
    llm = FakeLlm("bad", answer)
    c, requests, _, details = core(llm)
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)

    [accepted] = details.of_type(EntryType.REPORT_WRITTEN)
    assert accepted.attempts == 2 and accepted.error_type is None and accepted.stage == STAGE_LLM
    assert accepted.data["llm"]["rawOutput"] == answer and accepted.data["llm"]["input"] == llm.calls[1][1]
    assert accepted.data["llm"]["rejectReason"] is None


def test_edition_is_not_published_when_its_log_could_not_be_saved(caplog):
    requests, log_port = Requests(), Details(fail=True)
    c = DailyCore(requests, None, log_port, DailySettings())
    with caplog.at_level(logging.ERROR, logger="briefing.job_log"):
        c.on_collection_ready(tick(), MORNING_AT)
        c.on_daily_data_responded(daily_response(), MORNING_AT)
        c.on_filings_responded(filings_response(), MORNING_AT)
        answer_news(c, requests, MORNING_AT)

    assert published(requests) == []
    assert "result event not published" in caplog.text and "TEST01" not in caplog.text
    assert "result was not saved" in caplog.text
    assert c.next_deadline() is None  # 작업은 정리되어 더 기다리지 않는다


def test_post_close_edition_also_leaves_its_lines_under_its_own_job_kind(core):
    c, requests, _, details = core()
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    for n in (1, 2, 3):
        c.on_tick(POST_CLOSE_AT + timedelta(seconds=10 * n))

    assert {run.job_kind for run in details.starts} == {JobKind.POST_CLOSE}
    assert {e.job_id for e in details.entries} == {TICK_ID}
    assert [r.status for r in details.results()] == [RunStatus.SUCCEEDED]
    assert len(published(requests)) == 1


# ---- market.daily.responded v2: 일봉이 빈 ETF ----

def etf_v2_missing(symbol="TEST02", reason="no-daily-price-in-range", tickers=("TESTC",), name=None, open_flow=None):
    item = {"symbol": symbol, "date": None, "close": None, "prevClose": None, "rate": None, "fetchedAt": None,
            "missingReason": reason, "tickers": list(tickers), "tickersTruncated": False}
    if open_flow is not None:
        item["openFlow"] = open_flow
    if name is not None:
        item["etf"] = {"name": name, "members": [], "membersTruncated": False}
    return item


def etf_v2(symbol="TEST01", name=None, **kwargs):
    item = {**etf(symbol, **kwargs), "missingReason": None}
    if name is not None:
        item["etf"] = {"name": name, "members": [], "membersTruncated": False}
    return item


def post_close_core(core, llm=None):
    c, requests, failures, details = core(llm)
    c.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    return c, requests, failures, details


# 마감 후 판 범위(전날 17:00 ~ 그날 17:00) 안에 받은 배경 자료. 세계지수·환율은 그날 아침에 받은 값이다.
POST_CLOSE_BACKGROUND = [WORLD_C, EXCHANGE, {**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00"}]


def test_an_etf_with_no_daily_price_is_written_as_no_data_with_its_name_and_reason(core):
    c, requests, _, details = post_close_core(core)
    c.on_daily_data_responded(daily_response(etfs=[
        etf_v2(fetched="2026-10-20T16:30:02+09:00", rate="0.50", name="TEST ETF ONE"),
        etf_v2_missing(name="TEST ETF TWO"),
        etf_v2_missing("TEST03", "daily-price-unreadable")], background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert "TEST ETF ONE(TEST01) +0.50%" in payload["text"]
    assert f"{NO_DATA}: TEST ETF TWO, 범위 안에 받은 일봉이 없음" in payload["text"]
    # 이름을 모르면 종목코드로 쓴다.
    assert f"{NO_DATA}: TEST03, 일봉 원문을 읽지 못함" in payload["text"]
    assert payload["missingItems"] == [f"{ITEM_ETFS}: TEST ETF TWO, 범위 안에 받은 일봉이 없음",
                                       f"{ITEM_ETFS}: TEST03, 일봉 원문을 읽지 못함"]
    [line] = details.by_reason(REASON_MISSING_ITEMS)
    assert line.data["missingItems"] == payload["missingItems"]
    assert line.data["missingEtfs"] == [
        {"symbol": "TEST02", "name": "TEST ETF TWO", "reason": "no-daily-price-in-range"},
        {"symbol": "TEST03", "name": "TEST03", "reason": "daily-price-unreadable"}]


def test_when_every_etf_is_empty_the_whole_item_is_no_data_and_each_etf_is_still_listed(core):
    c, requests, _, _ = post_close_core(core)
    c.on_daily_data_responded(daily_response(etfs=[etf_v2_missing("TEST01"), etf_v2_missing("TEST02")],
                                             background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    payload = event.value["payload"]
    assert payload["missingItems"] == [f"{ITEM_ETFS}: TEST01, 범위 안에 받은 일봉이 없음",
                                       f"{ITEM_ETFS}: TEST02, 범위 안에 받은 일봉이 없음"]
    assert f"{NO_DATA}: TEST02, 범위 안에 받은 일봉이 없음" in payload["text"]


def test_an_etf_with_one_null_value_and_no_reason_is_no_data_with_an_unknown_reason(core):
    c, requests, _, _ = post_close_core(core)
    item = etf_v2("TEST03", fetched="2026-10-20T16:30:02+09:00", rate="0.50")
    item["close"] = None
    del item["missingReason"]
    c.on_daily_data_responded(daily_response(etfs=[item], background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    text = event.value["payload"]["text"]
    assert "자료 없음: TEST03, 일봉 값이 비어 있고 이유를 받지 못함" in text
    assert "None" not in text


def test_an_empty_etf_is_kept_even_though_it_has_no_received_time(core, caplog):
    c, requests, _, _ = post_close_core(core)
    with caplog.at_level(logging.WARNING, logger="briefing.daily_jobs"):
        c.on_daily_data_responded(daily_response(etfs=[etf_v2_missing()], background=POST_CLOSE_BACKGROUND),
                                  POST_CLOSE_AT)
        answer_news(c, requests, POST_CLOSE_AT)
    assert "dropped" not in caplog.text
    assert "TEST02" not in caplog.text
    assert len(published(requests)) == 1


def test_the_llm_is_told_which_etf_is_empty_and_not_to_invent_its_numbers(core):
    llm = FakeLlm(post_close_output(symbols=("TEST01",)))
    c, requests, _, _ = post_close_core(core, llm)
    c.on_daily_data_responded(daily_response(etfs=[
        etf_v2(fetched="2026-10-20T16:30:02+09:00", rate="0.50"), etf_v2_missing(name="TEST ETF TWO")],
        background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    assert event.value["payload"]["generatedBy"] == GENERATED_BY_LLM
    system, user, _ = llm.calls[0]
    item = next(i for i in json.loads(user)["items"] if i["name"] == ITEM_ETFS)
    assert item["missing"] == [{"symbol": "TEST02", "name": "TEST ETF TWO", "status": "없음",
                                "reason": "범위 안에 받은 일봉이 없음"}]
    assert [e["symbol"] for e in item["entries"]] == ["TEST01"]
    line = next(l for l in system.splitlines() if "missing" in l)
    assert all(word in line for word in ("자료 없음", "숫자", "쓰지 않"))


def test_morning_edition_ignores_an_empty_etf_in_its_missing_items(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    c.on_daily_data_responded(
        daily_response(etfs=[etf_v2_missing("TEST01", tickers=("TESTA",), open_flow=OPEN_FLOW)]), OPEN_AT)
    c.on_filings_responded(filings_response(), OPEN_AT)
    answer_news(c, requests, OPEN_AT)
    [event] = published(requests)
    # 아침 판에는 보유 ETF 항목이 없어 ETF 칸은 빠진 항목에 오르지 않는다.
    assert event.value["payload"]["missingItems"] == []


def test_an_etf_without_a_daily_price_still_gets_us_stocks_filings_and_research(core):
    c, requests, _, _ = core()
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(etfs=[etf_v2_missing("TEST01", tickers=("TESTA", "FAKEB"))]),
                              MORNING_AT)
    [filings] = requests.topic("news.filings.requested")
    assert filings.value["payload"]["tickers"] == ["TESTA", "FAKEB"]
    assert filings.value["payload"]["symbol"] == "TEST01"
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)
    [event] = published(requests)
    text = event.value["payload"]["text"]
    assert "미국 종목 1개(전체 2개 중)" in text and "8-K 공시" in text


def test_a_v1_response_without_the_missing_reason_is_still_read(core):
    c, requests, _, _ = post_close_core(core)
    c.on_daily_data_responded(daily_response(etfs=[etf(fetched="2026-10-20T16:30:02+09:00", rate="0.50")],
                                             background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(c, requests, POST_CLOSE_AT)
    [event] = published(requests)
    assert "TEST01 +0.50%" in event.value["payload"]["text"]
    assert event.value["payload"]["missingItems"] == []


def test_daily_llm_lines_keep_the_usage_and_a_call_failure_has_none(core):
    from fake_anthropic import FAKE_USAGE
    llm = FakeLlm(LlmCallError("LLM call failed: APIError"), "bad", good_output("B1"))
    c, requests, _, details = core(llm)
    c.on_collection_ready(tick(), MORNING_AT)
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)

    failed, rejected = details.of_type(EntryType.LLM_REJECTED)
    [accepted] = details.of_type(EntryType.REPORT_WRITTEN)
    assert failed.data["llm"]["usage"] is None
    assert rejected.data["llm"]["usage"] == accepted.data["llm"]["usage"] == FAKE_USAGE.as_record()
