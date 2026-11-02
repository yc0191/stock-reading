"""종목 유형별 시나리오: 가짜 응답과 가짜 LLM으로 변동 1건의 자료 수집부터 리포트까지 돌린다.

실제 Kafka·DB·LLM은 쓰지 않는다. 응답은 계약 샘플을 고쳐 만들고, 나가는 요청과 이벤트는 계약 스키마로 검증한다.
"""
import copy
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings
from briefing.jobs import BriefingCore, FailureReason
from briefing.report import (
    BACKGROUND_SOURCE,
    CLOSE_RATE_SOURCE,
    FILING_SOURCE,
    MARK_MISSING,
    MARK_NOT_APPLICABLE,
    MARK_PRESENT,
    MARK_TOO_LARGE,
    MARK_TRUNCATED,
    SYSTEM_PROMPT,
    LlmWriter,
    ReportStage,
    RuleBasedWriter,
    build_llm_input,
)
from contracts import sample, validate

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
S = CoreSettings()
SEC = timedelta(seconds=1)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
SYMBOL = "TEST01"
TICKERS = ("TESTA", "FAKEB")
ACCESSION = "0000000001-26-000001"
INDEX_RAW = sample("market.context.responded.sample.json")["payload"]["background"][2]["response"]


class Recorder:
    def __init__(self):
        self.sent, self.failed, self.readied = [], [], []

    def send(self, event):
        self.sent.append(event)

    def record(self, failure):
        self.failed.append(failure)

    def on_ready(self, report_input):
        self.readied.append(report_input)

    def of(self, topic):
        return [e for e in self.sent if e.topic == topic]


class FakeLlm:
    """정해 둔 응답을 차례로 돌려준다. 실제 API를 부르지 않는다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, system, user):
        self.calls.append((system, user))
        return self.responses.pop(0)


@pytest.fixture
def rec():
    return Recorder()


@pytest.fixture
def core(rec):
    return BriefingCore(rec, rec, rec)


# ---- 가짜 응답 ----

def move():
    event = sample("market.move.sample.json")
    event["eventId"] = JOB_ID
    event["payload"]["symbol"] = SYMBOL
    event["payload"]["asOf"] = T.isoformat()
    return json.dumps(event)


def completed():
    event = sample("collection.completed.sample.json")
    event["payload"].update(jobType="collect-news", startedAt=(T + S.delta).isoformat(), incompleteSymbols=[])
    return json.dumps(event)


def history(seq=1, count=2, truncated=False):
    event = sample("market.history.responded.sample.json")
    event["payload"].update(jobId=JOB_ID, seq=seq, symbol=SYMBOL, truncated=truncated,
                            points=[{"asOf": (T - n * 10 * SEC).isoformat(), "price": 10_000 + n}
                                    for n in range(count)])
    return json.dumps(event)


def article(article_id, published_at):
    return {"articleId": article_id, "title": f"기사 {article_id}", "summary": "", "publisher": "news.example.com",
            "publishedAt": published_at.isoformat(), "url": f"https://news.example.com/{article_id}"}


def articles(seq, items=None, truncated=False):
    event = sample("news.articles.responded.sample.json")
    event["payload"].update(jobId=JOB_ID, seq=seq, symbol=SYMBOL, truncated=truncated,
                            articles=items if items is not None else [article("a1", T - timedelta(minutes=30))])
    return json.dumps(event)


def background_item(api_type, target=None, fetched_at=T - timedelta(minutes=5), status="ok", response=None,
                    index_rates=None):
    item = {"apiType": api_type, "target": target, "status": status}
    if status == "ok":
        item.update(response=response or json.dumps({"api": api_type}), fetchedAt=fetched_at.isoformat())
    elif status == "too-large":
        item.update(fetchedAt=fetched_at.isoformat(), responseBytes=40_000)
    if index_rates is not None:
        item["indexRates"] = index_rates
    return item


INDEX_RATES = [
    {"code": "TESTIX", "name": "TEST INDEX", "businessDay": "20261020", "close": "100.00",
     "prevBusinessDay": "20261019", "prevClose": "98.00", "rate": "2.04"},
    {"code": "FAKEIX", "name": "FAKE INDEX", "businessDay": "20261020", "close": "50.00",
     "prevBusinessDay": None, "prevClose": None, "rate": None},
]


def full_background():
    return [
        background_item("exchange-rate-summary"),
        background_item("market-summary", status="too-large"),
        background_item("world-index", "S", response=INDEX_RAW, index_rates=INDEX_RATES),
        background_item("daily-price", SYMBOL, status="missing"),
    ]


def close_rate(ticker, close_at=T - timedelta(hours=11), fetched_at=T - timedelta(hours=7), rate="-1.25"):
    return {"ticker": ticker, "date": "20261019", "closeAt": close_at.isoformat(), "rate": rate,
            "fetchedAt": fetched_at.isoformat()}


def context(seq=1, tickers=TICKERS, close_rates=None, background=None, truncated=False):
    event = sample("market.context.responded.sample.json")
    event["payload"].update(
        jobId=JOB_ID, seq=seq, symbol=SYMBOL, tickers=list(tickers), truncated=truncated,
        closeRates=close_rates if close_rates is not None else [close_rate(t) for t in tickers[:1]],
        background=background if background is not None else full_background())
    return event


def filing(accession=ACCESSION, accepted_at=T - timedelta(hours=10), tickers=("TESTA",)):
    return {"accessionNumber": accession, "tickers": list(tickers), "form": "8-K", "items": ["2.02"],
            "acceptedAt": accepted_at.isoformat(), "url": f"https://www.sec.gov/Archives/test/{accession}.htm"}


def filings(seq=1, items=None, truncated=False):
    event = sample("news.filings.responded.sample.json")
    event["payload"].update(jobId=JOB_ID, seq=seq, symbol=SYMBOL, truncated=truncated,
                            filings=items if items is not None else [filing()])
    return event


def start(core):
    core.on_move(move(), T)
    core.on_collection_completed(completed(), T)


def stable_articles(core, items=None, truncated=False, now=T + SEC):
    """기사는 같은 목록이 두 번 연속 와야 안정이다."""
    core.on_articles_responded(articles(1, items, truncated), now)
    core.on_tick(now + S.recheck_interval)
    core.on_articles_responded(articles(2, items, truncated), now + S.recheck_interval)


def run_us(core, context_event=None, filings_event=None, history_event=None, article_items=None,
           articles_truncated=False):
    """미국 종목을 담은 ETF의 변동 1건을 모든 응답이 오는 경우로 끝까지 돌린다."""
    start(core)
    core.on_history_responded(history_event or history(), T + SEC)
    core.on_context_responded(json.dumps(context_event or context()), T + SEC)
    core.on_filings_responded(json.dumps(filings_event or filings()), T + 2 * SEC)
    stable_articles(core, article_items, articles_truncated)


# ---- 요청 구간과 순서 ----

def test_us_etf_requests_history_context_and_articles_first_then_filings_with_the_tickers(core, rec):
    start(core)

    assert [e.topic for e in rec.sent] == [
        "market.history.requested", "market.context.requested", "news.articles.requested"]
    (ctx,) = rec.of("market.context.requested")
    assert validate("market.context.requested.v1.schema.json", ctx.value) == []
    c = ctx.value["payload"]
    assert ctx.key == SYMBOL and (c["jobId"], c["seq"], c["symbol"]) == (JOB_ID, 1, SYMBOL)
    assert datetime.fromisoformat(c["asOf"]) == T
    assert datetime.fromisoformat(c["since"]) == T - timedelta(days=4)
    assert (c["maxResponseBytes"], c["maxTickers"]) == (32 * 1024, 100)
    (hist,) = rec.of("market.history.requested")
    h = hist.value["payload"]
    assert datetime.fromisoformat(h["since"]) == T - timedelta(minutes=5)
    assert datetime.fromisoformat(h["until"]) == T + S.delta
    assert (h["intervalSeconds"], h["maxPoints"]) == (10, 60)
    assert rec.of("news.articles.requested")[0].value["payload"]["maxArticles"] == 50

    core.on_context_responded(json.dumps(context()), T + SEC)

    (fil,) = rec.of("news.filings.requested")
    assert validate("news.filings.requested.v1.schema.json", fil.value) == []
    f = fil.value["payload"]
    assert fil.key == SYMBOL
    assert (f["jobId"], f["seq"], f["symbol"], f["tickers"], f["maxFilings"]) == (
        JOB_ID, 1, SYMBOL, list(TICKERS), 20)
    assert datetime.fromisoformat(f["since"]) == T - timedelta(hours=72)
    assert datetime.fromisoformat(f["until"]) == T


def test_us_etf_report_input_holds_every_kind_of_evidence(core, rec):
    run_us(core)

    (ready,) = rec.readied
    assert ready.tickers == TICKERS
    assert [f.accession_number for f in ready.filings] == [ACCESSION]
    assert [r.ticker for r in ready.close_rates] == ["TESTA"]
    assert [b.api_type for b in ready.background] == [
        "exchange-rate-summary", "market-summary", "world-index", "daily-price"]
    assert [a.article_id for a in ready.articles] == ["a1"]
    assert len(ready.price_points) == 2
    assert rec.failed == []


def test_report_waits_for_the_filings_response_of_a_us_etf(core, rec):
    start(core)
    core.on_history_responded(history(), T)
    core.on_context_responded(json.dumps(context()), T + 5 * SEC)
    stable_articles(core)
    assert rec.readied == []

    core.on_filings_responded(json.dumps(filings()), T + 12 * SEC)

    assert len(rec.readied) == 1


def test_etf_without_us_stocks_never_requests_filings_and_marks_them_not_applicable(core, rec):
    start(core)
    core.on_history_responded(history(), T + SEC)
    core.on_context_responded(json.dumps(context(tickers=(), close_rates=[])), T + SEC)
    stable_articles(core)
    for step in range(1, 5):
        core.on_tick(T + step * S.response_timeout)

    assert rec.of("news.filings.requested") == []
    (ready,) = rec.readied
    assert ready.tickers == () and ready.filings == () and ready.close_rates == ()
    data = build_llm_input(ready)
    assert data["filingsStatus"] == data["overseasCloseRatesStatus"] == MARK_NOT_APPLICABLE
    assert data["filings"] == data["overseasCloseRates"] == data["overseasCloseRatesMissing"] == []
    assert rec.failed == []


# ---- 응답이 끝내 오지 않을 때 ----

def test_context_no_response_is_retried_with_new_seq_then_fails_without_a_report(core, rec):
    start(core)
    core.on_history_responded(history(), T)
    stable_articles(core, now=T)
    for step in (1, 2, 3):
        core.on_tick(T + step * S.response_timeout)

    assert [e.value["payload"]["seq"] for e in rec.of("market.context.requested")] == [1, 2, 3]
    (failure,) = rec.failed
    assert (failure.reason, failure.attempts, failure.job_id) == (FailureReason.CONTEXT_NO_RESPONSE, 3, JOB_ID)
    assert rec.readied == [] and rec.of("news.filings.requested") == []


def test_filings_no_response_is_timed_from_its_own_request_then_fails_without_a_report(core, rec):
    start(core)
    core.on_history_responded(history(), T)
    sent_at = T + 5 * SEC
    core.on_context_responded(json.dumps(context()), sent_at)
    stable_articles(core, now=T)

    core.on_tick(sent_at + S.response_timeout - SEC)
    assert len(rec.of("news.filings.requested")) == 1
    for step in (1, 2, 3):
        core.on_tick(sent_at + step * S.response_timeout)

    assert [e.value["payload"]["seq"] for e in rec.of("news.filings.requested")] == [1, 2, 3]
    (failure,) = rec.failed
    assert (failure.reason, failure.attempts) == (FailureReason.FILINGS_NO_RESPONSE, 3)
    assert rec.readied == []


def test_history_no_response_fails_even_when_every_other_response_arrived(core, rec):
    start(core)
    core.on_context_responded(json.dumps(context()), T)
    core.on_filings_responded(json.dumps(filings()), T)
    core.on_articles_responded(articles(1), T)
    for step in (1, 2, 3):
        core.on_tick(T + step * S.response_timeout)
        core.on_articles_responded(articles(step + 1), T + step * S.response_timeout)

    assert [f.reason for f in rec.failed] == [FailureReason.MARKET_NO_RESPONSE]
    assert rec.readied == []


def test_articles_no_response_fails_even_when_every_other_response_arrived(core, rec):
    start(core)
    core.on_history_responded(history(), T)
    core.on_context_responded(json.dumps(context()), T)
    core.on_filings_responded(json.dumps(filings()), T)
    for step in (1, 2, 3):
        core.on_tick(T + step * S.response_timeout)

    assert [f.reason for f in rec.failed] == [FailureReason.NEWS_NO_RESPONSE]
    assert rec.readied == []


def test_next_deadline_follows_context_and_filings_requests(core):
    start(core)
    core.on_history_responded(history(), T)
    core.on_articles_responded(articles(1), T)
    assert core.next_deadline() == T + S.response_timeout  # 배경 자료 응답 대기

    core.on_context_responded(json.dumps(context()), T + 3 * SEC)
    assert core.next_deadline() == T + S.recheck_interval  # 기사 재확인이 공시 제한 시간보다 이르다
    core.on_tick(T + S.recheck_interval)
    core.on_articles_responded(articles(2), T + S.recheck_interval)
    assert core.next_deadline() == T + 3 * SEC + S.response_timeout


def test_stale_or_repeated_context_and_filings_responses_are_ignored(core, rec, caplog):
    start(core)
    core.on_tick(T + S.response_timeout)  # 배경 자료 seq 2로 재요청
    with caplog.at_level("INFO", logger="briefing.jobs"):
        core.on_context_responded(json.dumps(context(seq=1)), T + 11 * SEC)
    assert rec.of("news.filings.requested") == []
    assert "stale context response" in caplog.text

    core.on_context_responded(json.dumps(context(seq=2)), T + 12 * SEC)
    core.on_context_responded(json.dumps(context(seq=2)), T + 12 * SEC)  # 같은 응답 재도착
    assert len(rec.of("news.filings.requested")) == 1
    with caplog.at_level("INFO", logger="briefing.jobs"):
        core.on_filings_responded(json.dumps(filings(seq=9)), T + 13 * SEC)
    assert "stale filings response" in caplog.text


@pytest.mark.parametrize("handler", ["on_context_responded", "on_filings_responded"])
def test_malformed_context_or_filings_response_raises_value_error(core, handler):
    start(core)
    with pytest.raises(ValueError):
        getattr(core, handler)(json.dumps({"eventType": handler, "payload": {}}), T)


def test_malformed_items_in_a_context_response_raise_value_error(core):
    start(core)
    event = context()
    del event["payload"]["closeRates"][0]["closeAt"]

    with pytest.raises(ValueError):
        core.on_context_responded(json.dumps(event), T)


# ---- 받은 자료 재검사 ----

def test_data_after_the_move_outside_the_range_or_older_than_four_days_is_dropped_with_warnings(core, rec, caplog):
    four_days = timedelta(days=4)
    background = [
        background_item("exchange-rate-summary", fetched_at=T + SEC),            # T 뒤에 받음
        background_item("market-summary", fetched_at=T - four_days - SEC),       # 4일 초과
        background_item("world-index", "S", fetched_at=T - four_days, response=INDEX_RAW,
                        index_rates=INDEX_RATES),                                # 경계(포함)
        background_item("daily-price", SYMBOL, fetched_at=T),                    # 경계(포함)
    ]
    rates = [
        close_rate("TESTA"),
        close_rate("FAKEB", fetched_at=T + SEC),                                 # T 뒤에 받음
        close_rate("TESTC", close_at=T - four_days - SEC),                       # 4일 초과
    ]
    window = timedelta(hours=72)
    filing_items = [
        filing("0000000001-26-000001", T - window),                              # 경계(포함)
        filing("0000000001-26-000002", T),                                       # 경계(포함)
        filing("0000000001-26-000003", T - window - SEC),                        # 기간 밖
        filing("0000000001-26-000004", T + SEC),                                 # 변동 뒤 접수
    ]
    article_items = [article("in", T - timedelta(hours=1)), article("old", T - timedelta(hours=24) - SEC),
                     article("late", T + S.delta + SEC)]

    with caplog.at_level("WARNING", logger="briefing.jobs"):
        run_us(core, context(tickers=("TESTA", "FAKEB", "TESTC"), close_rates=rates, background=background),
               filings(items=filing_items), article_items=article_items)

    (ready,) = rec.readied
    assert [b.status for b in ready.background] == ["missing", "missing", "ok", "ok"]
    assert ready.background[0].response is None and ready.background[0].fetched_at is None
    assert [r.ticker for r in ready.close_rates] == ["TESTA"]
    assert [f.accession_number for f in ready.filings] == ["0000000001-26-000001", "0000000001-26-000002"]
    assert [a.article_id for a in ready.articles] == ["in"]
    for text in ("exchange-rate-summary background", "market-summary background", "overseas close rate",
                 "0000000001-26-000003", "0000000001-26-000004", "old", "late"):
        assert text in caplog.text
    # 경고 로그에 종목코드와 티커를 쓰지 않는다.
    for secret in (SYMBOL, "TESTA", "FAKEB", "TESTC"):
        assert secret not in caplog.text
    data = build_llm_input(ready)
    assert data["overseasCloseRatesMissing"] == ["FAKEB", "TESTC"]


def test_truncation_from_each_response_is_marked_in_the_llm_input(core, rec):
    run_us(core, context(truncated=True), filings(truncated=True), history(truncated=True),
           articles_truncated=True)

    data = build_llm_input(rec.readied[0])
    assert data["priceHistoryStatus"] == data["articlesStatus"] == MARK_TRUNCATED
    assert data["filingsStatus"] == data["overseasCloseRatesStatus"] == MARK_TRUNCATED


def test_empty_responses_are_marked_missing(core, rec):
    run_us(core, context(close_rates=[]), filings(items=[]), history(count=0), article_items=[])

    data = build_llm_input(rec.readied[0])
    assert data["priceHistoryStatus"] == data["articlesStatus"] == MARK_MISSING
    assert data["filingsStatus"] == data["overseasCloseRatesStatus"] == MARK_MISSING
    assert data["overseasCloseRatesMissing"] == list(TICKERS)


# ---- 수집 내역 로그 ----

def test_one_collection_log_line_per_move_with_counts_and_without_symbol_or_tickers(core, rec, caplog):
    rates = [close_rate("TESTA"), close_rate("FAKEB", fetched_at=T + SEC)]
    background = full_background()
    background[0] = background_item("exchange-rate-summary", fetched_at=T + SEC)
    with caplog.at_level("INFO", logger="briefing.jobs"):
        run_us(core, context(close_rates=rates, background=background, truncated=True),
               filings(items=[filing(), filing("0000000001-26-000009", T + SEC)], truncated=True),
               article_items=[article("in", T - timedelta(hours=1)), article("late", T + S.delta + SEC)],
               articles_truncated=True)

    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("collection ")]
    assert len(lines) == 1
    assert lines[0] == (
        f"collection job={JOB_ID} history=2 historyTruncated=False articles=1 articlesExcluded=1 "
        "articlesTruncated=True filings=1 filingsExcluded=1 filingsTruncated=True tickers=2 tickersTruncated=True "
        "closeRates=1 closeRatesExcluded=1 closeRatesMissing=1 "
        "background=1 backgroundMissing=2 backgroundTooLarge=1 backgroundExcluded=1")
    for secret in (SYMBOL, *TICKERS):
        assert secret not in lines[0]


def test_collection_log_marks_filings_and_close_rates_not_applicable_without_us_stocks(core, caplog):
    start(core)
    core.on_history_responded(history(), T)
    core.on_context_responded(json.dumps(context(tickers=(), close_rates=[])), T)
    with caplog.at_level("INFO", logger="briefing.jobs"):
        stable_articles(core)

    (line,) = [r.getMessage() for r in caplog.records if r.getMessage().startswith("collection ")]
    assert "filings=n/a" in line and "closeRates=n/a" in line and "closeRatesMissing=n/a" in line
    assert "tickers=0" in line


# ---- LLM 입력 ----

def test_llm_input_shows_time_gaps_raw_background_and_computed_index_rates(core, rec):
    run_us(core)
    data = build_llm_input(rec.readied[0])

    (f,) = data["filings"]
    assert f == {"ref": ACCESSION, "tickers": ["TESTA"], "form": "8-K", "items": ["2.02"],
                 "acceptedAt": "2026-10-20T06:00:31+09:00", "hoursBeforeMove": 10.0}
    (r,) = data["overseasCloseRates"]
    assert (r["ref"], r["ticker"], r["rate"], r["hoursBeforeMove"]) == ("U1", "TESTA", "-1.25", 11.0)
    assert data["articles"][0]["minutesFromMove"] == -30

    exchange, summary, index, daily = data["background"]
    assert exchange["category"] == "배경" and exchange["status"] == MARK_PRESENT and exchange["ref"] == "B1"
    assert exchange["receivedAt"] == "2026-10-20T15:55:31+09:00" and exchange["minutesBeforeMove"] == 5
    assert summary["status"] == MARK_TOO_LARGE and "ref" not in summary and "response" not in summary
    assert summary["responseBytes"] == 40_000
    assert daily["status"] == MARK_MISSING and "ref" not in daily and "receivedAt" not in daily
    # 원문은 해석하지 않고 그대로 준다(원문의 0.00도 그대로 남는다).
    assert index["ref"] == "B2" and index["response"] == INDEX_RAW and '" 0.00 "' in index["response"]
    assert index["indexRates"] == [
        {"code": "TESTIX", "name": "TEST INDEX", "businessDay": "20261020", "close": "100.00",
         "prevBusinessDay": "20261019", "prevClose": "98.00", "rate": "2.04"},
        {"code": "FAKEIX", "name": "FAKE INDEX", "businessDay": "20261020", "close": "50.00",
         "prevBusinessDay": MARK_MISSING, "prevClose": MARK_MISSING, "rate": MARK_MISSING},
    ]


def test_world_index_without_computed_rates_from_an_older_response_still_works(core, rec):
    background = [background_item("world-index", "C", response=INDEX_RAW)]
    run_us(core, context(background=background))

    (index,) = build_llm_input(rec.readied[0])["background"]
    assert index["response"] == INDEX_RAW and "indexRates" not in index


def test_system_prompt_explains_background_zero_rates_and_status_marks():
    assert "0.00" in SYSTEM_PROMPT and '"변동 없음"으로 읽지 말고' in SYSTEM_PROMPT
    assert "계산된 rate" in SYSTEM_PROMPT
    assert "배경 자료를 변동의 원인으로 단정하지 않고" in SYSTEM_PROMPT
    for mark in ("잘림", "없음", "너무 큼", "해당 없음"):
        assert f'"{mark}"' in SYSTEM_PROMPT


# ---- LLM 출력과 briefing.generated ----

def output(causes, insufficient=False):
    return json.dumps({"causes": [{"statement": s, "refs": refs} for s, refs in causes],
                       "insufficientEvidence": insufficient}, ensure_ascii=False)


ALL_KINDS = output([("공시가 영향을 줬을 수 있습니다.", [ACCESSION, "a1"]),
                    ("미국 종목 하락과 환율이 함께 나타났습니다.", ["U1", "B1"]),
                    ("세계지수 흐름도 배경으로 있습니다.", ["B2"])])


def report_stage(writer):
    published, failures = Recorder(), Recorder()
    return ReportStage(writer, published, failures, clock=lambda: T + timedelta(minutes=20)), published, failures


def test_evidence_carries_kind_and_ref_for_every_cited_source_and_passes_the_schema(core, rec):
    run_us(core)
    stage, published, failures = report_stage(LlmWriter(FakeLlm(ALL_KINDS)))

    stage.on_ready(rec.readied[0])

    (event,) = published.sent
    assert validate("briefing.generated.v1.schema.json", event.value) == []
    evidence = event.value["payload"]["evidence"]
    assert [(e["kind"], e["ref"]) for e in evidence] == [
        ("filing", ACCESSION), ("article", "a1"), ("overseas-close-rate", "U1"),
        ("exchange-rate-summary", "B1"), ("world-index", "B2")]
    assert [e["source"] for e in evidence] == [
        FILING_SOURCE, "news.example.com", CLOSE_RATE_SOURCE, BACKGROUND_SOURCE, BACKGROUND_SOURCE]
    assert evidence[0]["url"].startswith("https://www.sec.gov/") and "articleId" not in evidence[0]
    assert evidence[1]["articleId"] == "a1"
    assert "url" not in evidence[3] and "(배경)" in evidence[3]["title"]
    assert evidence[3]["publishedAt"] == "2026-10-20T15:55:31+09:00"
    text = event.value["payload"]["text"]
    assert "[근거 1, 2]" in text and "[근거 3, 4]" in text and "[근거 5]" in text
    assert failures.failed == []


@pytest.mark.parametrize("unknown", ["0000000009-26-000009", "B9", "U2", "B3"])
def test_refs_outside_the_given_list_are_rejected_and_the_llm_is_called_again(core, rec, unknown):
    # B3은 너무 큼·없음이라 번호를 받지 못한 배경 자료 자리다.
    run_us(core)
    llm = FakeLlm(output([("지어낸 근거가 영향을 줬을 수 있습니다.", [unknown])]), ALL_KINDS)
    stage, published, failures = report_stage(LlmWriter(llm))

    stage.on_ready(rec.readied[0])

    assert len(llm.calls) == 2
    assert "지어낸 근거" not in published.sent[0].value["payload"]["text"]
    assert failures.failed == []


def test_refs_outside_the_list_three_times_record_llm_error_without_a_report(core, rec):
    run_us(core)
    bad = output([("x", ["B9"])])
    stage, published, failures = report_stage(LlmWriter(FakeLlm(bad, bad, bad)))

    stage.on_ready(rec.readied[0])

    assert published.sent == []
    (failure,) = failures.failed
    assert (failure.reason, failure.attempts) == (FailureReason.LLM_ERROR, 3)


def test_llm_receives_refs_for_every_citable_item(core, rec):
    run_us(core)
    llm = FakeLlm(ALL_KINDS)
    LlmWriter(llm).write(rec.readied[0])

    data = json.loads(llm.calls[0][1])
    refs = [a["ref"] for a in data["articles"]] + [f["ref"] for f in data["filings"]] \
        + [r["ref"] for r in data["overseasCloseRates"]] + [b["ref"] for b in data["background"] if "ref" in b]
    assert refs == ["a1", ACCESSION, "U1", "B1", "B2"]


def test_rule_based_report_lists_articles_and_filings_with_kind_and_ref_and_passes_the_schema(core, rec):
    run_us(core)
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    event = published.sent[0].value
    assert validate("briefing.generated.v1.schema.json", event) == []
    assert [(e["kind"], e["ref"]) for e in event["payload"]["evidence"]] == [
        ("article", "a1"), ("filing", ACCESSION)]
    assert "접수" in event["payload"]["text"]


def test_rule_based_report_without_us_stocks_passes_the_schema(core, rec):
    start(core)
    core.on_history_responded(history(), T)
    core.on_context_responded(json.dumps(context(tickers=(), close_rates=[])), T)
    stable_articles(core)
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    event = published.sent[0].value
    assert validate("briefing.generated.v1.schema.json", event) == []
    assert [(e["kind"], e["ref"]) for e in event["payload"]["evidence"]] == [("article", "a1")]


def test_evidence_titles_never_contain_us_tickers_but_the_llm_input_still_does(core, rec):
    # briefing.generated는 발송기로 가므로 제목에서만 티커를 뺀다. LLM 입력은 그대로 둔다.
    run_us(core)
    llm = FakeLlm(ALL_KINDS)
    stage, published, _ = report_stage(LlmWriter(llm))

    stage.on_ready(rec.readied[0])

    titles = {e["kind"]: e["title"] for e in published.sent[0].value["payload"]["evidence"]}
    assert titles["filing"] == "미국 종목 8-K 공시 항목 2.02"
    assert titles["overseas-close-rate"] == "미국 종목 직전 정규장 등락률 -1.25% (20261019)"
    for title in titles.values():
        assert not any(t in title for t in TICKERS)
    data = json.loads(llm.calls[0][1])
    assert data["filings"][0]["tickers"] == ["TESTA"]
    assert data["overseasCloseRates"][0]["ticker"] == "TESTA"


def test_rule_based_report_titles_and_text_never_contain_us_tickers(core, rec):
    run_us(core)
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    payload = published.sent[0].value["payload"]
    (filing_entry,) = [e for e in payload["evidence"] if e["kind"] == "filing"]
    assert filing_entry["title"] == "미국 종목 8-K 공시 항목 2.02"
    assert "미국 종목 8-K 공시" in payload["text"]
    for t in TICKERS:
        assert t not in payload["text"]
        assert all(t not in e["title"] for e in payload["evidence"])


def test_sample_context_and_filings_responses_are_accepted_unchanged(core, rec):
    ctx = copy.deepcopy(sample("market.context.responded.sample.json"))
    fil = copy.deepcopy(sample("news.filings.responded.sample.json"))
    start(core)
    core.on_history_responded(history(), T)
    core.on_context_responded(json.dumps(ctx), T)
    core.on_filings_responded(json.dumps(fil), T)
    stable_articles(core)

    (ready,) = rec.readied
    assert ready.tickers == ("TESTA", "FAKEB")
    assert len(ready.filings) == 1 and len(ready.close_rates) == 1
    assert [b.status for b in ready.background] == ["ok", "too-large", "ok", "missing"]


def test_logs_of_a_full_run_never_contain_the_symbol_or_tickers(core, rec, caplog):
    with caplog.at_level(logging.DEBUG, logger="briefing"):
        run_us(core)
        stage, _, _ = report_stage(LlmWriter(FakeLlm(output([("x", ["B9"])]), ALL_KINDS)))
        stage.on_ready(rec.readied[0])

    for secret in (SYMBOL, *TICKERS):
        assert secret not in caplog.text
