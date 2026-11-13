"""종목 유형별 시나리오: 가짜 응답과 가짜 LLM으로 변동 1건의 자료 수집부터 리포트까지 돌린다.

실제 Kafka·DB·LLM은 쓰지 않는다. 응답은 계약 샘플을 고쳐 만들고, 나가는 요청과 이벤트는 계약 스키마로 검증한다.
"""
import copy
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from fake_anthropic import as_reply
from briefing.config import CoreSettings
from briefing.jobs import BriefingCore
from briefing.ports import FailureReason
from briefing.report import (
    SYSTEM_PROMPT,
    LlmWriter,
    ReportStage,
    RuleBasedWriter,
    build_llm_input,
)
from briefing.sources import (
    BACKGROUND_SOURCE,
    CLOSE_RATE_SOURCE,
    FILING_SOURCE,
    MARK_MISSING,
    MARK_NOT_APPLICABLE,
    MARK_PRESENT,
    MARK_TOO_LARGE,
    MARK_TRUNCATED,
)
from briefing.queries import QueryKind
from contracts import sample, validate, validate_payload
from fake_api import FakeApi
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
S = CoreSettings()
SEC = timedelta(seconds=1)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
SYMBOL = "TEST01"
TICKERS = ("TESTA", "FAKEB")
ACCESSION = "0000000001-26-000001"
INDEX_RAW = sample("market.context.responded.sample.json")["payload"]["background"][2]["response"]


HISTORY, CONTEXT, ARTICLES, FILINGS = QueryKind.HISTORY, QueryKind.CONTEXT, QueryKind.ARTICLES, QueryKind.FILINGS


class Recorder:
    def __init__(self):
        self.sent, self.readied = [], []
        self.log = FakeJobLog()
        self.api = FakeApi()

    @property
    def failed(self):
        """끝내 실패한 작업의 줄(이전의 실패 테이블 한 줄에 해당)"""
        return self.log.failed

    def send(self, event):
        self.sent.append(event)

    def on_ready(self, report_input):
        self.readied.append(report_input)

    def of(self, topic):
        return [e for e in self.sent if e.topic == topic]


class FakeLlm:
    """정해 둔 응답을 차례로 돌려준다. 실제 API를 부르지 않는다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
        self.calls.append((system, user))
        return self.responses.pop(0)


@pytest.fixture
def rec():
    return Recorder()


@pytest.fixture
def core(rec):
    return BriefingCore(rec, rec.api, rec, job_log=rec.log)


# ---- 가짜 응답 ----

def move():
    event = sample("market.move.sample.json")
    event["eventId"] = JOB_ID
    event["payload"]["symbol"] = SYMBOL
    event["payload"]["asOf"] = T.isoformat()
    return json.dumps(event)


def completed():
    event = sample("news.stored.sample.json")
    event["payload"].update(jobType="collect-news", startedAt=(T + S.delta).isoformat(), incompleteSymbols=[],
                            unprocessedEventIds=[])
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
    core.on_news_stored(completed(), T)


def answer_articles(core, items=None, truncated=False, now=T + SEC):
    """기사는 저장 끝 알림 뒤라 한 번 받으면 끝이다."""
    core.api.reply(articles(1, items, truncated), now)


def run_us(core, context_event=None, filings_event=None, history_event=None, article_items=None,
           articles_truncated=False):
    """미국 종목을 담은 ETF의 변동 1건을 모든 응답이 오는 경우로 끝까지 돌린다."""
    start(core)
    core.api.reply(history_event or history(), T + SEC)
    core.api.reply(json.dumps(context_event or context()), T + SEC)
    core.api.reply(json.dumps(filings_event or filings()), T + 2 * SEC)
    answer_articles(core, article_items, articles_truncated)


# ---- 요청 구간과 순서 ----

def test_us_etf_requests_history_context_and_articles_first_then_filings_with_the_tickers(core, rec):
    start(core)

    assert [q.kind for q in rec.api.submitted] == [HISTORY, CONTEXT, ARTICLES]
    (ctx,) = rec.api.of(CONTEXT)
    assert validate_payload("market.context.requested.v1.schema.json", ctx.payload) == []
    c = ctx.payload
    assert (c["jobId"], c["seq"], c["symbol"]) == (JOB_ID, 1, SYMBOL)
    assert datetime.fromisoformat(c["asOf"]) == T
    assert datetime.fromisoformat(c["since"]) == T - timedelta(days=4)
    assert (c["maxResponseBytes"], c["maxTickers"]) == (32 * 1024, 100)
    (hist,) = rec.api.of(HISTORY)
    h = hist.payload
    assert datetime.fromisoformat(h["since"]) == T - timedelta(minutes=5)
    assert datetime.fromisoformat(h["until"]) == T + S.delta
    assert (h["intervalSeconds"], h["maxPoints"]) == (10, 61)
    assert rec.api.of(ARTICLES)[0].payload["maxArticles"] == 50

    core.api.reply(json.dumps(context()), T + SEC)

    (fil,) = rec.api.of(FILINGS)
    assert validate_payload("news.filings.requested.v1.schema.json", fil.payload) == []
    f = fil.payload
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
    core.api.reply(history(), T)
    core.api.reply(json.dumps(context()), T + 5 * SEC)
    answer_articles(core)
    assert rec.readied == []

    core.api.reply(json.dumps(filings()), T + 12 * SEC)

    assert len(rec.readied) == 1


def test_etf_without_us_stocks_never_requests_filings_and_marks_them_not_applicable(core, rec):
    start(core)
    core.api.reply(history(), T + SEC)
    core.api.reply(json.dumps(context(tickers=(), close_rates=[])), T + SEC)
    answer_articles(core)

    assert rec.api.of(FILINGS) == []
    (ready,) = rec.readied
    assert ready.tickers == () and ready.filings == () and ready.close_rates == ()
    data = build_llm_input(ready)
    assert data["filingsStatus"] == data["overseasCloseRatesStatus"] == MARK_NOT_APPLICABLE
    assert data["filings"] == data["overseasCloseRates"] == data["overseasCloseRatesMissing"] == []
    assert rec.failed == []


# ---- 응답이 끝내 오지 않을 때 ----

def test_context_that_finally_fails_fails_without_a_report_or_filings(core, rec):
    start(core)
    core.api.reply(history(), T)
    answer_articles(core, now=T)
    core.api.fail(CONTEXT, T + timedelta(minutes=1), attempts=4)

    (failure,) = rec.failed
    assert (failure.reason, failure.attempts, failure.job_id) == (FailureReason.CONTEXT_NO_RESPONSE, 4, JOB_ID)
    assert rec.readied == [] and rec.api.of(FILINGS) == []


def test_filings_that_finally_fail_fail_without_a_report(core, rec):
    start(core)
    core.api.reply(history(), T)
    core.api.reply(json.dumps(context()), T + 5 * SEC)
    answer_articles(core, now=T)
    core.api.fail(FILINGS, T + timedelta(minutes=1), attempts=4)

    (failure,) = rec.failed
    assert (failure.reason, failure.attempts) == (FailureReason.FILINGS_NO_RESPONSE, 4)
    assert rec.readied == []


def test_history_failure_fails_even_when_every_other_response_arrived(core, rec):
    start(core)
    core.api.reply(json.dumps(context()), T)
    core.api.reply(json.dumps(filings()), T)
    core.api.reply(articles(1), T)
    core.api.fail(HISTORY, T + timedelta(minutes=1))

    assert [f.reason for f in rec.failed] == [FailureReason.MARKET_NO_RESPONSE]
    assert rec.readied == []


def test_articles_failure_fails_even_when_every_other_response_arrived(core, rec):
    start(core)
    core.api.reply(history(), T)
    core.api.reply(json.dumps(context()), T)
    core.api.reply(json.dumps(filings()), T)
    core.api.fail(ARTICLES, T + timedelta(minutes=1))

    assert [f.reason for f in rec.failed] == [FailureReason.NEWS_NO_RESPONSE]
    assert rec.readied == []


def test_waiting_for_query_results_sets_no_deadline(core):
    start(core)
    core.api.reply(history(), T)
    # 조회 결과는 콜백으로 오고 재시도는 조회 구현이 하므로, 근거 조회 중에는 깨어날 시각이 없다.
    assert core.next_deadline() is None
    core.api.reply(json.dumps(context()), T + 3 * SEC)
    assert core.next_deadline() is None


def test_a_context_result_for_a_finished_job_is_ignored(core, rec, caplog):
    start(core)
    core.api.fail(HISTORY, T)
    with caplog.at_level("INFO", logger="briefing.jobs"):
        core.api.reply(json.dumps(context()), T + SEC)
    assert rec.api.of(FILINGS) == []
    assert "context result for unknown or finished job" in caplog.text


@pytest.mark.parametrize("kind", [CONTEXT, FILINGS])
def test_a_context_or_filings_result_without_its_fields_counts_as_not_received(core, rec, kind):
    start(core)
    if kind is FILINGS:
        core.api.reply(json.dumps(context()), T)
    core.api.reply({"jobId": JOB_ID, "seq": 1}, T, kind=kind)

    (failure,) = rec.failed
    assert failure.error_type == "MalformedResponse"


def test_malformed_items_in_a_context_response_count_as_not_received(core, rec):
    start(core)
    event = context()
    del event["payload"]["closeRates"][0]["closeAt"]

    core.api.reply(json.dumps(event), T)
    (failure,) = rec.failed
    assert (failure.reason, failure.error_type) == (FailureReason.CONTEXT_NO_RESPONSE.value, "MalformedResponse")


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
    core.api.reply(history(), T)
    core.api.reply(json.dumps(context(tickers=(), close_rates=[])), T)
    with caplog.at_level("INFO", logger="briefing.jobs"):
        answer_articles(core)

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
    """(문장, 근거) 묶음을 원인 묶음으로 바꾼다. 구체 문장과 뜻 문장이 같은 근거를 인용한다."""
    return json.dumps({"causes": [{"title": s, "detail": s, "refs": refs, "meaning": s, "meaningRefs": refs}
                                  for s, refs in causes],
                       "insufficientEvidence": insufficient}, ensure_ascii=False)


ALL_KINDS = output([("공시가 영향을 줬을 수 있습니다.", [ACCESSION, "a1"]),
                    ("미국 종목 하락과 환율이 함께 나타났습니다.", ["U1", "B1"]),
                    ("세계지수 흐름도 배경으로 있습니다.", ["B2"])])


def report_stage(writer):
    published, job_log = Recorder(), FakeJobLog()
    return ReportStage(writer, published, job_log, clock=lambda: T + timedelta(minutes=20)), published, job_log


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
    core.api.reply(history(), T)
    core.api.reply(json.dumps(context(tickers=(), close_rates=[])), T)
    answer_articles(core)
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    event = published.sent[0].value
    assert validate("briefing.generated.v1.schema.json", event) == []
    assert [(e["kind"], e["ref"]) for e in event["payload"]["evidence"]] == [("article", "a1")]


def test_evidence_titles_name_the_us_ticker_so_each_company_can_be_told_apart(core, rec):
    # 본문이 회사를 티커로 말하므로 근거 제목에도 같은 티커가 있어야 어느 회사 자료인지 맞춰 볼 수 있다.
    run_us(core)
    llm = FakeLlm(ALL_KINDS)
    stage, published, _ = report_stage(LlmWriter(llm))

    stage.on_ready(rec.readied[0])

    titles = {e["kind"]: e["title"] for e in published.sent[0].value["payload"]["evidence"]}
    assert titles["filing"] == "TESTA 8-K 공시 항목 2.02"
    assert titles["overseas-close-rate"] == "TESTA 직전 정규장 등락률 -1.25% (20261019)"
    data = json.loads(llm.calls[0][1])
    assert data["filings"][0]["tickers"] == ["TESTA"]
    assert data["overseasCloseRates"][0]["ticker"] == "TESTA"


def test_rule_based_report_titles_and_text_name_the_us_ticker(core, rec):
    run_us(core)
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    payload = published.sent[0].value["payload"]
    (filing_entry,) = [e for e in payload["evidence"] if e["kind"] == "filing"]
    assert filing_entry["title"] == "TESTA 8-K 공시 항목 2.02"
    assert "TESTA 8-K 공시" in payload["text"]


def test_sample_context_and_filings_responses_are_accepted_unchanged(core, rec):
    ctx = copy.deepcopy(sample("market.context.responded.sample.json"))
    fil = copy.deepcopy(sample("news.filings.responded.sample.json"))
    start(core)
    core.api.reply(history(), T)
    core.api.reply(json.dumps(ctx), T)
    core.api.reply(json.dumps(fil), T)
    answer_articles(core)

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


# ---- 원인 묶음 본문과 비교 숫자 ----

def market_raw():
    rows = [{"indx_nm": "KOSPI종합", "now_indx_p2": "  2222.22", "bdy_cmpr_p2": "    12.34", "up_dwn_r_p2": "  -0.45",
             "bdy_cmpr_ccd": "5"},
            {"indx_nm": "KOSDAQ종합", "now_indx_p2": "   777.77", "bdy_cmpr_p2": "     3.21", "up_dwn_r_p2": "   0.30",
             "bdy_cmpr_ccd": "2"}]
    return json.dumps({"dataBody": {"out2": rows}}, ensure_ascii=False)


def figure_background():
    return [background_item("market-summary", response=market_raw()),
            background_item("world-index", "S", response=INDEX_RAW, index_rates=INDEX_RATES)]


def run_with_figures(core):
    run_us(core, context_event=context(background=figure_background()))


def figure_ref(report_input, name):
    from briefing.report import figure_entries
    (entry,) = [e for e in figure_entries(report_input) if e.name == name]
    return entry.ref


def test_meaning_sentence_cites_the_market_comparison_and_the_report_shows_the_article_link(core, rec):
    run_with_figures(core)
    ready = rec.readied[0]
    # 샘플 변동은 짧은 시간 변동이라 오늘 등락률로 시장과 비교한다.
    vs_kospi = figure_ref(ready, "오늘 등락률 - 코스피종합 등락률")
    llm_out = json.dumps({"causes": [{
        "title": "미국 종목 실적 공시", "detail": "미국 종목이 8-K 항목 2.02를 냈으며 어느 부분이 부진했는지는 자료에 없음.",
        "refs": [ACCESSION, "a1"], "meaning": "오늘 등락률이 코스피종합 등락률보다 낮아 시장 전체보다 더 빠졌습니다.",
        "meaningRefs": [vs_kospi]}], "insufficientEvidence": False}, ensure_ascii=False)
    stage, published, _ = report_stage(LlmWriter(FakeLlm(llm_out)))

    stage.on_ready(ready)

    event = published.sent[0].value
    assert validate("briefing.generated.v1.schema.json", event) == []
    text = event["payload"]["text"]
    assert "1. 미국 종목 실적 공시" in text
    assert "자료에 없음" in text
    assert "시장 전체보다 더 빠졌습니다. [근거 3]" in text
    assert "https://news.example.com/a1" in text and "https://www.sec.gov/Archives/test/" in text
    evidence = event["payload"]["evidence"]
    assert [e["ref"] for e in evidence] == [ACCESSION, "a1", vs_kospi]
    assert evidence[2]["kind"] == "market-summary" and evidence[2]["publishedAt"].startswith("2026-10-20T15:55")


def test_rule_based_report_states_code_computed_comparisons_without_guessing_causes(core, rec):
    run_with_figures(core)
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    event = published.sent[0].value
    assert validate("briefing.generated.v1.schema.json", event) == []
    text = event["payload"]["text"]
    # 샘플 변동은 짧은 시간 변동이라 오늘 등락률 -2.1로 비교한다. 코스피종합 -0.45 → 차이 -1.65%p, 코스닥종합 0.30 → -2.40%p
    assert "오늘 등락률이 코스피종합 등락률보다 1.65%p 낮습니다 (차이 -1.65%p" in text
    assert "오늘 등락률이 코스닥종합 등락률보다 2.40%p 낮습니다" in text
    assert "담긴 미국 종목은 직전 정규장에서 0개 오르고 1개 내렸으며 0개는 보합이었고, 평균 등락률은 -1.25%" in text
    assert "https://news.example.com/a1" in text and "확인하지 못한 것:" in text
    assert "1. " not in text.split("근거 후보:")[0]  # 원인 묶음 없음


def test_unchecked_items_name_missing_truncated_and_too_large_data_without_tickers(core, rec):
    run_us(core, context_event=context(truncated=True), filings_event=filings(truncated=True),
           history_event=history(truncated=True))
    stage, published, _ = report_stage(RuleBasedWriter())

    stage.on_ready(rec.readied[0])

    text = published.sent[0].value["payload"]["text"]
    unchecked = text.split("확인하지 못한 것:")[1]
    for line in ("- 시세 이력: 상한 때문에 일부만 받음", "- 미국 공시: 상한 때문에 일부만 받음",
                 "- 미국 종목 목록: 상한 때문에 일부만 받음",
                 "- 미국 종목 직전 정규장 등락률: 2개 가운데 1개 받지 못함",
                 "- 시장종합: 원문이 커서 빠짐", "- 국내 일봉: 기간 안에 받은 자료 없음"):
        assert line in unchecked
    for ticker in TICKERS:
        assert ticker not in unchecked


def test_insufficient_evidence_keeps_the_headline_and_unchecked_items_in_the_recorded_text_but_sends_nothing(core, rec):
    run_us(core)
    stage, published, job_log = report_stage(LlmWriter(FakeLlm(output([], insufficient=True))))

    stage.on_ready(rec.readied[0])

    # 인용한 근거가 없어 메일 이벤트는 없고, 본문은 작업 기록에 남는다.
    assert published.sent == []
    text = job_log.finishes[0].final_text
    assert text.splitlines()[0].startswith("[TEST ETF ONE(TEST01)] ")
    assert "추정하기 어렵습니다" in text and "- 시장종합: 원문이 커서 빠짐" in text
    assert "근거 1" not in text


# ---- 변동 T+5분 재검색: 정기 수집 없이 끝나는 변동, 근거가 없어 보내지 않는 변동 ----

ETF_WITH_NAME = {"name": "가짜 반도체 ETF", "membersTruncated": False,
                 "members": [{"market": "KR", "symbol": "TESTB", "rank": 1, "name": "첫째회사"}]}


def early_research_completed(research_id):
    event = sample("news.stored.sample.json")
    event["payload"].update(jobType="research-news", startedAt=(T + S.early_research_delay).isoformat(),
                            incompleteSymbols=[], unprocessedEventIds=[], researchJobId=research_id)
    return json.dumps(event)


def run_without_the_regular_collection(core, rec):
    """정기 수집 완료는 끝내 오지 않는다. T+5분 재검색의 완료만으로 리포트 입력까지 간다."""
    from briefing.jobs import early_research_id
    core.on_move(move(), T)
    core.on_tick(T + S.early_research_delay)
    request_at = T + S.early_research_delay
    context_event = context(tickers=())
    context_event["payload"]["etf"] = ETF_WITH_NAME
    core.api.reply(json.dumps(context_event), request_at + SEC)
    assert core.on_research_stored(early_research_completed(early_research_id(JOB_ID)), request_at + 20 * SEC)
    core.api.reply(history(), request_at + 21 * SEC)
    answer_articles(core, now=request_at + 21 * SEC)


def test_a_move_finishes_into_a_briefing_event_without_the_regular_news_collection(core, rec):
    run_without_the_regular_collection(core, rec)
    (research,) = rec.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", research.value) == []
    assert research.value["payload"]["queries"] == ["가짜 반도체 ETF", "첫째회사"]
    assert rec.failed == []
    stage, published, job_log = report_stage(LlmWriter(FakeLlm(output([("기사가 영향을 줬을 수 있습니다.", ["a1"])]))))

    stage.on_ready(rec.readied[0])

    (event,) = published.sent
    assert validate("briefing.generated.v1.schema.json", event.value) == []
    assert [e["articleId"] for e in event.value["payload"]["evidence"]] == ["a1"]
    assert job_log.finishes[0].result_event_id == event.value["eventId"]


def test_a_move_without_any_cited_evidence_sends_no_event_and_leaves_the_result_in_the_job_record(core, rec):
    run_without_the_regular_collection(core, rec)
    stage, published, job_log = report_stage(LlmWriter(FakeLlm(output([], insufficient=True))))

    stage.on_ready(rec.readied[0])

    assert published.sent == []
    (result,) = job_log.finishes
    assert (result.failure_reason, result.status.value, result.result_event_id) == ("no_evidence", "succeeded", None)
    (entry,) = job_log.of_reason("no_evidence")
    assert entry.data["receivedCounts"]["articles"] == 1 and entry.data["finalText"] == result.final_text


# ---- 급변동 실패 기록 (근거 조회) ----

@pytest.fixture
def failure_log(rec):
    return rec.log


@pytest.fixture
def logged_core(rec):
    return BriefingCore(rec, rec.api, rec, job_log=rec.log)


def filled(record):
    from dataclasses import fields
    # 문장 단위 칸(문장 번호, 회차, 고치기 전·후)은 문장 단위 줄에만 채워진다.
    optional = ("sentence_no", "round", "before", "after")
    return all(getattr(record, f.name) not in (None, "", {}, []) for f in fields(record) if f.name not in optional)


def test_a_job_that_succeeds_leaves_no_resend_or_recheck_lines(logged_core, rec, failure_log):
    run_us(logged_core)

    assert len(rec.readied) == 1 and rec.failed == []
    # 다시 부른 시도는 조회 구현이 job_step에 남기고, 기사 안정 확인 라운드는 없다.
    assert failure_log.of_type("resent") == [] and failure_log.of_type("check-round") == []
    assert len(rec.api.of(ARTICLES)) == 1


def test_a_job_that_finally_fails_records_both_the_old_failure_and_a_full_move_failure(logged_core, rec, failure_log):
    from briefing.job_log import STAGE_FILINGS
    start(logged_core)
    logged_core.api.reply(history(), T + SEC)
    logged_core.api.reply(json.dumps(context()), T + SEC)
    answer_articles(logged_core)
    logged_core.api.fail(FILINGS, T + timedelta(minutes=1), attempts=4, failure_kind="http-5xx")

    (old,) = rec.failed
    assert old.reason == FailureReason.FILINGS_NO_RESPONSE
    final = old
    (result,) = failure_log.results()
    assert result.status == "failed" and result.failure_reason == "filings_no_response"
    assert filled(final)
    assert (final.stage, final.reason, final.attempts, final.error_type) == (
        STAGE_FILINGS, "filings_no_response", 4, "http-5xx")
    assert final.data["notReceived"] == [STAGE_FILINGS]
    assert final.data["requestRange"]["tickers"] == list(TICKERS)
    assert len(final.data["requests"][STAGE_FILINGS]) == 1
    # 끝내 실패한 기록에도 받은 응답 내용 전부: 시세 점, 배경 자료 원문·지수별 계산값, 등락률, 기사
    responses = final.data["receivedResponses"]
    assert set(responses) == {"market.history", "market.context", "news.articles"}
    assert responses["market.history"]["points"] == json.loads(history())["payload"]["points"]
    world = [b for b in responses["market.context"]["background"] if b["apiType"] == "world-index"][0]
    assert world["response"] == INDEX_RAW and world["indexRates"] == INDEX_RATES
    assert responses["market.context"]["closeRates"] == [close_rate("TESTA")]
    assert responses["news.articles"]["articles"] == [article("a1", T - timedelta(minutes=30))]
    assert STAGE_FILINGS not in responses


def test_waiting_for_collection_until_the_limit_is_recorded(logged_core, rec, failure_log):
    from briefing.job_log import STAGE_COLLECTION
    logged_core.on_move(move(), T)
    logged_core.on_tick(T + S.collection_wait_limit)

    (record,) = failure_log.failed
    (result,) = failure_log.results()
    assert result.failure_reason == "collection_not_covered" and result.status == "failed"
    assert filled(record)
    assert (record.stage, record.reason, record.attempts) == (STAGE_COLLECTION, "collection_not_covered", 0)
    assert record.data["missingItems"] == ["news.stored"]


def test_move_failure_logs_never_contain_the_symbol_or_tickers(logged_core, rec, failure_log, caplog):
    with caplog.at_level(logging.DEBUG, logger="briefing"):
        start(logged_core)
        logged_core.api.reply(history(), T + SEC)
        logged_core.api.reply(json.dumps(context()), T + SEC)
        answer_articles(logged_core)
        logged_core.api.fail(FILINGS, T + timedelta(minutes=1))

    assert failure_log.entries
    for secret in (SYMBOL, *TICKERS):
        assert secret not in caplog.text


def test_a_move_that_succeeds_after_a_rejected_llm_attempt_leaves_every_kind_of_line_under_one_job_id(rec):
    published = Recorder()
    stage = ReportStage(LlmWriter(FakeLlm("not json", output([("기사가 영향을 줬을 수 있습니다.", ["a1"])]))),
                        published, rec.log, clock=lambda: T + timedelta(minutes=20))
    core = BriefingCore(rec, rec.api, stage, job_log=rec.log)
    start(core)
    core.api.reply(history(), T + SEC)
    core.api.reply(articles(1), T + SEC)
    core.api.reply(json.dumps(context()), T + 20 * SEC)
    core.api.reply(json.dumps(filings()), T + 21 * SEC)

    entries = rec.log.entries
    assert {e.job_id for e in entries} == {JOB_ID} and [r.job_kind for r in rec.log.starts] == ["move"]
    assert [e.entry_type for e in entries] == ["llm-rejected", "report-written", "missing-items"]
    assert rec.log.failed == [] and len(published.sent) == 1
    (result,) = rec.log.results()
    assert result.status == "succeeded" and result.job_id == JOB_ID and result.final_text
