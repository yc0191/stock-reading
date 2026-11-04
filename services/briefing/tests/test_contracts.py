import json
import re
from datetime import datetime, timedelta

import pytest

from briefing.compose import KST
from briefing.events import (EDITION_MORNING, EDITION_POST_CLOSE, JOB_TYPE_BY_EDITION, RESEARCH_JOB_KINDS,
                             daily_briefing_event, research_request_event)
from contracts import CONTRACTS_DIR, sample, validate


def test_committed_move_sample_matches_its_schema():
    assert validate("market.move.v1.schema.json", sample("market.move.sample.json")) == []


def test_committed_briefing_sample_matches_its_schema():
    assert validate("briefing.generated.v1.schema.json", sample("briefing.generated.sample.json")) == []


def test_timestamp_without_seconds_is_rejected_proving_format_checks_are_on():
    broken = sample("briefing.generated.sample.json")
    broken["occurredAt"] = "2026-10-20T16:00+09:00"

    assert validate("briefing.generated.v1.schema.json", broken) != []


def test_unknown_fields_are_tolerated_for_forward_compatibility():
    future = sample("market.move.sample.json")
    future["newTopLevelHint"] = "x"
    future["payload"]["venue"] = "future"

    assert validate("market.move.v1.schema.json", future) == []


def test_move_sample_carries_detection_fields_and_older_form_still_valid():
    current = sample("market.move.sample.json")
    assert current["payload"]["detectionType"] == "window"
    assert "moveStartedAt" in current["payload"]
    assert validate("market.move.v1.schema.json", current) == []

    older = sample("market.move.sample.json")
    del older["payload"]["detectionType"]
    del older["payload"]["moveStartedAt"]
    assert validate("market.move.v1.schema.json", older) == []


def test_move_with_unknown_detection_type_is_rejected():
    broken = sample("market.move.sample.json")
    broken["payload"]["detectionType"] = "other"

    assert validate("market.move.v1.schema.json", broken) != []


EVIDENCE_CONTRACTS = [
    ("collection.completed.v1.schema.json", "collection.completed.sample.json"),
    ("market.history.requested.v1.schema.json", "market.history.requested.sample.json"),
    ("market.history.responded.v1.schema.json", "market.history.responded.sample.json"),
    ("news.articles.requested.v1.schema.json", "news.articles.requested.sample.json"),
    ("news.articles.responded.v1.schema.json", "news.articles.responded.sample.json"),
    ("market.context.requested.v1.schema.json", "market.context.requested.sample.json"),
    ("market.context.responded.v1.schema.json", "market.context.responded.sample.json"),
    ("news.filings.requested.v1.schema.json", "news.filings.requested.sample.json"),
    ("news.filings.responded.v1.schema.json", "news.filings.responded.sample.json"),
    ("news.research.requested.v1.schema.json", "news.research.requested.sample.json"),
]


@pytest.mark.parametrize("schema_file, sample_file", EVIDENCE_CONTRACTS)
def test_evidence_samples_match_their_schemas(schema_file, sample_file):
    assert validate(schema_file, sample(sample_file)) == []


@pytest.mark.parametrize("schema_file, sample_file", EVIDENCE_CONTRACTS)
def test_evidence_events_tolerate_unknown_fields(schema_file, sample_file):
    future = sample(sample_file)
    future["newTopLevelHint"] = "x"
    future["payload"]["venue"] = "future"

    assert validate(schema_file, future) == []


@pytest.mark.parametrize("schema_file, sample_file", EVIDENCE_CONTRACTS)
def test_evidence_events_reject_missing_payload_field(schema_file, sample_file):
    for field in sample(sample_file)["payload"]:
        broken = sample(sample_file)
        del broken["payload"][field]

        assert validate(schema_file, broken) != [], field


@pytest.mark.parametrize("schema_file, sample_file", EVIDENCE_CONTRACTS)
def test_evidence_events_reject_wrong_event_type(schema_file, sample_file):
    broken = sample(sample_file)
    broken["eventType"] = "market.move"

    assert validate(schema_file, broken) != []


def test_evidence_job_id_is_the_move_event_id():
    move = sample("market.move.sample.json")
    for sample_file in (
        "market.history.requested.sample.json",
        "market.history.responded.sample.json",
        "news.articles.requested.sample.json",
        "news.articles.responded.sample.json",
        "market.context.requested.sample.json",
        "market.context.responded.sample.json",
        "news.filings.requested.sample.json",
        "news.filings.responded.sample.json",
        "news.research.requested.sample.json",
    ):
        assert sample(sample_file)["payload"]["jobId"] == move["eventId"], sample_file
        assert sample(sample_file)["correlationId"] == move["correlationId"], sample_file


def test_collection_completed_rejects_duplicate_or_malformed_incomplete_symbols():
    for bad in (["TEST01", "TEST01"], ["test01"], ["TEST1"], [""]):
        broken = sample("collection.completed.sample.json")
        broken["payload"]["incompleteSymbols"] = bad

        assert validate("collection.completed.v1.schema.json", broken) != [], bad


def test_collection_completed_accepts_empty_and_market_wide_incomplete_symbols():
    for ok in ([], ["*"], ["*", "TEST01"]):
        good = sample("collection.completed.sample.json")
        good["payload"]["incompleteSymbols"] = ok

        assert validate("collection.completed.v1.schema.json", good) == [], ok


def test_collection_completed_rejects_negative_published_count():
    broken = sample("collection.completed.sample.json")
    broken["payload"]["publishedCount"] = -1

    assert validate("collection.completed.v1.schema.json", broken) != []


def test_requests_reject_bad_sequence_symbol_and_limit():
    cases = [
        ("market.history.requested", "seq", 0),
        ("market.history.requested", "symbol", "*"),
        ("market.history.requested", "maxPoints", 0),
        ("news.articles.requested", "seq", 0),
        ("news.articles.requested", "symbol", "*"),
        ("news.articles.requested", "maxArticles", 0),
        ("market.context.requested", "seq", 0),
        ("market.context.requested", "symbol", "*"),
        ("market.context.requested", "maxResponseBytes", 0),
        ("market.context.requested", "maxTickers", 0),
        ("news.filings.requested", "seq", 0),
        ("news.filings.requested", "tickers", []),
        ("news.filings.requested", "tickers", ["TESTA", "TESTA"]),
        ("news.filings.requested", "tickers", ["testa"]),
        ("news.filings.requested", "maxFilings", 0),
        ("news.research.requested", "seq", 0),
        ("news.research.requested", "symbol", "test01"),
        ("news.research.requested", "jobKind", "noon"),
        ("news.research.requested", "queries", []),
        ("news.research.requested", "queries", ["테스트1", "테스트2", "테스트3", "테스트4"]),
        ("news.research.requested", "queries", ["테스트1", "테스트1"]),
        ("news.research.requested", "queries", [""]),
    ]
    for topic, field, value in cases:
        broken = sample(f"{topic}.sample.json")
        broken["payload"][field] = value

        assert validate(f"{topic}.v1.schema.json", broken) != [], (topic, field)


def test_requests_reject_timestamp_without_seconds():
    for topic in ("market.history.requested", "news.articles.requested", "market.context.requested",
                  "news.filings.requested"):
        broken = sample(f"{topic}.sample.json")
        broken["payload"]["since"] = "2026-10-20T15:55+09:00"

        assert validate(f"{topic}.v1.schema.json", broken) != [], topic


def test_responses_accept_zero_results_as_a_received_answer():
    empty_points = sample("market.history.responded.sample.json")
    empty_points["payload"]["points"] = []
    empty_articles = sample("news.articles.responded.sample.json")
    empty_articles["payload"]["articles"] = []

    empty_filings = sample("news.filings.responded.sample.json")
    empty_filings["payload"]["filings"] = []
    no_us_tickers = sample("market.context.responded.sample.json")
    no_us_tickers["payload"]["tickers"] = []
    no_us_tickers["payload"]["closeRates"] = []

    assert validate("market.history.responded.v1.schema.json", empty_points) == []
    assert validate("news.articles.responded.v1.schema.json", empty_articles) == []
    assert validate("news.filings.responded.v1.schema.json", empty_filings) == []
    assert validate("market.context.responded.v1.schema.json", no_us_tickers) == []


def test_responses_reject_malformed_items():
    negative_price = sample("market.history.responded.sample.json")
    negative_price["payload"]["points"][0]["price"] = -1
    no_article_id = sample("news.articles.responded.sample.json")
    del no_article_id["payload"]["articles"][0]["articleId"]
    no_truncated = sample("news.articles.responded.sample.json")
    del no_truncated["payload"]["truncated"]

    assert validate("market.history.responded.v1.schema.json", negative_price) != []
    assert validate("news.articles.responded.v1.schema.json", no_article_id) != []
    assert validate("news.articles.responded.v1.schema.json", no_truncated) != []


def test_briefing_evidence_carries_article_id_and_url_and_older_form_still_valid():
    current = sample("briefing.generated.sample.json")
    assert current["payload"]["evidence"][0]["articleId"]
    assert validate("briefing.generated.v1.schema.json", current) == []

    older = sample("briefing.generated.sample.json")
    for item in older["payload"]["evidence"]:
        del item["articleId"]
        del item["url"]
    assert validate("briefing.generated.v1.schema.json", older) == []


def test_briefing_evidence_rejects_empty_article_id():
    # url의 uri 형식은 검사하지 않는다. jsonschema가 선택 패키지 없이는 uri를 검사하지 못하고
    # contracts.py가 date-time만 직접 등록하기 때문이다(news.collected의 url도 같다).
    broken = sample("briefing.generated.sample.json")
    broken["payload"]["evidence"][0]["articleId"] = ""

    assert validate("briefing.generated.v1.schema.json", broken) != []


def test_history_request_accepts_optional_interval_and_rejects_non_positive():
    with_interval = sample("market.history.requested.sample.json")
    with_interval["payload"]["intervalSeconds"] = 10
    assert validate("market.history.requested.v1.schema.json", with_interval) == []

    for bad in (0, -10, "10"):
        broken = sample("market.history.requested.sample.json")
        broken["payload"]["intervalSeconds"] = bad
        assert validate("market.history.requested.v1.schema.json", broken) != [], bad


def test_briefing_evidence_carries_kind_and_ref_and_older_form_still_valid():
    current = sample("briefing.generated.sample.json")
    assert current["payload"]["evidence"][0]["kind"] == "article"
    assert current["payload"]["evidence"][0]["ref"]
    assert validate("briefing.generated.v1.schema.json", current) == []

    for kind in ("article", "filing", "overseas-close-rate", "exchange-rate-summary", "market-summary",
                 "world-index", "daily-price"):
        other = sample("briefing.generated.sample.json")
        other["payload"]["evidence"][0]["kind"] = kind
        assert validate("briefing.generated.v1.schema.json", other) == [], kind

    older = sample("briefing.generated.sample.json")
    for item in older["payload"]["evidence"]:
        del item["kind"]
        del item["ref"]
    assert validate("briefing.generated.v1.schema.json", older) == []


def test_briefing_evidence_rejects_unknown_kind_and_empty_ref():
    unknown_kind = sample("briefing.generated.sample.json")
    unknown_kind["payload"]["evidence"][0]["kind"] = "rumor"
    empty_ref = sample("briefing.generated.sample.json")
    empty_ref["payload"]["evidence"][0]["ref"] = ""

    assert validate("briefing.generated.v1.schema.json", unknown_kind) != []
    assert validate("briefing.generated.v1.schema.json", empty_ref) != []


def _background(status: str, **fields) -> dict:
    item = {"apiType": "market-summary", "target": None, "status": status}
    item.update(fields)
    return item


def test_context_background_fields_must_match_status():
    fetched = "2026-10-20T15:55:03+09:00"
    good = [
        _background("ok", response="{}", fetchedAt=fetched),
        _background("too-large", fetchedAt=fetched, responseBytes=40000),
        _background("missing"),
    ]
    bad = [
        _background("ok", fetchedAt=fetched),
        _background("ok", response="{}"),
        _background("ok", response="{}", fetchedAt=fetched, responseBytes=1),
        _background("too-large", fetchedAt=fetched),
        _background("too-large", response="{}", fetchedAt=fetched, responseBytes=40000),
        _background("missing", fetchedAt=fetched),
        _background("missing", response="{}"),
        _background("unknown"),
    ]
    for item in good:
        event = sample("market.context.responded.sample.json")
        event["payload"]["background"] = [item]
        assert validate("market.context.responded.v1.schema.json", event) == [], item
    for item in bad:
        event = sample("market.context.responded.sample.json")
        event["payload"]["background"] = [item]
        assert validate("market.context.responded.v1.schema.json", event) != [], item


def test_context_background_target_follows_its_kind():
    cases = [
        ("daily-price", "TEST01", True),
        ("daily-price", None, False),
        ("world-index", "S", True),
        ("world-index", None, False),
        ("exchange-rate-summary", None, True),
        ("exchange-rate-summary", "S", False),
        ("overseas-daily-price", "TESTA", False),
    ]
    for api_type, target, valid in cases:
        event = sample("market.context.responded.sample.json")
        event["payload"]["background"] = [{"apiType": api_type, "target": target, "status": "missing"}]
        errors = validate("market.context.responded.v1.schema.json", event)
        assert (errors == []) is valid, (api_type, target)


def test_context_response_rejects_malformed_tickers_and_close_rates():
    bad_ticker = sample("market.context.responded.sample.json")
    bad_ticker["payload"]["tickers"] = ["testa"]
    no_close_date = sample("market.context.responded.sample.json")
    del no_close_date["payload"]["closeRates"][0]["date"]
    no_truncated = sample("market.context.responded.sample.json")
    del no_truncated["payload"]["truncated"]

    for broken in (bad_ticker, no_close_date, no_truncated):
        assert validate("market.context.responded.v1.schema.json", broken) != []


def test_context_world_index_carries_computed_rates_next_to_the_raw_response():
    world = sample("market.context.responded.sample.json")["payload"]["background"][2]

    assert world["apiType"] == "world-index"
    assert world["response"]
    assert world["indexRates"][0]["rate"] == "2.04"
    # 직전 행이 없는 지수는 키를 남기고 값만 null 이다.
    assert world["indexRates"][1]["rate"] is None
    assert world["indexRates"][1]["prevClose"] is None


def test_context_index_rates_are_optional_and_only_on_world_index_with_data():
    without = sample("market.context.responded.sample.json")
    del without["payload"]["background"][2]["indexRates"]
    on_summary = sample("market.context.responded.sample.json")
    on_summary["payload"]["background"][0]["indexRates"] = []
    on_missing = sample("market.context.responded.sample.json")
    on_missing["payload"]["background"][2] = {
        "apiType": "world-index", "target": "S", "status": "missing", "indexRates": []}
    no_rate_key = sample("market.context.responded.sample.json")
    del no_rate_key["payload"]["background"][2]["indexRates"][0]["rate"]
    numeric_rate = sample("market.context.responded.sample.json")
    numeric_rate["payload"]["background"][2]["indexRates"][0]["rate"] = 2.04

    assert validate("market.context.responded.v1.schema.json", without) == []
    for broken in (on_summary, on_missing, no_rate_key, numeric_rate):
        assert validate("market.context.responded.v1.schema.json", broken) != []


def test_filings_response_carries_metadata_only():
    filing = sample("news.filings.responded.sample.json")["payload"]["filings"][0]
    assert set(filing) == {"accessionNumber", "tickers", "form", "items", "acceptedAt", "url"}

    bad_accession = sample("news.filings.responded.sample.json")
    bad_accession["payload"]["filings"][0]["accessionNumber"] = "1-26-1"
    no_tickers = sample("news.filings.responded.sample.json")
    no_tickers["payload"]["filings"][0]["tickers"] = []

    assert validate("news.filings.responded.v1.schema.json", bad_accession) != []
    assert validate("news.filings.responded.v1.schema.json", no_tickers) != []


def test_new_samples_use_fake_symbols_and_tickers_only():
    for topic in ("market.context.requested", "market.context.responded", "news.filings.requested",
                  "news.filings.responded"):
        payload = sample(f"{topic}.sample.json")["payload"]
        assert payload["symbol"].startswith("TEST"), topic
        tickers = list(payload.get("tickers", []))
        tickers += [t for filing in payload.get("filings", []) for t in filing["tickers"]]
        tickers += [rate["ticker"] for rate in payload.get("closeRates", [])]
        assert all(t.startswith(("TEST", "FAKE")) for t in tickers), topic


DAILY_SCHEMA = "briefing.daily.generated.v1.schema.json"
DAILY_SAMPLE = "briefing.daily.generated.sample.json"


def test_committed_daily_briefing_sample_matches_its_schema():
    assert validate(DAILY_SCHEMA, sample(DAILY_SAMPLE)) == []


def test_daily_briefing_tolerates_unknown_fields():
    future = sample(DAILY_SAMPLE)
    future["newTopLevelHint"] = "x"
    future["payload"]["venue"] = "future"

    assert validate(DAILY_SCHEMA, future) == []


def test_daily_briefing_rejects_missing_payload_field():
    for field in sample(DAILY_SAMPLE)["payload"]:
        broken = sample(DAILY_SAMPLE)
        del broken["payload"][field]

        assert validate(DAILY_SCHEMA, broken) != [], field


@pytest.mark.parametrize("field, value", [
    ("edition", "noon"),
    ("text", ""),
    ("rangeStart", "2026-10-19T17:00+09:00"),
    ("missingItems", [""]),
])
def test_daily_briefing_rejects_invalid_payload_values(field, value):
    broken = sample(DAILY_SAMPLE)
    broken["payload"][field] = value

    assert validate(DAILY_SCHEMA, broken) != []


def test_daily_briefing_carries_no_personal_fields():
    schema = json.loads((CONTRACTS_DIR / DAILY_SCHEMA).read_text(encoding="utf-8"))
    names = set(schema["properties"]["payload"]["properties"])

    # 보유 수량·금액·주소는 이 이벤트에 담지 않는다. 수신 주소는 발송기 설정에만 있다.
    assert not {n for n in names if re.search(r"quantity|amount|holding|recipient|email|address|account|balance", n, re.I)}


@pytest.mark.parametrize("edition, generated_by, missing", [
    (EDITION_MORNING, "llm", []),
    (EDITION_POST_CLOSE, "rule-based", ["us-filings"]),
])
def test_daily_briefing_event_built_by_the_producer_matches_the_schema(edition, generated_by, missing):
    at = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
    event = daily_briefing_event(
        tick_event_id="0b8c2d3e-4f50-4a6b-9c7d-1e2f3a4b5c6d", correlation_id="tick-correlation",
        scheduled_for=at, now=at + timedelta(seconds=40), edition=edition,
        range_start=at - timedelta(days=1), range_end=at, text="본문", missing_items=missing,
        generated_by=generated_by)

    assert event.topic == "briefing.daily.generated"
    assert event.key == edition
    assert validate(DAILY_SCHEMA, event.value) == []
    assert event.value["correlationId"] == "tick-correlation"
    assert event.value["occurredAt"] == "2026-10-20T17:00:00+09:00"


def test_daily_briefing_event_id_is_the_same_for_the_same_tick_and_edition():
    at = datetime(2026, 10, 20, 8, 45, tzinfo=KST)

    def build(tick_id, edition, now):
        return daily_briefing_event(tick_event_id=tick_id, correlation_id="c", scheduled_for=at, now=now,
                                    edition=edition, range_start=at - timedelta(days=1), range_end=at,
                                    text="본문", missing_items=[], generated_by="llm").value["eventId"]

    first = build("tick-1", EDITION_MORNING, at)
    assert build("tick-1", EDITION_MORNING, at + timedelta(minutes=5)) == first
    assert build("tick-2", EDITION_MORNING, at) != first
    assert build("tick-1", EDITION_POST_CLOSE, at) != first


def test_daily_briefing_event_rejects_an_unknown_edition():
    at = datetime(2026, 10, 20, 8, 45, tzinfo=KST)

    with pytest.raises(ValueError):
        daily_briefing_event(tick_event_id="t", correlation_id="c", scheduled_for=at, now=at, edition="noon",
                             range_start=at, range_end=at, text="본문", missing_items=[], generated_by="llm")


def test_daily_briefing_job_types_are_the_ones_listed_in_the_tick_schema():
    schema = json.loads((CONTRACTS_DIR / "schedule.tick.v1.schema.json").read_text(encoding="utf-8"))
    description = schema["properties"]["payload"]["properties"]["jobType"]["description"]

    assert set(JOB_TYPE_BY_EDITION) == {"morning", "post-close"}
    for job_type in JOB_TYPE_BY_EDITION.values():
        assert job_type in description


DAILY_DATA_CONTRACTS = [
    ("market.daily.requested.v1.schema.json", "market.daily.requested.sample.json"),
    ("market.daily.responded.v1.schema.json", "market.daily.responded.sample.json"),
]


@pytest.mark.parametrize("schema_file, sample_file", DAILY_DATA_CONTRACTS)
def test_daily_data_samples_match_their_schemas(schema_file, sample_file):
    assert validate(schema_file, sample(sample_file)) == []


@pytest.mark.parametrize("schema_file, sample_file", DAILY_DATA_CONTRACTS)
def test_daily_data_events_tolerate_unknown_fields(schema_file, sample_file):
    future = sample(sample_file)
    future["newTopLevelHint"] = "x"
    future["payload"]["venue"] = "future"

    assert validate(schema_file, future) == []


@pytest.mark.parametrize("schema_file, sample_file", DAILY_DATA_CONTRACTS)
def test_daily_data_events_reject_missing_payload_field(schema_file, sample_file):
    for field in sample(sample_file)["payload"]:
        broken = sample(sample_file)
        del broken["payload"][field]

        assert validate(schema_file, broken) != [], field


@pytest.mark.parametrize("schema_file, sample_file", DAILY_DATA_CONTRACTS)
def test_daily_data_events_reject_wrong_event_type(schema_file, sample_file):
    broken = sample(sample_file)
    broken["eventType"] = "market.context.requested"

    assert validate(schema_file, broken) != []


def test_daily_data_request_and_response_share_job_id_and_correlation_id():
    request = sample("market.daily.requested.sample.json")
    response = sample("market.daily.responded.sample.json")

    assert response["payload"]["jobId"] == request["payload"]["jobId"]
    assert response["payload"]["seq"] == request["payload"]["seq"]
    assert response["correlationId"] == request["correlationId"]
    # 데일리 판 단위라 종목 하나(symbol)는 요청하지 않는다.
    assert "symbol" not in request["payload"]


def test_daily_data_request_rejects_bad_sequence_limit_and_timestamp():
    for field, value in (("seq", 0), ("maxResponseBytes", 0), ("maxEtfs", 0), ("maxTickers", 0),
                         ("asOf", "2026-10-20T17:00+09:00"), ("since", "2026-10-16")):
        broken = sample("market.daily.requested.sample.json")
        broken["payload"][field] = value

        assert validate("market.daily.requested.v1.schema.json", broken) != [], field


def test_daily_data_response_accepts_no_etfs_and_an_etf_without_us_stocks():
    no_etfs = sample("market.daily.responded.sample.json")
    no_etfs["payload"]["etfs"] = []
    no_etfs["payload"]["closeRates"] = []

    assert validate("market.daily.responded.v1.schema.json", no_etfs) == []
    assert sample("market.daily.responded.sample.json")["payload"]["etfs"][1]["tickers"] == []


def test_daily_data_response_rejects_malformed_etfs():
    cases = []
    for field in ("symbol", "date", "close", "prevClose", "rate", "fetchedAt", "tickers", "tickersTruncated"):
        broken = sample("market.daily.responded.sample.json")
        del broken["payload"]["etfs"][0][field]
        cases.append((field, broken))
    bad_symbol = sample("market.daily.responded.sample.json")
    bad_symbol["payload"]["etfs"][0]["symbol"] = "*"
    bad_ticker = sample("market.daily.responded.sample.json")
    bad_ticker["payload"]["etfs"][0]["tickers"] = ["testa"]
    empty_rate = sample("market.daily.responded.sample.json")
    empty_rate["payload"]["etfs"][0]["rate"] = ""
    cases += [("symbol *", bad_symbol), ("lower ticker", bad_ticker), ("empty rate", empty_rate)]

    for name, broken in cases:
        assert validate("market.daily.responded.v1.schema.json", broken) != [], name


def test_daily_data_background_follows_the_context_rules_without_a_per_symbol_daily_price():
    cases = [
        ({"apiType": "world-index", "target": "S", "status": "missing"}, True),
        ({"apiType": "exchange-rate-summary", "target": None, "status": "missing"}, True),
        ({"apiType": "exchange-rate-summary", "target": "S", "status": "missing"}, False),
        ({"apiType": "daily-price", "target": "TEST01", "status": "missing"}, False),
        ({"apiType": "market-summary", "target": None, "status": "ok", "fetchedAt": "2026-10-20T16:30:06+09:00"},
         False),
        ({"apiType": "market-summary", "target": None, "status": "missing", "indexRates": []}, False),
    ]
    for item, valid in cases:
        event = sample("market.daily.responded.sample.json")
        event["payload"]["background"] = [item]
        assert (validate("market.daily.responded.v1.schema.json", event) == []) is valid, item


def test_daily_data_world_index_carries_computed_rates_next_to_the_raw_response():
    world = sample("market.daily.responded.sample.json")["payload"]["background"][2]

    assert world["apiType"] == "world-index"
    assert world["response"]
    assert world["indexRates"][0]["rate"] == "2.04"


RESEARCH_SCHEMA = "news.research.requested.v1.schema.json"
RESEARCH_JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"


def test_research_sample_uses_fake_symbol_and_queries_only():
    payload = sample("news.research.requested.sample.json")["payload"]

    # 검색어는 LLM이 만들어 보유 종목을 드러낼 수 있으므로 샘플에는 가짜 값만 둔다.
    assert payload["symbol"].startswith("TEST")
    assert payload["queries"] and all("테스트" in query for query in payload["queries"])


def test_research_request_has_no_response_event():
    assert not list(CONTRACTS_DIR.glob("news.research.responded*"))


@pytest.mark.parametrize("job_kind, symbol", [("move", "TEST01"), ("morning", "*"), ("post-close", "*")])
def test_research_request_built_by_the_producer_matches_the_schema(job_kind, symbol):
    at = datetime(2026, 10, 20, 16, 16, 10, tzinfo=KST)
    event = research_request_event(job_id=RESEARCH_JOB_ID, seq=1, job_kind=job_kind, symbol=symbol,
                                   queries=["테스트 재검색어"], correlation_id="move-correlation", now=at)

    assert event.topic == "news.research.requested"
    assert event.key == symbol
    assert validate(RESEARCH_SCHEMA, event.value) == []
    assert event.value["payload"]["jobKind"] == job_kind
    assert event.value["correlationId"] == "move-correlation"


def test_research_request_event_id_is_the_same_for_the_same_job_and_seq():
    at = datetime(2026, 10, 20, 16, 16, 10, tzinfo=KST)

    def build(job_id, seq, now):
        return research_request_event(job_id=job_id, seq=seq, job_kind="move", symbol="TEST01",
                                      queries=["테스트 재검색어"], correlation_id="c", now=now).value["eventId"]

    first = build(RESEARCH_JOB_ID, 1, at)
    assert build(RESEARCH_JOB_ID, 1, at + timedelta(seconds=5)) == first
    assert build(RESEARCH_JOB_ID, 2, at) != first
    assert build("0b8c2d3e-4f50-4a6b-9c7d-1e2f3a4b5c6d", 1, at) != first


def test_research_request_event_rejects_an_unknown_job_kind():
    at = datetime(2026, 10, 20, 16, 16, 10, tzinfo=KST)

    assert set(RESEARCH_JOB_KINDS) == {"move", "morning", "post-close"}
    with pytest.raises(ValueError):
        research_request_event(job_id=RESEARCH_JOB_ID, seq=1, job_kind="noon", symbol="*",
                               queries=["테스트 재검색어"], correlation_id="c", now=at)


@pytest.mark.parametrize("schema_file, sample_file", [
    ("news.collected.v1.schema.json", "news.collected.sample.json"),
    ("collection.completed.v1.schema.json", "collection.completed.sample.json"),
    ("news.articles.requested.v1.schema.json", "news.articles.requested.sample.json"),
])
def test_research_job_id_is_an_optional_uuid_on_existing_events(schema_file, sample_file):
    schema = json.loads((CONTRACTS_DIR / schema_file).read_text(encoding="utf-8"))
    assert "researchJobId" not in schema["properties"]["payload"]["required"]

    older = sample(sample_file)
    assert "researchJobId" not in older["payload"]
    assert validate(schema_file, older) == []

    with_research = sample(sample_file)
    with_research["payload"]["researchJobId"] = RESEARCH_JOB_ID
    assert validate(schema_file, with_research) == []

    with_research["payload"]["researchJobId"] = "not-a-uuid"
    assert validate(schema_file, with_research) != []
