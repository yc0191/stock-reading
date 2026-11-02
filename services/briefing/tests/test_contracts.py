import pytest

from contracts import sample, validate


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
