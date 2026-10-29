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
    ]
    for topic, field, value in cases:
        broken = sample(f"{topic}.sample.json")
        broken["payload"][field] = value

        assert validate(f"{topic}.v1.schema.json", broken) != [], (topic, field)


def test_requests_reject_timestamp_without_seconds():
    for topic in ("market.history.requested", "news.articles.requested"):
        broken = sample(f"{topic}.sample.json")
        broken["payload"]["since"] = "2026-10-20T15:55+09:00"

        assert validate(f"{topic}.v1.schema.json", broken) != [], topic


def test_responses_accept_zero_results_as_a_received_answer():
    empty_points = sample("market.history.responded.sample.json")
    empty_points["payload"]["points"] = []
    empty_articles = sample("news.articles.responded.sample.json")
    empty_articles["payload"]["articles"] = []

    assert validate("market.history.responded.v1.schema.json", empty_points) == []
    assert validate("news.articles.responded.v1.schema.json", empty_articles) == []


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
