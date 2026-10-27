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
