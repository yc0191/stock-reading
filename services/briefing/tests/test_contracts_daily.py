"""데일리 판 계약: 데일리 브리핑 이벤트, 판 시작 알림, 데일리 자료 요청·응답(v2 포함)의 스키마와 샘플을 검사한다."""
import json
import re
from datetime import datetime, timedelta

import pytest

from briefing.compose import KST
from briefing.daily_start import SOURCE_LABELS, STATUS_COLLECTED, STATUS_MARKET_CLOSED
from briefing.events import EDITION_MORNING, EDITION_POST_CLOSE, EDITIONS, daily_briefing_event
from briefing.price_series import parse_daily_closes, parse_intraday_series
from contracts import CONTRACTS_DIR, sample, validate

DAILY_SCHEMA = "briefing.daily.generated.v2.schema.json"
DAILY_SAMPLE = "briefing.daily.generated.sample.json"


def test_committed_daily_briefing_sample_matches_its_schema():
    assert validate(DAILY_SCHEMA, sample(DAILY_SAMPLE, "v2")) == []


def test_daily_briefing_tolerates_unknown_fields():
    future = sample(DAILY_SAMPLE, "v2")
    future["newTopLevelHint"] = "x"
    future["payload"]["venue"] = "future"

    assert validate(DAILY_SCHEMA, future) == []


DAILY_OPTIONAL_FIELDS = {"html", "images"}


def test_daily_briefing_rejects_missing_payload_field():
    for field in sample(DAILY_SAMPLE, "v2")["payload"]:
        if field in DAILY_OPTIONAL_FIELDS:
            continue
        broken = sample(DAILY_SAMPLE, "v2")
        del broken["payload"][field]

        assert validate(DAILY_SCHEMA, broken) != [], field


def test_daily_briefing_html_and_images_are_optional():
    text_only = sample(DAILY_SAMPLE, "v2")
    for field in DAILY_OPTIONAL_FIELDS:
        del text_only["payload"][field]

    assert validate(DAILY_SCHEMA, text_only) == []


def test_daily_briefing_rejects_a_bad_inline_image():
    for field, value in [("contentType", "image/svg+xml"), ("data", "not base64!"), ("contentId", "a b")]:
        broken = sample(DAILY_SAMPLE, "v2")
        broken["payload"]["images"][0][field] = value

        assert validate(DAILY_SCHEMA, broken) != [], field
    missing = sample(DAILY_SAMPLE, "v2")
    del missing["payload"]["images"][0]["data"]
    assert validate(DAILY_SCHEMA, missing) != []


@pytest.mark.parametrize("field, value", [
    ("edition", "noon"),
    ("text", ""),
    ("rangeStart", "2026-10-19T17:00+09:00"),
    ("missingItems", [""]),
])
def test_daily_briefing_rejects_invalid_payload_values(field, value):
    broken = sample(DAILY_SAMPLE, "v2")
    broken["payload"][field] = value

    assert validate(DAILY_SCHEMA, broken) != []


def test_daily_briefing_carries_no_personal_fields():
    schema = json.loads((CONTRACTS_DIR / DAILY_SCHEMA).read_text(encoding="utf-8"))
    names = set(schema["properties"]["payload"]["properties"])

    # 보유 수량·금액·주소는 이 이벤트에 담지 않는다. 받는 사람은 멤버 번호로만 가리키고 주소는 발송기가 DB에서 찾는다.
    assert not {n for n in names if re.search(r"quantity|amount|holding|recipient|email|address|account|balance|name", n, re.I)}
    assert "memberId" in names and "memberId" in schema["properties"]["payload"]["required"]


@pytest.mark.parametrize("edition, generated_by, missing", [
    (EDITION_MORNING, "llm", []),
    (EDITION_POST_CLOSE, "rule-based", ["us-filings"]),
])
def test_daily_briefing_event_built_by_the_producer_matches_the_schema(edition, generated_by, missing):
    at = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
    event = daily_briefing_event(
        job_id="0b8c2d3e-4f50-4a6b-9c7d-1e2f3a4b5c6d", correlation_id="tick-correlation",
        scheduled_for=at, now=at + timedelta(seconds=40), edition=edition,
        range_start=at - timedelta(days=1), range_end=at, text="본문", missing_items=missing,
        generated_by=generated_by, member_id=7)

    assert event.topic == "briefing.daily.generated"
    assert event.key == f"{edition}:7"
    assert event.value["schemaVersion"] == 2 and event.value["payload"]["memberId"] == 7
    assert validate(DAILY_SCHEMA, event.value) == []
    assert event.value["correlationId"] == "tick-correlation"
    assert event.value["occurredAt"] == "2026-10-20T17:00:00+09:00"


def test_daily_briefing_event_carries_html_and_images_only_when_given():
    at = datetime(2026, 10, 20, 17, 0, tzinfo=KST)

    def build(**extra):
        return daily_briefing_event(job_id="t", correlation_id="c", scheduled_for=at, now=at,
                                    edition=EDITION_POST_CLOSE, range_start=at, range_end=at, text="본문",
                                    missing_items=[], generated_by="llm", member_id=1, **extra)

    plain = build().value["payload"]
    assert "html" not in plain and "images" not in plain
    image = {"contentId": "chart-day", "contentType": "image/png", "data": "iVBORw0KGgo="}
    full = build(html="<p>본문</p>", images=[image])
    assert validate(DAILY_SCHEMA, full.value) == []
    assert full.value["payload"]["html"] == "<p>본문</p>" and full.value["payload"]["images"] == [image]
    # 그림은 HTML이 있을 때만 싣는다.
    assert "images" not in build(images=[image]).value["payload"]
    assert "images" not in build(html="<p>본문</p>", images=[]).value["payload"]


def test_daily_briefing_event_id_is_the_same_for_the_same_tick_and_edition():
    at = datetime(2026, 10, 20, 8, 45, tzinfo=KST)

    def build(tick_id, edition, now, member_id=1):
        return daily_briefing_event(job_id=tick_id, correlation_id="c", scheduled_for=at, now=now,
                                    edition=edition, range_start=at - timedelta(days=1), range_end=at,
                                    text="본문", missing_items=[], generated_by="llm",
                                    member_id=member_id).value["eventId"]

    first = build("tick-1", EDITION_MORNING, at)
    assert build("tick-1", EDITION_MORNING, at + timedelta(minutes=5)) == first
    assert build("tick-2", EDITION_MORNING, at) != first
    assert build("tick-1", EDITION_POST_CLOSE, at) != first
    # 같은 판이라도 멤버가 다르면 다른 이벤트다(멤버마다 한 통씩 나간다).
    assert build("tick-1", EDITION_MORNING, at, member_id=2) != first


def test_daily_briefing_event_rejects_an_unknown_edition():
    at = datetime(2026, 10, 20, 8, 45, tzinfo=KST)

    with pytest.raises(ValueError):
        daily_briefing_event(job_id="t", correlation_id="c", scheduled_for=at, now=at, edition="noon",
                             range_start=at, range_end=at, text="본문", missing_items=[], generated_by="llm", member_id=1)


DAILY_START_SCHEMA = "daily.collection.completed.v1.schema.json"


def test_daily_start_notice_sample_matches_its_schema_and_tolerates_unknown_fields():
    notice = sample("daily.collection.completed.sample.json")
    assert validate(DAILY_START_SCHEMA, notice) == []
    notice["newTopLevelHint"] = "x"
    notice["payload"]["venue"] = "future"
    assert validate(DAILY_START_SCHEMA, notice) == []
    for field in sample("daily.collection.completed.sample.json")["payload"]:
        broken = sample("daily.collection.completed.sample.json")
        del broken["payload"][field]
        assert validate(DAILY_START_SCHEMA, broken) != [], field


def test_daily_start_notice_editions_statuses_and_sources_are_the_ones_the_briefing_knows():
    schema = json.loads((CONTRACTS_DIR / DAILY_START_SCHEMA).read_text(encoding="utf-8"))
    payload = schema["properties"]["payload"]["properties"]

    assert set(payload["edition"]["enum"]) == set(EDITIONS) == {"morning", "post-close"}
    assert set(payload["status"]["enum"]) == {STATUS_COLLECTED, STATUS_MARKET_CLOSED}
    # 판에 한글 이름으로 쓰는 자료 종류는 계약 설명에 적힌 것과 같다.
    for source in SOURCE_LABELS:
        assert source in payload["missingSources"]["description"]


def test_daily_start_notice_rejects_ticker_like_missing_sources():
    broken = sample("daily.collection.completed.sample.json")
    broken["payload"]["missingSources"] = ["TESTA"]

    assert validate(DAILY_START_SCHEMA, broken) != []


DAILY_DATA_CONTRACTS = [
    ("market.daily.requested.v1.schema.json", "market.daily.requested.sample.json"),
    ("market.daily.responded.v1.schema.json", "market.daily.responded.sample.json"),
]


# 요청 샘플에 있어도 없는 요청이 유효해야 하는 선택 칸.
OPTIONAL_DAILY_REQUEST_FIELDS = {"includeOperations", "includeIntradayFallback"}


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
        if field in OPTIONAL_DAILY_REQUEST_FIELDS:
            continue
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


# ---- market.daily.responded v2 (일봉이 없는 ETF도 목록에 남는다) ----

DAILY_V2 = "market.daily.responded.v2.schema.json"


def daily_v2():
    return sample("market.daily.responded.sample.json", "v2")


def test_daily_responded_v2_sample_matches_its_schema_and_has_a_missing_etf():
    event = daily_v2()

    assert validate(DAILY_V2, event) == []
    assert event["schemaVersion"] == 2
    empty = [e for e in event["payload"]["etfs"] if e["missingReason"] is not None]
    plain = [e for e in empty if "intradayFallback" not in e]
    assert plain and all(e["rate"] is None and e["fetchedAt"] is None for e in plain)
    # 일봉이 없어도 장중 시세 대체값이 있으면 등락률만 차고 일봉 값(일자·종가·받은 시각)은 비어 있다.
    fallback = [e for e in empty if "intradayFallback" in e]
    assert fallback and all(e["rate"] is not None and e["fetchedAt"] is None and e["date"] is None for e in fallback)


def test_daily_responded_v2_intraday_fallback_needs_its_rate_and_no_daily_cells():
    event = daily_v2()
    assert validate(DAILY_V2, event) == []

    def broken(change):
        bad = daily_v2()
        change(next(e for e in bad["payload"]["etfs"] if "intradayFallback" in e))
        return bad

    assert validate(DAILY_V2, broken(lambda e: e.update(rate=None))) != []
    assert validate(DAILY_V2, broken(lambda e: e.update(date="20261020"))) != []
    assert validate(DAILY_V2, broken(lambda e: e["intradayFallback"].update(receivedAt="15:29"))) != []
    assert validate(DAILY_V2, broken(lambda e: e["intradayFallback"].pop("price"))) != []
    # 일봉이 있는 ETF에는 대체값 칸이 붙지 않는다.
    on_daily = daily_v2()
    on_daily["payload"]["etfs"][0]["intradayFallback"] = {"price": "9850", "receivedAt": "2026-10-20T15:29:58+09:00"}
    assert validate(DAILY_V2, on_daily) != []


def test_daily_responded_v2_member_changes_match_the_schema_and_a_broken_one_is_rejected():
    event = daily_v2()
    changes = event["payload"]["etfs"][0]["etf"]["memberChanges"]
    assert validate(DAILY_V2, event) == []
    assert changes["moved"][0]["rankBefore"] != changes["moved"][0]["rankAfter"]

    for broken in ({"rankBefore": 0}, {"rankAfter": "1"}):
        bad = daily_v2()
        bad["payload"]["etfs"][0]["etf"]["memberChanges"]["moved"][0].update(broken)
        assert validate(DAILY_V2, bad) != []
    no_moved = daily_v2()
    del no_moved["payload"]["etfs"][0]["etf"]["memberChanges"]["moved"]
    assert validate(DAILY_V2, no_moved) != []
    bad_date = daily_v2()
    bad_date["payload"]["etfs"][0]["etf"]["memberChanges"]["since"] = "10/12"
    assert validate(DAILY_V2, bad_date) != []
    # 변경이 없으면 칸 자체가 없어도 된다(v2 유지).
    without = daily_v2()
    del without["payload"]["etfs"][0]["etf"]["memberChanges"]
    assert validate(DAILY_V2, without) == []


def test_daily_responded_v2_tolerates_unknown_fields():
    future = daily_v2()
    future["payload"]["venue"] = "future"
    future["payload"]["etfs"][0]["hint"] = "x"

    assert validate(DAILY_V2, future) == []


def test_daily_responded_v2_rejects_values_that_disagree_with_the_missing_reason():
    with_reason_and_value = daily_v2()
    gap = next(e for e in with_reason_and_value["payload"]["etfs"] if e["missingReason"] is not None)
    gap["rate"] = "1.00"
    without_reason = daily_v2()
    next(e for e in without_reason["payload"]["etfs"] if e["missingReason"] is not None)["missingReason"] = None
    unknown_reason = daily_v2()
    next(e for e in unknown_reason["payload"]["etfs"] if e["missingReason"] is not None)["missingReason"] = "x"
    no_flag = daily_v2()
    del no_flag["payload"]["etfs"][0]["missingReason"]

    for broken in (with_reason_and_value, without_reason, unknown_reason, no_flag):
        assert validate(DAILY_V2, broken) != []


def test_daily_request_operations_flag_is_optional_and_must_be_boolean():
    request = sample("market.daily.requested.sample.json")
    assert request["payload"]["includeOperations"] is True
    del request["payload"]["includeOperations"]
    assert validate("market.daily.requested.v1.schema.json", request) == []

    request["payload"]["includeOperations"] = "yes"
    assert validate("market.daily.requested.v1.schema.json", request) != []


def test_daily_request_intraday_fallback_flag_is_optional_and_must_be_boolean():
    request = sample("market.daily.requested.sample.json")
    assert request["payload"]["includeIntradayFallback"] is True
    del request["payload"]["includeIntradayFallback"]
    assert validate("market.daily.requested.v1.schema.json", request) == []

    request["payload"]["includeIntradayFallback"] = "yes"
    assert validate("market.daily.requested.v1.schema.json", request) != []


def test_daily_responded_v2_operations_are_optional_and_follow_the_schema():
    event = daily_v2()
    operations = event["payload"]["operations"]
    assert [c["kind"] for c in operations["collections"]] == ["pre-open", "quote-1s", "intraday", "post-close"]
    assert operations["quoteFailures"]["TICK_SKIPPED_SUPERSEDED"] == 0

    without = daily_v2()
    del without["payload"]["operations"]
    assert validate(DAILY_V2, without) == []

    def broken(change):
        copy = daily_v2()
        change(copy["payload"]["operations"])
        return copy

    cases = {
        "missing failure kind": broken(lambda o: o["quoteFailures"].pop("TIMEOUT")),
        "negative count": broken(lambda o: o["collections"][0].update(receivedCount=-1)),
        "unknown collection kind": broken(lambda o: o["collections"][0].update(kind="night")),
        "bad time": broken(lambda o: o["collections"][0].update(firstReceivedAt="2026-10-20")),
        "non integer failure": broken(lambda o: o["quoteFailures"].update(TIMEOUT="1")),
        "zero dropped": broken(lambda o: o["droppedMessages"][0].update(count=0)),
        "no collections": broken(lambda o: o.pop("collections")),
    }
    for name, event in cases.items():
        assert validate(DAILY_V2, event) != [], name


OPEN_FLOW_CELL = {"status": "present", "prevClose": "10000", "open": "10200", "openSource": "raw", "openRate": "2.00",
                  "startAt": "2026-10-20T09:00:01+09:00", "startPrice": "10100", "endAt": "2026-10-20T09:09:58+09:00",
                  "endPrice": "10300", "flowRate": "1.98", "rateVsPrevClose": "3.00"}


def test_daily_data_request_accepts_the_optional_open_flow_start_and_rejects_a_bad_one():
    event = sample("market.daily.requested.sample.json")
    event["payload"]["openFlowSince"] = "2026-10-20T09:00:00+09:00"
    assert validate("market.daily.requested.v1.schema.json", event) == []

    event["payload"]["openFlowSince"] = "09:00"
    assert validate("market.daily.requested.v1.schema.json", event) != []


def test_daily_data_v2_response_accepts_an_open_flow_per_etf_and_rejects_a_broken_one():
    event = sample("market.daily.responded.sample.json", "v2")
    event["payload"]["etfs"][0]["openFlow"] = dict(OPEN_FLOW_CELL)
    assert validate("market.daily.responded.v2.schema.json", event) == []

    for field in OPEN_FLOW_CELL:
        broken = sample("market.daily.responded.sample.json", "v2")
        broken["payload"]["etfs"][0]["openFlow"] = {k: v for k, v in OPEN_FLOW_CELL.items() if k != field}
        assert validate("market.daily.responded.v2.schema.json", broken) != [], field
    other = sample("market.daily.responded.sample.json", "v2")
    other["payload"]["etfs"][0]["openFlow"] = {**OPEN_FLOW_CELL, "openSource": "guess"}
    assert validate("market.daily.responded.v2.schema.json", other) != []


def test_daily_data_request_accepts_the_optional_chart_fields_and_rejects_bad_ones():
    event = sample("market.daily.requested.sample.json")
    event["payload"].update(intradaySince="2026-10-20T09:00:00+09:00", maxIntradayPoints=420, maxDailyCloses=130)
    assert validate("market.daily.requested.v1.schema.json", event) == []

    for field, value in (("intradaySince", "09:00"), ("maxIntradayPoints", 0), ("maxDailyCloses", "130")):
        broken = json.loads(json.dumps(event))
        broken["payload"][field] = value
        assert validate("market.daily.requested.v1.schema.json", broken) != [], field


def test_daily_data_v2_sample_chart_cells_match_the_schema_and_are_readable_by_the_briefing():
    event = sample("market.daily.responded.sample.json", "v2")
    assert validate("market.daily.responded.v2.schema.json", event) == []
    first, _, third, _ = event["payload"]["etfs"]

    series = parse_intraday_series(first["intradaySeries"])
    closes = parse_daily_closes(first["dailyCloses"])
    assert series.present and len(series.points) == 3 and series.points[0].at.tzinfo is not None
    assert closes.present and [p.date for p in closes.points] == ["20261016", "20261019", "20261020"]
    assert not parse_intraday_series(third["intradaySeries"]).present
    assert not parse_daily_closes(third["dailyCloses"]).present


@pytest.mark.parametrize("cell, field", [
    ("intradaySeries", "status"), ("intradaySeries", "prevClose"), ("intradaySeries", "truncated"),
    ("intradaySeries", "points"), ("dailyCloses", "status"), ("dailyCloses", "fetchedAt"),
    ("dailyCloses", "truncated"), ("dailyCloses", "points"),
])
def test_daily_data_v2_chart_cells_reject_a_missing_field(cell, field):
    broken = sample("market.daily.responded.sample.json", "v2")
    del broken["payload"]["etfs"][0][cell][field]

    assert validate("market.daily.responded.v2.schema.json", broken) != []


def test_daily_data_v2_chart_points_reject_malformed_values():
    def broken(change):
        event = sample("market.daily.responded.sample.json", "v2")
        change(event["payload"]["etfs"][0])
        return event

    cases = {
        "bad point time": broken(lambda e: e["intradaySeries"]["points"][0].update(at="09:00")),
        "price with comma": broken(lambda e: e["intradaySeries"]["points"][0].update(price="10,050")),
        "unknown status": broken(lambda e: e["dailyCloses"].update(status="none")),
        "dashed date": broken(lambda e: e["dailyCloses"]["points"][0].update(date="2026-10-16")),
        "extra point field": broken(lambda e: e["dailyCloses"]["points"][0].update(volume="1")),
    }
    for name, event in cases.items():
        assert validate("market.daily.responded.v2.schema.json", event) != [], name
