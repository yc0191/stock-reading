"""배경 원문에서 숫자를 꺼내고 비교 숫자를 계산하는 move_figures와, 그 숫자가 LLM 입력·근거 목록에 들어가는 방식.

원문은 실제 응답 모양(본문 목록 out2, 앞에 공백이 붙은 숫자 문자열)만 본뜬 가짜 값이다.
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from briefing.jobs import ReportInput
from briefing.move_figures import (
    KOSDAQ_NAME,
    KOSPI_NAME,
    display,
    exchange_rate_figure,
    index_figure,
    move_figures,
    round_cent,
    signed,
    to_decimal,
    us_close_summary,
    versus,
)
from briefing.report import (
    FIGURE_REF_PREFIX,
    LlmWriter,
    briefing_event,
    build_llm_input,
    evidence_catalog,
    figure_entries,
)
from briefing.sources import Background, CloseRate, PricePoint
from contracts import validate

KST = timezone(timedelta(hours=9))
AS_OF = datetime(2031, 3, 3, 10, 0, 0, tzinfo=KST)
FETCHED = AS_OF - timedelta(minutes=4)
SCHEMA_KINDS = {"article", "filing", "overseas-close-rate", "exchange-rate-summary", "market-summary",
                "world-index", "daily-price"}


def fx_row(name="원/달러", value="     1111.1100", change="       -2.2200", rate="     -0.20", time="101530"):
    return {"crncy_cd_nm": name, "cls_prc_p4": value, "bdy_cmpr_p4": change, "bdy_cmpr_r_p2": rate,
            "fnl_rcp_tm": time, "bdy_cmpr_ccd": "5"}


def index_row(name, value="  2222.22", change="    12.34", rate="   0.56", direction="2"):
    return {"indx_nm": name, "now_indx_p2": value, "bdy_cmpr_p2": change, "up_dwn_r_p2": rate,
            "bdy_cmpr_ccd": direction}


def response(rows):
    return json.dumps({"dataHeader": {"resultCode": "200"}, "dataBody": {"out2": rows}}, ensure_ascii=False)


def background(api_type, rows=None, status="ok", raw=None):
    if status != "ok":
        return Background(api_type, None, status, fetched_at=FETCHED if status == "too-large" else None,
                          response_bytes=99_999 if status == "too-large" else None)
    return Background(api_type, None, "ok", response=raw if raw is not None else response(rows), fetched_at=FETCHED)


def fx_background(*rows):
    return background("exchange-rate-summary", list(rows) or [fx_row(name="TEST통화"), fx_row()])


def market_background(*rows):
    return background("market-summary", list(rows) or [
        index_row(KOSPI_NAME, "  2222.22", "    12.34", "   0.56", "2"),
        index_row("TEST지수", "  1000.00", "     1.00", "   0.10", "2"),
        index_row(KOSDAQ_NAME, "   777.77", "     3.21", "  -0.41", "5"),
    ])


def close_rate(ticker, rate, hours=10):
    return CloseRate(ticker, "20310228", AS_OF - timedelta(hours=hours), rate, AS_OF - timedelta(hours=5))


def report_input(change_percent=-1.5, background_items=None, close_rates=(), tickers=()):
    return ReportInput(
        job_id="00000000-0000-4000-8000-000000000001", symbol="TEST01", correlation_id="collect-1", as_of=AS_OF,
        previous_price=10_000, price=9_850, change_percent=change_percent,
        price_points=(PricePoint(AS_OF - timedelta(minutes=1), 10_000), PricePoint(AS_OF, 9_850)),
        articles=(), tickers=tickers, close_rates=close_rates,
        background=(fx_background(), market_background()) if background_items is None else background_items)


# ---- 꺼내기 ----

def test_exchange_rate_row_is_found_by_its_display_name_and_numbers_keep_the_raw_digits():
    figure = exchange_rate_figure((fx_background(),))

    assert figure.value == Decimal("1111.1100")
    assert figure.change == Decimal("-2.2200")
    assert figure.rate == Decimal("-0.20")
    assert figure.received_time == "10:15:30"
    assert figure.fetched_at == FETCHED


def test_market_summary_change_gets_its_sign_from_the_direction_field():
    item = market_background(index_row(KOSPI_NAME, change="  12.34", direction="2"),
                             index_row(KOSDAQ_NAME, change="   3.21", rate="  -0.41", direction="5"))

    assert index_figure((item,), KOSPI_NAME).change == Decimal("12.34")
    assert index_figure((item,), KOSDAQ_NAME).change == Decimal("-3.21")
    assert index_figure((item,), KOSDAQ_NAME).rate == Decimal("-0.41")


def test_flat_upper_and_lower_limit_directions_and_an_unknown_direction():
    rows = [index_row(KOSPI_NAME, change="0.00", direction="3"), index_row(KOSDAQ_NAME, change="1.00", direction="9")]
    item = market_background(*rows)

    assert index_figure((item,), KOSPI_NAME).change == Decimal("0.00")
    # 방향을 모르면 부호를 짐작하지 않고 그 값만 없음이다. 같은 행의 다른 값은 남는다.
    unknown = index_figure((item,), KOSDAQ_NAME)
    assert unknown.change is None and unknown.value is not None
    limits = market_background(index_row(KOSPI_NAME, change="5.00", direction="1"),
                               index_row(KOSDAQ_NAME, change="5.00", direction="4"))
    assert index_figure((limits,), KOSPI_NAME).change == Decimal("5.00")
    assert index_figure((limits,), KOSDAQ_NAME).change == Decimal("-5.00")


def test_background_missing_or_too_large_gives_no_figures_without_failing():
    for status in ("missing", "too-large"):
        items = (background("exchange-rate-summary", status=status), background("market-summary", status=status))
        figures = move_figures(-1.5, items, ())
        assert figures.exchange_rate is None and figures.kospi is None and figures.kosdaq is None
        assert figures.vs_kospi is None and figures.vs_kosdaq is None
    assert move_figures(-1.5, (), ()).exchange_rate is None


def test_missing_row_gives_none_for_that_figure_only():
    items = (fx_background(fx_row(name="TEST통화")), market_background(index_row(KOSPI_NAME)))
    figures = move_figures(-1.5, items, ())

    assert figures.exchange_rate is None
    assert figures.kosdaq is None and figures.vs_kosdaq is None
    assert figures.kospi is not None and figures.vs_kospi is not None


def test_empty_or_non_numeric_field_gives_none_for_that_value_only():
    figure = exchange_rate_figure((fx_background(fx_row(value="   ", change="TEST", rate=" -0.20", time="10:15")),))

    assert figure.value is None and figure.change is None and figure.received_time is None
    assert figure.rate == Decimal("-0.20")
    assert to_decimal(None) is None and to_decimal("NaN") is None and to_decimal(" 1e3 ") == Decimal("1E+3")


def test_broken_raw_text_or_unexpected_shape_gives_none():
    for raw in ("not json", json.dumps([1, 2]), json.dumps({"dataBody": {"out2": "TEST"}}), json.dumps({})):
        assert exchange_rate_figure((background("exchange-rate-summary", raw=raw),)) is None


def test_stored_raw_text_is_not_changed():
    item = fx_background()
    before = item.response

    move_figures(-1.5, (item, market_background()), ())

    assert item.response == before


# ---- 비교 숫자 ----

def test_difference_from_index_rate_is_rounded_half_up_to_two_decimals():
    kospi = index_figure((market_background(index_row(KOSPI_NAME, rate="   0.56")),), KOSPI_NAME)

    assert versus(-1.5, kospi) == Decimal("-2.06")
    # 사사오입: 소수 셋째 자리 5는 올린다(float 반올림의 이진 오차에 끌리지 않는다).
    assert versus(1.005, index_figure((market_background(index_row(KOSPI_NAME, rate="0.00")),), KOSPI_NAME)) \
        == Decimal("1.01")
    assert round_cent(Decimal("-0.125")) == Decimal("-0.13")
    assert versus(-1.5, None) is None


def test_us_close_summary_counts_up_down_flat_and_averages():
    rates = (close_rate("TESTA", "  1.25"), close_rate("TESTB", " -0.50", hours=8), close_rate("TESTC", "0.00"),
             close_rate("TESTD", "TEST"))
    summary = us_close_summary(rates)

    assert (summary.up, summary.down, summary.flat) == (1, 1, 1)
    # (1.25 - 0.50 + 0.00) / 3 = 0.25. 숫자가 아닌 등락률은 세지 않는다.
    assert summary.average == Decimal("0.25")
    assert summary.latest_close_at == AS_OF - timedelta(hours=8)


def test_us_close_summary_is_none_without_us_stocks_or_readable_rates():
    assert us_close_summary(()) is None
    assert us_close_summary((close_rate("TESTA", " "),)) is None


def test_us_close_average_rounding_is_fixed():
    rates = tuple(close_rate(f"TEST{n}", r) for n, r in enumerate(("0.01", "0.02", "0.02")))
    # 0.05 / 3 = 0.01666… → 0.02
    assert us_close_summary(rates).average == Decimal("0.02")


def test_signed_writes_plus_for_positive_only():
    assert (signed(Decimal("1.20")), signed(Decimal("-0.40")), signed(Decimal("0.00"))) == ("+1.20", "-0.40", "0.00")


def test_display_marks_a_missing_value_as_missing():
    assert display(None) == "없음" and display(None, sign=True) == "없음"
    assert display(Decimal("1.20"), sign=True) == "+1.20" and display(Decimal("1.20")) == "1.20"
    assert display(3) == "3"


# ---- LLM 입력과 근거 목록 ----

def test_figures_get_refs_and_go_into_the_llm_input_and_the_catalog():
    i = report_input(close_rates=(close_rate("TESTA", "1.25"), close_rate("TESTB", "-0.75")),
                     tickers=("TESTA", "TESTB"))
    data = build_llm_input(i)
    figures = {f["name"]: f for f in data["figures"]}

    assert [f["ref"] for f in data["figures"]] == [f"{FIGURE_REF_PREFIX}{n}" for n in range(1, 7)]
    assert figures["원/달러 환율"] == {
        "name": "원/달러 환율", "status": "있음", "ref": "F1", "source": "시세 자료(배경)", "value": "1111.1100",
        "change": "-2.2200", "ratePercent": "-0.20", "receivedTime": "10:15:30",
        "basedOn": "2031-03-03T09:56:00+09:00", "minutesBeforeMove": 4}
    assert figures["코스피종합"]["change"] == "+12.34" and figures["코스닥종합"]["change"] == "-3.21"
    assert figures["변동률 - 코스피종합 등락률"]["differencePoint"] == "-2.06"
    assert figures["변동률 - 코스닥종합 등락률"]["differencePoint"] == "-1.09"
    us = figures["미국 종목 직전 정규장 등락 집계"]
    assert (us["up"], us["down"], us["flat"], us["averageRatePercent"]) == ("1", "1", "0", "+0.25")

    catalog = evidence_catalog(i)
    kinds = {ref: catalog[ref].kind for ref in figures_refs(data)}
    assert kinds == {"F1": "exchange-rate-summary", "F2": "market-summary", "F3": "market-summary",
                     "F4": "market-summary", "F5": "market-summary", "F6": "overseas-close-rate"}
    assert set(kinds.values()) <= SCHEMA_KINDS
    assert catalog["F4"].title == "변동률 - 코스피종합 등락률 차이(%p) -2.06"
    assert catalog["F6"].published_at == AS_OF - timedelta(hours=10)


def figures_refs(data):
    return [f["ref"] for f in data["figures"] if "ref" in f]


def test_missing_figures_have_no_ref_and_refs_stay_consecutive():
    i = report_input(background_items=(market_background(index_row(KOSDAQ_NAME)),))
    data = build_llm_input(i)

    assert [(f["name"], f.get("ref"), f["status"]) for f in data["figures"]] == [
        ("원/달러 환율", None, "없음"), ("코스피종합", None, "없음"), ("코스닥종합", "F1", "있음"),
        ("변동률 - 코스피종합 등락률", None, "없음"), ("변동률 - 코스닥종합 등락률", "F2", "있음"),
        ("미국 종목 직전 정규장 등락 집계", None, "없음")]
    assert "F3" not in evidence_catalog(i)


def test_existing_inputs_stay_as_they_were():
    i = report_input()
    data = build_llm_input(i)

    # 배경 원문은 그대로 들어가고 B 번호도 그대로다.
    assert [b["ref"] for b in data["background"]] == ["B1", "B2"]
    assert data["background"][0]["response"] == i.background[0].response
    assert {"priceHistory", "articles", "filings", "overseasCloseRates"} <= set(data)


def test_a_cited_figure_reaches_the_event_and_passes_the_schema():
    class OneAnswer:
        def complete(self, system, user, output_schema):
            return json.dumps({"causes": [{"title": "시장 전체 하락", "detail": "시장 전체가 내렸습니다.", "refs": ["F2"],
                                           "meaning": "시장 전체보다 더 내렸을 수 있습니다.", "meaningRefs": ["F4", "F2"]}],
                               "insufficientEvidence": False}, ensure_ascii=False)

    i = report_input()
    report = LlmWriter(OneAnswer()).write(i)
    event = briefing_event(i, report, AS_OF + timedelta(minutes=1)).value

    assert validate("briefing.generated.v1.schema.json", event) == []
    entries = event["payload"]["evidence"]
    assert [(e["ref"], e["kind"], e["source"]) for e in entries] == [
        ("F2", "market-summary", "시세 자료(배경)"), ("F4", "market-summary", "시세 자료로 코드가 계산")]
    assert all("url" not in e for e in entries)


def test_figure_entries_never_carry_symbol_or_tickers():
    i = report_input(close_rates=(close_rate("TESTA", "1.25"),), tickers=("TESTA",))
    text = json.dumps([e.title for e in figure_entries(i)], ensure_ascii=False)

    assert "TEST01" not in text and "TESTA" not in text
