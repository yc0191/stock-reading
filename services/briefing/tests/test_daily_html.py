"""데일리 판의 결론·보유/관심 묶음·HTML·차트 그림을 확인한다. 외부 의존 없이 값만 넣어 돌린다."""
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.daily_chart import (
    FONT_DIR_ENV,
    WATCH_SUFFIX,
    Bar,
    chart_bars,
    no_data_names,
    render_day_chart,
)
from briefing.daily_html import ChartSlot, render_html
from briefing.daily_report import (
    DAILY_DISCLAIMER,
    DailyInput,
    DailyOutput,
    EtfDay,
    Point,
    build_sections,
    conclusion,
    daily_text,
    etf_groups,
)
from briefing.sources import EtfInfo

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
FETCHED = datetime(2026, 10, 20, 16, 30, tzinfo=KST)
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def etf(symbol, name, rate, relation="HELD"):
    info = EtfInfo((), False, name)
    if rate is None:
        return EtfDay(symbol, None, None, None, None, None, (), False, info, "no-daily-price-in-range", relation)
    return EtfDay(symbol, "20261020", "9850", "10000", rate, FETCHED, (), False, info, None, relation)


def post_close(etfs):
    return DailyInput("job", "post-close", AT - timedelta(days=1), AT, tuple(etfs), False, (), (), (), False)


def morning():
    return DailyInput("job", "morning", AT - timedelta(days=1), AT, (etf("TEST01", "TEST 하나", "1.0"),), False,
                      (), (), (), False)


FOUR = (etf("TEST01", "TEST 하나", "-1.50"), etf("TEST02", "TEST 둘", "+2.10"),
        etf("TEST03", "TEST 셋", "0.40", "WATCH"), etf("TEST04", "TEST 넷", None, "WATCH"))


def output(statement="요약 문장입니다."):
    return DailyOutput((Point(statement, ("E1",), "더 필요한 자료"),), False)


# ---- 결론 ----

def test_conclusion_counts_held_and_watched_separately_and_names_the_extremes():
    assert conclusion(post_close(FOUR)) == (
        "결론: 보유 2개 중 1개 상승 · 관심 2개 중 1개 상승(자료 없음 1) · "
        "가장 크게 오름 TEST 둘 +2.1% · 가장 크게 내림 TEST 하나 −1.5%")


def test_conclusion_is_one_group_when_no_etf_has_a_relation():
    plain = tuple(EtfDay(e.symbol, e.date, e.close, e.prev_close, e.rate, e.fetched_at, (), False, e.etf,
                         e.missing_reason, None) for e in FOUR)
    text = conclusion(post_close(plain))
    assert text.startswith("결론: ETF 4개 중 2개 상승(자료 없음 1)")
    assert "보유" not in text and "관심" not in text


def test_conclusion_says_no_data_when_no_etf_has_a_value():
    assert conclusion(post_close((etf("TEST01", "TEST 하나", None),))) == "결론: 등락 자료 없음"
    assert conclusion(post_close(())) == "결론: 등락 자료 없음"


def test_conclusion_leaves_out_a_rise_or_fall_that_did_not_happen():
    text = conclusion(post_close((etf("TEST01", "TEST 하나", "1.00"), etf("TEST02", "TEST 둘", "0.00"))))
    assert "가장 크게 오름 TEST 하나 +1.0%" in text and "내림" not in text
    flat = conclusion(post_close((etf("TEST01", "TEST 하나", "0.00"),)))
    assert flat == "결론: 보유 1개 중 0개 상승"


def test_a_rate_that_rounds_to_zero_is_neither_a_rise_nor_a_fall():
    text = conclusion(post_close((etf("TEST01", "TEST 하나", "0.004"), etf("TEST02", "TEST 둘", "-0.004"))))
    assert text == "결론: 보유 2개 중 0개 상승"
    # 반올림하면 0.1이 되는 값은 오름이다.
    assert "1개 상승" in conclusion(post_close((etf("TEST01", "TEST 하나", "0.06"),)))
    assert "가장 크게 오름 TEST 하나 +0.1%" in conclusion(post_close((etf("TEST01", "TEST 하나", "0.06"),)))


@pytest.mark.parametrize("rate, shown", [("0.15", "+0.2%"), ("0.25", "+0.3%"), ("-0.15", "−0.2%"), ("-0.25", "−0.3%")])
def test_the_conclusion_rounds_a_half_up_in_decimal_not_in_binary_float(rate, shown):
    # 이진 float 은 0.15 를 0.1, 0.25 를 0.2 로 내려 버린다.
    text = conclusion(post_close((etf("TEST01", "TEST 하나", rate),)))
    assert shown in text


@pytest.mark.parametrize("bad", ["nan", "inf", "-inf"])
def test_a_rate_that_is_not_a_finite_number_counts_as_no_data(bad):
    i = post_close((etf("TEST01", "TEST 하나", bad), etf("TEST02", "TEST 둘", "1.0")))
    assert [b.label for b in chart_bars(i)] == ["TEST 둘"]
    assert "TEST 하나" in no_data_names(i)
    assert conclusion(i) == "결론: 보유 2개 중 1개 상승(자료 없음 1) · 가장 크게 오름 TEST 둘 +1.0%"
    assert conclusion(post_close((etf("TEST01", "TEST 하나", bad),))) == "결론: 등락 자료 없음"
    assert render_day_chart(chart_bars(i)).startswith(PNG_MAGIC)


def test_split_section_title_shows_held_and_watched_but_the_item_name_stays():
    i = post_close(FOUR)
    [section] = [s for s in build_sections(i) if s.name == "보유 ETF별 하루 등락"]
    text, html = daily_text(i, build_sections(i), None), render_html(i, build_sections(i), None)
    assert "■ 보유·관심 ETF별 하루 등락" in text and "■ 보유·관심 ETF별 하루 등락" in html
    assert "■ 보유 ETF별 하루 등락" not in text and section.groups
    plain = post_close((etf("TEST01", "TEST 하나", "1.00", None),))
    assert "■ 보유 ETF별 하루 등락" in daily_text(plain, build_sections(plain), None)


def test_a_rise_gets_a_plus_in_the_etf_lines_but_the_raw_value_is_kept():
    i = post_close((etf("TEST01", "TEST 하나", "2.10"), etf("TEST02", "TEST 둘", "-1.50"),
                    etf("TEST03", "TEST 셋", "0.00"), etf("TEST04", "TEST 넷", "+0.30")))
    sections = build_sections(i)
    text = daily_text(i, sections, None)
    assert "TEST 하나(TEST01) +2.10%" in text and "TEST 둘(TEST02) -1.50%" in text
    assert "TEST 셋(TEST03) 0.00%" in text and "TEST 넷(TEST04) +0.30%" in text and "++" not in text
    assert "+2.10%" in render_html(i, sections, None)
    [section] = [s for s in sections if s.entries and s.entries[0].ref.startswith("E")]
    assert [e.llm["rate"] for e in section.entries] == ["2.10", "-1.50", "0.00", "+0.30"]


def test_unknown_relation_gets_its_own_group_only_when_others_are_known():
    mixed = (etf("TEST01", "TEST 하나", "1.0"), etf("TEST02", "TEST 둘", "1.0", None))
    assert [title for title, _ in etf_groups(mixed)] == ["보유", "구분 없음"]
    assert [title for title, _ in etf_groups((mixed[1],))] == [""]
    assert etf_groups(()) == ()


# ---- 글자판 ----

def test_post_close_text_order_is_title_conclusion_facts_summary_disclaimer_status_evidence():
    i = post_close(FOUR)
    sections = build_sections(i)
    text = daily_text(i, sections, output(), "──── 서버 상태 ────\n- 주의: 없음")
    marks = ["[마감 후 판]", "결론:", "■ 그날 한국 증시", "[보유]", "[관심]", "요약:", DAILY_DISCLAIMER, "서버 상태"]
    positions = [text.index(m) for m in marks]
    assert positions == sorted(positions)
    # 보유 ETF와 관심 ETF가 섞이지 않는다.
    assert text.index("TEST 둘(TEST02)") < text.index("[관심]") < text.index("TEST 셋(TEST03)")
    assert "[근거 1]" in text


def test_truncation_note_stays_at_the_end_of_the_etf_item_in_both_forms():
    i = DailyInput("job", "post-close", AT - timedelta(days=1), AT, FOUR, True, (), (), (), False)
    text = daily_text(i, build_sections(i), None)
    assert text.index("TEST 셋(TEST03)") < text.index("(잘림: 상한까지만 받음)")
    html = render_html(i, build_sections(i), None)
    assert html.index("TEST 셋(TEST03)") < html.index("(잘림: 상한까지만 받음)")


def test_morning_text_has_no_conclusion_and_no_groups():
    i = morning()
    text = daily_text(i, build_sections(i), None)
    assert "결론:" not in text and "[보유]" not in text and "관심" not in text


def test_rule_based_text_without_relation_stays_one_list():
    plain = (etf("TEST01", "TEST 하나", "1.00", None),)
    text = daily_text(post_close(plain), build_sections(post_close(plain)), None)
    assert "[보유]" not in text and "- TEST 하나(TEST01) +1.00%" in text


# ---- HTML ----

def test_html_keeps_the_text_order_and_puts_status_small_and_only_attention_in_orange():
    i = post_close(FOUR)
    status = "──────── 서버 상태 (10-19 17:00 ~ 10-20 17:00) ────────\n- 수집: 장 시작 전 08:30\n- 주의: 호출 상한 건너뜀 5"
    html = render_html(i, build_sections(i), output(), status, ChartSlot("chart-day", False, ("TEST 넷",)))
    marks = ["결론:", 'src="cid:chart-day"', "TEST 넷", "■ 그날 한국 증시", "요약", DAILY_DISCLAIMER, "서버 상태", "근거:"]
    assert [html.index(m) for m in marks] == sorted(html.index(m) for m in marks)
    assert html.count("#e65100") == 1 and "주의: 호출 상한 건너뜀 5</p>" in html
    assert "<style" not in html and "<script" not in html and "http" not in html
    assert "font-family:'Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic','맑은 고딕',sans-serif" in html


def test_attention_none_is_not_orange():
    i = post_close(FOUR)
    html = render_html(i, build_sections(i), None, "- 주의: 없음")
    assert "#e65100" not in html


def test_rates_are_painted_by_direction_in_the_facts_and_the_conclusion():
    i = post_close(FOUR)
    html = render_html(i, build_sections(i), None)
    assert '<span style="color:#d32f2f;font-weight:700">+2.1%</span>' in html
    assert '<span style="color:#1565c0;font-weight:700">−1.5%</span>' in html
    assert '<span style="color:#1565c0;font-weight:700">-1.50%</span>' in html
    assert '<span style="color:#757575;font-weight:700">0.00%</span>' in render_html(
        post_close((etf("TEST01", "TEST 하나", "0.00"),)), build_sections(post_close((etf("TEST01", "TEST 하나", "0.00"),))),
        None)


def test_every_value_from_the_event_is_escaped():
    risky = '<b>&"'
    etfs = (etf("TEST01", f"TEST{risky}", "1.00"), etf("TEST02", f"NODATA{risky}", None))
    i = post_close(etfs)
    html = render_html(i, build_sections(i), output(risky), f"- 수집: {risky}\n- 주의: {risky}",
                       ChartSlot("chart-day", False, (f"NODATA{risky}",)))
    assert risky not in html
    assert html.count("&lt;b&gt;&amp;&quot;") >= 6


def test_chart_slot_shows_the_image_the_too_large_note_or_nothing():
    i = post_close(FOUR)
    sections = build_sections(i)
    assert "차트 없음(크기 초과)" in render_html(i, sections, None, None, ChartSlot(None, True, ("TEST 넷",)))
    assert "cid:" not in render_html(i, sections, None, None, ChartSlot(None, True))
    nothing = render_html(i, sections, None, None, ChartSlot(None, False, ("TEST 넷",)))
    assert "차트" not in nothing and "cid:" not in nothing
    assert "cid:" not in render_html(i, sections, None)


def test_morning_html_has_no_conclusion_chart_or_status():
    i = morning()
    html = render_html(i, build_sections(i), None, None, None)
    assert "결론:" not in html and "cid:" not in html and "서버 상태" not in html


def test_insufficient_evidence_html_says_so_without_the_sentences():
    i = post_close(FOUR)
    html = render_html(i, build_sections(i), DailyOutput((Point("숨길 문장", ("E1",)),), True))
    assert "근거가 부족합니다" in html and "숨길 문장" not in html and "근거:" not in html


# ---- 차트 ----

def test_chart_bars_put_held_first_then_watched_with_a_suffix_and_sort_within_a_group():
    bars = chart_bars(post_close(FOUR))
    assert [b.label for b in bars] == ["TEST 둘", "TEST 하나", "TEST 셋" + WATCH_SUFFIX]
    assert [b.rate for b in bars] == [2.1, -1.5, 0.4]
    assert no_data_names(post_close(FOUR)) == ("TEST 넷",)


def test_chart_has_no_bars_when_no_etf_has_a_value():
    i = post_close((etf("TEST01", "TEST 하나", None),))
    assert chart_bars(i) == () and render_day_chart(()) is None
    assert no_data_names(i) == ("TEST 하나",)


def test_unreadable_rate_is_left_out_of_the_bars():
    broken = (etf("TEST01", "TEST 하나", "abc"), etf("TEST02", "TEST 둘", "1.0"))
    assert [b.label for b in chart_bars(post_close(broken))] == ["TEST 둘"]


def test_chart_is_a_png_and_a_missing_font_only_warns(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(FONT_DIR_ENV, str(tmp_path))
    with caplog.at_level(logging.WARNING, logger="briefing.daily_chart"):
        png = render_day_chart((Bar("TEST 하나", -1.5), Bar("TEST 둘", 2.1), Bar("TEST 영", 0.0)))
    assert png.startswith(PNG_MAGIC) and len(png) < 300 * 1024
    assert "font" in caplog.text and "install_font" in caplog.text


def test_chart_with_the_installed_font_does_not_warn(monkeypatch, caplog):
    from briefing.daily_chart import DEFAULT_FONT_DIR, REGULAR_FONT, BOLD_FONT
    if not ((DEFAULT_FONT_DIR / REGULAR_FONT).is_file() and (DEFAULT_FONT_DIR / BOLD_FONT).is_file()):
        pytest.skip("글꼴 미설치: python3 scripts/install_font.py")
    monkeypatch.delenv(FONT_DIR_ENV, raising=False)
    with caplog.at_level(logging.WARNING, logger="briefing.daily_chart"):
        assert render_day_chart((Bar("TEST 하나", -1.5),)).startswith(PNG_MAGIC)
    assert caplog.text == ""
