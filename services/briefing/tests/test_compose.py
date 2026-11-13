from datetime import datetime, timedelta, timezone

import pytest

from briefing.compose import DISCLAIMER, NO_EVIDENCE, compose, headline, window_direction, window_headline
from briefing.evidence import NewsItem

KST = timezone(timedelta(hours=9))
AS_OF = datetime(2026, 10, 20, 16, 0, 0, tzinfo=KST)
ITEM = NewsItem("n1", ("TEST01",), "Sample headline", "sample", datetime(2026, 10, 20, 9, 0, 0, tzinfo=KST))


def test_headline_shows_symbol_time_direction_percent_and_prices():
    text = compose("TEST01", 10_000, 9_850, -1.5, AS_OF, [ITEM])

    assert text.splitlines()[0] == "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)"


def test_rise_is_labelled_with_a_plus_sign():
    text = compose("TEST02", 10_000, 10_237, 2.37, AS_OF, [])

    assert text.splitlines()[0] == "[TEST02] 16:00 기준 상승 +2.37% (10,000 → 10,237)"


def test_evidence_is_listed_and_never_stated_as_a_cause():
    text = compose("TEST01", 10_000, 9_850, -1.5, AS_OF, [ITEM])

    assert "근거 후보:" in text
    assert "1. Sample headline — sample, 발행 10-20 09:00" in text
    assert "원인을 단정하지 않습니다" in text


def test_without_evidence_the_briefing_says_so_instead_of_inventing_a_reason():
    text = compose("TEST01", 10_000, 9_850, -1.5, AS_OF, [])

    assert NO_EVIDENCE in text
    assert "근거 후보:" not in text


def test_disclaimer_is_always_present():
    assert compose("TEST01", 10_000, 9_850, -1.5, AS_OF, []).endswith(DISCLAIMER)
    assert compose("TEST01", 10_000, 9_850, -1.5, AS_OF, [ITEM]).endswith(DISCLAIMER)


@pytest.mark.parametrize("name, label", [("TEST 샘플 ETF", "TEST 샘플 ETF(TEST01)"), (" TEST 샘플 ETF ", "TEST 샘플 ETF(TEST01)"),
                                         (None, "TEST01"), ("", "TEST01"), ("  ", "TEST01")])
def test_headline_shows_name_with_symbol_only_when_the_name_is_known(name, label):
    assert headline("TEST01", 10_000, 9_850, -1.5, AS_OF, name) == f"[{label}] 16:00 기준 하락 -1.50% (10,000 → 9,850)"


def test_window_headline_shows_today_change_before_the_short_change():
    line = window_headline("TEST01", 12_000, 12_120, 1.0, -2.0, 1, AS_OF, "TEST 샘플 ETF")

    assert line == "[TEST 샘플 ETF(TEST01)] 16:00 기준 오늘 -2.00% · 1분 새 +1.00% 반등 (12,000 → 12,120)"


@pytest.mark.parametrize("change,today,word", [
    (1.0, -2.0, "반등"), (-1.0, 2.0, "반락"), (1.0, 2.0, "상승"), (-1.0, -2.0, "하락"),
    # 오늘 등락이 0.00이면(반올림해 0이어도) 반대 방향이 아니다.
    (1.0, 0.0, "상승"), (-1.0, 0.004, "하락"),
])
def test_window_direction_word_depends_on_today_and_short_change(change, today, word):
    assert window_direction(change, today) == word
