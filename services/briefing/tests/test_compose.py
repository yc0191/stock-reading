from datetime import datetime, timedelta, timezone

from briefing.compose import DISCLAIMER, NO_EVIDENCE, compose
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
