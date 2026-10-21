from datetime import datetime, timedelta, timezone
from pathlib import Path

from briefing.evidence import NewsItem, load_sample_news, select_evidence

KST = timezone(timedelta(hours=9))
AS_OF = datetime(2026, 10, 20, 16, 0, 0, tzinfo=KST)
SAMPLE_FILE = Path(__file__).resolve().parents[1] / "data" / "sample_news.json"


def news(item_id: str, symbols: tuple[str, ...], hours_before_move: float) -> NewsItem:
    return NewsItem(item_id, symbols, f"title {item_id}", "sample", AS_OF - timedelta(hours=hours_before_move))


def ids(items: list[NewsItem]) -> list[str]:
    return [item.id for item in items]


def test_news_published_after_the_move_is_never_evidence():
    items = [news("before", ("TEST01",), 2), news("after", ("TEST01",), -1)]

    assert ids(select_evidence(items, "TEST01", AS_OF)) == ["before"]


def test_news_published_exactly_at_the_move_time_is_allowed():
    assert ids(select_evidence([news("same-instant", ("TEST01",), 0)], "TEST01", AS_OF)) == ["same-instant"]


def test_news_older_than_the_window_is_excluded():
    items = [news("fresh", ("TEST01",), 23), news("stale", ("TEST01",), 25)]

    assert ids(select_evidence(items, "TEST01", AS_OF)) == ["fresh"]


def test_only_news_for_the_symbol_or_market_wide_is_selected():
    items = [news("mine", ("TEST01",), 1), news("other", ("TEST02",), 1), news("everyone", ("*",), 1)]

    assert sorted(ids(select_evidence(items, "TEST01", AS_OF))) == ["everyone", "mine"]


def test_most_recent_first_and_limited_to_three():
    items = [news(f"n{hours}", ("TEST01",), hours) for hours in (5, 1, 9, 3, 7)]

    assert ids(select_evidence(items, "TEST01", AS_OF)) == ["n1", "n3", "n5"]


def test_no_candidates_returns_an_empty_list():
    assert select_evidence([news("other", ("TEST02",), 1)], "TEST01", AS_OF) == []


def test_sample_file_keeps_a_future_item_and_an_old_item_out_of_the_evidence():
    items = load_sample_news(SAMPLE_FILE, anchor=AS_OF)

    selected = ids(select_evidence(items, "TEST01", AS_OF))

    assert "sample-006" not in selected  # 변동 이후에 발행됨
    assert "sample-005" not in selected  # 24시간 창 밖
    assert selected == ["sample-003", "sample-001", "sample-002"]
