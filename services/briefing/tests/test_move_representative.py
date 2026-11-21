"""레벨 중 급변동의 대표 기사 고르기 규칙(move_representative.py). 가짜 기사만 쓰고 다른 서비스는 부르지 않는다.

규칙: 같은 사건 묶음이 있으면 기사가 가장 많은 묶음에서 변동 시각에 가장 가까운 기사, 묶음이 없으면 변동 직전에 발행된
가장 최근 기사, 고를 기사가 없으면 관련 기사 없음.
"""
from datetime import datetime, timedelta, timezone

from briefing.move_representative import pick_representative
from briefing.sources import Article

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 14, 39, tzinfo=KST)


def art(article_id, title, minutes_from_move, publisher="test.example.com"):
    return Article(article_id, title, "", T + timedelta(minutes=minutes_from_move), publisher, f"https://x.test/{article_id}")


SAME_A = "테스트 반도체 공급 계약 체결 발표"
SAME_B = "테스트 반도체 공급 계약 체결 소식"
SAME_C = "테스트 반도체 공급 계약 체결 확대"
OTHER_A = "다른 사건 신제품 출시 예정"
OTHER_B = "다른 사건 신제품 출시 확정"


def test_no_articles_means_no_related_article():
    picked = pick_representative([], T)

    assert (picked.article, picked.basis, picked.article_count) == (None, "no-article", 0)
    assert picked.record()["article"] is None


def test_without_a_group_the_latest_article_before_the_move_is_picked():
    picked = pick_representative([art("a", "첫째 소식 하나", -50), art("b", "둘째 전혀 다른 내용", -5),
                                  art("c", "셋째 별개의 뉴스", -20)], T)

    assert (picked.article.article_id, picked.basis, picked.group_size) == ("b", "latest-before", 0)


def test_an_article_published_exactly_at_the_move_time_counts_as_before_it():
    assert pick_representative([art("a", "하나 소식", -3), art("b", "둘 다른 내용", 0)], T).article.article_id == "b"


def test_articles_only_after_the_move_give_no_article_when_there_is_no_group():
    picked = pick_representative([art("a", "하나 소식", 1), art("b", "둘 다른 내용", 4)], T)

    assert (picked.article, picked.basis, picked.article_count) == (None, "no-article", 2)


def test_with_a_group_the_article_closest_to_the_move_time_is_picked_even_if_it_is_after_it():
    picked = pick_representative([art("a", SAME_A, -40), art("b", SAME_B, -12), art("c", SAME_C, 2),
                                  art("solo", "묶이지 않은 별개 소식", -1)], T)

    # 묶음이 있으면 묶이지 않은 가장 최근 기사(solo)보다 묶음의 기사를 고른다. 묶음 안에서는 |-12|보다 |2|가 가깝다.
    assert (picked.article.article_id, picked.basis, picked.group_size) == ("c", "group", 3)


def test_the_group_with_the_most_articles_wins_over_a_group_closer_to_the_move():
    picked = pick_representative([art("a", SAME_A, -60), art("b", SAME_B, -50), art("c", SAME_C, -40),
                                  art("x", OTHER_A, -2), art("y", OTHER_B, -1)], T)

    assert (picked.article.article_id, picked.group_size) == ("c", 3)


def test_groups_of_the_same_size_are_decided_by_the_article_closest_to_the_move():
    picked = pick_representative([art("a", SAME_A, -60), art("b", SAME_B, -50),
                                  art("x", OTHER_A, -30), art("y", OTHER_B, -4)], T)

    assert (picked.article.article_id, picked.group_size) == ("y", 2)


def test_equally_close_articles_in_a_group_pick_the_earlier_one():
    picked = pick_representative([art("late", SAME_B, 3), art("early", SAME_A, -3)], T)

    assert picked.article.article_id == "early"


def test_the_record_carries_what_the_post_close_edition_reads():
    picked = pick_representative([art("a", "하나 소식", -5, publisher="publisher.test")], T)

    assert picked.record() == {
        "article": {"articleId": "a", "title": "하나 소식", "publisher": "publisher.test",
                    "publishedAt": "2026-10-20T14:34:00+09:00", "url": "https://x.test/a"},
        "basis": "latest-before", "articleCount": 1, "groupSize": 0}
