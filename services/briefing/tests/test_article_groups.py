from dataclasses import replace
from datetime import datetime, timedelta, timezone

from briefing.article_groups import (
    RESEARCH_MARK,
    SAME_EVENT_KEY,
    ArticleRow,
    group_article_lists,
    same_event,
    same_event_groups,
    title_words,
)
from briefing.config import CoreSettings

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 11, 10, 14, 0, tzinfo=KST)
# 기준값을 테스트 안에서 정해 기본값이 바뀌어도 경계 검사가 흔들리지 않게 한다.
SETTINGS = CoreSettings(article_group_min_overlap=0.5, article_group_min_shared_words=2,
                        article_group_max_gap=timedelta(hours=2), article_group_min_word_length=2)


def row(ref: str, title: str, minutes: float = 0) -> ArticleRow:
    return ArticleRow({"ref": ref, "title": title}, title, AT + timedelta(minutes=minutes))


def test_title_words_drop_bracket_heads_short_words_and_case():
    assert title_words("[속보] 테스트상사 3분기 영업익 A 최대", SETTINGS) == {"테스트상사", "3분기", "영업익", "최대"}
    assert title_words("KODEX ETF", SETTINGS) == {"kodex", "etf"}


def test_two_articles_with_enough_shared_words_close_in_time_are_one_event():
    # 공통 2개 / 합집합 4개 = 0.5 (기준과 같음, 포함)
    a = row("a1", "테스트상사 영업익 최대")
    b = row("a2", "테스트상사 영업익 기록", minutes=120)

    assert same_event(a, b, SETTINGS)


def test_articles_further_apart_than_the_gap_are_not_one_event_even_with_the_same_title():
    a = row("a1", "테스트상사 영업익 최대")
    b = row("a2", "테스트상사 영업익 최대", minutes=121)

    assert not same_event(a, b, SETTINGS)


def test_articles_below_the_overlap_ratio_are_not_one_event_even_at_the_same_time():
    # 공통 2개 / 합집합 5개 = 0.4 < 0.5
    a = row("a1", "테스트상사 영업익 최대")
    b = row("a2", "테스트상사 영업익 기록 경신")

    assert not same_event(a, b, SETTINGS)


def test_one_shared_word_is_not_enough_even_when_the_ratio_is_high():
    loose = replace(SETTINGS, article_group_min_overlap=0.1)

    assert not same_event(row("a1", "코스피 급락"), row("a2", "코스피 반등"), loose)
    assert not same_event(row("a1", "[속보]"), row("a2", "[속보]"), replace(loose, article_group_min_shared_words=0))


def test_an_article_joins_a_group_only_when_it_matches_every_member():
    rows = [row("a1", "코스피 급락 매도 사이드카"),
            row("a2", "코스피 급락 매도 사이드카 발동", minutes=5),
            # a2와는 공통 3/6으로 맞지만 a1과는 공통 2/6이라 묶이지 않는다(사슬처럼 이어지지 않음).
            row("a3", "매도 사이드카 발동 올해", minutes=10)]

    assert sorted(map(sorted, same_event_groups(rows, SETTINGS))) == [[0, 1], [2]]


def test_ungrouped_articles_stay_one_per_slot_and_groups_take_the_first_member_slot():
    initial = [row("a1", "반도체 수출 증가"), row("a2", "테스트상사 영업익 최대", minutes=10),
               row("a3", "환율 상승 마감", minutes=20)]
    research = [row("r1", "테스트상사 영업익 기록 최대", minutes=30), row("r2", "금리 동결")]

    grouped = group_article_lists([initial, research], SETTINGS, marks=({}, RESEARCH_MARK))

    articles, research_articles = grouped.lists
    assert articles == [{"ref": "a1", "title": "반도체 수출 증가"},
                        {SAME_EVENT_KEY: [{"ref": "a2", "title": "테스트상사 영업익 최대"},
                                          {"ref": "r1", "title": "테스트상사 영업익 기록 최대", "research": True}]},
                        {"ref": "a3", "title": "환율 상승 마감"}]
    assert research_articles == [{"ref": "r2", "title": "금리 동결"}]
    assert grouped.record() == {"groups": 1, "groupedArticles": 2}


def test_no_group_keeps_the_lists_as_they_were():
    rows = [row("a1", "반도체 수출 증가"), row("a2", "환율 상승 마감")]

    grouped = group_article_lists([rows], SETTINGS)

    assert grouped.lists == ([r.llm for r in rows],)
    assert grouped.record() == {"groups": 0, "groupedArticles": 0}
