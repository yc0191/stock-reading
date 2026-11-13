"""같은 사건 기사 묶기. 급변동·데일리가 LLM 입력을 만들 때 제목 낱말 겹침과 발행 시각 차이로 기사를 묶는다.

묶음은 LLM이 여러 기사의 요약을 함께 읽게 하는 표시일 뿐 근거 번호를 새로 만들지 않는다. 기사마다 원래 ref가 그대로 남아
코드가 붙인 번호만 인용하고 목록 밖 번호는 거부하는 규칙이 바뀌지 않는다. 묶는 대상은 부르는 쪽이 시간 범위로 거른 기사뿐이다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

from briefing.config import DEFAULT_SETTINGS, CoreSettings

# LLM 입력에서 묶음 하나를 담는 칸. 묶이지 않은 기사는 지금처럼 기사 하나가 목록의 한 칸이다.
SAME_EVENT_KEY = "sameEventArticles"
# 묶음이 처음 모은 기사 목록에 놓였는데 재검색으로 찾은 기사가 함께 들었을 때 그 기사에 붙이는 표시
RESEARCH_MARK: Mapping[str, Any] = {"research": True}

_HEAD_TAG = re.compile(r"\[[^\]]*\]")
_WORD = re.compile(r"[0-9A-Za-z가-힣]+")


@dataclass(frozen=True)
class ArticleRow:
    """묶을 기사 하나. llm은 LLM 입력에 그대로 넣는 값이다."""

    llm: dict[str, Any]
    title: str
    published_at: datetime


@dataclass(frozen=True)
class GroupedArticles:
    """묶음을 반영한 기사 목록(들어온 목록마다 하나)과 묶음 수. 묶음은 기사 2건 이상인 것만 센다."""

    lists: tuple[list[dict[str, Any]], ...]
    groups: int
    grouped_articles: int

    def record(self) -> dict[str, int]:
        return {"groups": self.groups, "groupedArticles": self.grouped_articles}


def title_words(title: str, settings: CoreSettings = DEFAULT_SETTINGS) -> frozenset[str]:
    # [속보]·[포토] 같은 머리말은 사건과 무관하게 여러 기사에 붙어 겹침을 부풀리므로 뗀다.
    text = _HEAD_TAG.sub(" ", title)
    return frozenset(w.lower() for w in _WORD.findall(text) if len(w) >= settings.article_group_min_word_length)


def same_event(a: ArticleRow, b: ArticleRow, settings: CoreSettings = DEFAULT_SETTINGS) -> bool:
    """두 기사가 같은 사건인가: 발행 시각 차이가 기준 이내이고 제목 낱말이 기준 이상 겹친다(경계값 포함)."""
    return _matches(a.published_at, title_words(a.title, settings), b.published_at, title_words(b.title, settings),
                    settings)


def _matches(at_a: datetime, words_a: frozenset[str], at_b: datetime, words_b: frozenset[str],
             settings: CoreSettings) -> bool:
    if abs(at_a - at_b) > settings.article_group_max_gap:
        return False
    shared = len(words_a & words_b)
    # 낱말이 하나도 없는 제목끼리는 겹침을 잴 수 없으므로 묶지 않는다.
    if shared < settings.article_group_min_shared_words or shared == 0:
        return False
    return shared / len(words_a | words_b) >= settings.article_group_min_overlap


def same_event_groups(rows: Sequence[ArticleRow], settings: CoreSettings = DEFAULT_SETTINGS) -> list[list[int]]:
    """rows의 위치를 묶음별로 돌려준다. 발행 순으로 보며 묶음의 모든 기사와 같은 사건일 때만 그 묶음에 넣는다.

    한 기사와만 닮아도 넣으면 비슷한 제목이 사슬처럼 이어져 다른 사건까지 한 묶음이 되므로 모두와 맞아야 한다.
    """
    words = [title_words(r.title, settings) for r in rows]
    groups: list[list[int]] = []
    for n in sorted(range(len(rows)), key=lambda n: (rows[n].published_at, n)):
        for group in groups:
            if all(_matches(rows[n].published_at, words[n], rows[m].published_at, words[m], settings) for m in group):
                group.append(n)
                break
        else:
            groups.append([n])
    return groups


def group_article_lists(lists: Sequence[Sequence[ArticleRow]], settings: CoreSettings = DEFAULT_SETTINGS,
                        marks: Sequence[Mapping[str, Any]] = ()) -> GroupedArticles:
    """여러 기사 목록(처음 모은 기사, 재검색 기사)을 함께 묶고 목록마다 LLM 입력 칸을 만든다.

    묶음은 그 묶음에서 앞 목록·앞 자리에 있는 기사의 자리에 한 칸으로 놓이고, 다른 목록에서 온 기사에는 marks의 그 목록 표시를
    붙인다. 묶이지 않은 기사는 원래 자리에 하나씩 남는다.
    """
    flat = [(li, pos, row) for li, rows in enumerate(lists) for pos, row in enumerate(rows)]
    groups = [g for g in same_event_groups([row for _, _, row in flat], settings) if len(g) > 1]
    host: dict[int, list[int]] = {}
    member_of: set[int] = set()
    for group in groups:
        host[min(group)] = group
        member_of.update(group)
    out: tuple[list[dict[str, Any]], ...] = tuple([] for _ in lists)
    for n, (li, _, row) in enumerate(flat):
        if n in host:
            members = []
            for m in host[n]:
                other_li, _, other = flat[m]
                mark = marks[other_li] if other_li != li and other_li < len(marks) else {}
                members.append({**other.llm, **mark})
            out[li].append({SAME_EVENT_KEY: members})
        elif n not in member_of:
            out[li].append(row.llm)
    return GroupedArticles(out, len(groups), sum(len(g) for g in groups))
