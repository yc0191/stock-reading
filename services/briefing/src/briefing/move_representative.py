"""레벨 중 급변동의 대표 기사 고르기. LLM 없이 코드가 근거 시간 범위의 종목 기사 하나를 고른다. Kafka와 DB를 모른다.

'대표 기사'는 원인으로 단정한 기사가 아니라 그 변동 때 가장 눈에 띈 기사다. 규칙은 다음 순서다.
1. 같은 사건 묶음(article_groups)이 있으면, 기사가 가장 많은 묶음에서 변동 시각에 가장 가까운 기사
2. 묶음이 없으면 변동 직전(변동 시각 이전)에 발행된 가장 최근 기사
3. 고를 기사가 없으면 '관련 기사 없음'
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from briefing.article_groups import ArticleRow, same_event_groups
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import format_timestamp
from briefing.job_log import REASON_REPRESENTATIVE_GROUP, REASON_REPRESENTATIVE_LATEST, REASON_REPRESENTATIVE_NONE
from briefing.sources import Article


@dataclass(frozen=True)
class Representative:
    """고른 결과. article이 None이면 관련 기사 없음이다. basis는 고른 근거(job_log의 REASON_REPRESENTATIVE_*)다."""

    article: Article | None
    basis: str
    # 근거 시간 범위의 기사 수와, 기사를 고른 묶음의 기사 수(묶음에서 고르지 않았으면 0)
    article_count: int
    group_size: int = 0

    def record(self) -> dict:
        """작업 기록(representative-article 줄의 data)에 담을 값. 마감 후 판이 title·publisher·publishedAt을 읽는다."""
        a = self.article
        return {
            "article": None if a is None else {"articleId": a.article_id, "title": a.title, "publisher": a.publisher,
                                                "publishedAt": format_timestamp(a.published_at), "url": a.url},
            "basis": self.basis, "articleCount": self.article_count, "groupSize": self.group_size,
        }


def pick_representative(articles: Sequence[Article], as_of: datetime,
                        settings: CoreSettings = DEFAULT_SETTINGS) -> Representative:
    """부르는 쪽이 근거 시간 범위로 거른 종목 기사에서 대표 기사 하나를 고른다."""
    rows = [ArticleRow({}, a.title, a.published_at) for a in articles]
    groups = [g for g in same_event_groups(rows, settings) if len(g) > 1]
    if groups:
        # 기사가 가장 많은 묶음. 같으면 변동 시각에 더 가까운 기사가 든 묶음이 이긴다.
        best = max(groups, key=lambda g: (len(g), -min(_gap(articles[n], as_of) for n in g)))
        chosen = min((articles[n] for n in best), key=lambda a: (_gap(a, as_of), a.published_at, a.article_id))
        return Representative(chosen, REASON_REPRESENTATIVE_GROUP, len(articles), len(best))
    before = [a for a in articles if a.published_at <= as_of]
    if before:
        latest = max(before, key=lambda a: (a.published_at, a.article_id))
        return Representative(latest, REASON_REPRESENTATIVE_LATEST, len(articles))
    return Representative(None, REASON_REPRESENTATIVE_NONE, len(articles))


def _gap(article: Article, as_of: datetime):
    return abs(article.published_at - as_of)
