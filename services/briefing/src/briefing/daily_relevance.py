"""ETF와 관계없는 기사 걸러내기. Kafka를 모르며 ETF 정보와 기사를 인자로 받는다.

뉴스 서비스는 종목 태그로 기사를 모아 주므로 그 ETF와 상관없는 기사가 섞일 수 있다. ETF 이름·추종 대상·구성 종목 이름이
제목과 요약에 하나도 나오지 않는 기사는 그 ETF의 이유 근거로 쓰지 않는다. 띄어쓰기와 대소문자는 무시하고, ETF 이름은
운용사 접두어를 뗀 나머지로도 비교한다. 비교할 이름이 없는 ETF(정보가 비어 온 ETF)는 거르지 않고 그대로 둔다.
"""
from __future__ import annotations

import re
from typing import Iterable

from briefing.config import DEFAULT_DAILY_SETTINGS, DailySettings
from briefing.sources import Article, EtfInfo

# 한 글자 이름은 아무 글에나 들어 있어 비교 대상에서 뺀다.
MIN_TERM_LENGTH = 2
_BRAND = re.compile(r"[A-Z][A-Z0-9]*")
_LETTER = re.compile(r"[A-Za-z가-힣]")


def _normalize(text: str) -> str:
    """띄어쓰기와 대소문자 차이로 같은 이름을 놓치지 않게 맞춘다."""
    return re.sub(r"\s+", "", text).casefold()


def _without_brand(name: str) -> str | None:
    """이름 첫 낱말이 영문 대문자·숫자뿐인 브랜드(운용사 접두어)이면 그것을 뗀 나머지. 뗄 것이 없거나 나머지에
    글자가 없으면(숫자·기호뿐이면 '1200원' 같은 아무 기사에나 걸린다) None."""
    first, _, rest = name.strip().partition(" ")
    return rest if _LETTER.search(rest) and _BRAND.fullmatch(first) else None


def _trimmed(name: str, settings: DailySettings) -> str | None:
    """공백을 없앤 이름에서 앞의 나라 이름과 끝의 접미어를 뗀 것. 뗄 것이 없거나 나머지에 글자가 없으면 None."""
    text = _normalize(name)
    for prefix in settings.relevance_country_prefixes:
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    for suffix in settings.relevance_suffixes:
        if text.endswith(suffix):
            text = text[:-len(suffix)]
            break
    return text if text != _normalize(name) and _LETTER.search(text) else None


def etf_terms(info: EtfInfo | None, settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[str, ...]:
    """기사에 나오는지 비교할 이름: ETF 이름(접두어를 뗀 것도), 추종 대상, 구성 종목 이름, 그리고 이름·추종 대상에서
    나라 이름·접미어를 뗀 것. 정보가 없으면 비어 있다."""
    if info is None:
        return ()
    base = [n for n in (info.name, _without_brand(info.name) if info.name else None, info.tracks) if n]
    names = (*base, *(_trimmed(n, settings) for n in base), *(m.name for m in info.members))
    return tuple(dict.fromkeys(n for n in (_normalize(n) for n in names if n) if len(n) >= MIN_TERM_LENGTH))


def market_terms(info: EtfInfo | None, settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[str, ...]:
    """미국 종목을 담은 ETF의 기사에 시장 단어가 있으면 관계있다고 본다. 그렇지 않은 ETF는 비어 있다."""
    if info is None or not info.us_tickers():
        return ()
    return tuple(_normalize(t) for t in settings.relevance_us_market_terms)


def is_related(article: Article, terms: tuple[str, ...]) -> bool:
    text = _normalize(f"{article.title} {article.summary}")
    return any(term in text for term in terms)


def split_related(info: EtfInfo | None, articles: Iterable[Article],
                  settings: DailySettings = DEFAULT_DAILY_SETTINGS) -> tuple[tuple[Article, ...], tuple[Article, ...]]:
    """(남긴 기사, 뺀 기사). 순서는 그대로다. 비교할 이름이 없으면 모두 남긴다."""
    articles = tuple(articles)
    terms = etf_terms(info, settings)
    if not terms:
        return articles, ()
    terms += market_terms(info, settings)
    kept = tuple(a for a in articles if is_related(a, terms))
    return kept, tuple(a for a in articles if not is_related(a, terms))
