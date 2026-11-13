"""데일리 판이 맡긴 기사·공시 조회에 가짜 뉴스처리기가 답하는 도움 함수."""
from briefing.queries import QueryKind


def answer_news(core, requests, now, articles=None, filings=None, silent=(), filings_silent=False):
    """아직 답하지 않은 기사 조회마다 articles[종목코드](기사 dict 목록, 없으면 0건)로, 공시 조회마다 filings(없으면 0건)로
    답한다. silent의 ETF 기사 조회는, filings_silent면 공시 조회는 답하지 않고 남겨 둔다."""
    articles = articles or {}
    for query in core.api.waiting():
        payload = query.payload
        if query.kind is QueryKind.ARTICLES and payload["symbol"] not in silent:
            core.api.reply({"jobId": payload["jobId"], "seq": payload["seq"], "symbol": payload["symbol"],
                            "articles": articles.get(payload["symbol"], []), "truncated": False},
                           now, kind=QueryKind.ARTICLES)
        elif query.kind is QueryKind.FILINGS and not filings_silent:
            core.api.reply({"jobId": payload["jobId"], "seq": payload["seq"], "symbol": payload["symbol"],
                            "filings": filings or [], "truncated": False}, now, kind=QueryKind.FILINGS)


def article(article_id, title, published_at, publisher="TEST 신문"):
    return {"articleId": article_id, "title": title, "summary": "TEST 요약", "publishedAt": published_at,
            "publisher": publisher, "url": f"https://example.test/{article_id}"}
