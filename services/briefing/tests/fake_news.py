"""마감 후 판이 보낸 기사·공시 요청에 가짜 news가 답하는 도움 함수. 같은 요청에 두 번 답해도 DailyCore가 두 번째를 무시한다."""
import json


def _envelope(event_type, payload, at):
    return json.dumps({
        "eventId": "00000000-0000-4000-8000-000000000008", "eventType": event_type, "schemaVersion": 1,
        "occurredAt": at.isoformat(), "producedAt": at.isoformat(), "correlationId": "c", "payload": payload,
    })


def answer_news(core, requests, now, articles=None, filings=None, silent=()):
    """받은 기사 요청마다 articles[종목코드](기사 dict 목록, 없으면 0건)로, 공시 요청마다 filings(없으면 0건)로 답한다. silent의 ETF 기사 요청은 답하지 않는다."""
    articles = articles or {}
    for event in list(requests.sent):
        payload = event.value["payload"]
        if event.topic == "news.articles.requested" and payload["symbol"] not in silent:
            core.on_articles_responded(_envelope("news.articles.responded", {
                "jobId": payload["jobId"], "seq": payload["seq"], "symbol": payload["symbol"],
                "articles": articles.get(payload["symbol"], []), "truncated": False}, now), now)
        elif event.topic == "news.filings.requested":
            core.on_filings_responded(_envelope("news.filings.responded", {
                "jobId": payload["jobId"], "seq": payload["seq"], "symbol": payload["symbol"],
                "filings": filings or [], "truncated": False}, now), now)


def article(article_id, title, published_at, publisher="TEST 신문"):
    return {"articleId": article_id, "title": title, "summary": "TEST 요약", "publishedAt": published_at,
            "publisher": publisher, "url": f"https://example.test/{article_id}"}
