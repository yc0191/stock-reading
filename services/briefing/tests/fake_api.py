"""QueryPort 가짜(시세처리기·뉴스처리기 내부 조회 API 대신). 맡긴 조회를 모아 두고, 테스트가 답하거나 끝내 실패시킨다.

답은 옛 Kafka 응답 이벤트 모양의 JSON(테스트 도우미가 만들던 것)이나 응답 본문(dict)으로 줄 수 있다. 응답 본문은 옛 응답
이벤트의 payload와 같다(docs/local-development.md "내부 조회 API"). 실제 HTTP는 쓰지 않는다.
"""
import json

from briefing.queries import QueryKind, QueryResult

# 옛 응답 이벤트 종류 -> 조회 종류. 테스트의 응답 도우미가 만든 JSON을 그대로 답으로 쓰려고 둔다.
RESPONSE_KINDS = {
    "market.history.responded": QueryKind.HISTORY,
    "market.context.responded": QueryKind.CONTEXT,
    "market.daily.responded": QueryKind.DAILY,
    "news.articles.responded": QueryKind.ARTICLES,
    "news.filings.responded": QueryKind.FILINGS,
}


class FakeApi:
    def __init__(self):
        # 맡긴 순서대로의 모든 조회와, 아직 답하지 않은 (조회, 콜백)
        self.submitted = []
        self.pending = []
        # 실행부(BriefingRunner)가 꺼내 갈 끝난 결과
        self.done = []
        self.closed = False

    # ---- QueryPort ----

    def submit(self, query, on_result):
        self.submitted.append(query)
        self.pending.append((query, on_result))

    # ---- 실행부용(drain·close) ----

    def drain(self):
        done, self.done = self.done, []
        return done

    def close(self):
        self.closed = True

    # ---- 테스트 도우미 ----

    def of(self, kind):
        return [q for q in self.submitted if q.kind is kind]

    def waiting(self, kind=None):
        return [q for q, _ in self.pending if kind is None or q.kind is kind]

    def reply(self, response, now, kind=None, attempts=1):
        """답을 준다. response는 옛 응답 이벤트 JSON(문자열) 또는 응답 본문(dict). 맞는 조회가 없으면 False.

        짝은 종류와 응답의 jobId·seq(기사는 symbol도)로 찾는다. 콜백은 바로 부른다(실행부를 거치지 않는 코어 테스트용).
        """
        found = self._take(response, kind)
        if found is None:
            return False
        (query, on_result), payload = found
        on_result(QueryResult(query, payload, attempts), now)
        return True

    def queue_reply(self, response, kind=None, attempts=1):
        """답을 실행부가 꺼내 갈 결과로 쌓는다(BriefingRunner.deliver_results 테스트용)."""
        found = self._take(response, kind)
        assert found is not None, "no pending query matches the response"
        (query, on_result), payload = found
        self.done.append((on_result, QueryResult(query, payload, attempts)))

    def fail(self, kind, now, job_id=None, symbol=None, attempts=4, failure_kind="timeout"):
        """맡긴 조회 하나를 끝내 받지 못한 것으로 끝낸다(가장 먼저 맡긴 것). 맞는 조회가 없으면 False."""
        for index, (query, on_result) in enumerate(self.pending):
            if query.kind is kind and (job_id is None or query.payload["jobId"] == job_id) \
                    and (symbol is None or query.payload.get("symbol") == symbol):
                del self.pending[index]
                on_result(QueryResult(query, None, attempts, failure_kind), now)
                return True
        return False

    def _take(self, response, kind):
        if isinstance(response, (str, bytes)):
            event = json.loads(response)
            kind = kind or RESPONSE_KINDS[event["eventType"]]
            payload = event["payload"]
        else:
            payload = response
        for index, (query, on_result) in enumerate(self.pending):
            if query.kind is not kind or query.payload["jobId"] != payload.get("jobId"):
                continue
            if "seq" in payload and query.payload["seq"] != payload["seq"]:
                continue
            if kind in (QueryKind.ARTICLES,) and payload.get("symbol") not in (None, query.payload["symbol"]) \
                    and "researchJobId" not in query.payload:
                continue
            del self.pending[index]
            return (query, on_result), payload
        return None
