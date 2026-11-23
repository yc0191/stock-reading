"""QueryPort 가짜(시세처리기·뉴스처리기 내부 조회 API 대신). 맡긴 조회를 모아 두고, 테스트가 답하거나 끝내 실패시킨다.

답은 옛 Kafka 응답 이벤트 모양의 JSON(테스트 도우미가 만들던 것)이나 응답 본문(dict)으로 줄 수 있다. 응답 본문은 옛 응답
이벤트의 payload와 같다(docs/conventions.md "내부 조회 API"). 실제 HTTP는 쓰지 않는다.
"""
import json
from datetime import datetime, timedelta, timezone

from briefing.daily_jobs import STAGE_PREV_MARKET
from briefing.job_log import STAGE_CHART_HISTORY
from briefing.queries import QueryKind, QueryResult

# 옛 응답 이벤트 종류 -> 조회 종류. 테스트의 응답 도우미가 만든 JSON을 그대로 답으로 쓰려고 둔다.
RESPONSE_KINDS = {
    "market.history.responded": QueryKind.HISTORY,
    "market.context.responded": QueryKind.CONTEXT,
    "market.daily.responded": QueryKind.DAILY,
    "news.articles.responded": QueryKind.ARTICLES,
    "news.filings.responded": QueryKind.FILINGS,
}


def prev_market_background(as_of, kospi="-1.23", kosdaq="0.45", fetched_at=None):
    """전 거래일 시장종합 조회에 답할 시장종합 배경 한 건. 받은 시각은 기본으로 asOf(판 날짜 0시) 7시간 30분 전, 곧 전날 16:30이다."""
    at = fetched_at or (datetime.fromisoformat(as_of) - timedelta(hours=7, minutes=30)).isoformat(timespec="seconds")
    rows = [{"indx_nm": "KOSPI종합", "now_indx_p2": "2500.00", "bdy_cmpr_p2": "30.00",
             "bdy_cmpr_ccd": "5" if kospi.startswith("-") else "2", "up_dwn_r_p2": kospi},
            {"indx_nm": "KOSDAQ종합", "now_indx_p2": "800.00", "bdy_cmpr_p2": "3.50",
             "bdy_cmpr_ccd": "5" if kosdaq.startswith("-") else "2", "up_dwn_r_p2": kosdaq}]
    return {"apiType": "market-summary", "target": None, "status": "ok",
            "response": json.dumps({"dataBody": {"out2": rows}}, ensure_ascii=False), "fetchedAt": at}


class FakeApi:
    def __init__(self):
        # 맡긴 순서대로의 모든 조회와, 아직 답하지 않은 (조회, 콜백)
        self.submitted = []
        self.pending = []
        # 실행부(BriefingRunner)가 꺼내 갈 끝난 결과
        self.done = []
        self.closed = False
        # 장중 차트용 시세 조회(레벨 상)는 기본으로 점 없이 바로 답하고 submitted에 넣지 않는다. 대부분의 테스트가 차트를 보지 않아
        # 조회 순서·개수 확인을 건드리지 않게 하려는 것이다. 차트를 시험하는 테스트는 auto_chart를 False로 두고 직접 답한다.
        self.auto_chart = True
        # 아침 판의 전 거래일 시장종합 조회도 같은 까닭으로 기본은 바로 답한다. 기본 답은 운영에서 보통 그렇듯 전날 16:30에 받은
        # 정상 시장종합이다(prev_market_background). 그 칸을 시험하는 테스트는 prev_market에 답할 배경 한 건(dict)을 넣거나,
        # auto_prev_market을 False로 두고 직접 답한다.
        self.auto_prev_market = True
        self.prev_market = None

    # ---- QueryPort ----

    def submit(self, query, on_result):
        if self.auto_chart and query.stage == STAGE_CHART_HISTORY:
            on_result(QueryResult(query, {"points": [], "truncated": False}, 1), datetime.now(timezone.utc))
            return
        if self.auto_prev_market and query.stage == STAGE_PREV_MARKET:
            background = self.prev_market or prev_market_background(query.payload["asOf"])
            on_result(QueryResult(query, {"jobId": query.payload["jobId"], "seq": query.payload["seq"], "etfs": [],
                                          "closeRates": [], "background": [background], "truncated": False}, 1),
                      datetime.now(timezone.utc))
            return
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
