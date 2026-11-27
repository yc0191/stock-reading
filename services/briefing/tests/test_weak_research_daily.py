"""문장 단위 근거 약함과 재검색 시나리오 중 데일리 판(아침 판·마감 후 판): 가짜 LLM·가짜 응답으로 판마다 돌린다.

경우와 원칙은 급변동 쪽(test_weak_research_move.py)과 같다. 실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다.
"""
import json
from datetime import timedelta

import pytest

from fake_anthropic import as_reply
from briefing.config import CoreSettings, DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_sections import DAILY_INSUFFICIENT
from briefing.daily_report import DailyLlmWriter
from briefing.job_log import EntryType, RunStatus
from briefing.llm_output import WEAK_NOTE_PREFIX
from briefing.research import QueryWriter, ResearchReason, ResearchStep
from briefing.sentence_research import SentenceResearch, research_job_id
from briefing.status import parse_collection
from briefing.queries import QueryKind
from contracts import validate, validate_payload
from fake_news import answer_news
from fake_job_log import FakeJobLog
from test_daily_jobs import (
    MORNING_AT,
    MORNING_JOB,
    POST_CLOSE_AT,
    POST_CLOSE_JOB,
    TICK_ID,
    MARKET,
    FakeLlm,
    Requests,
    daily_response,
    envelope,
    etf,
    filings_response,
    tick,
)
from test_research import QUERIES, queries
from test_weak_research_move import S, SEC, SECOND_SHORTFALL, SHORTFALL, STILL_SHORTFALL, STRONG_TEXT


# ---- 데일리 판 ----

EDITIONS = {
    # 판: (작업 종류, 예정 시각, 첫 요약이 인용할 근거 번호)
    "morning": (MORNING_JOB, MORNING_AT, "B1"),
    "post-close": (POST_CLOSE_JOB, POST_CLOSE_AT, "E1"),
}


def point(statement, ref, shortfall=None):
    return {"statement": statement, "refs": [ref], "weak": shortfall is not None, "shortfall": shortfall or ""}


def daily_output(*points, insufficient=False):
    return json.dumps({"points": list(points), "insufficientEvidence": insufficient}, ensure_ascii=False)


class DailyLlm(FakeLlm):
    """판마다 같은 문장 목록(points)으로 쓴 응답을 마감 후 판 출력 형식으로 바꿔 주는 가짜 LLM.

    처음 쓰기에서 n번째 문장은 n번째 ETF의 이유가 되고(입력 순서), 남는 ETF는 튼튼한 이유로 뒤에 채운다.
    ETF가 없으면 문장은 눈에 띄는 점이 된다. 다시 쓰기 응답은 두 판 모두 points 하나라 그대로 둔다."""

    def __init__(self, edition, *outputs):
        super().__init__(*outputs)
        self.edition = edition

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
        output = super().complete(system, user, output_schema).text
        if "다시 쓰기 요청" in system or isinstance(output, Exception):
            return output
        if self.edition == "morning":
            return self._morning(json.loads(output), json.loads(user))
        return self._post_close(json.loads(output), json.loads(user))

    @staticmethod
    def _morning(data, user):
        """문장 목록은 그대로 요약(points)에 두고, 모든 ETF에는 튼튼한 방향 예상과 환율·금리 이슈 칸을 채운다."""
        if data["insufficientEvidence"]:
            return json.dumps({"points": [], "etfs": [], "insufficientEvidence": True})
        issue = {"statement": "환율이 영향을 줄 수 있습니다.", "refs": ["B1"], "weak": False, "shortfall": ""}
        strong = {"direction": "상승", "reason": "미국 지수가 영향을 줄 수 있습니다.", "refs": ["B1"], "weak": False,
                  "shortfall": ""}
        # etfs를 준 응답은 그 예상을 앞 ETF부터 차례로 쓰고, 남는 ETF는 튼튼한 예상으로 채운다.
        given = data.pop("etfs", [])
        etfs = [{**(given[n] if n < len(given) else strong), "symbol": e["symbol"]}
                for n, e in enumerate(user["outlookEtfs"])]
        return json.dumps({**data, "etfs": etfs, "fxIssue": issue, "rateIssue": issue}, ensure_ascii=False)

    @staticmethod
    def _post_close(data, user):
        if data["insufficientEvidence"]:
            return json.dumps({"summary": [], "etfs": [], "notables": [], "insufficientEvidence": True})
        day = next(i for i in user["items"] if i["name"] == "보유 ETF별 하루 등락")["entries"]
        etfs, notables = [], []
        for n, p in enumerate(data["points"]):
            if n < len(day):
                etfs.append({"symbol": day[n]["symbol"], "reason": p["statement"], "refs": p["refs"],
                             "weak": p["weak"], "shortfall": p["shortfall"]})
            elif user["notableCandidates"]:
                notables.append(p)
        etfs += [{"symbol": e["symbol"], "reason": "시장 흐름과 함께 움직였을 수 있습니다.", "refs": [e["ref"]],
                  "weak": False, "shortfall": ""} for e in day[len(etfs):]]
        return json.dumps({"summary": ["첫째 줄입니다.", "둘째 줄입니다."], "etfs": etfs, "notables": notables,
                           "insufficientEvidence": False}, ensure_ascii=False)


class DailyFlow:
    """판 하나를 자료 수집 -> 요약 -> 문장 단위 재검색 -> 다시 쓰기까지 돌리는 도우미."""

    def __init__(self, edition, report_outputs, query_outputs, etfs=None, settings=S, status=None, daily_settings=None):
        self.edition = edition
        self.job_type, self.at, self.ref = EDITIONS[edition]
        self.requests, self.records = Requests(), FakeJobLog()
        self.report_llm, self.query_llm = DailyLlm(edition, *report_outputs), FakeLlm(*query_outputs)
        daily_settings = daily_settings or DailySettings()
        self.sentences = SentenceResearch(
            QueryWriter(self.query_llm, settings.max_research_queries, settings.research_query_attempts),
            self.requests, self.requests.api, self.records, settings)
        self.research = self.sentences.research
        self.core = DailyCore(
            self.requests, self.requests.api, DailyLlmWriter(self.report_llm, daily_settings.llm_max_attempts), self.records,
            daily_settings, sentences=self.sentences, status=status)
        self.now = self.at
        self.etfs = etfs

    def rid(self, no, round_no=1):
        return research_job_id(TICK_ID, no, round_no)

    def to_first_report(self, articles=None):
        """articles: 종목코드별 ETF 기사 dict 목록(없으면 0건)."""
        self.core.on_collection_ready(tick(self.job_type, self.at), self.now)
        etfs, background, close_rates = self.etfs, None, None
        if self.edition == "post-close":
            # 마감 후 판의 범위는 그날 오후라 그 안에 받은 값으로 바꾼다.
            fetched = "2026-10-20T16:30:02+09:00"
            if etfs is None:
                # 마감 후 판은 ETF마다 이유 한 줄을 쓰므로 문장 세 개를 담을 ETF 셋을 둔다.
                etfs = [etf(fetched=fetched), etf("TEST02", tickers=(), fetched=fetched),
                        etf("TEST03", tickers=(), fetched=fetched)]
            else:
                etfs = [{**e, "fetchedAt": fetched} for e in etfs]
            background = [{**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00"}]
            close_rates = []
        self.core.api.reply(
            daily_response(etfs=etfs, background=background, close_rates=close_rates), self.now)
        # 두 판 모두 ETF별 기사를 받아야 나가고, 아침 판은 미국 종목이 있으면 공시도 받는다.
        filings = json.loads(filings_response())["payload"]["filings"] if self.edition == "morning" else None
        answer_news(self.core, self.requests, self.now, articles=articles, filings=filings)

    def news(self, seq, items, research_id):
        return envelope("news.articles.responded", {
            "jobId": research_id, "seq": seq, "symbol": "TEST01", "truncated": False, "articles": items})

    def complete(self, research_id):
        self.now += SEC
        event = json.loads(envelope("news.stored", {
            "jobType": "research-news", "startedAt": self.now.isoformat(), "incompleteSymbols": [],
            "unprocessedEventIds": [], "researchJobId": research_id}))
        return self.research.on_stored(json.dumps(event), self.now)

    def stable(self, items, research_id):
        """재검색 기사 조회에 답한다. 저장 끝 알림 뒤라 한 번 받으면 끝이다."""
        self.now += SEC
        assert self.research.api.reply(self.news(1, items, research_id), self.now) is True

    def stable_jobs(self, items_by_job):
        """여러 재검색 작업의 기사 조회에 차례로 답한다."""
        for research_id, items in items_by_job.items():
            self.stable(items, research_id)

    def published(self):
        events = self.requests.topic("briefing.daily.generated")
        for event in events:
            assert validate("briefing.daily.generated.v2.schema.json", event.value) == []
        return events

    def research_requests(self):
        return self.requests.topic("news.research.requested")


def news_article(article_id, published_at):
    return {"articleId": article_id, "title": f"기사 {article_id}", "summary": "", "publisher": "news.example.com",
            "publishedAt": published_at.isoformat(), "url": f"https://news.example.com/{article_id}"}


@pytest.fixture(params=sorted(EDITIONS))
def edition(request):
    return request.param


def in_range(flow, article_id="r1"):
    return news_article(article_id, flow.at - timedelta(hours=1))


def out_of_range(flow):
    return news_article("r2", flow.at - timedelta(days=5))


def weak_shown(text, shortfall):
    """본문에 부족한 점이 보이는가. 마감 후 판은 약한 줄 바로 아래에, 아침 판은 면책 위 근거 약함 모음에 쓴다."""
    return (WEAK_NOTE_PREFIX + shortfall) in text or f": {shortfall}" in text


def two_weak(flow):
    return daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref),
                        point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL),
                        point("공시가 영향을 줬을 수 있습니다.", flow.ref, SECOND_SHORTFALL))


def test_daily_with_every_sentence_strong_is_published_without_research(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref))]
    flow.to_first_report()

    assert flow.query_llm.calls == [] and flow.research_requests() == []
    (event,) = flow.published()
    assert "근거 약함" not in event.value["payload"]["text"]
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (0, 0)


def test_daily_with_one_weak_sentence_researches_only_that_sentence_and_rewrites_only_it(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [
        daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref),
                     point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1")),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()

    ((_, user, _),) = flow.query_llm.calls
    context = json.loads(user)
    assert context["sentence"] == "업종 소식이 영향을 줬을 수 있습니다." and context["shortfall"] == SHORTFALL
    assert context["foundArticleTitles"] == [] and context["edition"]
    assert flow.published() == []
    (request,) = flow.research_requests()
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    # 데일리 재검색은 시장 전체를 뜻하는 태그 *로 부르고, 작업 종류는 판 이름이다. 문장마다 따로 id를 가진다.
    assert request.value["payload"] == {"jobId": flow.rid(2), "seq": 1, "jobKind": edition, "symbol": "*",
                                        "queries": QUERIES}

    assert flow.complete(flow.rid(2)) is True
    (first,) = [q for q in flow.requests.api.of(QueryKind.ARTICLES) if "researchJobId" in q.payload]
    assert validate_payload("news.articles.requested.v1.schema.json", first.payload) == []
    # 재검색 기사 조회의 job_step은 판 작업에 남는다.
    assert first.job_id == TICK_ID
    payload = first.payload
    # 기사 시간 범위는 판 범위(예정 시각 포함)이고, 조회 symbol은 정렬한 첫 ETF 코드다.
    assert payload["since"] == (flow.core._jobs[TICK_ID].range_start).isoformat()
    assert payload["until"] == flow.at.isoformat()
    assert (payload["symbol"], payload["researchJobId"], payload["seq"]) == ("TEST01", flow.rid(2), 1)

    # 범위 밖 기사는 버리고 범위 안 기사만 다시 쓰는 데 쓴다.
    flow.stable([in_range(flow), out_of_range(flow)], flow.rid(2))

    rewrite = json.loads(flow.report_llm.calls[1][1])
    # 재검색 기사는 items 밖에 찾은 순서대로 붙는다(처음 작성의 근거 칸을 그대로 두어 캐시를 읽는다).
    assert all(i["name"] != "재검색 기사" for i in rewrite["items"])
    assert [e["ref"] for e in rewrite["researchArticles"]] == ["r1"]
    assert rewrite["rewrite"]["sentenceNo"] == 2 and rewrite["rewrite"]["newRefs"] == ["r1"]
    assert all(i["name"] != "재검색 기사" for i in json.loads(flow.report_llm.calls[0][1])["items"])
    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert "근거 약함" not in text and "지수가 오른 가운데 자료가 있습니다." in text
    assert "업종 소식이 영향을 줬을 수 있습니다." not in text
    # 재검색 기사도 기존 자료처럼 근거 번호와 링크로 본문에 나온다.
    assert "https://news.example.com/r1" in text and "r2" not in text
    assert len(flow.research_requests()) == 1
    (entry,) = flow.records.of_type(EntryType.SENTENCE_REWRITTEN)
    assert (entry.sentence_no, entry.round) == (2, 1)
    assert entry.before == "업종 소식이 영향을 줬을 수 있습니다." and entry.after == "다시 찾은 기사가 영향을 줬을 수 있습니다."
    (result,) = flow.records.results()
    assert (result.status, result.research_count, result.weak_count) == (RunStatus.SUCCEEDED, 1, 0)


def test_morning_rewrite_leaves_exactly_one_outlook_record_that_matches_the_final_body():
    flow = DailyFlow("morning", [], [])
    flow.report_llm.outputs = [
        daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref),
                     point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1")),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    # 다시 쓰기가 끝나기 전에는 예상을 남기지 않는다.
    assert flow.records.of_type(EntryType.ETF_OUTLOOK) == []
    flow.complete(flow.rid(2))
    flow.stable([in_range(flow)], flow.rid(2))

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    (entry,) = flow.records.of_type(EntryType.ETF_OUTLOOK)
    assert entry.data["outlooks"]
    for outlook in entry.data["outlooks"]:
        assert f"{outlook['direction']} 예상" in text and outlook["reason"] in text
    assert "다시 찾은 기사가 영향을 줬을 수 있습니다." in text


def test_a_rewritten_morning_outlook_leaves_its_new_direction_in_the_outlook_record():
    flow = DailyFlow("morning", [], [])
    unsure = {"direction": "예상 어려움", "reason": "근거가 부족합니다.", "refs": [], "weak": True, "shortfall": SHORTFALL}
    rewritten = json.dumps({"points": [{"direction": "하락", "statement": "새 기사로 내릴 수 있습니다.", "refs": ["r1"],
                                        "weak": False, "shortfall": ""}], "insufficientEvidence": False},
                           ensure_ascii=False)
    flow.report_llm.outputs = [json.dumps({**json.loads(daily_output(point(STRONG_TEXT, flow.ref))), "etfs": [unsure]},
                                          ensure_ascii=False), rewritten]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    # 문장 번호: 밤사이 이슈 1, 환율 2, 금리 3, 첫 ETF 예상 4.
    flow.complete(flow.rid(4))
    flow.stable([in_range(flow)], flow.rid(4))

    (event,) = flow.published()
    (entry,) = flow.records.of_type(EntryType.ETF_OUTLOOK)
    first = entry.data["outlooks"][0]
    # 마감 후 판이 다시 읽어 대조하는 기록에 다시 쓴 방향과 이유가 남는다.
    assert (first["direction"], first["reason"], first["weak"]) == ("하락", "새 기사로 내릴 수 있습니다.", False)
    assert "하락 예상" in event.value["payload"]["text"]


def test_daily_with_several_weak_sentences_sends_one_separate_research_job_per_sentence(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [two_weak(flow), daily_output(point("둘째 문장을 다시 썼습니다.", "r1")),
                               daily_output(point("셋째 문장을 다시 썼습니다.", "r3"))]
    flow.query_llm.outputs = [queries("검색어 하나"), queries("검색어 둘")]
    flow.to_first_report()

    assert [r.value["payload"]["jobId"] for r in flow.research_requests()] == [flow.rid(2), flow.rid(3)]
    assert [json.loads(c[1])["shortfall"] for c in flow.query_llm.calls] == [SHORTFALL, SECOND_SHORTFALL]
    flow.complete(flow.rid(2))
    flow.complete(flow.rid(3))
    flow.stable_jobs({flow.rid(2): [in_range(flow)], flow.rid(3): [in_range(flow, "r3")]})

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert "둘째 문장을 다시 썼습니다." in text and "셋째 문장을 다시 썼습니다." in text
    assert "지수가 오른 가운데 자료가 있습니다." in text and "근거 약함" not in text
    (result,) = flow.records.results()
    assert (result.research_count, result.weak_count) == (2, 0)


def test_daily_sentence_that_becomes_strong_on_the_second_search_is_searched_twice(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [
        daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾았지만 아직 약합니다.", "r1", STILL_SHORTFALL)),
        daily_output(point("한 번 더 찾은 기사로 확인했습니다.", "r2")),
    ]
    flow.query_llm.outputs = [queries("검색어 하나"), queries("검색어 둘")]
    flow.to_first_report()
    flow.complete(flow.rid(1, 1))
    flow.stable([in_range(flow)], flow.rid(1, 1))
    assert flow.published() == []
    assert [r.value["payload"]["jobId"] for r in flow.research_requests()] == [flow.rid(1, 1), flow.rid(1, 2)]
    assert json.loads(flow.query_llm.calls[1][1])["shortfall"] == STILL_SHORTFALL
    flow.complete(flow.rid(1, 2))
    flow.stable([in_range(flow, "r2")], flow.rid(1, 2))

    (event,) = flow.published()
    assert "한 번 더 찾은 기사로 확인했습니다." in event.value["payload"]["text"]
    assert "근거 약함" not in event.value["payload"]["text"]
    assert [(e.sentence_no, e.round) for e in flow.records.of_type(EntryType.SENTENCE_REWRITTEN)] == [(1, 1), (1, 2)]


def test_daily_sentence_still_weak_after_two_searches_carries_the_shortfall_and_is_not_searched_again(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [
        daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾았지만 아직 약합니다.", "r1", STILL_SHORTFALL)),
        daily_output(point("한 번 더 찾았지만 여전히 약합니다.", "r2", SECOND_SHORTFALL)),
    ]
    flow.query_llm.outputs = [queries("검색어 하나"), queries("검색어 둘")]
    flow.to_first_report()
    flow.complete(flow.rid(1, 1))
    flow.stable([in_range(flow)], flow.rid(1, 1))
    flow.complete(flow.rid(1, 2))
    flow.stable([in_range(flow, "r2")], flow.rid(1, 2))

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert weak_shown(text, SECOND_SHORTFALL) and not weak_shown(text, STILL_SHORTFALL)
    assert len(flow.research_requests()) == 2 and len(flow.query_llm.calls) == 2
    (limit,) = flow.records.of_reason(ResearchReason.SENTENCE_LIMIT)
    assert (limit.stage, limit.sentence_no) == (ResearchStep.LIMIT.value, 1)
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)


def test_daily_sentences_over_the_request_limit_are_not_searched_and_keep_the_weak_note(edition):
    # 판의 요청 합계 상한은 급변동(CoreSettings)이 아니라 판 설정(DailySettings)을 따른다.
    flow = DailyFlow(edition, [], [], settings=CoreSettings(max_research_requests=99),
                     daily_settings=DailySettings(max_research_requests=1))
    flow.report_llm.outputs = [two_weak(flow), daily_output(point("둘째 문장을 다시 썼습니다.", "r1"))]
    flow.query_llm.outputs = [queries("검색어 하나")]
    flow.to_first_report()

    assert [r.value["payload"]["jobId"] for r in flow.research_requests()] == [flow.rid(2)]
    assert len(flow.query_llm.calls) == 1
    (limit,) = flow.records.of_reason(ResearchReason.REQUEST_LIMIT)
    assert limit.sentence_no == 3
    flow.complete(flow.rid(2))
    flow.stable([in_range(flow)], flow.rid(2))

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert weak_shown(text, SECOND_SHORTFALL) and "둘째 문장을 다시 썼습니다." in text
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 1)


def test_daily_total_time_limit_publishes_the_body_at_that_moment(edition):
    # 완료 알림 대기 상한(5분)보다 짧게 줄여, 전체 시간 상한이 먼저 닿는 경우를 만든다.
    limit = timedelta(minutes=2)
    flow = DailyFlow(edition, [], [], settings=CoreSettings(research_total_limit=limit))
    flow.report_llm.outputs = [two_weak(flow), daily_output(point("둘째 문장을 다시 썼습니다.", "r1"))]
    flow.query_llm.outputs = [queries("검색어 하나"), queries("검색어 둘")]
    flow.to_first_report()
    started = flow.now
    flow.complete(flow.rid(2))
    flow.stable([in_range(flow)], flow.rid(2))
    assert flow.published() == []

    flow.now = started + limit - SEC
    flow.sentences.on_tick(flow.now)
    assert flow.published() == []
    flow.now = started + limit
    flow.sentences.on_tick(flow.now)

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert "둘째 문장을 다시 썼습니다." in text and weak_shown(text, SECOND_SHORTFALL)
    (limit,) = flow.records.of_reason(ResearchReason.TIME_LIMIT)
    assert limit.sentence_no == 3
    flow.complete(flow.rid(3))
    assert flow.research.api.reply(flow.news(1, [in_range(flow, "r3")], flow.rid(3)), flow.now) is False
    assert len(flow.published()) == 1 and flow.sentences.next_deadline() is None


def test_daily_article_already_used_by_another_sentence_is_not_used_again(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [two_weak(flow), daily_output(point("둘째 문장을 다시 썼습니다.", "r1")),
                               daily_output(point("셋째 문장을 다시 썼습니다.", "r3"))]
    flow.query_llm.outputs = [queries("검색어 하나"), queries("검색어 둘")]
    flow.to_first_report()
    flow.complete(flow.rid(2))
    flow.complete(flow.rid(3))
    flow.stable_jobs({flow.rid(2): [in_range(flow)], flow.rid(3): [in_range(flow), in_range(flow, "r3")]})

    third = json.loads(flow.report_llm.calls[2][1])["rewrite"]
    assert third["sentenceNo"] == 3 and third["newRefs"] == ["r3"]
    (event,) = flow.published()
    assert "https://news.example.com/r1" in event.value["payload"]["text"]


def test_daily_sentence_rewrite_citing_a_ref_outside_the_list_is_rejected_and_the_old_sentence_stays(edition):
    flow = DailyFlow(edition, [], [])
    unknown = daily_output(point("지어낸 근거가 영향을 줬을 수 있습니다.", "r9"))
    flow.report_llm.outputs = [daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
                               unknown, unknown, unknown]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(1))
    flow.stable([in_range(flow)], flow.rid(1))

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert "지어낸 근거" not in text and "업종 소식이 영향을 줬을 수 있습니다." in text
    assert weak_shown(text, SHORTFALL)
    rejected = [e for e in flow.records.of_type(EntryType.LLM_REJECTED) if e.sentence_no == 1]
    assert [e.attempts for e in rejected] == [1, 2, 3]
    assert "were not given" in rejected[0].data["llm"]["rejectReason"]
    assert flow.records.of_reason(ResearchReason.REWRITE_FAILED)
    (result,) = flow.records.results()
    assert (result.status, result.weak_count) == (RunStatus.SUCCEEDED, 1)


def test_daily_with_insufficient_evidence_overall_researches_the_whole_report_and_keeps_the_notice(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(insufficient=True), daily_output(insufficient=True)]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(0))
    flow.stable([in_range(flow)], flow.rid(0))

    (event,) = flow.published()
    assert DAILY_INSUFFICIENT in event.value["payload"]["text"]
    assert len(flow.research_requests()) == 1
    assert flow.records.of_reason(ResearchReason.STILL_INSUFFICIENT)


def test_daily_research_that_never_answers_publishes_the_first_result_with_its_weak_mark(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()

    flow.now += S.research_wait_limit
    flow.sentences.on_tick(flow.now)

    (event,) = flow.published()
    assert weak_shown(event.value["payload"]["text"], SHORTFALL)
    assert [r.reason for r in flow.records.failures() if r.stage == ResearchStep.COMPLETION.value] == [
        ResearchReason.COMPLETION_TIMEOUT.value]
    assert len(flow.report_llm.calls) == 1


def test_daily_research_steps_are_logged_under_the_same_job_id_with_sentence_numbers_and_rounds(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
                               daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1"))]
    flow.query_llm.outputs = ["검색어가 아닌 글", queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(1))
    flow.stable([in_range(flow)], flow.rid(1))

    entries = flow.records.entries
    assert {e.job_id for e in entries} == {TICK_ID}
    assert [r.job_kind.value for r in flow.records.starts] == [edition]
    steps = [(e.stage, e.entry_type) for e in entries]
    # 검색어 LLM 거부와 성공, 재검색 요청, 받은 기사, 다시 쓰기가 모두 남는다. 안정 확인 라운드는 없다.
    assert ("query", EntryType.LLM_REJECTED) in steps and ("query", EntryType.QUERY_WRITTEN) in steps
    assert ("research-request", EntryType.RESEARCH_REQUESTED) in steps
    assert not [e for e in entries if e.entry_type == EntryType.CHECK_ROUND]
    assert ("filter", EntryType.ARTICLES_RECEIVED) in steps and ("select", EntryType.ARTICLES_RECEIVED) in steps
    assert ("rewrite", EntryType.SENTENCE_REWRITTEN) in steps
    assert [e.sentence_no for e in entries if e.stage == "query"] == [1, 1]
    assert {e.round for e in entries if e.stage in ("query", "research-request", "filter", "rewrite")} == {1}
    rejected = next(e for e in entries if e.stage == "query" and e.entry_type == EntryType.LLM_REJECTED)
    assert rejected.data["llm"]["rejectReason"]
    received = next(e for e in entries if e.stage == "filter")
    assert received.data["usable"][0]["articleId"] == "r1" and received.data["collectorCompletion"]["jobType"] == "research-news"


class RangedStatus:
    """StatusPort 가짜. DB 구현처럼 끝난 시각이 범위(양끝 포함) 안인 수집 완료만 돌려준다."""

    def __init__(self):
        self.collections = []

    def save_collection(self, record):
        self.collections.append(record)
        return True

    def collections_between(self, since, until):
        return [c for c in self.collections if since <= c.finished_at <= until]

    def job_runs_between(self, since, until):
        return []


def test_post_close_server_status_counts_the_research_this_edition_sent_after_its_as_of():
    status = RangedStatus()
    flow = DailyFlow("post-close", [], [], status=status)
    flow.report_llm.outputs = [
        daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref),
                     point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1")),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(2))
    # 운영에서는 main이 받은 재검색 완료를 저장소에 남긴다. 완료 시각은 판 기준 시각보다 늦다.
    status.save_collection(parse_collection(envelope("collection.completed", {
        "jobType": "research-news", "scheduledFor": flow.now.isoformat(), "startedAt": flow.now.isoformat(),
        "finishedAt": flow.now.isoformat(), "publishedCount": 1, "incompleteSymbols": [],
        "researchJobId": flow.rid(2)})))
    flow.stable([in_range(flow)], flow.rid(2))

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    (news_line,) = [line for line in text.split("\n") if line.startswith("- 뉴스:")]
    assert news_line.endswith("· 재검색 1회")


def test_daily_without_etf_data_skips_the_research_and_publishes_the_first_result():
    flow = DailyFlow("post-close", [], [], etfs=[])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL))]
    flow.to_first_report()

    # 기사 조회 요청에는 종목코드가 필수라 보유 ETF 자료가 없으면 재검색하지 않는다.
    assert flow.query_llm.calls == [] and flow.research_requests() == []
    (event,) = flow.published()
    # 눈에 띄는 점 후보가 없는 날은 그 절이 없고, 후보 없이 쓴 점은 가짜 LLM이 버린다.
    text = event.value["payload"]["text"]
    assert "눈에 띄는 점" not in text and (WEAK_NOTE_PREFIX + SHORTFALL) not in text


def test_daily_research_symbol_is_the_first_etf_code_in_sorted_order():
    flow = DailyFlow("post-close", [], [], etfs=[etf("TEST09"), etf("TEST02")])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL)),
                               daily_output(point("그날 증시 자료가 있습니다.", "B1"))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(1))

    (request,) = [q for q in flow.requests.api.of(QueryKind.ARTICLES) if "researchJobId" in q.payload]
    assert request.payload["symbol"] == "TEST02"


def test_daily_query_llm_gets_the_etf_info_when_the_response_has_it():
    flow = DailyFlow("post-close", [], [], etfs=[{**etf(), "etf": {
        "name": "TEST ETF ONE", "members": [{"market": "US", "symbol": "TESTA", "rank": 1, "name": "테스트회사"}],
        "membersTruncated": False}}])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()

    ((_, user, _),) = flow.query_llm.calls
    info = json.loads(user)["etfInfo"]
    assert info[0]["name"] == "TEST ETF ONE" and info[0]["members"][0]["name"] == "테스트회사"
