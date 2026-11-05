"""항목별 근거 약함과 재검색 시나리오: 가짜 LLM·가짜 응답으로 급변동·아침 판·마감 후 판을 각각 돌린다.

경우: 근거 충분, 항목 하나 약함 -> 재검색 -> 다시 쓰기, 다시 써도 약함 표시, 전체 근거 부족, 재검색 실패.
실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다. 나가는 요청과 이벤트는 계약 스키마로 검증한다.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings, DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_report import DAILY_INSUFFICIENT, DailyLlmWriter
from briefing.daily_report import parse_output as parse_daily
from briefing.llm_output import InvalidLlmOutput, WEAK_NOTE_PREFIX, output_weak_shortfall
from briefing.report import LLM_INSUFFICIENT
from briefing.report import parse_output as parse_move
from briefing.research import NewsResearch, QueryWriter, ResearchReason, ResearchStep
from contracts import validate
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
from test_research import (
    A1,
    JOB_ID,
    QUERIES,
    R1,
    SYMBOL,
    Flow,
    queries,
    research_articles_requests,
)

KST = timezone(timedelta(hours=9))
S = CoreSettings()
SEC = timedelta(seconds=1)
SHORTFALL = "변동 직전에 나온 해당 업종 기사가 더 필요합니다."
STILL_SHORTFALL = "공시의 구체 항목을 설명한 자료가 더 필요합니다."


# ---- 공통 검사 ----

class _Invalid(InvalidLlmOutput):
    pass


def test_weak_item_needs_a_shortfall_and_a_boolean_flag():
    assert output_weak_shortfall({"weak": True, "shortfall": " 부족 "}, "item", _Invalid) == "부족"
    assert output_weak_shortfall({"weak": False, "shortfall": "무시"}, "item", _Invalid) is None
    # 칸이 없는 출력은 약하지 않은 것으로 읽는다(출력 형식이 칸을 요구한다).
    assert output_weak_shortfall({}, "item", _Invalid) is None
    with pytest.raises(_Invalid, match="no shortfall"):
        output_weak_shortfall({"weak": True, "shortfall": " "}, "item", _Invalid)
    with pytest.raises(_Invalid, match="not a boolean"):
        output_weak_shortfall({"weak": "yes", "shortfall": "부족"}, "item", _Invalid)


def test_move_and_daily_parsers_read_the_weakness_of_each_item():
    move = json.dumps({"causes": [
        {"title": "t", "detail": "d", "refs": ["a1"], "meaning": "m", "meaningRefs": ["a1"],
         "weak": True, "shortfall": SHORTFALL},
        {"title": "t", "detail": "d", "refs": ["a1"], "meaning": "m", "meaningRefs": ["a1"],
         "weak": False, "shortfall": ""}], "insufficientEvidence": False})
    assert [c.shortfall for c in parse_move(move, frozenset({"a1"})).causes] == [SHORTFALL, None]

    daily = json.dumps({"points": [
        {"statement": "s", "refs": ["B1"], "weak": True, "shortfall": SHORTFALL},
        {"statement": "s", "refs": ["B1"], "weak": False, "shortfall": ""}], "insufficientEvidence": False})
    assert [p.shortfall for p in parse_daily(daily, frozenset({"B1"})).points] == [SHORTFALL, None]


def test_daily_shortfall_naming_a_us_ticker_is_rejected():
    output = json.dumps({"points": [
        {"statement": "s", "refs": ["B1"], "weak": True, "shortfall": "TESTA 실적 기사가 더 필요합니다."}],
        "insufficientEvidence": False})
    with pytest.raises(InvalidLlmOutput, match="shortfall mentions a US ticker"):
        parse_daily(output, frozenset({"B1"}), frozenset({"TESTA"}))


# ---- 급변동 ----

def move_output(*items, insufficient=False):
    """items: (문장, 근거 번호, 부족한 점 또는 None)"""
    return json.dumps({"causes": [
        {"title": s, "detail": s, "refs": refs, "meaning": s, "meaningRefs": refs,
         "weak": shortfall is not None, "shortfall": shortfall or ""} for s, refs, shortfall in items],
        "insufficientEvidence": insufficient}, ensure_ascii=False)


MOVE_STRONG = move_output(("기사가 영향을 줬을 수 있습니다.", ["a1"], None))
MOVE_WEAK = move_output(("기사가 영향을 줬을 수 있습니다.", ["a1"], None), ("업종 소식이 영향을 줬을 수 있습니다.", ["a1"], SHORTFALL))
MOVE_REWRITTEN = move_output(("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"], None),
                             ("기존 기사도 있습니다.", ["a1"], None))
MOVE_STILL_WEAK = move_output(("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"], STILL_SHORTFALL),
                              ("기존 기사도 있습니다.", ["a1"], None))
MOVE_INSUFFICIENT = move_output(insufficient=True)


def test_move_with_every_item_strong_is_published_without_research():
    flow = Flow([MOVE_STRONG], [])
    flow.to_report()

    assert flow.query_llm.calls == [] and flow.requests.of("news.research.requested") == []
    (briefing,) = flow.briefings()
    assert "근거 약함" not in briefing.value["payload"]["text"]


def test_move_with_one_weak_item_researches_with_its_shortfall_and_rewrites_once():
    flow = Flow([MOVE_WEAK, MOVE_REWRITTEN], [queries(*QUERIES)])
    flow.to_report()

    # 부족한 점이 검색어 LLM 입력에 들어간다. 첫 보고서는 아직 나가지 않는다.
    ((_, user, _),) = flow.query_llm.calls
    assert json.loads(user)["shortfalls"] == [SHORTFALL]
    assert flow.briefings() == []
    (request,) = flow.requests.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []

    flow.complete_research()
    flow.stable_research([R1, A1])

    assert len(flow.report_llm.calls) == 2
    assert [a["ref"] for a in json.loads(flow.report_llm.calls[1][1])["researchArticles"]] == ["r1"]
    (briefing,) = flow.briefings()
    assert validate("briefing.generated.v1.schema.json", briefing.value) == []
    assert "근거 약함" not in briefing.value["payload"]["text"]
    assert len(research_articles_requests(flow)) >= 1
    assert len(flow.requests.of("news.research.requested")) == 1
    (result,) = flow.records.results()
    assert result.researched is True and result.data["weakItems"] == []


def test_move_item_still_weak_after_the_rewrite_carries_the_shortfall_and_is_not_researched_again():
    flow = Flow([MOVE_WEAK, MOVE_STILL_WEAK], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1, A1])

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert (WEAK_NOTE_PREFIX + STILL_SHORTFALL) in text
    assert (WEAK_NOTE_PREFIX + SHORTFALL) not in text
    # 작업 하나당 재검색은 1번이다.
    assert len(flow.requests.of("news.research.requested")) == 1 and len(flow.query_llm.calls) == 1
    (record,) = flow.records.of(ResearchReason.STILL_WEAK)
    assert record.stage == ResearchStep.REWRITE
    (result,) = flow.records.results()
    assert result.data["weakItems"] == [STILL_SHORTFALL]


def test_move_with_insufficient_evidence_overall_researches_too():
    flow = Flow([MOVE_INSUFFICIENT, MOVE_REWRITTEN], [queries(*QUERIES)])
    flow.to_report()

    assert json.loads(flow.query_llm.calls[0][1]).get("shortfalls") is None
    flow.complete_research()
    flow.stable_research([R1, A1])
    (briefing,) = flow.briefings()
    assert LLM_INSUFFICIENT not in briefing.value["payload"]["text"]


def test_move_research_that_never_answers_publishes_the_first_report_with_its_weak_mark():
    flow = Flow([MOVE_WEAK], [queries(*QUERIES)])
    flow.to_report()

    flow.clock.now += S.research_wait_limit
    flow.stage.on_tick(flow.clock.now)

    (briefing,) = flow.briefings()
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in briefing.value["payload"]["text"]
    assert [r.reason for r in flow.records.items if r.entry_type == "failed"] == [
        ResearchReason.COMPLETION_TIMEOUT.value]
    assert len(flow.report_llm.calls) == 1


def test_move_rewrite_that_loses_every_cause_publishes_the_first_report_instead():
    flow = Flow([MOVE_WEAK, MOVE_INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1, A1])

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert LLM_INSUFFICIENT not in text and (WEAK_NOTE_PREFIX + SHORTFALL) in text
    assert flow.records.of(ResearchReason.STILL_INSUFFICIENT)


# ---- 데일리 판 ----

EDITIONS = {
    # 판: (작업 종류, 예정 시각, 첫 요약이 인용할 근거 번호, 범위 안 기사 발행 시각)
    "morning": (MORNING_JOB, MORNING_AT, "B1"),
    "post-close": (POST_CLOSE_JOB, POST_CLOSE_AT, "E1"),
}


def point(statement, ref, shortfall=None):
    return {"statement": statement, "refs": [ref], "weak": shortfall is not None, "shortfall": shortfall or ""}


def daily_output(*points, insufficient=False):
    return json.dumps({"points": list(points), "insufficientEvidence": insufficient}, ensure_ascii=False)


class DailyFlow:
    """판 하나를 자료 수집 -> 요약 -> 재검색 -> 다시 쓰기까지 돌리는 도우미."""

    def __init__(self, edition, report_outputs, query_outputs, etfs=None):
        self.edition = edition
        self.job_type, self.at, self.ref = EDITIONS[edition]
        self.requests, self.records = Requests(), FakeJobLog()
        self.report_llm, self.query_llm = FakeLlm(*report_outputs), FakeLlm(*query_outputs)
        settings = DailySettings()
        self.core = DailyCore(
            self.requests, DailyLlmWriter(self.report_llm, settings.llm_max_attempts), self.records, settings,
            queries=QueryWriter(self.query_llm, S.max_research_queries, S.research_query_attempts))
        self.research = NewsResearch(self.requests, self.records, self.core, S)
        self.core.research = self.research
        self.now = self.at
        self.etfs = etfs

    def to_first_report(self):
        self.core.on_schedule_tick(tick(self.job_type, self.at), self.now, self.edition)
        etfs, background, close_rates = self.etfs, None, None
        if self.edition == "post-close":
            # 마감 후 판의 범위는 그날 오후라 그 안에 받은 값으로 바꾼다.
            fetched = "2026-10-20T16:30:02+09:00"
            if etfs is None:
                etfs = [etf(fetched=fetched)]
            else:
                etfs = [{**e, "fetchedAt": fetched} for e in etfs]
            background = [{**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00"}]
            close_rates = []
        self.core.on_daily_data_responded(
            daily_response(etfs=etfs, background=background, close_rates=close_rates), self.now)
        if self.edition == "morning" and self.etfs != []:
            self.core.on_filings_responded(filings_response(), self.now)

    def news(self, seq, items):
        return envelope("news.articles.responded", {
            "jobId": TICK_ID, "seq": seq, "symbol": "TEST01", "truncated": False, "articles": items})

    def complete(self):
        self.now += SEC
        event = json.loads(envelope("collection.completed", {
            "jobType": "research-news", "startedAt": self.now.isoformat(), "finishedAt": self.now.isoformat(),
            "incompleteSymbols": [], "researchJobId": TICK_ID}))
        return self.research.on_collection_completed(json.dumps(event), self.now)

    def stable(self, items):
        for n in range(2):
            self.now += SEC
            assert self.research.on_articles_responded(self.news(n + 1, items), self.now) is True
            if n == 0:
                self.now += S.recheck_interval
                self.research.on_tick(self.now)

    def published(self):
        events = self.requests.topic("briefing.daily.generated")
        for event in events:
            assert validate("briefing.daily.generated.v1.schema.json", event.value) == []
        return events

    def research_articles_requests(self):
        return [e for e in self.requests.topic("news.articles.requested") if "researchJobId" in e.value["payload"]]


def news_article(article_id, published_at):
    return {"articleId": article_id, "title": f"기사 {article_id}", "summary": "", "publisher": "news.example.com",
            "publishedAt": published_at.isoformat(), "url": f"https://news.example.com/{article_id}"}


@pytest.fixture(params=sorted(EDITIONS))
def edition(request):
    return request.param


def in_range(flow):
    return news_article("r1", flow.at - timedelta(hours=1))


def out_of_range(flow):
    return news_article("r2", flow.at - timedelta(days=5))


def test_daily_with_every_sentence_strong_is_published_without_research(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref))]
    flow.to_first_report()

    assert flow.query_llm.calls == [] and flow.requests.topic("news.research.requested") == []
    (event,) = flow.published()
    assert "근거 약함" not in event.value["payload"]["text"]


def test_daily_with_one_weak_sentence_researches_with_its_shortfall_and_rewrites_once(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [
        daily_output(point("지수가 오른 가운데 자료가 있습니다.", flow.ref),
                     point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1"),
                     point("지수가 오른 가운데 자료가 있습니다.", flow.ref)),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()

    ((_, user, _),) = flow.query_llm.calls
    assert json.loads(user)["shortfalls"] == [SHORTFALL]
    assert flow.published() == []
    (request,) = flow.requests.topic("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    # 데일리 재검색은 시장 전체를 뜻하는 태그 *로 부르고, 작업 종류는 판 이름이다.
    assert request.value["payload"] == {"jobId": TICK_ID, "seq": 1, "jobKind": edition, "symbol": "*",
                                        "queries": QUERIES}

    assert flow.complete() is True
    (first,) = flow.research_articles_requests()
    assert validate("news.articles.requested.v1.schema.json", first.value) == []
    payload = first.value["payload"]
    # 기사 시간 범위는 판 범위(예정 시각 포함)이고, 조회 symbol은 정렬한 첫 ETF 코드다.
    assert payload["since"] == (flow.core._jobs[TICK_ID].range_start).isoformat()
    assert payload["until"] == flow.at.isoformat()
    assert (payload["symbol"], payload["researchJobId"]) == ("TEST01", TICK_ID)

    # 범위 밖 기사는 버리고 범위 안 기사만 다시 쓰는 데 쓴다.
    flow.stable([in_range(flow), out_of_range(flow)])

    rewritten = json.loads(flow.report_llm.calls[1][1])
    research_item = next(i for i in rewritten["items"] if i["name"] == "재검색 기사")
    assert [e["ref"] for e in research_item["entries"]] == ["r1"]
    assert all(i["name"] != "재검색 기사" for i in json.loads(flow.report_llm.calls[0][1])["items"])
    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert "근거 약함" not in text
    # 재검색 기사도 기존 자료처럼 근거 번호와 링크로 본문에 나온다.
    assert "https://news.example.com/r1" in text and "r2" not in text
    assert len(flow.requests.topic("news.research.requested")) == 1


def test_daily_sentence_still_weak_after_the_rewrite_carries_the_shortfall(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [
        daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
        daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1", STILL_SHORTFALL)),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete()
    flow.stable([in_range(flow)])

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert (WEAK_NOTE_PREFIX + STILL_SHORTFALL) in text and (WEAK_NOTE_PREFIX + SHORTFALL) not in text
    assert len(flow.requests.topic("news.research.requested")) == 1 and len(flow.query_llm.calls) == 1
    (record,) = [r for r in flow.records.entries if r.reason == ResearchReason.STILL_WEAK.value]
    assert (record.stage, record.job_kind, record.researched) == (ResearchStep.REWRITE.value, edition, True)
    (result,) = flow.records.results()
    assert result.data["weakItems"] == [STILL_SHORTFALL]


def test_daily_with_insufficient_evidence_overall_researches_and_keeps_the_notice_when_still_insufficient(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(insufficient=True), daily_output(insufficient=True)]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete()
    flow.stable([in_range(flow)])

    (event,) = flow.published()
    assert DAILY_INSUFFICIENT in event.value["payload"]["text"]
    assert len(flow.requests.topic("news.research.requested")) == 1


def test_daily_research_that_never_answers_publishes_the_first_result_with_its_weak_mark(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()

    flow.now += S.research_wait_limit
    flow.research.on_tick(flow.now)

    (event,) = flow.published()
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in event.value["payload"]["text"]
    assert [r.reason for r in flow.records.failures() if r.stage == ResearchStep.COMPLETION.value] == [
        ResearchReason.COMPLETION_TIMEOUT.value]
    assert len(flow.report_llm.calls) == 1


def test_daily_rewrite_that_loses_every_sentence_publishes_the_first_result_instead(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
                               daily_output(insufficient=True)]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete()
    flow.stable([in_range(flow)])

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert DAILY_INSUFFICIENT not in text and (WEAK_NOTE_PREFIX + SHORTFALL) in text


def test_daily_research_steps_are_logged_under_the_same_job_id_in_the_shared_format(edition):
    flow = DailyFlow(edition, [], [])
    flow.report_llm.outputs = [daily_output(point("업종 소식이 영향을 줬을 수 있습니다.", flow.ref, SHORTFALL)),
                               daily_output(point("다시 찾은 기사가 영향을 줬을 수 있습니다.", "r1"))]
    flow.query_llm.outputs = ["검색어가 아닌 글", queries(*QUERIES)]
    flow.to_first_report()
    flow.complete()
    flow.stable([in_range(flow)])

    entries = flow.records.entries
    assert {e.job_id for e in entries} == {TICK_ID}
    assert {e.job_kind for e in entries} == {edition}
    steps = [(e.stage, e.entry_type, e.researched) for e in entries]
    # 검색어 LLM 거부와 성공, 안정 확인 라운드, 다시 쓴 LLM 시도, 최종 결과가 모두 남는다.
    assert ("query", "llm-rejected", True) in steps and ("query", "llm-succeeded", True) in steps
    assert ("articles-confirm", "check-round", True) in steps
    assert [e.entry_type for e in entries if e.stage == "llm" and e.researched] == ["llm-succeeded"]
    assert steps[-1] == ("publish", "result", True)
    rejected = next(e for e in entries if e.stage == "query" and e.entry_type == "llm-rejected")
    assert rejected.data["llm"]["rejectReason"]


def test_daily_without_etf_data_skips_the_research_and_publishes_the_first_result():
    flow = DailyFlow("post-close", [], [], etfs=[])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL))]
    flow.to_first_report()

    # 기사 조회 요청에는 종목코드가 필수라 보유 ETF 자료가 없으면 재검색하지 않는다.
    assert flow.query_llm.calls == [] and flow.requests.topic("news.research.requested") == []
    (event,) = flow.published()
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in event.value["payload"]["text"]


def test_daily_research_symbol_is_the_first_etf_code_in_sorted_order():
    flow = DailyFlow("post-close", [], [], etfs=[etf("TEST09"), etf("TEST02")])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL)),
                               daily_output(point("그날 증시 자료가 있습니다.", "B1"))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete()

    (request,) = flow.research_articles_requests()
    assert request.value["payload"]["symbol"] == "TEST02"
