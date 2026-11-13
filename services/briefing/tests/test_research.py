"""근거 부족 시 재검색 시나리오: 가짜 보고서 LLM·가짜 검색어 LLM·가짜 응답으로 변동 1건을 재검색과 다시 쓰기까지 돌린다.

실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다. 나가는 요청과 이벤트는 계약 스키마로 검증한다.
"""
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from fake_anthropic import as_reply
from briefing.config import CoreSettings
from briefing.jobs import BriefingCore
from briefing.llm import LlmCallError
from briefing.ports import FailureReason
from briefing.job_log import REASON_NO_EVIDENCE, EntryType, GeneratedBy, RunStatus
from briefing.report import LLM_INSUFFICIENT, LlmWriter, ReportStage
from briefing.research import (
    QUERY_SYSTEM_PROMPT,
    InvalidQueries,
    NewsResearch,
    QueryWriter,
    ResearchReason,
    ResearchStep,
    ResearchTarget,
    parse_queries,
    query_output_schema,
)
from briefing.sentence_research import WHOLE_REPORT, ResearchReportStage, SentenceResearch, research_job_id
from contracts import sample, validate
from fake_anthropic import FAKE_KEY, fake_sdk  # noqa: F401 - 픽스처
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)
S = CoreSettings()
SEC = timedelta(seconds=1)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
SYMBOL = "TEST01"
# 가짜 검색어. 로그에 이 문자열이 나오면 안 된다.
QUERIES = ["가짜검색어 하나", "가짜검색어 둘"]
# 보고서 전체가 근거 부족일 때의 재검색 작업 id. 문장 단위 재검색은 문장마다 따로 id를 가진다.
RID = research_job_id(JOB_ID, WHOLE_REPORT, 1)


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class FakeLlm:
    """정해 둔 응답을 차례로 돌려준다. 실제 API를 부르지 않는다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        system = system + addendum
        self.calls.append((system, user, output_schema))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class Requests:
    def __init__(self):
        self.sent = []

    def send(self, event):
        self.sent.append(event)

    def of(self, topic):
        return [e for e in self.sent if e.topic == topic]


class Records(FakeJobLog):
    """작업 기록 가짜. items는 그 가운데 재검색 단계의 줄만 돌려준다."""

    @property
    def items(self):
        steps = {step.value for step in ResearchStep}
        return [e for e in self.entries if e.stage in steps]

    @property
    def failed(self):
        """재검색이 아니라 작업 자체가 끝내 실패한 줄(이전의 실패 테이블 한 줄에 해당)"""
        steps = {step.value for step in ResearchStep}
        return [e for e in self.entries if e.entry_type == EntryType.STEP_FAILED and e.stage not in steps]

    def of(self, reason):
        return [r for r in self.items if r.reason == reason.value]


def report(causes=(), insufficient=False):
    # 원인 묶음 출력 형식(제목·구체 문장·뜻 문장마다 근거 번호)에 맞춘다.
    return json.dumps({"causes": [{"title": s, "detail": s, "refs": refs, "meaning": s, "meaningRefs": refs}
                                  for s, refs in causes],
                       "insufficientEvidence": insufficient}, ensure_ascii=False)


INSUFFICIENT = report(insufficient=True)
WITH_RESEARCH = report([("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"]), ("기존 기사도 있습니다.", ["a1"])])
SUFFICIENT = report([("기사가 영향을 줬을 수 있습니다.", ["a1"])])


def queries(*items):
    return json.dumps({"queries": list(items)}, ensure_ascii=False)


class Flow:
    """변동 1건을 근거 조회 -> 보고서 -> 재검색까지 돌리는 도우미."""

    def __init__(self, report_outputs, query_outputs, settings=S):
        self.clock = Clock(T)
        self.requests, self.records = Requests(), Records()
        self.failures = self.records
        self.report_llm, self.query_llm = FakeLlm(*report_outputs), FakeLlm(*query_outputs)
        stage = ReportStage(LlmWriter(self.report_llm), self.requests, self.records, clock=self.clock)
        self.sentences = SentenceResearch(
            QueryWriter(self.query_llm, settings.max_research_queries, settings.research_query_attempts),
            self.requests, self.records, settings)
        self.research = self.sentences.research
        self.stage = ResearchReportStage(stage, self.sentences)
        self.core = BriefingCore(self.requests, self.stage, settings, self.records)
        self.settings = settings

    def to_report(self, article_items=None, background=(), etf=None):
        """보고서를 처음 쓰기까지(기사 안정 판정 포함) 돌린다."""
        core = self.core
        core.on_move(move(), T)
        core.on_collection_completed(regular_completed(), T)
        core.on_history_responded(history(), T + SEC)
        core.on_context_responded(context(background=background, etf=etf), T + SEC)
        core.on_articles_responded(articles(1, article_items), T + SEC)
        core.on_tick(T + SEC + S.recheck_interval)
        self.clock.now = T + SEC + S.recheck_interval
        core.on_articles_responded(articles(2, article_items), self.clock.now)

    def complete_research(self, incomplete=(), research_id=RID):
        self.clock.now += SEC
        return self.research.on_collection_completed(research_completed(incomplete, research_id), self.clock.now)

    def stable_research(self, items, rounds=2, research_id=RID):
        """재검색 기사 조회에 같은 목록을 rounds번 돌려준다."""
        research = self.research
        for n in range(rounds):
            seq = 1 + n
            self.clock.now += SEC
            assert research.on_articles_responded(articles(seq, items, job_id=research_id), self.clock.now) is True
            if n < rounds - 1:
                self.clock.now += S.recheck_interval
                research.on_tick(self.clock.now)

    def stable_research_jobs(self, items_by_job):
        """여러 재검색 작업에 같은 목록을 두 라운드 돌려준다. 라운드마다 모든 작업에 먼저 답한 뒤 확인 시각을 지나게 한다."""
        research = self.research
        for n in range(2):
            for research_id, items in items_by_job.items():
                self.clock.now += SEC
                assert research.on_articles_responded(articles(1 + n, items, job_id=research_id),
                                                      self.clock.now) is True
            if n == 0:
                self.clock.now += S.recheck_interval
                research.on_tick(self.clock.now)

    def briefings(self):
        return self.requests.of("briefing.generated")

    def not_sent(self):
        """근거 없어 결과 이벤트를 발행하지 않은 작업 결과"""
        return [f for f in self.records.finishes if f.failure_reason == REASON_NO_EVIDENCE]


# ---- 가짜 응답 ----

def move():
    event = sample("market.move.sample.json")
    event["eventId"] = JOB_ID
    event["payload"]["symbol"] = SYMBOL
    event["payload"]["asOf"] = T.isoformat()
    return json.dumps(event)


def regular_completed():
    event = sample("collection.completed.sample.json")
    event["payload"].update(jobType="collect-news", startedAt=(T + S.delta).isoformat(), incompleteSymbols=[])
    return json.dumps(event)


def research_completed(incomplete=(), research_id=RID):
    event = sample("collection.completed.sample.json")
    event["payload"].update(jobType="research-news", startedAt=(T + timedelta(minutes=1)).isoformat(),
                            incompleteSymbols=list(incomplete), researchJobId=research_id)
    assert validate("collection.completed.v1.schema.json", event) == []
    return json.dumps(event)


def history(seq=1):
    event = sample("market.history.responded.sample.json")
    event["payload"].update(jobId=JOB_ID, seq=seq, symbol=SYMBOL, truncated=False,
                            points=[{"asOf": (T - 10 * SEC).isoformat(), "price": 10_000}])
    return json.dumps(event)


def context(seq=1, background=(), etf=None):
    event = sample("market.context.responded.sample.json")
    event["payload"].update(jobId=JOB_ID, seq=seq, symbol=SYMBOL, tickers=[], closeRates=[],
                            background=list(background), truncated=False)
    if etf is not None:
        event["payload"]["etf"] = etf
    return json.dumps(event)


def article(article_id, published_at):
    return {"articleId": article_id, "title": f"기사 {article_id}", "summary": "", "publisher": "news.example.com",
            "publishedAt": published_at.isoformat(), "url": f"https://news.example.com/{article_id}"}


A1 = article("a1", T - timedelta(minutes=30))
R1 = article("r1", T - timedelta(hours=3))


def articles(seq, items=None, truncated=False, job_id=JOB_ID):
    event = sample("news.articles.responded.sample.json")
    event["payload"].update(jobId=job_id, seq=seq, symbol=SYMBOL, truncated=truncated,
                            articles=items if items is not None else [A1])
    return json.dumps(event)


def research_articles_requests(flow):
    return [e for e in flow.requests.of("news.articles.requested") if "researchJobId" in e.value["payload"]]


# ---- 근거 충분 ----

def test_sufficient_evidence_is_published_without_research():
    flow = Flow([SUFFICIENT], [])
    flow.to_report()

    assert flow.query_llm.calls == []
    assert flow.requests.of("news.research.requested") == []
    (briefing,) = flow.briefings()
    assert [e["articleId"] for e in briefing.value["payload"]["evidence"]] == ["a1"]
    assert flow.records.items == []


# ---- 근거 부족 -> 재검색 -> 다시 쓰기 ----

def test_insufficient_evidence_researches_once_and_rewrites_with_the_research_articles():
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report()

    # 검색어 LLM은 자기 지시문·출력 형식으로 따로 불리고, 입력은 보고서와 같은 공개 정보다.
    ((system, user, schema),) = flow.query_llm.calls
    assert system == QUERY_SYSTEM_PROMPT.format(max_queries=S.max_research_queries)
    assert schema == query_output_schema(S.max_research_queries)
    assert json.loads(user)["foundArticleTitles"] == ["기사 a1"]
    assert flow.briefings() == []

    (request,) = flow.requests.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    assert request.key == SYMBOL
    # 재검색 작업은 부른 작업과 따로 uuid를 가진다(요청 계약이 uuid를 요구한다).
    assert request.value["payload"] == {"jobId": RID, "seq": 1, "jobKind": "move", "symbol": SYMBOL,
                                        "queries": QUERIES}
    assert RID != JOB_ID
    assert request.value["correlationId"] == json.loads(move())["correlationId"]

    assert flow.complete_research() is True
    (first,) = research_articles_requests(flow)
    assert validate("news.articles.requested.v1.schema.json", first.value) == []
    payload = first.value["payload"]
    # 재검색 작업 id가 따로라 첫 근거 조회의 응답과 섞이지 않아 순번은 1부터이고, 범위는 기존 기사와 같다.
    assert (payload["seq"], payload["researchJobId"], payload["symbol"]) == (1, RID, SYMBOL)
    assert payload["jobId"] == RID
    assert payload["since"] == (T - S.evidence_window).isoformat()
    assert payload["until"] == (T + S.delta).isoformat()

    flow.stable_research([R1, A1])

    assert len(flow.report_llm.calls) == 2
    rewritten = json.loads(flow.report_llm.calls[1][1])
    assert [a["ref"] for a in rewritten["articles"]] == ["a1"]
    # 이미 받은 a1은 한 번만 쓰고, 재검색 기사는 따로 둔 칸에 들어간다.
    assert [a["ref"] for a in rewritten["researchArticles"]] == ["r1"]
    assert "researchArticles" not in json.loads(flow.report_llm.calls[0][1])

    (briefing,) = flow.briefings()
    assert validate("briefing.generated.v1.schema.json", briefing.value) == []
    evidence = briefing.value["payload"]["evidence"]
    assert [(e["kind"], e["articleId"], e["url"]) for e in evidence] == [
        ("article", "r1", "https://news.example.com/r1"), ("article", "a1", "https://news.example.com/a1")]
    assert flow.failures.failed == []


def test_research_articles_request_follows_seq_timeout_and_total_attempts_then_reports_existing_evidence():
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    research = flow.research

    for _ in range(S.max_attempts):
        assert research.next_deadline() == flow.clock.now + S.response_timeout
        flow.clock.now += S.response_timeout
        research.on_tick(flow.clock.now)

    seqs = [e.value["payload"]["seq"] for e in research_articles_requests(flow)]
    assert seqs == [1, 2, 3]
    # 인용한 근거가 없어 메일을 보내지 않고, 결과와 본문은 작업 기록에 남긴다.
    assert flow.briefings() == []
    (skipped,) = flow.not_sent()
    assert LLM_INSUFFICIENT in skipped.final_text
    # 새 실패 사유 없이 보고서를 낸다.
    assert flow.failures.failed == []

    resent = flow.records.of(ResearchReason.ARTICLES_RESENT)
    assert [(r.stage, r.attempts, r.data["awaited"]["seq"]) for r in resent] == [
        (ResearchStep.ARTICLES, 1, 1), (ResearchStep.ARTICLES, 2, 2)]
    assert resent[0].data["request"]["seq"] == 1 and resent[0].data["queries"] == QUERIES
    (gave_up,) = flow.records.of(ResearchReason.ARTICLES_NO_RESPONSE)
    assert (gave_up.stage, gave_up.attempts, gave_up.data["awaited"]["eventType"], gave_up.data["awaited"]["seq"]) == (
        ResearchStep.ARTICLES, 3, "news.articles.responded", 3)
    assert gave_up.data["received"] == []
    assert research.next_deadline() is None


def test_research_completion_is_matched_by_the_research_job_id_only():
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    research = flow.research

    # 정기 수집 완료는 재검색이 가져가지 않고, 다른 작업의 재검색 완료는 이 작업을 움직이지 않는다.
    assert research.on_collection_completed(regular_completed(), flow.clock.now) is False
    other = research_completed(research_id="00000000-0000-4000-8000-0000000000ff")
    assert research.on_collection_completed(other, flow.clock.now) is True
    assert research_articles_requests(flow) == []

    assert flow.complete_research() is True
    assert len(research_articles_requests(flow)) == 1


def test_late_first_round_articles_response_is_left_to_the_core_and_stale_research_seq_is_ignored():
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    research = flow.research

    assert research.on_articles_responded(articles(2, [A1]), flow.clock.now) is False
    flow.clock.now += S.response_timeout
    research.on_tick(flow.clock.now)  # seq 1 무응답 -> seq 2로 재요청
    # 보내지 않은 순번(3)의 응답은 낡은 것으로 보고 무시한다. 여전히 seq 2를 기다린다.
    assert research.on_articles_responded(articles(3, [R1], job_id=RID), flow.clock.now) is True
    assert research.next_deadline() == flow.clock.now + S.response_timeout


def test_response_to_the_first_send_is_accepted_after_the_request_was_resent():
    # 두 값이 다르면 "응답을 받았다"(다음 기한이 recheck_interval 뒤)와 "무시하고 재전송을 기다린다"(response_timeout 뒤)가 갈린다.
    settings = CoreSettings(recheck_interval=timedelta(seconds=3))
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)], settings)
    flow.to_report()
    flow.complete_research()
    research = flow.research

    flow.clock.now += settings.response_timeout
    research.on_tick(flow.clock.now)  # seq 1 무응답 -> seq 2로 재요청
    # 브리핑이 다른 일로 막혀 늦게 읽었을 뿐 seq 1의 응답도 같은 조회의 응답이므로 받고 안정 확인 라운드로 간다.
    assert research.on_articles_responded(articles(1, [R1], job_id=RID), flow.clock.now) is True
    assert research.next_deadline() == flow.clock.now + settings.recheck_interval
    # 이미 받았으니 재전송한 seq 2의 응답은 두 번 세지 않는다.
    assert research.on_articles_responded(articles(2, [R1], job_id=RID), flow.clock.now) is True
    assert flow.records.of(ResearchReason.ARTICLES_NO_RESPONSE) == []
    assert len(flow.records.of(ResearchReason.ARTICLES_RECEIVED)) == 0  # 아직 안정 판정 전


# ---- 검색어 LLM ----

def test_query_llm_failing_every_attempt_reports_existing_evidence_and_records_each_attempt():
    bad = ["검색어가 아닌 글", json.dumps({"terms": QUERIES}), LlmCallError("LLM call failed: APIError")]
    flow = Flow([INSUFFICIENT], bad)
    flow.to_report()

    assert len(flow.query_llm.calls) == S.research_query_attempts
    assert flow.requests.of("news.research.requested") == []
    # 인용한 근거가 없어 메일을 보내지 않고, 결과와 본문은 작업 기록에 남긴다.
    assert flow.briefings() == []
    (skipped,) = flow.not_sent()
    assert LLM_INSUFFICIENT in skipped.final_text
    assert flow.failures.failed == []

    attempts = [r for r in flow.records.items if r.reason in (ResearchReason.QUERY_REJECTED,
                                                              ResearchReason.QUERY_CALL_FAILED)]
    assert [(r.stage, r.attempts, r.reason) for r in attempts] == [
        (ResearchStep.QUERY, 1, ResearchReason.QUERY_REJECTED),
        (ResearchStep.QUERY, 2, ResearchReason.QUERY_REJECTED),
        (ResearchStep.QUERY, 3, ResearchReason.QUERY_CALL_FAILED)]
    assert [r.data["llm"]["rawOutput"] for r in attempts] == ["검색어가 아닌 글", json.dumps({"terms": QUERIES}), None]
    assert [r.data["llm"]["rejectReason"] for r in attempts] == ["output is not valid JSON", "output does not have queries",
                                               "LLM call failed: APIError"]
    assert [r.error_type for r in attempts] == ["InvalidQueries", "InvalidQueries", "LlmCallError"]
    # 응답을 받은 시도는 사용량이 남고, 호출 실패는 사용량 없음으로 남는다.
    assert [r.data["llm"]["usage"] for r in attempts] == [{"cacheWrite": 11, "cacheRead": 22, "input": 33, "output": 44}] * 2 + [None]
    assert all(r.data["llm"]["input"] == flow.query_llm.calls[0][1] for r in attempts)
    (gave_up,) = flow.records.of(ResearchReason.QUERY_GAVE_UP)
    assert (gave_up.stage, gave_up.attempts, gave_up.data["llm"]["rejectReason"]) == (
        ResearchStep.QUERY, 3, "LLM call failed: APIError")
    # 기록은 재검색을 부른 작업 id로 묶이고, 보고서 전체의 재검색은 문장 번호가 없다.
    assert (gave_up.job_id, gave_up.sentence_no, gave_up.round) == (JOB_ID, None, 1)
    assert gave_up.occurred_at == flow.clock.now


def test_query_llm_with_no_queries_reports_existing_evidence_without_research():
    flow = Flow([INSUFFICIENT], [queries("  ", "")])
    flow.to_report()

    assert len(flow.query_llm.calls) == 1
    assert flow.requests.of("news.research.requested") == []
    assert flow.briefings() == [] and len(flow.not_sent()) == 1
    (record,) = flow.records.of(ResearchReason.NO_QUERIES)
    assert (record.stage, record.reason, record.data["llm"]["rawOutput"], record.data["llm"]["rejectReason"]) == (
        ResearchStep.QUERY, ResearchReason.NO_QUERIES, queries("  ", ""), None)


def test_rejected_query_attempt_and_resent_request_are_recorded_even_when_research_succeeds():
    over_limit = queries("하나", "둘", "셋", "넷")
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [over_limit, queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    research = flow.research

    flow.clock.now += S.response_timeout
    research.on_tick(flow.clock.now)  # seq 1 무응답 -> seq 2
    flow.clock.now += SEC
    research.on_articles_responded(articles(2, [R1], job_id=RID), flow.clock.now)
    flow.clock.now += S.recheck_interval
    research.on_tick(flow.clock.now)  # 확인 라운드 seq 3
    flow.clock.now += SEC
    research.on_articles_responded(articles(3, [R1], job_id=RID), flow.clock.now)

    assert len(flow.briefings()) == 1
    (rejected,) = flow.records.of(ResearchReason.QUERY_REJECTED)
    assert (rejected.attempts, rejected.data["llm"]["rawOutput"], rejected.data["llm"]["rejectReason"]) == (
        1, over_limit, "4 queries over the limit of 3")
    assert rejected.data["llm"]["input"] == flow.query_llm.calls[0][1]
    (resent,) = flow.records.of(ResearchReason.ARTICLES_RESENT)
    assert (resent.stage, resent.attempts, resent.data["awaited"]["seq"], resent.data["request"]["seq"]) == (
        ResearchStep.ARTICLES, 1, 1, 1)
    # 확인 라운드는 무응답 재요청과 단계·사유가 다르고, 그 라운드에 받은 기사 목록을 담는다.
    (confirm,) = flow.records.of(ResearchReason.ARTICLES_CONFIRM_ROUND)
    assert (confirm.stage, confirm.attempts, confirm.data["request"]["seq"]) == (ResearchStep.ARTICLES_CONFIRM, 2, 3)
    assert confirm.data["received"][0]["articles"] == [R1]
    assert (confirm.stage, confirm.reason) != (resent.stage, resent.reason)


def test_parse_queries_trims_dedupes_and_rejects_bad_shapes():
    assert parse_queries(queries(" 가 ", "가", "나"), 3) == ("가", "나")
    assert parse_queries(queries(), 3) == ()
    for bad in ("[]", json.dumps({"queries": "가"}), json.dumps({"queries": [1]}), queries("x" * 101),
                queries("가", "나", "다", "라")):
        with pytest.raises(InvalidQueries):
            parse_queries(bad, 3)


def test_query_output_schema_is_a_string_array_and_the_prompt_names_the_limit():
    schema = query_output_schema(3)
    assert schema["properties"]["queries"]["type"] == "array"
    assert schema["properties"]["queries"]["items"] == {"type": "string"}
    assert "3개 이하" in QUERY_SYSTEM_PROMPT.format(max_queries=3)


def test_query_prompt_asks_for_short_queries_grounded_in_the_input():
    # 실제 확인에서 긴 낱말 조합과 추측한 주제가 재검색 기사 0건으로 이어졌다. 수집기는 모든 낱말이 제목·요약에 있어야 통과시킨다.
    prompt = QUERY_SYSTEM_PROMPT.format(max_queries=3)
    assert "낱말 2~3개" in prompt
    assert "모든 낱말이 기사 제목이나 요약에 그대로 나와야" in prompt
    assert "추측한 주제를 쓰지 않고 입력 자료에 근거해" in prompt
    assert "figures" in prompt
    assert "입력에 근거가 없으면 빈 목록" in prompt


def test_query_llm_input_carries_the_background_figures():
    rates = {"apiType": "exchange-rate-summary", "target": None, "status": "ok",
             "response": '{"dataBody": {"items": [{"name": "TEST", "value": " 1000.00 "}]}}',
             "fetchedAt": (T - 60 * SEC).isoformat()}
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report(background=[rates])

    ((_, user, _),) = flow.query_llm.calls
    # 검색어 LLM은 배경 원문 전체가 아니라 코드가 꺼낸 배경 숫자(figures)를 받는다.
    figures = json.loads(user)["figures"]
    assert [f["name"] for f in figures][:3] == ["원/달러 환율", "코스피종합", "코스닥종합"]


def test_without_a_query_writer_insufficient_evidence_is_not_published():
    clock, requests, records = Clock(T), Requests(), Records()
    stage = ResearchReportStage(ReportStage(LlmWriter(FakeLlm(INSUFFICIENT)), requests, records, clock=clock),
                                SentenceResearch(None, requests, records, S))
    core = BriefingCore(requests, stage, S, records)
    core.on_move(move(), T)
    core.on_collection_completed(regular_completed(), T)
    core.on_history_responded(history(), T)
    core.on_context_responded(context(), T)
    core.on_articles_responded(articles(1), T)
    core.on_tick(T + S.recheck_interval)
    core.on_articles_responded(articles(2), T + S.recheck_interval)

    assert requests.of("news.research.requested") == []
    # 인용한 근거가 없으면 보내지 않는다(결과는 작업 기록에 남는다).
    assert requests.of("briefing.generated") == []
    assert [f.failure_reason for f in records.finishes] == [REASON_NO_EVIDENCE]
    assert records.items == []


# ---- 수집기가 다 모으지 못함, 완료 알림 시간 초과 ----

@pytest.mark.parametrize("incomplete", [[SYMBOL], ["*"]])
def test_daily_limit_or_search_failure_reports_existing_evidence_without_querying_articles(incomplete):
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research(incomplete)

    assert research_articles_requests(flow) == []
    # 인용한 근거가 없어 메일을 보내지 않고, 결과와 본문은 작업 기록에 남긴다.
    assert flow.briefings() == []
    (skipped,) = flow.not_sent()
    assert LLM_INSUFFICIENT in skipped.final_text
    assert flow.failures.failed == []
    (record,) = flow.records.of(ResearchReason.COLLECTION_INCOMPLETE)
    assert (record.stage, record.reason, record.data["missing"], record.data["queries"]) == (
        ResearchStep.COMPLETION, ResearchReason.COLLECTION_INCOMPLETE, incomplete, QUERIES)
    assert record.data["received"][0]["researchJobId"] == RID
    assert record.data["request"]["queries"] == QUERIES


def test_research_completion_timeout_reports_existing_evidence():
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    research = flow.research
    requested_at = flow.clock.now

    assert research.next_deadline() == requested_at + S.research_wait_limit
    research.on_tick(requested_at + S.research_wait_limit - SEC)
    assert flow.briefings() == []
    research.on_tick(requested_at + S.research_wait_limit)

    assert flow.briefings() == [] and len(flow.not_sent()) == 1
    assert flow.failures.failed == []
    (record,) = flow.records.of(ResearchReason.COMPLETION_TIMEOUT)
    assert (record.stage, record.reason) == (ResearchStep.COMPLETION, ResearchReason.COMPLETION_TIMEOUT)
    assert record.data["awaited"] == {"eventType": "collection.completed", "jobType": "research-news",
                              "researchJobId": RID, "requestedAt": requested_at.isoformat()}
    # 늦게 온 완료 알림은 무시된다.
    flow.complete_research()
    assert research_articles_requests(flow) == []


# ---- 재검색 기사 거르기 ----

def test_research_articles_outside_the_range_are_dropped_with_a_warning(caplog):
    late = article("late", T + S.delta + SEC)
    old = article("old", T - S.evidence_window - SEC)
    edge = article("edge", T + S.delta)
    flow = Flow([INSUFFICIENT, report([("경계 기사가 영향을 줬을 수 있습니다.", ["edge"])])], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    with caplog.at_level(logging.WARNING, logger="briefing.research"):
        flow.stable_research([late, edge, old])

    rewritten = json.loads(flow.report_llm.calls[1][1])
    assert [a["ref"] for a in rewritten["researchArticles"]] == ["edge"]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("late" in m and "outside the requested range" in m for m in warnings)
    assert any("old" in m and "outside the requested range" in m for m in warnings)
    assert len(flow.briefings()) == 1


def test_no_research_article_left_after_dropping_reports_existing_evidence_with_counts():
    late = article("late", T + S.delta + SEC)
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([late, A1])

    assert len(flow.report_llm.calls) == 1
    # 인용한 근거가 없어 메일을 보내지 않고, 결과와 본문은 작업 기록에 남긴다.
    assert flow.briefings() == []
    (skipped,) = flow.not_sent()
    assert LLM_INSUFFICIENT in skipped.final_text
    (record,) = flow.records.of(ResearchReason.NO_USABLE_ARTICLES)
    assert (record.stage, record.data["excluded"], record.data["duplicates"]) == (ResearchStep.FILTER, 1, 1)
    assert [a["articleId"] for a in record.data["received"][0]["articles"]] == ["late", "a1"]


def test_research_articles_that_keep_changing_report_existing_evidence():
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    research = flow.research
    for n in range(S.max_rounds):
        flow.clock.now += SEC
        research.on_articles_responded(articles(1 + n, [article(f"r{n}", T)], job_id=RID), flow.clock.now)
        if n < S.max_rounds - 1:
            flow.clock.now += S.recheck_interval
            research.on_tick(flow.clock.now)

    assert flow.briefings() == [] and len(flow.not_sent()) == 1
    assert len(flow.records.of(ResearchReason.ARTICLES_CONFIRM_ROUND)) == S.max_rounds - 1
    (record,) = flow.records.of(ResearchReason.ARTICLES_UNSTABLE)
    assert (record.attempts, len(record.data["received"])) == (S.max_rounds, S.max_rounds)


# ---- 다시 쓰기 ----

def test_rewrite_citing_refs_outside_the_list_is_retried_and_three_failures_record_llm_error():
    unknown = report([("지어낸 근거가 영향을 줬을 수 있습니다.", ["r9"])])
    flow = Flow([INSUFFICIENT, unknown, unknown, unknown], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1])

    assert len(flow.report_llm.calls) == 4
    (failure,) = flow.failures.failed
    assert (failure.reason, failure.attempts) == (FailureReason.LLM_ERROR, 3)
    (record,) = flow.records.of(ResearchReason.REWRITE_FAILED)
    assert record.stage == ResearchStep.REWRITE
    # 다시 쓰기가 끝내 실패해도 이미 쓴 첫 보고서가 있으므로 작업을 실패로 닫지 않고 그것을 낸다.
    # 인용한 근거가 없어 메일을 보내지 않고, 결과와 본문은 작업 기록에 남긴다.
    assert flow.briefings() == []
    (skipped,) = flow.not_sent()
    assert LLM_INSUFFICIENT in skipped.final_text
    (result,) = flow.records.results()
    assert result.status == RunStatus.SUCCEEDED and result.research_count == 1


def test_rewrite_rejecting_one_unknown_ref_then_valid_is_published():
    unknown = report([("지어낸 근거가 영향을 줬을 수 있습니다.", ["r9"])])
    flow = Flow([INSUFFICIENT, unknown, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1])

    (briefing,) = flow.briefings()
    assert "지어낸 근거" not in briefing.value["payload"]["text"]
    assert flow.failures.failed == []


def test_still_insufficient_after_rewrite_is_not_published_and_recorded():
    flow = Flow([INSUFFICIENT, INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1])

    # 인용한 근거가 없어 메일을 보내지 않고, 결과와 본문은 작업 기록에 남긴다.
    assert flow.briefings() == []
    (skipped,) = flow.not_sent()
    assert LLM_INSUFFICIENT in skipped.final_text
    assert len(flow.query_llm.calls) == 1  # 다시 써도 부족하면 또 재검색하지 않는다
    assert len(flow.requests.of("news.research.requested")) == 1
    (record,) = flow.records.of(ResearchReason.STILL_INSUFFICIENT)
    assert (record.stage, [a["articleId"] for a in record.data["received"]]) == (ResearchStep.REWRITE, ["r1"])


# ---- 작업 하나당 1번 ----

def test_same_job_is_researched_only_once():
    requests, records = Requests(), Records()
    done = []

    class Done:
        def on_research_done(self, target, result, now):
            done.append(result)

    research = NewsResearch(requests, records, Done(), S)
    target = ResearchTarget("move", RID, JOB_ID, WHOLE_REPORT, 1, "corr-1", SYMBOL, SYMBOL,
                            T - S.evidence_window, T + S.delta)

    assert research.start(target, tuple(QUERIES), T) is True
    assert research.start(target, ("다른 검색어",), T + SEC) is False
    assert len(requests.of("news.research.requested")) == 1
    requested, duplicate = records.items
    assert requested.reason == ResearchReason.REQUESTED.value
    assert (duplicate.reason, duplicate.data["queries"]) == (ResearchReason.ALREADY_RESEARCHED, ["다른 검색어"])


def test_duplicate_move_does_not_research_again():
    flow = Flow([INSUFFICIENT], [queries(*QUERIES)])
    flow.to_report()
    flow.core.on_move(move(), flow.clock.now)

    assert len(flow.requests.of("news.research.requested")) == 1
    assert len(flow.query_llm.calls) == 1


# ---- 데일리 판도 부를 수 있는 모양 ----

def test_research_takes_the_job_kind_so_a_daily_edition_can_request_with_the_market_wide_tag():
    requests, records = Requests(), Records()

    class Done:
        def on_research_done(self, target, result, now):
            pass

    research = NewsResearch(requests, records, Done(), S)
    since, until = T - timedelta(hours=16), T
    target = ResearchTarget("morning", "6d1f0b7e-2a4c-4e8f-9b3d-5a7c1e9f2b40", "tick-1", 2, 1, "c", "*", SYMBOL,
                            since, until)
    research.start(target, ("시장 전체",), T)
    research.on_tick(T + S.research_wait_limit)

    (request,) = requests.of("news.research.requested")
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    assert (request.value["payload"]["jobKind"], request.value["payload"]["symbol"]) == ("morning", "*")
    requested, timeout = records.items
    # 기록은 부른 작업(tick) id와 문장 번호·회차로 남는다.
    assert (requested.job_id, requested.sentence_no, requested.round) == ("tick-1", 2, 1)
    assert requested.entry_type == EntryType.RESEARCH_REQUESTED
    assert timeout.reason == ResearchReason.COMPLETION_TIMEOUT.value


# ---- 기록 값과 로그 ----

def test_record_has_every_field_group_and_no_secret_or_personal_names():
    import dataclasses

    from briefing.job_log import JobLogEntry

    names = [f.name for f in dataclasses.fields(JobLogEntry)]
    for needed in ("job_id", "entry_type", "stage", "sentence_no", "round", "before", "after", "reason",
                   "error_type", "attempts", "data", "occurred_at"):
        assert needed in names
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1])
    keys = set()
    for entry in flow.records.items:
        keys.update(entry.data)
    assert {"queries", "request", "received", "awaited", "missing", "excluded", "duplicates"} <= keys
    for name in [*names, *keys]:
        for word in ("key", "secret", "password", "token", "recipient", "email", "address", "quantity",
                     "amount", "balance", "holding"):
            assert word not in name.lower()


def test_logs_of_a_research_run_never_contain_queries_or_the_symbol(caplog):
    flow = Flow([INSUFFICIENT, report([("x", ["r9"])]), WITH_RESEARCH],
                [queries("하나", "둘", "셋", "넷"), queries(*QUERIES)])
    with caplog.at_level(logging.DEBUG):
        flow.to_report()
        flow.complete_research()
        flow.clock.now += S.response_timeout
        flow.research.on_tick(flow.clock.now)
        # 무응답 재요청 뒤라 seq 2부터 직접 돌린다.
        research = flow.research
        research.on_articles_responded(articles(2, [R1], job_id=RID), flow.clock.now)
        flow.clock.now += S.recheck_interval
        research.on_tick(flow.clock.now)
        research.on_articles_responded(articles(3, [R1], job_id=RID), flow.clock.now)

    assert len(flow.briefings()) == 1
    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("research requested job=") and "queryCount=2" in m for m in messages)
    assert any(m.startswith("research job=") and "researched=True" in m and "result=found" in m
               for m in messages)
    for text in [SYMBOL, *QUERIES, "하나", "넷"]:
        assert not [m for m in messages if text in m], text


def test_collection_log_marks_research_not_needed_when_evidence_is_sufficient(caplog):
    flow = Flow([SUFFICIENT], [])
    with caplog.at_level(logging.INFO, logger="briefing.sentence_research"):
        flow.to_report()

    (line,) = [r.getMessage() for r in caplog.records if r.getMessage().startswith("research job=")]
    assert "researched=False" in line and "result=not_needed" in line



# ---- 작업 기록 한 작업 전체 ----

def test_a_researched_job_that_finally_succeeds_keeps_every_line_under_one_job_id_and_ends_with_one_result():
    over_limit = queries("하나", "둘", "셋", "넷")
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [over_limit, queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1])

    entries = flow.records.entries
    # 재검색 작업 id는 따로지만 기록은 모두 재검색을 부른 작업 id 아래에 모인다.
    assert {e.job_id for e in entries} == {JOB_ID}
    assert [r.job_kind for r in flow.records.starts] == ["move"]
    assert [e.entry_type for e in entries if e.stage == "news.articles.recheck"] == [EntryType.CHECK_ROUND]  # 첫 근거 조회
    assert [(e.entry_type, e.reason) for e in entries if e.stage == "query"] == [
        (EntryType.LLM_REJECTED, "query_rejected"), (EntryType.QUERY_WRITTEN, "query_accepted")]
    assert [e.entry_type for e in entries if e.stage == "research-request"] == [EntryType.RESEARCH_REQUESTED]
    assert [e.entry_type for e in entries if e.stage == "articles-confirm"] == [EntryType.CHECK_ROUND]
    assert [e.entry_type for e in entries if e.stage == "filter"] == [EntryType.ARTICLES_RECEIVED]
    # 첫 보고서와 다시 쓴 보고서의 LLM 시도가 모두 남는다.
    written = [e for e in entries if e.stage == "llm-report" and e.entry_type == EntryType.REPORT_WRITTEN]
    assert len(written) == 2
    (result,) = flow.records.results()
    assert (result.status, result.generated_by, result.research_count) == (RunStatus.SUCCEEDED, GeneratedBy.LLM, 1)
    assert flow.failures.failed == []
    (briefing,) = flow.briefings()
    assert result.final_text == briefing.value["payload"]["text"]
    assert result.result_event_id == briefing.value["eventId"]


def test_a_researched_job_is_not_published_when_its_log_cannot_be_saved(caplog):
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.records.fail = True
    with caplog.at_level(logging.ERROR, logger="briefing.job_log"):
        flow.stable_research([R1])

    assert flow.briefings() == []
    assert "result event not published" in caplog.text
    assert SYMBOL not in caplog.text and not [q for q in QUERIES if q in caplog.text]


def test_job_log_values_carry_no_key_address_quantity_or_amount(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-secret-value")
    flow = Flow([INSUFFICIENT, WITH_RESEARCH], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research()
    flow.stable_research([R1])

    dumped = json.dumps([[e.data, e.stage, e.reason, e.error_type, e.before, e.after] for e in flow.records.entries],
                        ensure_ascii=False, default=str)
    for forbidden in ("sk-test-secret-value", "@", "quantity", "amount", "holdings", "수량", "금액"):
        assert forbidden not in dumped


# ---- 구성 ----

def test_report_marks_insufficient_evidence_only_when_the_llm_says_so():
    from briefing.jobs import ReportInput
    from briefing.sources import Article, PricePoint

    report_input = ReportInput(JOB_ID, SYMBOL, "corr-1", T, 10_000, 9_850, -1.5, (PricePoint(T, 9_850),),
                               (Article("a1", "기사", "", T, "news.example.com", "https://news.example.com/a1"),))
    assert LlmWriter(FakeLlm(INSUFFICIENT)).write(report_input).insufficient_evidence is True
    assert LlmWriter(FakeLlm(SUFFICIENT)).write(report_input).insufficient_evidence is False


def test_query_writer_is_built_only_with_an_llm_key(fake_sdk):
    from briefing.llm import AnthropicClient
    from briefing.research import build_query_writer

    assert build_query_writer({"ANTHROPIC_API_KEY": "change-me"}, AnthropicClient) is None
    writer = build_query_writer({"ANTHROPIC_API_KEY": FAKE_KEY}, AnthropicClient)
    assert (writer.max_queries, writer.max_attempts) == (S.max_research_queries, S.research_query_attempts)
    assert FAKE_KEY not in repr(writer)
