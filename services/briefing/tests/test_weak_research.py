"""문장 단위 근거 약함과 재검색 시나리오: 가짜 LLM·가짜 응답으로 급변동·아침 판·마감 후 판을 각각 돌린다.

경우: 근거 충분, 문장 하나 약함, 여러 문장 약함(문장마다 별도 재검색 작업 id로 동시에 요청), 2번째에 튼튼해짐,
2번 다 약함, 요청 합계 상한, 전체 시간 상한, 다른 문장이 쓴 기사 빼기, 다시 쓰기 검증, 재검색 실패.
실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다. 나가는 요청과 이벤트는 계약 스키마로 검증한다.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings, DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_report import DAILY_INSUFFICIENT, DailyLlmWriter
from briefing.daily_report import parse_output as parse_daily
from briefing.job_log import EntryType, RunStatus
from briefing.llm_output import InvalidLlmOutput, WEAK_NOTE_PREFIX, output_weak_shortfall
from briefing.report import parse_output as parse_move
from briefing.research import QueryWriter, ResearchReason, ResearchStep
from briefing.sentence_research import SentenceResearch, research_job_id
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
    article,
    articles,
    queries,
    research_articles_requests,
)

KST = timezone(timedelta(hours=9))
S = CoreSettings()
SEC = timedelta(seconds=1)
SHORTFALL = "변동 직전에 나온 해당 업종 기사가 더 필요합니다."
SECOND_SHORTFALL = "공시의 구체 항목을 설명한 자료가 더 필요합니다."
STILL_SHORTFALL = "해당 회사의 실적을 설명한 자료가 더 필요합니다."
# 범위 안(변동 3시간 전)에 나온 재검색 기사의 발행 시각
T_ARTICLE = datetime.fromisoformat(R1["publishedAt"])


# ---- 설정값 ----

def test_sentence_research_limits_are_settings_with_the_plan_values():
    assert S.research_rounds_per_sentence == 2
    assert S.max_research_requests == 10
    assert S.research_total_limit == timedelta(minutes=15)


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


def test_move_and_daily_parsers_read_the_weakness_of_each_sentence():
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
    """items: (문장, 근거 번호, 부족한 점 또는 None). 문장 하나가 원인 하나다."""
    return json.dumps({"causes": [
        {"title": s, "detail": s, "refs": refs, "meaning": s, "meaningRefs": refs,
         "weak": shortfall is not None, "shortfall": shortfall or ""} for s, refs, shortfall in items],
        "insufficientEvidence": insufficient}, ensure_ascii=False)


STRONG_TEXT = "기사가 영향을 줬을 수 있습니다."
WEAK_TEXT = "업종 소식이 영향을 줬을 수 있습니다."
THIRD_TEXT = "공시가 영향을 줬을 수 있습니다."
MOVE_STRONG = move_output((STRONG_TEXT, ["a1"], None))
MOVE_ONE_WEAK = move_output((STRONG_TEXT, ["a1"], None), (WEAK_TEXT, ["a1"], SHORTFALL))
MOVE_TWO_WEAK = move_output((WEAK_TEXT, ["a1"], SHORTFALL), (STRONG_TEXT, ["a1"], None),
                            (THIRD_TEXT, ["a1"], SECOND_SHORTFALL))
MOVE_THREE_WEAK = move_output((WEAK_TEXT, ["a1"], SHORTFALL), (THIRD_TEXT, ["a1"], SECOND_SHORTFALL),
                              (STRONG_TEXT, ["a1"], "세 번째 부족한 점입니다."))


def one_cause(text, refs, shortfall=None):
    """다시 쓰기 응답: 다시 쓴 원인 하나만 담는다."""
    return move_output((text, refs, shortfall))


def rid(no, round_no=1):
    return research_job_id(JOB_ID, no, round_no)


def sentence_requests(flow):
    return flow.requests.of("news.research.requested")


def move_sentence_text(briefing_text, index):
    return briefing_text.split(f"{index}. ")[1].split("\n")[0]


def test_move_with_every_sentence_strong_is_published_without_research():
    flow = Flow([MOVE_STRONG], [])
    flow.to_report()

    assert flow.query_llm.calls == [] and sentence_requests(flow) == []
    (briefing,) = flow.briefings()
    assert "근거 약함" not in briefing.value["payload"]["text"]
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (0, 0)


def test_move_with_one_weak_sentence_researches_only_that_sentence_and_rewrites_only_it():
    rewritten = one_cause("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"])
    flow = Flow([MOVE_ONE_WEAK, rewritten], [queries(*QUERIES)])
    flow.to_report()

    # 검색어 LLM에는 약한 문장, 부족한 점, 이미 찾은 기사 제목, 배경 숫자가 간다.
    ((_, user, _),) = flow.query_llm.calls
    context = json.loads(user)
    assert context["sentence"].startswith(WEAK_TEXT) and context["shortfall"] == SHORTFALL
    assert context["foundArticleTitles"] == ["기사 a1"] and "figures" in context
    assert flow.briefings() == []
    (request,) = sentence_requests(flow)
    assert validate("news.research.requested.v1.schema.json", request.value) == []
    # 문장 번호와 회차로 정해지는 별도 재검색 작업 id로 요청한다.
    assert request.value["payload"]["jobId"] == rid(2)

    flow.complete_research(research_id=rid(2))
    flow.stable_research([R1, A1], research_id=rid(2))

    # 다시 쓰기는 그 문장만 담은 요청이며, 약하지 않은 첫 문장은 그대로다.
    assert len(flow.report_llm.calls) == 2
    rewrite = json.loads(flow.report_llm.calls[1][1])["rewrite"]
    assert rewrite["sentenceNo"] == 2 and rewrite["shortfall"] == SHORTFALL and rewrite["newRefs"] == ["r1"]
    (briefing,) = flow.briefings()
    assert validate("briefing.generated.v1.schema.json", briefing.value) == []
    text = briefing.value["payload"]["text"]
    assert STRONG_TEXT in text and "다시 찾은 기사가 영향을 줬을 수 있습니다." in text and WEAK_TEXT not in text
    assert "근거 약함" not in text
    assert [e["articleId"] for e in briefing.value["payload"]["evidence"]] == ["a1", "r1"]
    # 문장을 고친 줄에는 고치기 전·후 문장과 문장 번호·회차가 남는다.
    (entry,) = flow.records.of_type(EntryType.SENTENCE_REWRITTEN)
    assert (entry.sentence_no, entry.round, entry.stage) == (2, 1, "rewrite")
    assert entry.before.startswith(WEAK_TEXT) and entry.after.startswith("다시 찾은 기사가")
    (result,) = flow.records.results()
    assert (result.status, result.weak_count, result.research_count) == (RunStatus.SUCCEEDED, 0, 1)


def test_move_with_several_weak_sentences_sends_one_separate_research_job_per_sentence_at_the_same_time():
    # 셋째 문장의 재검색이 먼저 끝나므로 다시 쓰기 응답도 셋째 문장 것이 먼저 쓰인다.
    flow = Flow([MOVE_TWO_WEAK, one_cause("셋째 문장을 다시 썼습니다.", ["r3"]),
                 one_cause("첫 문장을 다시 썼습니다.", ["r1"])],
                [queries("검색어 하나"), queries("검색어 둘")])
    flow.to_report()

    # 응답을 기다리기 전에 문장마다 요청이 이미 나가 있다. 작업 id가 서로 다르다.
    requests = sentence_requests(flow)
    assert [r.value["payload"]["jobId"] for r in requests] == [rid(1), rid(3)]
    assert [r.value["payload"]["queries"] for r in requests] == [["검색어 하나"], ["검색어 둘"]]
    assert len({r.value["eventId"] for r in requests}) == 2 and len(flow.query_llm.calls) == 2
    assert [json.loads(c[1])["shortfall"] for c in flow.query_llm.calls] == [SHORTFALL, SECOND_SHORTFALL]

    # 두 재검색이 따로 끝나고, 먼저 끝난 문장부터 그 문장만 다시 쓴다.
    flow.complete_research(research_id=rid(3))
    flow.complete_research(research_id=rid(1))
    flow.stable_research_jobs({rid(3): [article("r3", T_ARTICLE)], rid(1): [R1]})

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "첫 문장을 다시 썼습니다." in text and "셋째 문장을 다시 썼습니다." in text and STRONG_TEXT in text
    assert WEAK_TEXT not in text and THIRD_TEXT not in text
    rewrites = flow.records.of_type(EntryType.SENTENCE_REWRITTEN)
    assert sorted(e.sentence_no for e in rewrites) == [1, 3]
    (result,) = flow.records.results()
    assert (result.research_count, result.weak_count) == (2, 0)


def test_move_sentence_that_becomes_strong_on_the_second_search_is_searched_twice():
    first = one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)
    second = one_cause("한 번 더 찾은 기사로 확인했습니다.", ["r2"])
    flow = Flow([MOVE_ONE_WEAK, first, second], [queries("검색어 하나"), queries("검색어 둘")])
    flow.to_report()
    flow.complete_research(research_id=rid(2, 1))
    flow.stable_research([R1], research_id=rid(2, 1))

    # 다시 써도 약하면 그 문장만 한 번 더 찾는다. 이때는 새 회차의 작업 id와 새 부족한 점으로 검색어를 정한다.
    assert [r.value["payload"]["jobId"] for r in sentence_requests(flow)] == [rid(2, 1), rid(2, 2)]
    assert json.loads(flow.query_llm.calls[1][1])["shortfall"] == STILL_SHORTFALL
    assert flow.briefings() == []
    flow.complete_research(research_id=rid(2, 2))
    flow.stable_research([article("r2", T_ARTICLE)], research_id=rid(2, 2))

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "한 번 더 찾은 기사로 확인했습니다." in text and "근거 약함" not in text
    assert [(e.sentence_no, e.round) for e in flow.records.of_type(EntryType.SENTENCE_REWRITTEN)] == [(2, 1), (2, 2)]
    (result,) = flow.records.results()
    assert (result.research_count, result.weak_count) == (2, 0)


def test_move_sentence_still_weak_after_two_searches_keeps_the_weak_note_and_is_not_searched_a_third_time():
    first = one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)
    second = one_cause("한 번 더 찾았지만 여전히 약합니다.", ["r2"], SECOND_SHORTFALL)
    flow = Flow([MOVE_ONE_WEAK, first, second], [queries("검색어 하나"), queries("검색어 둘")])
    flow.to_report()
    flow.complete_research(research_id=rid(2, 1))
    flow.stable_research([R1], research_id=rid(2, 1))
    flow.complete_research(research_id=rid(2, 2))
    flow.stable_research([article("r2", T_ARTICLE)], research_id=rid(2, 2))

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert (WEAK_NOTE_PREFIX + SECOND_SHORTFALL) in text and (WEAK_NOTE_PREFIX + STILL_SHORTFALL) not in text
    assert len(sentence_requests(flow)) == 2 and len(flow.query_llm.calls) == 2
    (limit,) = flow.records.of(ResearchReason.SENTENCE_LIMIT)
    assert (limit.stage, limit.sentence_no) == (ResearchStep.LIMIT, 2)
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)


def test_move_sentences_over_the_request_limit_are_not_searched_and_keep_the_weak_note():
    settings = CoreSettings(max_research_requests=2)
    flow = Flow([MOVE_THREE_WEAK, one_cause("첫 문장을 다시 썼습니다.", ["r1"]),
                 one_cause("둘째 문장을 다시 썼습니다.", ["r3"])],
                [queries("검색어 하나"), queries("검색어 둘")], settings)
    flow.to_report()

    # 요청 합계가 상한(여기서는 2)에 닿으면 나머지 문장은 검색어를 묻지도 않고 요청하지도 않는다.
    assert [r.value["payload"]["jobId"] for r in sentence_requests(flow)] == [rid(1), rid(2)]
    assert len(flow.query_llm.calls) == 2
    (limit,) = flow.records.of(ResearchReason.REQUEST_LIMIT)
    assert (limit.sentence_no, limit.data["requests"], limit.data["requestLimit"]) == (3, 2, 2)

    flow.complete_research(research_id=rid(1))
    flow.complete_research(research_id=rid(2))
    flow.stable_research_jobs({rid(1): [R1], rid(2): [article("r3", T_ARTICLE)]})

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "세 번째 부족한 점입니다." in text and (WEAK_NOTE_PREFIX + "세 번째 부족한 점입니다.") in text
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)


def test_the_request_limit_counts_second_searches_too():
    settings = CoreSettings(max_research_requests=1)
    flow = Flow([MOVE_ONE_WEAK, one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)],
                [queries("검색어 하나")], settings)
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.stable_research([R1], research_id=rid(2))

    assert len(sentence_requests(flow)) == 1 and len(flow.query_llm.calls) == 1
    (briefing,) = flow.briefings()
    assert (WEAK_NOTE_PREFIX + STILL_SHORTFALL) in briefing.value["payload"]["text"]
    assert [e.reason for e in flow.records.entries if e.stage == ResearchStep.LIMIT.value] == [
        ResearchReason.REQUEST_LIMIT.value]


def test_move_total_time_limit_publishes_the_body_at_that_moment_and_ignores_late_answers():
    # 완료 알림 대기 상한(5분)보다 짧게 줄여, 전체 시간 상한이 먼저 닿는 경우를 만든다.
    limit = timedelta(minutes=2)
    flow = Flow([MOVE_TWO_WEAK, one_cause("첫 문장을 다시 썼습니다.", ["r1"])],
                [queries("검색어 하나"), queries("검색어 둘")], CoreSettings(research_total_limit=limit))
    flow.to_report()
    started = flow.clock.now
    flow.complete_research(research_id=rid(1))
    flow.stable_research([R1], research_id=rid(1))  # 첫 문장은 제때 끝난다
    assert flow.briefings() == []

    assert flow.sentences.next_deadline() == started + limit
    flow.clock.now = started + limit - SEC
    flow.sentences.on_tick(flow.clock.now)
    assert flow.briefings() == []
    flow.clock.now = started + limit
    flow.sentences.on_tick(flow.clock.now)

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    # 끝난 문장은 다시 쓴 대로, 끝나지 못한 문장은 약하다는 표시와 함께 그때의 본문으로 나간다.
    assert "첫 문장을 다시 썼습니다." in text and (WEAK_NOTE_PREFIX + SECOND_SHORTFALL) in text
    (limit,) = flow.records.of(ResearchReason.TIME_LIMIT)
    assert (limit.sentence_no, limit.stage) == (3, ResearchStep.LIMIT)
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)
    # 늦게 온 답은 이미 낸 작업을 다시 움직이지 않는다.
    flow.complete_research(research_id=rid(3))
    late = articles(1, [article("r3", T_ARTICLE)], job_id=rid(3))
    assert flow.research.on_articles_responded(late, flow.clock.now) is False
    assert len(flow.briefings()) == 1 and flow.sentences.next_deadline() is None


def test_move_article_already_used_by_another_sentence_is_not_used_again():
    flow = Flow([MOVE_TWO_WEAK, one_cause("첫 문장을 다시 썼습니다.", ["r1"]),
                 one_cause("셋째 문장을 다시 썼습니다.", ["r3"])],
                [queries("검색어 하나"), queries("검색어 둘")])
    flow.to_report()
    other = article("r3", T_ARTICLE)
    flow.complete_research(research_id=rid(1))
    flow.complete_research(research_id=rid(3))
    # 둘째 재검색은 r1도 찾았지만, 첫 문장이 이미 쓴 r1은 빼고 새 기사 r3만 쓴다.
    flow.stable_research_jobs({rid(1): [R1], rid(3): [R1, other]})

    third = json.loads(flow.report_llm.calls[2][1])["rewrite"]
    assert third["sentenceNo"] == 3 and third["newRefs"] == ["r3"]
    assert [a["ref"] for a in json.loads(flow.report_llm.calls[2][1])["researchArticles"]] == ["r1", "r3"]
    selected = [e for e in flow.records.entries if e.reason == ResearchReason.ARTICLES_SELECTED.value]
    assert [e.data["used"] and [a["articleId"] for a in e.data["used"]] for e in selected] == [["r1"], ["r3"]]
    assert [a["articleId"] for a in selected[1].data["dropped"]] == ["r1"]
    assert selected[1].data["dropped"][0]["reason"] == "used_by_other_sentence"


def test_move_sentence_whose_articles_were_all_used_by_another_sentence_is_not_rewritten():
    flow = Flow([MOVE_TWO_WEAK, one_cause("첫 문장을 다시 썼습니다.", ["r1"])],
                [queries("검색어 하나"), queries("검색어 둘")])
    flow.to_report()
    flow.complete_research(research_id=rid(1))
    flow.complete_research(research_id=rid(3))
    flow.stable_research_jobs({rid(1): [R1], rid(3): [R1]})

    assert len(flow.report_llm.calls) == 2
    (record,) = flow.records.of(ResearchReason.ALL_USED_BY_OTHERS)
    assert record.sentence_no == 3
    (briefing,) = flow.briefings()
    assert (WEAK_NOTE_PREFIX + SECOND_SHORTFALL) in briefing.value["payload"]["text"]


def test_move_sentence_rewrite_citing_a_ref_outside_the_list_is_rejected_and_the_old_sentence_stays():
    unknown = one_cause("지어낸 근거가 영향을 줬을 수 있습니다.", ["r9"])
    flow = Flow([MOVE_ONE_WEAK, unknown, unknown, unknown], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.stable_research([R1], research_id=rid(2))

    assert len(flow.report_llm.calls) == 4
    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "지어낸 근거" not in text and WEAK_TEXT in text and (WEAK_NOTE_PREFIX + SHORTFALL) in text
    rejected = [e for e in flow.records.of_type(EntryType.LLM_REJECTED) if e.sentence_no == 2]
    assert [e.attempts for e in rejected] == [1, 2, 3] and all(e.round == 1 for e in rejected)
    assert "not given" in rejected[0].data["rejection"]
    (failed,) = flow.records.of(ResearchReason.REWRITE_FAILED)
    assert failed.sentence_no == 2
    # 작업은 실패가 아니라 원래 문장으로 나간다.
    (result,) = flow.records.results()
    assert result.status == RunStatus.SUCCEEDED and result.weak_count == 1


def test_move_sentence_rewrite_that_returns_two_causes_or_insufficient_is_rejected():
    two = move_output(("하나", ["r1"], None), ("둘", ["r1"], None))
    nothing = move_output(insufficient=True)
    good = one_cause("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"])
    flow = Flow([MOVE_ONE_WEAK, two, nothing, good], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.stable_research([R1], research_id=rid(2))

    rejected = [e for e in flow.records.of_type(EntryType.LLM_REJECTED) if e.sentence_no == 2]
    assert [e.data["rejection"] for e in rejected] == ["rewrite must return exactly one cause"] * 2
    (briefing,) = flow.briefings()
    assert "다시 찾은 기사가 영향을 줬을 수 있습니다." in briefing.value["payload"]["text"]


def test_move_research_articles_outside_the_time_range_are_dropped_again_and_never_used_to_rewrite():
    late = article("late", datetime(2026, 10, 20, 16, 30, tzinfo=KST))
    flow = Flow([MOVE_ONE_WEAK], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.stable_research([late], research_id=rid(2))

    assert len(flow.report_llm.calls) == 1
    (record,) = flow.records.of(ResearchReason.NO_USABLE_ARTICLES)
    assert record.sentence_no == 2 and record.data["excluded"] == 1
    (briefing,) = flow.briefings()
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in briefing.value["payload"]["text"]
    received = flow.records.of(ResearchReason.ARTICLES_RECEIVED)
    assert received[0].data["dropped"][0]["reason"] == "outside_range"


def test_move_research_that_never_answers_publishes_the_first_report_with_its_weak_mark():
    flow = Flow([MOVE_ONE_WEAK], [queries(*QUERIES)])
    flow.to_report()

    flow.clock.now += S.research_wait_limit
    flow.sentences.on_tick(flow.clock.now)

    (briefing,) = flow.briefings()
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in briefing.value["payload"]["text"]
    assert [r.reason for r in flow.records.entries if r.entry_type == EntryType.STEP_FAILED] == [
        ResearchReason.COMPLETION_TIMEOUT.value]
    assert len(flow.report_llm.calls) == 1


def test_move_query_llm_gets_the_etf_info_and_is_told_to_prefer_korean_names_for_us_stocks():
    from briefing.research import QUERY_SYSTEM_PROMPT

    etf_info = {"name": "TEST ETF ONE", "members": [
        {"market": "US", "symbol": "TESTA", "rank": 1, "name": "테스트회사", "weight": "12.5"}], "membersTruncated": False}
    flow = Flow([MOVE_ONE_WEAK], [queries(*QUERIES)])
    flow.to_report(etf=etf_info)

    ((system, user, _),) = flow.query_llm.calls
    sent = json.loads(user)["etf"]
    assert sent["name"] == "TEST ETF ONE" and sent["members"][0]["name"] == "테스트회사"
    for word in ("문장", "부족한 점", "한글", "티커"):
        assert word in system
    assert system == QUERY_SYSTEM_PROMPT.format(max_queries=S.max_research_queries)


def test_move_with_insufficient_evidence_overall_still_researches_the_whole_report_once():
    flow = Flow([move_output(insufficient=True), move_output(("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"], None))],
                [queries(*QUERIES)])
    flow.to_report()

    assert "sentence" not in json.loads(flow.query_llm.calls[0][1])
    (request,) = sentence_requests(flow)
    assert request.value["payload"]["jobId"] == rid(0)
    flow.complete_research(research_id=rid(0))
    flow.stable_research([R1], research_id=rid(0))
    (briefing,) = flow.briefings()
    assert "다시 찾은 기사가 영향을 줬을 수 있습니다." in briefing.value["payload"]["text"]


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


class DailyFlow:
    """판 하나를 자료 수집 -> 요약 -> 문장 단위 재검색 -> 다시 쓰기까지 돌리는 도우미."""

    def __init__(self, edition, report_outputs, query_outputs, etfs=None, settings=S):
        self.edition = edition
        self.job_type, self.at, self.ref = EDITIONS[edition]
        self.requests, self.records = Requests(), FakeJobLog()
        self.report_llm, self.query_llm = FakeLlm(*report_outputs), FakeLlm(*query_outputs)
        daily_settings = DailySettings()
        self.sentences = SentenceResearch(
            QueryWriter(self.query_llm, settings.max_research_queries, settings.research_query_attempts),
            self.requests, self.records, settings)
        self.research = self.sentences.research
        self.core = DailyCore(
            self.requests, DailyLlmWriter(self.report_llm, daily_settings.llm_max_attempts), self.records,
            daily_settings, sentences=self.sentences)
        self.now = self.at
        self.etfs = etfs

    def rid(self, no, round_no=1):
        return research_job_id(TICK_ID, no, round_no)

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

    def news(self, seq, items, research_id):
        return envelope("news.articles.responded", {
            "jobId": research_id, "seq": seq, "symbol": "TEST01", "truncated": False, "articles": items})

    def complete(self, research_id):
        self.now += SEC
        event = json.loads(envelope("collection.completed", {
            "jobType": "research-news", "startedAt": self.now.isoformat(), "finishedAt": self.now.isoformat(),
            "incompleteSymbols": [], "researchJobId": research_id}))
        return self.research.on_collection_completed(json.dumps(event), self.now)

    def stable(self, items, research_id):
        for n in range(2):
            self.now += SEC
            assert self.research.on_articles_responded(self.news(n + 1, items, research_id), self.now) is True
            if n == 0:
                self.now += S.recheck_interval
                self.research.on_tick(self.now)

    def stable_jobs(self, items_by_job):
        """여러 재검색 작업에 같은 목록을 두 라운드 돌려준다. 라운드마다 모든 작업에 먼저 답한 뒤 확인 시각을 지나게 한다."""
        for n in range(2):
            for research_id, items in items_by_job.items():
                self.now += SEC
                assert self.research.on_articles_responded(self.news(n + 1, items, research_id), self.now) is True
            if n == 0:
                self.now += S.recheck_interval
                self.research.on_tick(self.now)

    def published(self):
        events = self.requests.topic("briefing.daily.generated")
        for event in events:
            assert validate("briefing.daily.generated.v1.schema.json", event.value) == []
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
    (first,) = [e for e in flow.requests.topic("news.articles.requested") if "researchJobId" in e.value["payload"]]
    assert validate("news.articles.requested.v1.schema.json", first.value) == []
    payload = first.value["payload"]
    # 기사 시간 범위는 판 범위(예정 시각 포함)이고, 조회 symbol은 정렬한 첫 ETF 코드다.
    assert payload["since"] == (flow.core._jobs[TICK_ID].range_start).isoformat()
    assert payload["until"] == flow.at.isoformat()
    assert (payload["symbol"], payload["researchJobId"], payload["seq"]) == ("TEST01", flow.rid(2), 1)

    # 범위 밖 기사는 버리고 범위 안 기사만 다시 쓰는 데 쓴다.
    flow.stable([in_range(flow), out_of_range(flow)], flow.rid(2))

    rewrite = json.loads(flow.report_llm.calls[1][1])
    research_item = next(i for i in rewrite["items"] if i["name"] == "재검색 기사")
    assert [e["ref"] for e in research_item["entries"]] == ["r1"]
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
    assert (WEAK_NOTE_PREFIX + SECOND_SHORTFALL) in text and (WEAK_NOTE_PREFIX + STILL_SHORTFALL) not in text
    assert len(flow.research_requests()) == 2 and len(flow.query_llm.calls) == 2
    (limit,) = flow.records.of_reason(ResearchReason.SENTENCE_LIMIT)
    assert (limit.stage, limit.sentence_no) == (ResearchStep.LIMIT.value, 1)
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)


def test_daily_sentences_over_the_request_limit_are_not_searched_and_keep_the_weak_note(edition):
    flow = DailyFlow(edition, [], [], settings=CoreSettings(max_research_requests=1))
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
    assert (WEAK_NOTE_PREFIX + SECOND_SHORTFALL) in text and "둘째 문장을 다시 썼습니다." in text
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
    assert "둘째 문장을 다시 썼습니다." in text and (WEAK_NOTE_PREFIX + SECOND_SHORTFALL) in text
    (limit,) = flow.records.of_reason(ResearchReason.TIME_LIMIT)
    assert limit.sentence_no == 3
    flow.complete(flow.rid(3))
    assert flow.research.on_articles_responded(flow.news(1, [in_range(flow, "r3")], flow.rid(3)), flow.now) is False
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
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in text
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
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in event.value["payload"]["text"]
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
    # 검색어 LLM 거부와 성공, 재검색 요청, 안정 확인 라운드, 받은 기사, 다시 쓰기가 모두 남는다.
    assert ("query", EntryType.LLM_REJECTED) in steps and ("query", EntryType.QUERY_WRITTEN) in steps
    assert ("research-request", EntryType.RESEARCH_REQUESTED) in steps
    assert ("articles-confirm", EntryType.CHECK_ROUND) in steps
    assert ("filter", EntryType.ARTICLES_RECEIVED) in steps and ("select", EntryType.ARTICLES_RECEIVED) in steps
    assert ("rewrite", EntryType.SENTENCE_REWRITTEN) in steps
    assert [e.sentence_no for e in entries if e.stage == "query"] == [1, 1]
    assert {e.round for e in entries if e.stage in ("query", "research-request", "articles-confirm", "rewrite")} == {1}
    rejected = next(e for e in entries if e.stage == "query" and e.entry_type == EntryType.LLM_REJECTED)
    assert rejected.data["llm"]["rejectReason"]
    received = next(e for e in entries if e.stage == "filter")
    assert received.data["usable"][0]["articleId"] == "r1" and received.data["collectorCompletion"]["jobType"] == "research-news"


def test_daily_without_etf_data_skips_the_research_and_publishes_the_first_result():
    flow = DailyFlow("post-close", [], [], etfs=[])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL))]
    flow.to_first_report()

    # 기사 조회 요청에는 종목코드가 필수라 보유 ETF 자료가 없으면 재검색하지 않는다.
    assert flow.query_llm.calls == [] and flow.research_requests() == []
    (event,) = flow.published()
    assert (WEAK_NOTE_PREFIX + SHORTFALL) in event.value["payload"]["text"]


def test_daily_research_symbol_is_the_first_etf_code_in_sorted_order():
    flow = DailyFlow("post-close", [], [], etfs=[etf("TEST09"), etf("TEST02")])
    flow.report_llm.outputs = [daily_output(point("그날 증시 자료가 있습니다.", "B1", SHORTFALL)),
                               daily_output(point("그날 증시 자료가 있습니다.", "B1"))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    flow.complete(flow.rid(1))

    (request,) = [e for e in flow.requests.topic("news.articles.requested") if "researchJobId" in e.value["payload"]]
    assert request.value["payload"]["symbol"] == "TEST02"


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
