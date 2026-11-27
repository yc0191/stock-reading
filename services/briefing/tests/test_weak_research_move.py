"""문장 단위 근거 약함과 재검색 시나리오 중 설정값·공통 검사와 급변동: 가짜 LLM·가짜 응답으로 돌린다.
데일리 판(아침 판·마감 후 판)은 test_weak_research_daily.py에 있다.

경우: 근거 충분, 문장 하나 약함, 여러 문장 약함(문장마다 별도 재검색 작업 id로 동시에 요청), 2번째에 튼튼해짐,
2번 다 약함, 요청 합계 상한, 전체 시간 상한, 다른 문장이 쓴 기사 빼기, 다시 쓰기 검증, 재검색 실패.
실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다. 나가는 요청과 이벤트는 계약 스키마로 검증한다.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings
from briefing.daily_report import parse_output as parse_daily
from briefing.job_log import EntryType, RunStatus
from briefing.llm_output import InvalidLlmOutput, output_weak_shortfall
from briefing.report import parse_output as parse_move
from briefing.research import ResearchReason, ResearchStep
from briefing.sentence_research import research_job_id
from contracts import validate
from test_research import A1, JOB_ID, QUERIES, R1, Flow, article, articles, queries

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
    assert S.research_total_limit == timedelta(minutes=4)


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


def weak_items(flow):
    """작업 기록(missing-items 줄)에 남은 근거 약함 부족한 점."""
    return [x for e in flow.records.of_type(EntryType.MISSING_ITEMS) for x in e.data.get("weakItems", [])]


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
    flow.answer_research([R1, A1], research_id=rid(2))

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
    flow.answer_research_jobs({rid(3): [article("r3", T_ARTICLE)], rid(1): [R1]})

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
    flow.answer_research([R1], research_id=rid(2, 1))

    # 다시 써도 약하면 그 문장만 한 번 더 찾는다. 이때는 새 회차의 작업 id와 새 부족한 점으로 검색어를 정한다.
    assert [r.value["payload"]["jobId"] for r in sentence_requests(flow)] == [rid(2, 1), rid(2, 2)]
    assert json.loads(flow.query_llm.calls[1][1])["shortfall"] == STILL_SHORTFALL
    assert flow.briefings() == []
    flow.complete_research(research_id=rid(2, 2))
    flow.answer_research([article("r2", T_ARTICLE)], research_id=rid(2, 2))

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "한 번 더 찾은 기사로 확인했습니다." in text and "근거 약함" not in text
    assert [(e.sentence_no, e.round) for e in flow.records.of_type(EntryType.SENTENCE_REWRITTEN)] == [(2, 1), (2, 2)]
    (result,) = flow.records.results()
    assert (result.research_count, result.weak_count) == (2, 0)


def test_a_second_search_is_told_the_used_search_terms_and_does_not_repeat_them():
    first = one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)
    second = one_cause("한 번 더 찾은 기사로 확인했습니다.", ["r2"])
    # 둘째 회차의 검색어 LLM이 첫 회차 검색어를 띄어쓰기만 바꿔 다시 낸다.
    flow = Flow([MOVE_ONE_WEAK, first, second], [queries("검색어 하나"), queries("검색어  하나", "검색어 둘")])
    flow.to_report()
    first_context = json.loads(flow.query_llm.calls[0][1])
    flow.complete_research(research_id=rid(2, 1))
    flow.answer_research([R1], research_id=rid(2, 1))

    # 처음 찾을 때의 입력 모양은 그대로이고, 둘째 회차에만 이미 쓴 검색어를 알려 준다.
    assert "usedQueries" not in first_context
    assert json.loads(flow.query_llm.calls[1][1])["usedQueries"] == ["검색어 하나"]
    # 이미 쓴 검색어는 빼고 새 검색어로만 찾는다.
    assert sentence_requests(flow)[1].value["payload"]["queries"] == ["검색어 둘"]


def test_a_second_search_whose_terms_were_all_used_is_not_sent():
    first = one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)
    flow = Flow([MOVE_ONE_WEAK, first], [queries("검색어 하나"), queries("검색어 하나")])
    flow.to_report()
    flow.complete_research(research_id=rid(2, 1))
    flow.answer_research([R1], research_id=rid(2, 1))

    assert [r.value["payload"]["jobId"] for r in sentence_requests(flow)] == [rid(2, 1)]
    (briefing,) = flow.briefings()
    # 레벨 상 메일 본문에는 근거 약함을 쓰지 않고 작업 기록에만 남긴다.
    assert "근거 약함" not in briefing.value["payload"]["text"] and weak_items(flow) == [STILL_SHORTFALL]


def test_move_sentence_still_weak_after_two_searches_keeps_the_weak_note_and_is_not_searched_a_third_time():
    first = one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)
    second = one_cause("한 번 더 찾았지만 여전히 약합니다.", ["r2"], SECOND_SHORTFALL)
    flow = Flow([MOVE_ONE_WEAK, first, second], [queries("검색어 하나"), queries("검색어 둘")])
    flow.to_report()
    flow.complete_research(research_id=rid(2, 1))
    flow.answer_research([R1], research_id=rid(2, 1))
    flow.complete_research(research_id=rid(2, 2))
    flow.answer_research([article("r2", T_ARTICLE)], research_id=rid(2, 2))

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "근거 약함" not in text and weak_items(flow) == [SECOND_SHORTFALL]
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
    flow.answer_research_jobs({rid(1): [R1], rid(2): [article("r3", T_ARTICLE)]})

    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "근거 약함" not in text and weak_items(flow) == ["세 번째 부족한 점입니다."]
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)


def test_the_request_limit_counts_second_searches_too():
    settings = CoreSettings(max_research_requests=1)
    flow = Flow([MOVE_ONE_WEAK, one_cause("다시 찾았지만 아직 약합니다.", ["r1"], STILL_SHORTFALL)],
                [queries("검색어 하나")], settings)
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.answer_research([R1], research_id=rid(2))

    assert len(sentence_requests(flow)) == 1 and len(flow.query_llm.calls) == 1
    (briefing,) = flow.briefings()
    assert "근거 약함" not in briefing.value["payload"]["text"] and weak_items(flow) == [STILL_SHORTFALL]
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
    flow.answer_research([R1], research_id=rid(1))  # 첫 문장은 제때 끝난다
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
    assert "첫 문장을 다시 썼습니다." in text and "근거 약함" not in text and weak_items(flow) == [SECOND_SHORTFALL]
    (limit,) = flow.records.of(ResearchReason.TIME_LIMIT)
    assert (limit.sentence_no, limit.stage) == (3, ResearchStep.LIMIT)
    (result,) = flow.records.results()
    assert (result.weak_count, result.research_count) == (1, 2)
    # 늦게 온 답은 이미 낸 작업을 다시 움직이지 않는다.
    flow.complete_research(research_id=rid(3))
    late = articles(1, [article("r3", T_ARTICLE)], job_id=rid(3))
    assert flow.research.api.reply(late, flow.clock.now) is False
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
    flow.answer_research_jobs({rid(1): [R1], rid(3): [R1, other]})

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
    flow.answer_research_jobs({rid(1): [R1], rid(3): [R1]})

    assert len(flow.report_llm.calls) == 2
    (record,) = flow.records.of(ResearchReason.ALL_USED_BY_OTHERS)
    assert record.sentence_no == 3
    (briefing,) = flow.briefings()
    assert "근거 약함" not in briefing.value["payload"]["text"] and weak_items(flow) == [SECOND_SHORTFALL]


def test_move_sentence_rewrite_citing_a_ref_outside_the_list_is_rejected_and_the_old_sentence_stays():
    unknown = one_cause("지어낸 근거가 영향을 줬을 수 있습니다.", ["r9"])
    flow = Flow([MOVE_ONE_WEAK, unknown, unknown, unknown], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.answer_research([R1], research_id=rid(2))

    assert len(flow.report_llm.calls) == 4
    (briefing,) = flow.briefings()
    text = briefing.value["payload"]["text"]
    assert "지어낸 근거" not in text and WEAK_TEXT in text and "근거 약함" not in text
    assert weak_items(flow) == [SHORTFALL]
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
    flow.answer_research([R1], research_id=rid(2))

    rejected = [e for e in flow.records.of_type(EntryType.LLM_REJECTED) if e.sentence_no == 2]
    assert [e.data["rejection"] for e in rejected] == ["rewrite must return exactly one cause"] * 2
    (briefing,) = flow.briefings()
    assert "다시 찾은 기사가 영향을 줬을 수 있습니다." in briefing.value["payload"]["text"]


def test_move_research_articles_outside_the_time_range_are_dropped_again_and_never_used_to_rewrite():
    late = article("late", datetime(2026, 10, 20, 16, 30, tzinfo=KST))
    flow = Flow([MOVE_ONE_WEAK], [queries(*QUERIES)])
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.answer_research([late], research_id=rid(2))

    assert len(flow.report_llm.calls) == 1
    (record,) = flow.records.of(ResearchReason.NO_USABLE_ARTICLES)
    assert record.sentence_no == 2 and record.data["excluded"] == 1
    (briefing,) = flow.briefings()
    assert "근거 약함" not in briefing.value["payload"]["text"] and weak_items(flow) == [SHORTFALL]
    received = flow.records.of(ResearchReason.ARTICLES_RECEIVED)
    assert received[0].data["dropped"][0]["reason"] == "outside_range"


def test_move_research_that_never_answers_publishes_the_first_report_with_its_weak_mark():
    flow = Flow([MOVE_ONE_WEAK], [queries(*QUERIES)])
    flow.to_report()

    flow.clock.now += S.research_wait_limit
    flow.sentences.on_tick(flow.clock.now)

    (briefing,) = flow.briefings()
    assert "근거 약함" not in briefing.value["payload"]["text"] and weak_items(flow) == [SHORTFALL]
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
    flow.answer_research([R1], research_id=rid(0))
    (briefing,) = flow.briefings()
    assert "다시 찾은 기사가 영향을 줬을 수 있습니다." in briefing.value["payload"]["text"]
