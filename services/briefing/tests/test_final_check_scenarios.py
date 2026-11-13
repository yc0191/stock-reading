"""발행 직전 마지막 검사 시나리오: 가짜 LLM·가짜 응답으로 급변동 1건과 데일리 판(아침·마감 후)을 처음부터 발행까지 돌린다.

경우(급변동·데일리 각각): 다시 쓴 문장 없음·문제 없음, 다시 쓴 문장 있음·문제 없음, 문제 있음·고침 성공,
고친 뒤에도 문제 남음, 검토 호출 실패. 검토·고침 호출은 보고서 LLM과 따로 둔 가짜 LLM이 받는다.
실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다.
"""
import json
from datetime import timedelta

import pytest

from briefing.daily_final_check import FIX_ADDENDUM as DAILY_FIX_ADDENDUM
from briefing.daily_final_check import DailyFinalCheck
from briefing.daily_prompts import SYSTEM_PROMPT as MORNING_PROMPT
from briefing.daily_prompts import SYSTEM_PROMPT_POST_CLOSE
from briefing.final_check import CHOSE_AFTER, CHOSE_BEFORE, REVIEW_SYSTEM_PROMPT, FinalCheck
from briefing.job_log import EntryType
from briefing.jobs import BriefingCore
from briefing.llm import LlmCallError
from briefing.move_final_check import FIX_ADDENDUM as MOVE_FIX_ADDENDUM
from briefing.move_final_check import MoveFinalCheck
from briefing.report import SYSTEM_PROMPT as MOVE_PROMPT
from briefing.report import LlmWriter
from briefing.sentence_research import ResearchReportStage
from contracts import validate
from test_research import A1, QUERIES, R1, T, Flow, FakeLlm, article, queries
from test_weak_research import (
    SHORTFALL,
    STRONG_TEXT,
    WEAK_TEXT,
    DailyFlow,
    daily_output,
    in_range,
    move_output,
    one_cause,
    point,
    rid,
)

FAILED = LlmCallError("LLM call failed: APIConnectionError")
REWRITTEN = "다시 찾은 기사가 영향을 줬을 수 있습니다."
FIXED = "고친 문장이 다른 문장과 맞게 영향을 설명합니다."


def review(*nos):
    return json.dumps({"problems": [{"sentenceNo": n, "reason": f"{n}번 문장이 다른 문장과 어긋납니다."} for n in nos]},
                      ensure_ascii=False)


def final_entries(records, reason):
    return [e for e in records.of_type(EntryType.FINAL_CHECK) if e.reason == reason]


def chosen(records):
    (entry,) = final_entries(records, "body-chosen")
    return entry.data


# ---- 급변동 ----

def move_flow(report_outputs, query_outputs, check_outputs):
    """Flow에 마지막 검사를 붙인다. 검토·고침은 check_llm이 받는다."""
    flow = Flow(report_outputs, query_outputs)
    flow.check_llm = FakeLlm(*check_outputs)
    final = FinalCheck(MoveFinalCheck(LlmWriter(flow.report_llm)), flow.check_llm, 3, flow.records)
    flow.stage = ResearchReportStage(flow.stage.stage, flow.sentences, final)
    flow.core = BriefingCore(flow.requests, flow.stage, flow.settings, flow.records)
    return flow


def researched_move(check_outputs):
    """둘째 원인이 약해 재검색으로 다시 쓰고 발행하기까지 돌린다."""
    flow = move_flow([move_output((STRONG_TEXT, ["a1"], None), (WEAK_TEXT, ["a1"], SHORTFALL)),
                      one_cause(REWRITTEN, ["r1"])], [queries(*QUERIES)], check_outputs)
    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.stable_research([R1], research_id=rid(2))
    return flow


def published_text(flow):
    (briefing,) = flow.briefings()
    assert validate("briefing.generated.v1.schema.json", briefing.value) == []
    return briefing.value["payload"]["text"]


def test_move_without_rewrite_or_problem_is_code_checked_and_published_without_a_review():
    flow = move_flow([move_output((STRONG_TEXT, ["a1"], None))], [], [])
    flow.to_report()

    assert flow.check_llm.calls == []
    assert STRONG_TEXT in published_text(flow)
    (checked,) = final_entries(flow.records, "code-checked")
    assert checked.data["problems"] == [] and checked.data["passed"] is True
    assert chosen(flow.records)["chosen"] == CHOSE_BEFORE


def test_move_with_only_a_weak_mark_left_records_it_and_calls_no_review():
    # 검색어가 없어 재검색을 못 해 근거 약함 표시가 남는다. 고칠 수 없는 문제라 검토를 부르지 않는다.
    flow = move_flow([move_output((STRONG_TEXT, ["a1"], SHORTFALL))], [queries()], [])
    flow.to_report()

    assert flow.check_llm.calls == [] and "근거 약함" in published_text(flow)
    (checked,) = final_entries(flow.records, "code-checked")
    assert [p["kind"] for p in checked.data["problems"]] == ["weak-mark-left"]


def test_move_with_a_rewritten_sentence_and_no_problem_reviews_the_whole_mail_once():
    flow = researched_move([review()])

    ((system, user, _),) = flow.check_llm.calls
    assert system == REVIEW_SYSTEM_PROMPT
    data = json.loads(user)
    assert [(s["no"], s["rewritten"]) for s in data["sentences"]] == [(1, False), (2, True)]
    assert data["firstLine"] in data["mail"] and REWRITTEN in data["mail"]
    assert data["codeCheck"] == []
    text = published_text(flow)
    assert REWRITTEN in text and STRONG_TEXT in text
    assert chosen(flow.records)["chosen"] == CHOSE_BEFORE


def test_move_with_a_problem_fixes_only_that_sentence_and_publishes_the_fixed_body():
    flow = researched_move([review(1), one_cause(FIXED, ["zz"]), one_cause(FIXED, ["a1"]), review()])

    # 고치는 호출: 처음 작성과 같은 지시문 블록 + 고치기 덧붙임. 목록 밖 근거 번호를 쓴 첫 응답은 거부한다.
    system, user, _ = flow.check_llm.calls[1]
    assert system == MOVE_PROMPT + MOVE_FIX_ADDENDUM
    data = json.loads(user)
    assert data["fix"]["sentenceNo"] == 1 and data["fix"]["problems"] == ["1번 문장이 다른 문장과 어긋납니다."]
    # 근거 자료 입력은 묶음과 근거 번호를 처음 작성과 같게 담는다.
    assert {a["ref"] for a in data["researchArticles"]} == {"r1"}
    assert len(flow.check_llm.calls) == 4
    (rejected,) = [e for e in flow.records.of_type(EntryType.LLM_REJECTED) if e.stage == "final-check"]
    assert rejected.data["rejection"] == "cause cites 1 ref(s) that were not given"

    text = published_text(flow)
    assert f"1. {FIXED}" in text and f"2. {REWRITTEN}" in text and f"1. {STRONG_TEXT}" not in text
    (fixed,) = flow.records.of_type(EntryType.SENTENCE_FIXED)
    assert fixed.sentence_no == 1 and fixed.before.startswith(STRONG_TEXT) and fixed.after.startswith(FIXED)
    # 재검색으로 고친 문장은 그대로 sentence-rewritten이다.
    (rewritten,) = flow.records.of_type(EntryType.SENTENCE_REWRITTEN)
    assert rewritten.after.startswith(REWRITTEN)
    assert chosen(flow.records)["chosen"] == CHOSE_AFTER


def test_move_with_problems_left_after_the_fix_publishes_the_body_before_the_fix():
    flow = researched_move([review(1), one_cause(FIXED, ["a1"]), review(1, 2)])

    assert len(flow.check_llm.calls) == 3
    text = published_text(flow)
    assert f"1. {STRONG_TEXT}" in text and FIXED not in text
    data = chosen(flow.records)
    assert data["chosen"] == CHOSE_BEFORE and data["problemCounts"] == {"beforeFix": 1, "afterFix": 2}


def test_move_review_failing_every_attempt_publishes_the_current_body():
    flow = researched_move([FAILED, FAILED, FAILED])

    assert len(flow.check_llm.calls) == 3
    text = published_text(flow)
    assert REWRITTEN in text and STRONG_TEXT in text
    assert len(final_entries(flow.records, "review-failed")) == 1
    assert chosen(flow.records)["why"] == "검토 호출이 끝내 실패해 지금 본문을 냄"


def test_move_two_sentences_citing_the_same_article_are_reviewed_and_fixed_without_a_rewrite():
    a2 = article("a2", T - timedelta(minutes=20))
    flow = move_flow([move_output((STRONG_TEXT, ["a1"], None), ("둘째 원인입니다.", ["a1"], None))], [],
                     [review(), one_cause(STRONG_TEXT, ["a1"]), one_cause("둘째 원인을 고쳤습니다.", ["a2"]), review()])
    flow.to_report([A1, a2])

    first = json.loads(flow.check_llm.calls[0][1])
    assert {p["kind"] for p in first["codeCheck"]} == {"article-cited-by-two-sentences"}
    text = published_text(flow)
    assert "둘째 원인을 고쳤습니다." in text
    assert chosen(flow.records)["problemCounts"] == {"beforeFix": 2, "afterFix": 0}


# ---- 데일리 판 ----

@pytest.fixture(params=["morning", "post-close"])
def edition(request):
    return request.param


STRONG_DAILY = "지수가 오른 가운데 자료가 있습니다."
WEAK_DAILY = "업종 소식이 영향을 줬을 수 있습니다."


def daily_flow(edition, check_outputs, weak=True):
    flow = DailyFlow(edition, [], [])
    flow.check_llm = FakeLlm(*check_outputs)
    flow.core.final = FinalCheck(DailyFinalCheck(flow.core.writer), flow.check_llm, 3, flow.records)
    if not weak:
        flow.report_llm.outputs = [daily_output(point(STRONG_DAILY, flow.ref))]
        flow.to_first_report()
        return flow
    flow.report_llm.outputs = [daily_output(point(STRONG_DAILY, flow.ref), point(WEAK_DAILY, flow.ref, SHORTFALL)),
                               daily_output(point(REWRITTEN, "r1"))]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report()
    assert flow.complete(flow.rid(2)) is True
    flow.stable([in_range(flow)], flow.rid(2))
    return flow


def daily_text(flow):
    (event,) = flow.published()
    return event.value["payload"]["text"]


def strong_no(flow):
    """처음 검토 입력에서 첫 문장(약하지 않은 문장)의 메일 문장 번호"""
    sentences = json.loads(flow.check_llm.calls[0][1])["sentences"]
    return next(s["no"] for s in sentences if s["text"] == STRONG_DAILY)


def first_check_outputs(flow_edition):
    """마감 후 판은 요약 2줄이 1·2번이라 첫 문장이 3번, 아침 판은 1번이다."""
    return 3 if flow_edition == "post-close" else 1


def fixed_to(edition):
    """고친 문장 응답. 다시 쓴 문장이 인용한 재검색 기사(r1)를 함께 쓰면 겹침이 되므로 첫 요약의 근거 번호를 쓴다."""
    return json.dumps({"statement": FIXED, "refs": ["B1" if edition == "morning" else "E1"]}, ensure_ascii=False)


def test_daily_fix_citing_the_article_another_sentence_uses_keeps_the_body_before_the_fix(edition):
    no = first_check_outputs(edition)
    flow = daily_flow(edition, [review(no), json.dumps({"statement": FIXED, "refs": ["r1"]}), review()])

    # 고친 본문은 검토 문제는 없지만 두 문장이 같은 기사(r1)를 이유로 써서 문제 수가 줄지 않는다.
    assert FIXED not in daily_text(flow)
    data = chosen(flow.records)
    assert data["chosen"] == CHOSE_BEFORE and data["problemCounts"] == {"beforeFix": 1, "afterFix": 2}


def test_daily_without_rewrite_or_problem_is_code_checked_and_published_without_a_review(edition):
    flow = daily_flow(edition, [], weak=False)

    assert flow.check_llm.calls == []
    assert STRONG_DAILY in daily_text(flow)
    assert len(final_entries(flow.records, "code-checked")) == 1
    assert chosen(flow.records)["chosen"] == CHOSE_BEFORE


def test_daily_with_a_rewritten_sentence_and_no_problem_reviews_the_whole_mail_once(edition):
    flow = daily_flow(edition, [review()])

    ((system, user, _),) = flow.check_llm.calls
    assert system == REVIEW_SYSTEM_PROMPT
    data = json.loads(user)
    rewritten = [s for s in data["sentences"] if s["rewritten"]]
    assert [s["text"] for s in rewritten] == [REWRITTEN]
    if edition == "post-close":
        # 마감 후 판은 코드가 쓴 결론 줄을 첫 줄로, Claude가 쓴 요약 2줄을 summary 문장으로 준다.
        assert data["firstLine"].startswith("결론: ")
        assert [s["kind"] for s in data["sentences"][:2]] == ["summary", "summary"]
        assert data["sentences"][2]["kind"] == "etf-reason" and data["sentences"][2]["about"]
    assert REWRITTEN in daily_text(flow)
    assert chosen(flow.records)["chosen"] == CHOSE_BEFORE


def test_daily_with_a_problem_fixes_only_that_sentence_and_publishes_the_fixed_body(edition):
    no = first_check_outputs(edition)
    fix_bad = json.dumps({"statement": FIXED, "refs": ["zz"]}, ensure_ascii=False)
    flow = daily_flow(edition, [review(no), fix_bad, fixed_to(edition), review()])
    assert strong_no(flow) == no

    system, user, _ = flow.check_llm.calls[1]
    assert system == (MORNING_PROMPT if edition == "morning" else SYSTEM_PROMPT_POST_CLOSE) + DAILY_FIX_ADDENDUM
    data = json.loads(user)
    assert data["fix"]["sentence"] == STRONG_DAILY and "items" in data
    assert len(flow.check_llm.calls) == 4
    text = daily_text(flow)
    assert FIXED in text and REWRITTEN in text and STRONG_DAILY not in text
    (fixed,) = flow.records.of_type(EntryType.SENTENCE_FIXED)
    assert (fixed.sentence_no, fixed.before, fixed.after) == (no, STRONG_DAILY, FIXED)
    assert chosen(flow.records)["chosen"] == CHOSE_AFTER


def test_daily_with_problems_left_after_the_fix_publishes_the_body_before_the_fix(edition):
    no = first_check_outputs(edition)
    flow = daily_flow(edition, [review(no), fixed_to(edition), review(no, no + 1)])

    assert len(flow.check_llm.calls) == 3
    text = daily_text(flow)
    assert STRONG_DAILY in text and FIXED not in text
    assert chosen(flow.records)["chosen"] == CHOSE_BEFORE


def test_daily_review_failing_every_attempt_publishes_the_current_body(edition):
    flow = daily_flow(edition, [FAILED, FAILED, FAILED])

    assert len(flow.check_llm.calls) == 3
    assert REWRITTEN in daily_text(flow)
    assert len(final_entries(flow.records, "review-failed")) == 1
    assert chosen(flow.records)["why"] == "검토 호출이 끝내 실패해 지금 본문을 냄"
