"""발행 직전 마지막 검사의 단위 시험: 코드 검사 4가지, 검토 응답 검증, 검토·고침·본문 고르기 규칙.

본문은 가짜 어댑터(문장 목록만 있는 draft)로 다룬다. 실제 LLM·Kafka·DB는 쓰지 않는다.
"""
import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

import pytest

from fake_anthropic import as_reply
from fake_job_log import FakeJobLog
from briefing.final_check import (
    CHOSE_AFTER,
    CHOSE_BEFORE,
    PROBLEM_OUT_OF_RANGE,
    PROBLEM_REVIEW,
    PROBLEM_SHARED_ARTICLE,
    PROBLEM_UNKNOWN_REF,
    PROBLEM_WEAK_LEFT,
    REVIEW_SYSTEM_PROMPT,
    FinalCheck,
    FixCall,
    InvalidReview,
    MailSentence,
    Problem,
    Source,
    code_check,
    parse_review,
    passes,
    quality_count,
)
from briefing.job_log import EntryType
from briefing.llm import LlmCallError
from briefing.llm_output import InvalidLlmOutput

KST = timezone(timedelta(hours=9))
SINCE = datetime(2026, 10, 20, 9, 0, tzinfo=KST)
UNTIL = datetime(2026, 10, 20, 16, 0, tzinfo=KST)
SOURCES = {"a1": Source(True, SINCE), "a2": Source(True, UNTIL), "a3": Source(True, UNTIL + timedelta(seconds=1)),
           "B1": Source(False, SINCE - timedelta(days=1))}


def sentence(no, refs, **changes):
    return MailSentence(no, "summary", f"문장 {no}입니다.", tuple(refs), **changes)


# ---- 코드 검사 ----

def test_code_check_finds_each_of_the_four_problems():
    problems = code_check((sentence(1, ["a1", "zz"]), sentence(2, ["a1"], weak=True), sentence(3, ["a3"])),
                          SOURCES, SINCE, UNTIL)
    kinds = [(p.sentence_no, p.kind) for p in problems]
    assert (1, PROBLEM_UNKNOWN_REF) in kinds
    # 같은 기사를 두 문장이 이유로 쓰면 두 문장 모두에 남는다.
    assert (1, PROBLEM_SHARED_ARTICLE) in kinds and (2, PROBLEM_SHARED_ARTICLE) in kinds
    assert (2, PROBLEM_WEAK_LEFT) in kinds
    assert (3, PROBLEM_OUT_OF_RANGE) in kinds
    assert len(problems) == 5


def test_code_check_range_includes_both_ends_and_ignores_non_articles():
    # a1은 범위 시작, a2는 범위 끝에 나온 기사다. 배경 자료(B1)는 시각과 겹침을 보지 않는다.
    assert code_check((sentence(1, ["a1", "B1"]), sentence(2, ["a2", "B1"])), SOURCES, SINCE, UNTIL) == ()


def test_code_check_does_not_count_one_sentence_citing_an_article_twice():
    assert code_check((sentence(1, ["a1", "a1"]),), SOURCES, SINCE, UNTIL) == ()


def test_only_rule_problems_fail_the_code_check_and_weak_marks_are_not_counted():
    weak = Problem(1, PROBLEM_WEAK_LEFT, "")
    shared = Problem(1, PROBLEM_SHARED_ARTICLE, "")
    assert passes((weak, shared)) is True
    assert passes((Problem(1, PROBLEM_UNKNOWN_REF, ""),)) is False
    assert passes((Problem(1, PROBLEM_OUT_OF_RANGE, ""),)) is False
    assert quality_count((weak, shared), (Problem(2, PROBLEM_REVIEW, ""),)) == 2


# ---- 검토 응답 ----

def test_review_reads_numbers_and_reasons_and_merges_the_same_sentence():
    text = json.dumps({"problems": [{"sentenceNo": 2, "reason": "1번과 모순입니다."},
                                    {"sentenceNo": 2, "reason": "요약과 다릅니다."}]}, ensure_ascii=False)
    assert parse_review(text, frozenset({1, 2})) == (Problem(2, PROBLEM_REVIEW, "1번과 모순입니다. / 요약과 다릅니다."),)
    assert parse_review(json.dumps({"problems": []}), frozenset({1})) == ()


@pytest.mark.parametrize("text", [
    "not json",
    json.dumps({"issues": []}),
    json.dumps({"problems": [{"sentenceNo": 3, "reason": "목록 밖 번호"}]}),
    json.dumps({"problems": [{"sentenceNo": True, "reason": "참거짓은 번호가 아님"}]}),
    json.dumps({"problems": [{"sentenceNo": 1, "reason": " "}]}),
])
def test_review_outside_the_fixable_list_or_malformed_is_rejected(text):
    with pytest.raises(InvalidReview):
        parse_review(text, frozenset({1, 2}))


def test_review_prompt_asks_for_contradictions_overlaps_and_summary_mismatch():
    for words in ("모순", "같은 내용 겹침", "첫 요약 줄과 본문 불일치", "codeCheck", "fixable이 true인 문장 번호만"):
        assert words in REVIEW_SYSTEM_PROMPT


# ---- 흐름: 가짜 어댑터 ----

@dataclass(frozen=True)
class Draft:
    sentences: tuple[MailSentence, ...]


class Adapter:
    """draft는 문장 목록. 고친 응답은 {"text": ..., "refs": [...]}이고 refs가 SOURCES 밖이면 거부한다."""

    def job_id(self, draft):
        return "job-1"

    def sentences(self, draft):
        return draft.sentences

    def sources(self, draft):
        return SOURCES

    def window(self, draft):
        return SINCE, UNTIL

    def mail(self, draft):
        return "첫 줄", "\n".join(s.text for s in draft.sentences)

    def fix_call(self, draft, s):
        return FixCall("지시문", "고치기 덧붙임", {"type": "object"}, {"evidence": ["a1", "a2"]})

    def parse_fix(self, draft, s, text):
        data = json.loads(text)
        if any(ref not in SOURCES for ref in data["refs"]):
            raise InvalidLlmOutput("fix cites a ref that was not given")
        return data

    def apply(self, draft, fixed):
        return Draft(tuple(replace(s, text=fixed[s.no]["text"], refs=tuple(fixed[s.no]["refs"]))
                           if s.no in fixed else s for s in draft.sentences))


class Llm:
    def __init__(self, *outputs):
        self.outputs, self.calls = list(outputs), []

    @as_reply
    def complete(self, system, user, output_schema, addendum=""):
        self.calls.append((system, addendum, json.loads(user)))
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return output


def review(*nos):
    return json.dumps({"problems": [{"sentenceNo": n, "reason": f"{n}번 문장이 다른 문장과 어긋납니다."} for n in nos]},
                      ensure_ascii=False)


def fix(text, *refs):
    return json.dumps({"text": text, "refs": list(refs)}, ensure_ascii=False)


NOW = UNTIL
FAILED = LlmCallError("LLM call failed: APIConnectionError")


def run(draft, *outputs):
    llm, log = Llm(*outputs), FakeJobLog()
    result = FinalCheck(Adapter(), llm, 3, log).run(draft, NOW)
    return result, llm, log


def chosen(log):
    (entry,) = [e for e in log.of_type(EntryType.FINAL_CHECK) if e.reason == "body-chosen"]
    return entry.data


PLAIN = Draft((sentence(1, ["a1"]), sentence(2, ["a2"])))
REWRITTEN = Draft((sentence(1, ["a1"]), sentence(2, ["a2"], rewritten=True)))


def test_without_rewrite_or_fixable_problem_only_the_code_check_runs():
    draft = Draft((sentence(1, ["a1"], weak=True),))
    result, llm, log = run(draft)
    assert result is draft and llm.calls == []
    (checked,) = [e for e in log.of_type(EntryType.FINAL_CHECK) if e.reason == "code-checked"]
    assert [p["kind"] for p in checked.data["problems"]] == [PROBLEM_WEAK_LEFT]
    assert chosen(log)["chosen"] == CHOSE_BEFORE


def test_a_rewritten_sentence_calls_the_review_with_the_whole_mail_and_the_code_check():
    result, llm, log = run(REWRITTEN, review())
    assert result is REWRITTEN
    ((system, addendum, user),) = llm.calls
    assert (system, addendum) == (REVIEW_SYSTEM_PROMPT, "")
    assert user["mail"] == "문장 1입니다.\n문장 2입니다." and user["firstLine"] == "첫 줄"
    assert [s["rewritten"] for s in user["sentences"]] == [False, True] and user["codeCheck"] == []
    assert chosen(log)["why"] == "검토에서 문제를 찾지 못함"


def test_a_fixable_code_problem_calls_the_review_even_without_a_rewrite():
    draft = Draft((sentence(1, ["a1"]), sentence(2, ["a1"])))
    result, llm, log = run(draft, review(), fix("문장 1을 고쳤습니다.", "a1"), fix("문장 2를 고쳤습니다.", "a2"), review())
    # 검토가 문제를 내지 않아도 코드 검사가 찾은 두 문장을 고친다. 고친 뒤 겹침이 없어져 고친 본문을 낸다.
    assert [c[2]["codeCheck"][0]["kind"] for c in llm.calls[:1]] == [PROBLEM_SHARED_ARTICLE]
    assert [s.refs for s in result.sentences] == [("a1",), ("a2",)]
    assert chosen(log)["chosen"] == CHOSE_AFTER and chosen(log)["problemCounts"] == {"beforeFix": 2, "afterFix": 0}


def test_fix_changes_only_the_named_sentences_and_records_before_and_after():
    result, llm, log = run(REWRITTEN, review(2), fix("bad", "zz"), fix("문장 2를 고쳤습니다.", "a2"), review())
    assert [s.text for s in result.sentences] == ["문장 1입니다.", "문장 2를 고쳤습니다."]
    # 고치는 호출은 근거 자료 입력에 메일 문장과 고칠 문장·문제를 더한다. 목록 밖 번호는 거부되고 다시 시도한다.
    system, addendum, user = llm.calls[1]
    assert (system, addendum) == ("지시문", "고치기 덧붙임") and user["evidence"] == ["a1", "a2"]
    assert user["fix"]["sentenceNo"] == 2 and user["fix"]["problems"] == ["2번 문장이 다른 문장과 어긋납니다."]
    assert len(user["mail"]["sentences"]) == 2
    (rejected,) = log.of_type(EntryType.LLM_REJECTED)
    assert (rejected.stage, rejected.sentence_no, rejected.attempts) == ("final-check", 2, 1)
    (fixed,) = log.of_type(EntryType.SENTENCE_FIXED)
    assert (fixed.sentence_no, fixed.before, fixed.after, fixed.attempts) == (2, "문장 2입니다.", "문장 2를 고쳤습니다.", 2)
    assert log.of_type(EntryType.SENTENCE_REWRITTEN) == []
    assert chosen(log)["chosen"] == CHOSE_AFTER and len(llm.calls) == 4


@pytest.mark.parametrize("first, second, expected", [
    ((1, 2), (1,), CHOSE_AFTER),   # 고친 본문의 문제가 더 적다
    ((1,), (1,), CHOSE_BEFORE),    # 같으면 고치기 전 본문
    ((1,), (1, 2), CHOSE_BEFORE),  # 고친 본문의 문제가 더 많다
])
def test_problems_left_after_the_fix_choose_the_body_with_fewer_problems(first, second, expected):
    fixes = [fix(f"문장 {n}을 고쳤습니다.", "a1" if n == 1 else "a2") for n in first]
    result, llm, log = run(REWRITTEN, review(*first), *fixes, review(*second))
    # 고침은 한 번, 검토는 처음과 다시 두 번뿐이다.
    assert len(llm.calls) == 2 + len(first)
    assert chosen(log)["chosen"] == expected
    assert (result is REWRITTEN) == (expected == CHOSE_BEFORE)


def test_a_fixed_body_that_fails_the_code_check_is_not_chosen():
    class LooseAdapter(Adapter):
        def parse_fix(self, draft, s, text):
            return json.loads(text)

    llm, log = Llm(review(2), fix("범위 밖 기사를 씁니다.", "a3"), review()), FakeJobLog()
    result = FinalCheck(LooseAdapter(), llm, 3, log).run(REWRITTEN, NOW)
    assert result is REWRITTEN
    assert chosen(log)["why"] == "고친 본문이 코드 검사를 통과하지 못함"


def test_review_failing_every_attempt_publishes_the_current_body():
    result, llm, log = run(REWRITTEN, FAILED, FAILED, FAILED)
    assert result is REWRITTEN and len(llm.calls) == 3
    assert [e.reason for e in log.of_type(EntryType.LLM_REJECTED)] == ["llm-call-failed"] * 3
    assert "review-failed" in [e.reason for e in log.of_type(EntryType.FINAL_CHECK)]
    assert chosen(log)["why"] == "검토 호출이 끝내 실패해 지금 본문을 냄"


def test_re_review_failing_publishes_the_fixed_body_as_the_current_one():
    result, llm, log = run(REWRITTEN, review(2), fix("문장 2를 고쳤습니다.", "a2"), FAILED, FAILED, FAILED)
    assert result.sentences[1].text == "문장 2를 고쳤습니다." and chosen(log)["chosen"] == CHOSE_AFTER


def test_a_fix_failing_every_attempt_keeps_the_body_before_the_fix():
    result, llm, log = run(REWRITTEN, review(2), FAILED, FAILED, FAILED)
    assert result is REWRITTEN and len(llm.calls) == 4
    assert chosen(log)["why"] == "문제 문장을 하나도 고치지 못함"


def test_an_unfixable_sentence_is_shown_to_the_review_but_cannot_be_named():
    draft = Draft((sentence(1, ["a1"], fixable=False), sentence(2, ["a2"], rewritten=True)))
    result, llm, log = run(draft, review(1), review())
    assert result is draft and len(llm.calls) == 2
    assert llm.calls[0][2]["sentences"][0]["fixable"] is False
    (rejected,) = log.of_type(EntryType.LLM_REJECTED)
    assert rejected.data["rejection"] == "problem names a sentence that was not given as fixable"
