"""LLM 호출의 job_step 기록: 호출마다 한 줄, 그 호출을 부른 작업(job_run)과 추적 ID로 남는다. 실제 LLM은 부르지 않는다."""
import copy
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.job_step import STEP_LLM, StepRecordingLlm, StepResult, current_job, job_scope
from briefing.llm import LlmCallError, LlmReply
from briefing.trace import correlation_context
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T = datetime(2026, 10, 20, 16, 0, 31, tzinfo=KST)


class Clock:
    def __init__(self):
        self.now = T

    def __call__(self):
        current, self.now = self.now, self.now + timedelta(seconds=2)
        return current


class Client:
    model = "test-model"

    def __init__(self, *answers):
        self.answers = list(answers)

    def complete(self, system, user, output_schema, addendum=""):
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return LlmReply(answer, None)


def test_each_llm_call_inside_a_job_is_one_step_with_its_trace_id_and_times():
    steps = FakeJobLog()
    llm = StepRecordingLlm(Client("a", "b"), steps, Clock())

    with job_scope("job-1"), correlation_context("corr-1"):
        assert llm.complete("s", "u", {}).text == "a"
        assert llm.complete("s", "u", {}).text == "b"

    assert [(s.job_id, s.step, s.target, s.result, s.attempt, s.correlation_id) for s in steps.steps] == [
        ("job-1", STEP_LLM, "llm/test-model", StepResult.SUCCEEDED, 1, "corr-1"),
        ("job-1", STEP_LLM, "llm/test-model", StepResult.SUCCEEDED, 2, "corr-1")]
    first = steps.steps[0]
    assert (first.started_at, first.finished_at) == (T, T + timedelta(seconds=2))


def test_a_failed_llm_call_is_recorded_and_the_error_still_reaches_the_caller():
    steps = FakeJobLog()
    llm = StepRecordingLlm(Client(LlmCallError("LLM call failed: APIError")), steps, Clock())

    with job_scope("job-1"), pytest.raises(LlmCallError):
        llm.complete("s", "u", {})

    (step,) = steps.steps
    assert (step.result, step.failure_kind) == (StepResult.FAILED, "LlmCallError")


def test_a_call_outside_a_job_is_not_recorded_but_warned(caplog):
    steps = FakeJobLog()
    llm = StepRecordingLlm(Client("a"), steps, Clock())

    with caplog.at_level(logging.WARNING, logger="briefing.job_step"):
        assert llm.complete("s", "u", {}).text == "a"

    assert steps.steps == [] and "outside a job scope" in caplog.text


def test_the_job_scope_is_restored_after_it_ends():
    with job_scope("outer"):
        with job_scope("inner"):
            assert current_job() == "inner"
        assert current_job() == "outer"
    assert current_job() is None


def test_the_wrapper_does_not_show_the_key():
    from briefing.llm import AnthropicClient
    assert "sk-" not in repr(StepRecordingLlm(Client(), FakeJobLog(), Clock()))
    assert "StepRecordingLlm" in repr(StepRecordingLlm(Client(), FakeJobLog(), Clock()))
    assert AnthropicClient  # 실제 클라이언트도 같은 감싸기를 쓴다(main.build_runner)


# ---- 급변동·데일리 흐름에서 LLM 호출이 그 작업의 job_step으로 남는다 ----

def recorded(fake, steps):
    """가짜 LLM의 호출이 StepRecordingLlm을 거치게 한다. 응답 목록과 호출 기록은 원래 가짜와 함께 쓴다."""
    inner = copy.copy(fake)
    fake.complete = StepRecordingLlm(inner, steps, Clock()).complete
    return fake


def test_a_move_report_and_its_research_rewrite_leave_llm_steps_under_the_move_job():
    from test_research import JOB_ID, R1, Flow, queries
    from test_weak_research import MOVE_ONE_WEAK, one_cause, rid
    flow = Flow([MOVE_ONE_WEAK, one_cause("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"])], [queries("검색어")])
    recorded(flow.report_llm, flow.records)
    recorded(flow.query_llm, flow.records)

    flow.to_report()
    flow.complete_research(research_id=rid(2))
    flow.answer_research([R1], research_id=rid(2))

    assert len(flow.briefings()) == 1
    # 첫 보고서, 검색어, 다시 쓰기 세 번의 LLM 호출이 모두 그 변동 작업 아래 시도 순번과 함께 남는다.
    assert [(s.job_id, s.step) for s in flow.records.steps] == [(JOB_ID, STEP_LLM)] * 3
    assert sorted(s.attempt for s in flow.records.steps) == [1, 1, 2]  # 보고서 LLM 두 번, 검색어 LLM 한 번


def test_a_daily_edition_leaves_its_llm_step_under_the_edition_job():
    from test_daily_jobs import FakeLlm, Requests, TICK_ID, daily_response, good_output, tick
    from briefing.daily_jobs import DailyCore
    from briefing.daily_report import DailyLlmWriter
    from fake_news import answer_news
    from test_daily_jobs import MORNING_AT, Details

    requests, details = Requests(), Details()
    llm = recorded(FakeLlm(good_output("B1")), details)
    core = DailyCore(requests, requests.api, DailyLlmWriter(llm, 3), details)
    core.on_collection_ready(tick(), MORNING_AT)
    core.api.reply(daily_response(), MORNING_AT)
    answer_news(core, requests, MORNING_AT)

    assert [(s.job_id, s.step, s.result) for s in details.steps] == [(TICK_ID, STEP_LLM, StepResult.SUCCEEDED)]
