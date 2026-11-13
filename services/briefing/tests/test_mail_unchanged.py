"""같은 입력이면 메일 내용이 바뀌지 않았는지 본다: Kafka 요청·응답을 내부 조회 API 호출로 바꾸기 전 코드로 뽑아 둔 기준과 비교한다.

기준 파일 golden/mail_before_direct_calls.json은 바꾸기 전 코드(Kafka 요청·응답, 기사 안정 확인 라운드)로 같은 가짜 응답과
가짜 LLM을 넣어 만든 결과다. 메일 이벤트(본문, 근거와 번호, 자료 없음 목록, HTML)와 LLM이 받은 입력 전체가 같아야 한다.
그림(차트)은 그리는 환경에 따라 바이트가 달라질 수 있어 길이만 비교한다. 기준을 다시 만들면 안 되며, 다르면 원인을 고친다.
"""
import json
from pathlib import Path

import pytest

import test_daily_mail as dm
import test_research as rs
import test_scenarios as sc
import test_weak_research as wr
from briefing.config import DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_report import DailyLlmWriter
from briefing.jobs import BriefingCore
from briefing.report import LlmWriter
from fake_members import FakeMembers
from fake_news import answer_news, article as news_article
from test_daily_jobs import (
    MORNING_AT,
    MORNING_JOB,
    POST_CLOSE_AT,
    POST_CLOSE_BACKGROUND,
    POST_CLOSE_JOB,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    good_output,
    post_close_output,
    tick,
)

GOLDEN = json.loads((Path(__file__).parent / "golden" / "mail_before_direct_calls.json").read_text(encoding="utf-8"))
ARTICLES = {"TEST01": [news_article("n1", "TEST 하나 관련 소식", "2026-10-20T10:00:00+09:00")]}


def strip_images(value):
    value = json.loads(json.dumps(value))
    for image in value["payload"].get("images", []):
        image["data"] = f"<{len(image['data'])} chars>"
    return value


def move_us():
    """미국 종목을 담은 ETF의 변동 1건: 시세 이력·배경 자료·공시·기사를 모두 받아 LLM이 모든 종류의 근거를 인용한다."""
    rec = sc.Recorder()
    core = BriefingCore(rec, rec.api, rec, job_log=rec.log)
    sc.run_us(core)
    llm = sc.FakeLlm(sc.ALL_KINDS)
    stage, published, _ = sc.report_stage(LlmWriter(llm))
    stage.on_ready(rec.readied[0])
    (event,) = published.sent
    return {"event": event.value, "llmInputs": [u for _, u in llm.calls]}


def move_weak_research():
    """근거 약한 문장 하나를 재검색해 그 문장만 다시 쓴다."""
    rewritten = wr.one_cause("다시 찾은 기사가 영향을 줬을 수 있습니다.", ["r1"])
    flow = rs.Flow([wr.MOVE_ONE_WEAK, rewritten], [rs.queries(*rs.QUERIES)])
    flow.to_report()
    flow.complete_research(research_id=wr.rid(2))
    flow.answer_research([rs.R1, rs.A1], research_id=wr.rid(2))
    (briefing,) = flow.briefings()
    return {"event": briefing.value, "llmInputs": [c[1] for c in flow.report_llm.calls],
            "queryInputs": [c[1] for c in flow.query_llm.calls]}


def daily(edition):
    requests, details = Requests(), Details()
    settings = DailySettings()
    output = post_close_output() if edition == "post-close" else good_output("B1", symbols=("TEST04",))
    llm = FakeLlm(output)
    core = DailyCore(requests, requests.api, DailyLlmWriter(llm, settings.llm_max_attempts), details, settings,
                     members=FakeMembers(dm.ONE_MEMBER))
    job, at = (POST_CLOSE_JOB, POST_CLOSE_AT) if edition == "post-close" else (MORNING_JOB, MORNING_AT)
    core.on_collection_ready(tick(job, at), at)
    background = POST_CLOSE_BACKGROUND if edition == "post-close" else None
    core.api.reply(daily_response(etfs=dm.etfs(), background=background), at)
    answer_news(core, requests, at, articles=ARTICLES)
    [event] = requests.topic("briefing.daily.generated")
    return {"event": strip_images(event.value), "llmInputs": [c[1] for c in llm.calls]}


SCENARIOS = {"moveUs": move_us, "moveWeakResearch": move_weak_research,
             "dailyPostClose": lambda: daily("post-close"), "dailyMorning": lambda: daily("morning")}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_the_same_input_makes_the_same_mail_and_the_same_llm_input_as_before(name):
    now = json.loads(json.dumps(SCENARIOS[name]()))
    before = GOLDEN[name]
    # LLM이 받은 입력(근거 시간 범위로 거른 자료와 근거 번호 목록)이 같다.
    assert now["llmInputs"] == before["llmInputs"]
    assert now.get("queryInputs") == before.get("queryInputs")
    # 메일 이벤트(본문, 인용한 근거 번호와 근거 목록, 자료 없음 목록, HTML, eventId)가 같다. 발행 시각(producedAt)만은
    # 다를 수 있다: 기사 안정 확인 라운드(10초씩)가 없어져 같은 가짜 시계에서 더 일찍 발행된다. 늦어지지는 않는다.
    assert {**now["event"], "producedAt": None} == {**before["event"], "producedAt": None}
    assert now["event"]["producedAt"] <= before["event"]["producedAt"]


def test_the_golden_file_covers_every_kind_of_mail():
    assert set(GOLDEN) == set(SCENARIOS)
    assert {GOLDEN[n]["event"]["eventType"] for n in GOLDEN} == {"briefing.generated", "briefing.daily.generated"}
