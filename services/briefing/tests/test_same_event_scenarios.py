"""같은 사건 기사 묶음 시나리오: 가짜 LLM·가짜 응답으로 급변동 1건과 데일리 판 1개를 처음부터 발행까지 돌린다.

처음 모은 기사 중 같은 사건 기사가 묶여 첫 보고서 입력에 들어가고, 재검색으로 찾은 같은 사건 기사가 그 묶음에 더해져
문장 다시 쓰기 입력에 들어가는지, 근거 번호는 기사마다 그대로이고 묶음 수가 작업 기록에 남는지 본다.
실제 Kafka·DB·LLM·뉴스 검색은 쓰지 않는다.
"""
import json
from dataclasses import replace
from datetime import timedelta

import pytest

from briefing.article_groups import SAME_EVENT_KEY
from briefing.daily_prompts import REWRITE_ADDENDUM, REWRITE_ADDENDUM_POST_CLOSE
from briefing.daily_prompts import SYSTEM_PROMPT as MORNING_PROMPT
from briefing.daily_prompts import SYSTEM_PROMPT_POST_CLOSE
from briefing.job_log import EntryType
from briefing.report import REWRITE_ADDENDUM as MOVE_REWRITE_ADDENDUM
from briefing.report import SYSTEM_PROMPT as MOVE_PROMPT
from briefing.report import InvalidOutput, build_llm_input, evidence_catalog, parse_output
from briefing.research import QUERY_SYSTEM_PROMPT
from briefing.sources import Article
from contracts import validate
from test_report import report_input
from test_research import QUERIES, Flow, T, queries
from test_weak_research import (
    SHORTFALL,
    DailyFlow,
    daily_output,
    move_output,
    one_cause,
    rid,
)

# 같은 사건(테스트상사 실적)을 다룬 기사 둘과 다른 사건 기사 하나, 재검색으로 찾은 같은 사건 기사와 다른 사건 기사
SAME_1 = "테스트상사 3분기 영업익 역대 최대"
SAME_2 = "테스트상사 3분기 영업익 최대 기록"
SAME_RESEARCH = "테스트상사 3분기 영업익 역대 최대 증권가 전망"
OTHER = "환율 상승 마감"
OTHER_RESEARCH = "금리 동결 결정"


def news(article_id, title, published_at):
    return {"articleId": article_id, "title": title, "summary": f"{article_id} 요약", "publisher": "news.example.com",
            "publishedAt": published_at.isoformat(), "url": f"https://news.example.com/{article_id}"}


def refs_of(entries):
    """LLM 입력의 기사 칸 목록을 근거 번호로 바꾼다. 묶음은 번호 목록(정렬)이 된다."""
    return [sorted(e["ref"] for e in x[SAME_EVENT_KEY]) if SAME_EVENT_KEY in x else x["ref"] for x in entries]


def group_in(entries):
    (group,) = [x[SAME_EVENT_KEY] for x in entries if SAME_EVENT_KEY in x]
    return {e["ref"]: e for e in group}


# ---- 급변동 ----

STRONG = "테스트상사 실적이 영향을 줬을 수 있습니다."
WEAK = "환율 소식이 영향을 줬을 수 있습니다."
REWRITTEN = "여러 보도가 실적 전망을 함께 전했습니다."


def test_move_groups_same_event_articles_in_the_first_report_and_the_rewrite_and_publishes_each_ref():
    flow = Flow([move_output((STRONG, ["a1", "a2"], None), (WEAK, ["a3"], SHORTFALL)),
                 one_cause(REWRITTEN, ["r1", "a1"])], [queries(*QUERIES)])
    initial = [news("a1", SAME_1, T - timedelta(minutes=30)), news("a2", SAME_2, T - timedelta(minutes=20)),
               news("a3", OTHER, T - timedelta(minutes=10)),
               # 제목은 같은 사건이지만 변동 시각 + Δ 뒤에 나와 시간 범위에서 버려진다. 묶음으로도 들어오지 않는다.
               news("a9", SAME_1, T + timedelta(hours=1))]
    flow.to_report(initial)

    first = json.loads(flow.report_llm.calls[0][1])
    assert sorted(refs_of(first["articles"]), key=str) == [["a1", "a2"], "a3"]
    assert "researchArticles" not in first and "a9" not in flow.report_llm.calls[0][1]
    # 묶음 안에서도 기사마다 원래 근거 번호가 붙고, 묶음 자체에는 번호가 없다.
    assert set(group_in(first["articles"])) == {"a1", "a2"}
    assert all("ref" not in x for x in first["articles"] if SAME_EVENT_KEY in x)

    flow.complete_research(research_id=rid(2))
    flow.stable_research([news("r1", SAME_RESEARCH, T - timedelta(hours=1)),
                          news("r2", OTHER_RESEARCH, T - timedelta(hours=2))], research_id=rid(2))

    # 다시 쓰기 입력: 재검색으로 찾은 같은 사건 기사는 처음 모은 기사의 묶음에 더해지고 재검색 기사 표시가 붙는다.
    system, user = flow.report_llm.calls[1][:2]
    assert system == MOVE_PROMPT + MOVE_REWRITE_ADDENDUM
    rewrite = json.loads(user)
    assert sorted(refs_of(rewrite["articles"]), key=str) == [["a1", "a2", "r1"], "a3"]
    group = group_in(rewrite["articles"])
    assert group["r1"]["research"] is True and "research" not in group["a1"]
    assert refs_of(rewrite["researchArticles"]) == ["r2"]

    (briefing,) = flow.briefings()
    assert validate("briefing.generated.v1.schema.json", briefing.value) == []
    assert REWRITTEN in briefing.value["payload"]["text"] and WEAK not in briefing.value["payload"]["text"]
    # 묶인 기사도 저마다의 번호로 인용되어 근거 목록에 각각 나온다.
    assert [e["articleId"] for e in briefing.value["payload"]["evidence"]] == ["a1", "a2", "r1"]
    # 받아들인 보고서 시도마다 묶음 수가 작업 기록에 남는다(첫 보고서, 다시 쓰기).
    written = flow.records.of_type(EntryType.REPORT_WRITTEN)
    assert [e.data["articleGroups"] for e in written] == [{"groups": 1, "groupedArticles": 2},
                                                          {"groups": 1, "groupedArticles": 3}]


@pytest.fixture
def grouped_move_input():
    """같은 사건 기사 둘이 묶인 급변동 보고서 입력"""
    def article(article_id, title, minutes):
        return Article(article_id, title, f"{article_id} 요약", T + timedelta(minutes=minutes), "news.example.com",
                       f"https://news.example.com/{article_id}")

    return report_input(articles=(article("a1", SAME_1, -30), article("a2", SAME_2, -20)))


def test_grouping_adds_no_citable_ref_so_grouped_refs_pass_and_others_fail(grouped_move_input):
    data = build_llm_input(grouped_move_input)
    assert refs_of(data["articles"]) == [["a1", "a2"]]
    allowed = frozenset(evidence_catalog(grouped_move_input))
    assert {"a1", "a2"} <= allowed and SAME_EVENT_KEY not in allowed

    ok = move_output((STRONG, ["a1", "a2"], None))
    assert parse_output(ok, allowed).causes[0].refs == ("a1", "a2")
    with pytest.raises(InvalidOutput):
        parse_output(move_output((STRONG, [SAME_EVENT_KEY], None)), allowed)


# ---- 데일리 판 ----

def item_entries(user, name):
    return next(i for i in json.loads(user)["items"] if i["name"] == name)["entries"]


@pytest.fixture(params=["morning", "post-close"])
def edition(request):
    return request.param


def test_daily_groups_same_event_articles_in_the_first_write_and_the_rewrite_and_publishes_each_ref(edition):
    flow = DailyFlow(edition, [], [])
    at = flow.at
    flow.report_llm.outputs = [
        json.dumps({"points": [
            {"statement": STRONG, "refs": ["e1", "e2"], "weak": False, "shortfall": ""},
            {"statement": WEAK, "refs": [flow.ref], "weak": True, "shortfall": SHORTFALL}],
            "insufficientEvidence": False}, ensure_ascii=False),
        daily_output({"statement": REWRITTEN, "refs": ["r1", "e1"], "weak": False, "shortfall": ""}),
    ]
    flow.query_llm.outputs = [queries(*QUERIES)]
    flow.to_first_report({"TEST01": [
        news("e1", SAME_1, at - timedelta(hours=2)), news("e2", SAME_2, at - timedelta(hours=1)),
        news("e3", OTHER, at - timedelta(minutes=30)),
        # 제목은 같은 사건이지만 판 범위보다 오래되어 버려진다. 묶음으로도 들어오지 않는다.
        news("e9", SAME_1, at - timedelta(days=9))]})

    system, user = flow.report_llm.calls[0][:2]
    assert system == (MORNING_PROMPT if edition == "morning" else SYSTEM_PROMPT_POST_CLOSE)
    first = item_entries(user, "ETF별 기사")
    assert sorted(refs_of(first), key=str) == [["e1", "e2"], "e3"] and "e9" not in user
    assert all("ref" not in x for x in first if SAME_EVENT_KEY in x)

    assert flow.complete(flow.rid(2)) is True
    flow.stable([news("r1", SAME_RESEARCH, at - timedelta(minutes=40)),
                 news("r2", OTHER_RESEARCH, at - timedelta(minutes=50))], flow.rid(2))

    system, user = flow.report_llm.calls[1][:2]
    addendum = REWRITE_ADDENDUM if edition == "morning" else REWRITE_ADDENDUM_POST_CLOSE
    assert system.endswith(addendum)
    rewrite = item_entries(user, "ETF별 기사")
    assert sorted(refs_of(rewrite), key=str) == [["e1", "e2", "r1"], "e3"]
    assert group_in(rewrite)["r1"]["research"] is True
    assert refs_of(item_entries(user, "재검색 기사")) == ["r2"]

    (event,) = flow.published()
    text = event.value["payload"]["text"]
    assert REWRITTEN in text and WEAK not in text
    # 묶인 기사도 저마다의 근거 번호로 본문 출처에 나온다.
    for ref in ("e1", "e2", "r1"):
        assert f"https://news.example.com/{ref}" in text
    written = flow.records.of_type(EntryType.REPORT_WRITTEN)
    assert [e.data["llm"]["articleGroups"] for e in written] == [{"groups": 1, "groupedArticles": 2},
                                                                 {"groups": 1, "groupedArticles": 3}]


# ---- 지시문 ----

@pytest.mark.parametrize("prompt", [MOVE_PROMPT, MORNING_PROMPT, SYSTEM_PROMPT_POST_CLOSE])
def test_report_prompts_explain_groups_synthesis_and_conflicting_articles(prompt):
    assert SAME_EVENT_KEY in prompt and "research가 true" in prompt
    assert "여러 기사에 나뉘어 나온 사실" in prompt
    assert "단정하지 않고 양쪽을 모두 씁니다" in prompt and "2천억~3천억 원으로 엇갈립니다" in prompt
    assert "문장 안에 근거 번호나 ref·articleId 값을 직접 쓰지 않습니다" in prompt


@pytest.mark.parametrize("addendum", [MOVE_REWRITE_ADDENDUM, REWRITE_ADDENDUM, REWRITE_ADDENDUM_POST_CLOSE])
def test_rewrite_addenda_tell_the_llm_to_read_the_whole_group(addendum):
    assert "같은 사건 묶음(sameEventArticles)" in addendum and "단정하지 않습니다" in addendum


def test_query_prompt_asks_for_concrete_events_or_other_articles_on_a_found_event():
    prompt = QUERY_SYSTEM_PROMPT.format(max_queries=3)
    assert "구체적 사건의 주체와 행위(누가·무엇을" in prompt
    assert "그 사건을 다룬 다른 기사(다른 수치, 관계자 발언, 후속 보도)" in prompt
    # 검색어 수 상한은 그대로 지시문에 들어간다.
    assert "검색어는 3개 이하로" in prompt


# ---- 검색어 입력과 설정 연결 ----

def _no_grouping(*args, **kwargs):
    raise AssertionError("검색어 입력에서 기사 묶기를 불렀습니다")


def test_the_search_query_input_does_not_group_articles_and_keeps_the_same_fields(grouped_move_input, monkeypatch):
    import briefing.daily_report as daily_report
    import briefing.report as report
    from briefing.daily_jobs import DailySentences
    from briefing.sentence_research import MoveDraft, MoveSentences
    from test_daily_writing import weak_draft

    move = MoveSentences(stage=None)
    move_draft = MoveDraft(grouped_move_input, None)
    grouped = build_llm_input(grouped_move_input)
    daily = DailySentences(type("Core", (), {"settings": daily_report.DEFAULT_DAILY_SETTINGS})())
    day_draft = weak_draft("0.50")
    grouped_day = daily_report.build_llm_input(day_draft.input, day_draft.sections)

    monkeypatch.setattr(report, "article_groups", _no_grouping)
    monkeypatch.setattr(daily_report, "article_groups", _no_grouping)
    context = move.query_context(move_draft, None)
    day_context = daily.query_context(day_draft, None)

    # 묶기를 건너뛰어도 검색어 LLM이 받는 칸은 묶은 입력에서 꺼낸 값과 같다.
    assert {k: context[k] for k in ("symbol", "direction", "changePercent", "moveAt", "figures")} == \
        {k: grouped[k] for k in ("symbol", "direction", "changePercent", "moveAt", "figures")}
    assert context["foundArticleTitles"] == [SAME_1, SAME_2]
    assert {k: day_context[k] for k in ("edition", "rangeStart", "asOf", "etfInfo")} == \
        {k: grouped_day[k] for k in ("edition", "rangeStart", "asOf", "etfInfo")}


def test_the_daily_writer_groups_with_the_core_settings_the_service_passes(monkeypatch):
    import briefing.main as main
    from briefing.config import DEFAULT_SETTINGS
    from briefing.daily_inputs import EtfArticles
    from briefing.daily_report import build_daily_writer, build_sections
    from test_daily_writing import AT, Sequence, day_input, etf_row, llm_output

    def article(article_id, title, hours):
        return Article(article_id, title, f"{article_id} 요약", AT - timedelta(hours=hours), "news.example.com",
                       f"https://news.example.com/{article_id}")

    # 같은 사건 기사 둘이 3시간 떨어져 있다. 기본 시각 차이 상한(6시간)이면 묶이고, 1시간으로 줄이면 묶이지 않는다.
    i = day_input([etf_row("TEST01", "1.00"), etf_row("TEST02", "-1.00")],
                  articles=(EtfArticles("TEST01", (article("E1", SAME_1, 4), article("E2", SAME_2, 1)), True),))
    narrow = replace(DEFAULT_SETTINGS, article_group_max_gap=timedelta(hours=1))
    users = {}
    for name, core in (("default", DEFAULT_SETTINGS), ("narrow", narrow)):
        llm = Sequence(llm_output())
        writer = build_daily_writer({"ANTHROPIC_API_KEY": "sk-test-value"}, lambda key, model: llm, core=core)
        assert writer.core is core
        writer.write(i, build_sections(i))
        users[name] = llm.calls[0]
    assert sorted(refs_of(item_entries(users["default"], "ETF별 기사")), key=str) == [["E1", "E2"]]
    assert sorted(refs_of(item_entries(users["narrow"], "ETF별 기사")), key=str) == ["E1", "E2"]

    # 서비스 구성에서도 작성기들과 실행부가 같은 CoreSettings를 받는다.
    monkeypatch.setattr(main, "DEFAULT_SETTINGS", narrow)
    monkeypatch.setattr(main, "build_daily_writer", lambda env, core: build_daily_writer(
        {"ANTHROPIC_API_KEY": "sk-test-value"}, lambda key, model: Sequence(), core=core))
    for name in ("PostgresJobLog", "PostgresStatusStore", "PostgresMemberStore"):
        monkeypatch.setattr(main, name, lambda connect: None)
    monkeypatch.setattr(main, "psycopg_connector", lambda conninfo: None)
    monkeypatch.setattr(main, "db_conninfo", lambda env: "")
    runner = main.build_runner({})
    assert runner.settings is narrow and runner.daily_writer.core is narrow
