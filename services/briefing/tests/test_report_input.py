"""LLM 입력의 ETF 정보, 급변동 출력의 미국 티커 거부, 근거 약함 표시 모양. 가짜 LLM·가짜 응답만 쓴다."""
import copy
import json
from datetime import date
from dataclasses import replace

import pytest

from briefing.config import CoreSettings, DailySettings
from briefing.daily_report import SYSTEM_PROMPT as DAILY_PROMPT
from briefing.daily_report import DailyLlmWriter, build_llm_input as build_daily_input, build_sections
from briefing.daily_report import parse_output as parse_daily
from briefing.llm_output import WEAK_NOTE_PREFIX, InvalidLlmOutput, weak_note_line
from briefing.report import SYSTEM_PROMPT as MOVE_PROMPT
from briefing.report import (
    InvalidOutput,
    LlmWriter,
    ReportError,
    build_llm_input as build_move_input,
    parse_output as parse_move,
)
from briefing.sources import EtfInfo, EtfMember, MemberChanges, MovedMember, etf_info_input, parse_etf_info
from fake_news import answer_news
from test_daily_jobs import FakeLlm as DailyFakeLlm
from test_daily_jobs import MORNING_AT, core as daily_core, daily_response, etf as etf_response, filings_response, \
    good_output, published, tick
from test_daily_report import daily_input, etf as etf_day, morning_reply
from test_scenarios import FakeLlm, context, core, rec, run_us, report_stage  # noqa: F401 - 픽스처

MEMBERS = [
    {"market": "US", "symbol": "TESTA", "name": "TEST CORP A", "weight": "12.5", "rank": 1},
    {"market": "US", "symbol": "FAKEB", "weight": "8.25", "rank": 2},
    {"market": "KR", "symbol": "TEST09", "name": "TEST CORP KR", "rank": 3},
]
ETF_INFO = {"name": "TEST ETF ONE", "tracks": "TEST INDEX", "sector": "TEST SECTOR", "members": MEMBERS,
            "membersTruncated": False}


def info(count=3, truncated=False):
    members = [{"market": "US", "symbol": f"TEST{n}", "weight": str(30 - n), "rank": n} for n in range(1, count + 1)]
    return parse_etf_info({"name": "TEST ETF ONE", "members": list(reversed(members)), "membersTruncated": truncated})


# ---- 응답 해석 ----

def test_etf_info_is_read_from_the_response_and_missing_info_is_none():
    parsed = parse_etf_info(ETF_INFO)
    assert (parsed.name, parsed.tracks, parsed.sector) == ("TEST ETF ONE", "TEST INDEX", "TEST SECTOR")
    assert parsed.members[1] == EtfMember("US", "FAKEB", 2, None, "8.25")
    assert parsed.us_tickers() == {"TESTA", "FAKEB"}
    assert parse_etf_info(None) is None
    with pytest.raises(KeyError):
        parse_etf_info({"members": []})


def test_member_changes_are_read_into_value_objects_and_absent_means_none():
    changes = {"since": "2026-10-12", "asOf": "2026-10-19",
               "removed": [{"market": "US", "symbol": "OLDC", "name": "OLD CORP C", "rank": 3}],
               "added": [{"market": "US", "symbol": "NEWD", "rank": 2}],
               "moved": [{"market": "US", "symbol": "TESTA", "rankBefore": 2, "rankAfter": 1}]}
    parsed = parse_etf_info({**ETF_INFO, "memberChanges": changes})

    assert parsed.member_changes == MemberChanges(
        since=date(2026, 10, 12), as_of=date(2026, 10, 19),
        removed=(EtfMember("US", "OLDC", 3, "OLD CORP C"),), added=(EtfMember("US", "NEWD", 2),),
        moved=(MovedMember("US", "TESTA", 2, 1),))
    assert parse_etf_info(ETF_INFO).member_changes is None
    with pytest.raises(KeyError):
        parse_etf_info({**ETF_INFO, "memberChanges": {"since": "2026-10-12", "asOf": "2026-10-19"}})
    with pytest.raises(ValueError):
        parse_etf_info({**ETF_INFO, "memberChanges": {**changes, "since": "10/12"}})


def test_member_changes_do_not_enter_the_llm_input():
    # 메일 줄은 판 쪽이 그린다. 근거 번호가 없는 변경이 LLM 입력에 섞이면 인용할 수 없는 자료가 된다.
    parsed = parse_etf_info({**ETF_INFO, "memberChanges": {"since": "2026-10-12", "asOf": "2026-10-19",
                                                          "removed": [], "added": [], "moved": []}})

    assert "memberChanges" not in etf_info_input(parsed, max_members=3)


def test_etf_input_lists_the_largest_members_first_up_to_the_setting():
    shown = etf_info_input(info(count=5), max_members=3)

    assert [m["rank"] for m in shown["members"]] == [1, 2, 3]
    assert shown["membersTruncated"] is True
    assert shown["name"] == "TEST ETF ONE" and "tracks" not in shown and "sector" not in shown
    assert etf_info_input(info(count=3), max_members=3)["membersTruncated"] is False
    # 응답이 이미 잘렸다고 하면 상한 안이어도 잘림을 그대로 알린다.
    assert etf_info_input(info(count=2, truncated=True), max_members=3)["membersTruncated"] is True


def test_the_member_limit_is_a_setting_in_config():
    assert isinstance(CoreSettings().max_etf_members, int) and CoreSettings().max_etf_members > 0
    assert isinstance(DailySettings().max_etf_members, int) and DailySettings().max_etf_members > 0


# ---- 급변동 입력 ----

def test_move_input_carries_the_etf_info_with_members_in_rank_order(core, rec):
    event = context()
    event["payload"]["etf"] = ETF_INFO
    run_us(core, context_event=event)

    data = build_move_input(rec.readied[0])

    assert data["etf"]["name"] == "TEST ETF ONE" and data["etf"]["tracks"] == "TEST INDEX"
    assert [m["symbol"] for m in data["etf"]["members"]] == ["TESTA", "FAKEB", "TEST09"]
    assert data["etf"]["members"][0]["weightPercent"] == "12.5"


def test_move_input_cuts_members_at_the_setting(core, rec):
    run_us(core)

    data = build_move_input(rec.readied[0], CoreSettings(max_etf_members=2))

    assert [m["rank"] for m in data["etf"]["members"]] == [1, 2]
    assert data["etf"]["membersTruncated"] is True


def test_move_with_an_older_response_without_etf_info_still_builds_the_input_and_the_report(core, rec):
    event = context()
    del event["payload"]["etf"]
    run_us(core, context_event=event)
    ready = rec.readied[0]

    assert ready.etf is None
    assert "etf" not in build_move_input(ready)
    stage, sent, _ = report_stage(LlmWriter(FakeLlm(json.dumps(
        {"causes": [{"title": "t", "detail": "d", "refs": ["a1"], "meaning": "m", "meaningRefs": ["a1"],
                     "weak": False, "shortfall": ""}], "insufficientEvidence": False}))))
    stage.on_ready(ready)
    assert len(sent.sent) == 1


def test_move_llm_receives_the_etf_info_in_its_input(core, rec):
    run_us(core)
    llm = FakeLlm(json.dumps({"causes": [], "insufficientEvidence": True}))

    LlmWriter(llm).write(rec.readied[0])

    _, user = llm.calls[0]
    assert json.loads(user)["etf"]["members"][0]["symbol"] == "TESTA"


def test_both_prompts_tell_the_llm_to_use_etf_info_as_context_not_as_a_cause():
    for prompt in (MOVE_PROMPT, DAILY_PROMPT):
        line = next(x for x in prompt.splitlines() if x.startswith("- etf"))
        assert "맥락" in line and "단정하지 않습니다" in line and "인용할 수 없" in line


# ---- 데일리 입력 ----

def with_etf(day, **kwargs):
    return replace(day, etf=parse_etf_info({**ETF_INFO, **kwargs}))


def test_daily_input_carries_the_etf_info_of_each_etf_for_every_edition():
    for edition in ("morning", "post-close"):
        i = daily_input(edition, etfs=(with_etf(etf_day()),))

        data = build_daily_input(i, build_sections(i))

        [entry] = data["etfInfo"]
        assert entry["symbol"] == "TEST01" and entry["name"] == "TEST ETF ONE"
        assert [m["symbol"] for m in entry["members"]] == ["TESTA", "FAKEB", "TEST09"]


def test_daily_input_cuts_members_at_the_setting_and_skips_etfs_without_info():
    i = daily_input(etfs=(with_etf(etf_day()), etf_day("TEST02")))

    data = build_daily_input(i, build_sections(i), DailySettings(max_etf_members=1))

    [entry] = data["etfInfo"]
    assert [m["rank"] for m in entry["members"]] == [1] and entry["membersTruncated"] is True


def test_daily_input_without_any_etf_info_keeps_its_old_shape():
    i = daily_input()
    assert "etfInfo" not in build_daily_input(i, build_sections(i))


def test_daily_data_response_carries_the_etf_info_into_the_llm_input(daily_core):
    llm = DailyFakeLlm(good_output("B1"))
    c, requests, _, _ = daily_core(llm)
    with_info = {**etf_response(), "etf": ETF_INFO}

    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(etfs=[with_info]), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)

    published(requests)
    _, user, _ = llm.calls[0]
    assert json.loads(user)["etfInfo"][0]["members"][0]["symbol"] == "TESTA"


def test_daily_data_response_from_an_older_format_without_etf_info_still_works(daily_core):
    llm = DailyFakeLlm(good_output("B1"))
    c, requests, _, _ = daily_core(llm)

    c.on_schedule_tick(tick(), MORNING_AT, "morning")
    c.on_daily_data_responded(daily_response(), MORNING_AT)
    c.on_filings_responded(filings_response(), MORNING_AT)
    answer_news(c, requests, MORNING_AT)

    assert len(published(requests)) == 1
    assert "etfInfo" not in json.loads(llm.calls[0][1])


def test_daily_summary_naming_a_member_ticker_from_the_etf_info_is_rejected():
    i = daily_input(etfs=(with_etf(etf_day(tickers=())),), close_rates=())
    sections = build_sections(i)
    bad = morning_reply([{"statement": "FAKEB가 내렸을 수 있습니다.", "refs": ["B1"]}])
    good = morning_reply([{"statement": "미국 종목이 내렸을 수 있습니다.", "refs": ["B1"]}])
    llm = DailyFakeLlm(bad, good)

    result = DailyLlmWriter(llm).write(i, sections)

    assert len(llm.calls) == 2 and result.rejected[0].reason == "point mentions a US ticker"


# ---- 급변동의 미국 티커 거부 ----

def cause_output(**fields):
    cause = {"title": "미국 종목 실적", "detail": "공시가 영향을 줬을 수 있습니다.", "refs": ["a1"],
             "meaning": "변동 전에 나온 공시입니다.", "meaningRefs": ["a1"], "weak": False, "shortfall": ""}
    cause.update(fields)
    return json.dumps({"causes": [cause], "insufficientEvidence": False}, ensure_ascii=False)


@pytest.mark.parametrize("field", ["title", "detail", "meaning", "shortfall"])
def test_move_parser_rejects_a_us_ticker_in_any_sentence(field):
    fields = {field: "TESTA 실적이 영향을 줬을 수 있습니다."}
    if field == "shortfall":
        fields["weak"] = True
    with pytest.raises(InvalidOutput, match="mentions a US ticker"):
        parse_move(cause_output(**fields), frozenset({"a1"}), frozenset({"TESTA"}))


def test_move_parser_matches_a_ticker_followed_by_a_particle_but_not_inside_a_word():
    forbidden = frozenset({"TESTA", "NEXT-B"})
    with pytest.raises(InvalidOutput):
        parse_move(cause_output(detail="TESTA가 영향을 줬을 수 있습니다."), frozenset({"a1"}), forbidden)
    with pytest.raises(InvalidOutput):
        parse_move(cause_output(detail="NEXT.B 공시가 있었습니다."), frozenset({"a1"}), forbidden)
    assert parse_move(cause_output(detail="TESTAB와 미국 종목이 영향을 줬을 수 있습니다."),
                      frozenset({"a1"}), forbidden).causes


def test_move_parser_without_forbidden_tickers_keeps_the_old_behavior():
    assert parse_move(cause_output(detail="TESTA가 영향을 줬을 수 있습니다."), frozenset({"a1"})).causes


def test_move_writer_asks_again_after_a_ticker_answer_and_the_published_body_has_no_ticker(core, rec):
    run_us(core)
    good = cause_output(detail="미국 종목 공시가 영향을 줬을 수 있습니다.", refs=["a1"], meaningRefs=["a1"])
    llm = FakeLlm(cause_output(detail="TESTA 공시가 영향을 줬을 수 있습니다."), good)

    report = LlmWriter(llm).write(rec.readied[0])

    assert len(llm.calls) == 2
    [rejected] = report.failed_attempts
    assert rejected.rejection == "cause mentions a US ticker" and rejected.error_type == "InvalidOutput"
    assert "TESTA" not in report.text.split("1. ")[1].split("확인하지 못한 것")[0]


def test_move_writer_rejects_a_member_ticker_that_only_the_etf_info_names(core, rec):
    run_us(core, context_event=_context_with_unlisted_member())
    llm = FakeLlm(*[cause_output(detail="MEMBX 소식이 영향을 줬을 수 있습니다.")] * 3)

    with pytest.raises(ReportError) as raised:
        LlmWriter(llm).write(rec.readied[0])

    assert raised.value.attempts == 3 and len(raised.value.failed_attempts) == 3


def _context_with_unlisted_member():
    event = context()
    etf = copy.deepcopy(ETF_INFO)
    etf["members"].append({"market": "US", "symbol": "MEMBX", "rank": 4})
    event["payload"]["etf"] = etf
    return event


def test_move_stage_records_llm_error_and_publishes_nothing_when_every_answer_names_a_ticker(core, rec):
    run_us(core)
    bad = cause_output(detail="TESTA 공시가 영향을 줬을 수 있습니다.")
    stage, published_events, job_log = report_stage(LlmWriter(FakeLlm(bad, bad, bad)))

    stage.on_ready(rec.readied[0])

    assert published_events.sent == []
    [failure] = job_log.failed
    assert failure.reason == "llm_error" and failure.attempts == 3


def test_the_evidence_list_titles_may_still_name_the_filing_ticker(core, rec):
    run_us(core)
    from test_scenarios import ACCESSION
    ok = cause_output(detail="미국 종목 공시가 영향을 줬을 수 있습니다.", refs=[ACCESSION], meaningRefs=[ACCESSION])
    stage, sent, _ = report_stage(LlmWriter(FakeLlm(ok)))

    stage.on_ready(rec.readied[0])

    [event] = sent.sent
    assert "TESTA 8-K 공시" in event.value["payload"]["text"]
    assert event.value["payload"]["evidence"][0]["title"].startswith("TESTA 8-K")


# ---- 근거 약함 표시 ----

def test_the_weak_note_is_one_line_the_same_for_every_kind():
    assert weak_note_line("자료가 더 필요합니다.", "  ") == "  " + WEAK_NOTE_PREFIX + "자료가 더 필요합니다."
    assert WEAK_NOTE_PREFIX == "근거 약함: "


def test_move_and_daily_bodies_show_the_weak_note_in_the_same_shape(core, rec):
    shortfall = "더 필요한 자료입니다."
    run_us(core)
    move_out = cause_output(weak=True, shortfall=shortfall)
    stage, sent, _ = report_stage(LlmWriter(FakeLlm(move_out)))
    stage.on_ready(rec.readied[0])
    move_lines = sent.sent[0].value["payload"]["text"].splitlines()

    i = daily_input()
    daily_out = morning_reply([{"statement": "미국 종목이 내렸을 수 있습니다.", "refs": ["U1"], "weak": True,
                                "shortfall": shortfall}])
    daily_lines = DailyLlmWriter(DailyFakeLlm(daily_out)).write(i, build_sections(i)).body.text.splitlines()

    def note(lines):
        index = next(n for n, line in enumerate(lines) if line.strip() == WEAK_NOTE_PREFIX + shortfall)
        # 약한 항목 바로 아래 줄이고, 줄 앞에 목록 기호가 없다
        assert not lines[index].lstrip().startswith("-")
        return index, lines[index - 1]

    move_index, move_parent = note(move_lines)
    daily_index, daily_parent = note(daily_lines)
    assert "[근거" in daily_parent
    assert move_lines[move_index].strip() == daily_lines[daily_index].strip()
