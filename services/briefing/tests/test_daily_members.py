"""데일리 판을 멤버마다 따로 짜는 부분을 확인한다. LLM은 판 하나에 한 번만 부르고, 멤버마다 자기 포트폴리오의 ETF만
보유/관심 묶음과 그래프로 받는다. 단위(멤버 자료 줄이기)와 가상(가짜 LLM·가짜 멤버 저장소로 한 판을 끝까지) 두 가지다."""
import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import briefing.daily_jobs as daily_jobs
from briefing.config import DailySettings
from briefing.daily_inputs import EtfArticles, MemberIssue, NotableCandidate
from briefing.daily_jobs import DailyCore
from briefing.daily_members import MemberPortfolio, member_input, member_output, member_sections
from briefing.daily_report import (
    POINT_ETF,
    POINT_FX,
    POINT_NOTABLE,
    DailyInput,
    DailyLlmWriter,
    DailyOutput,
    EtfDay,
    Point,
    build_sections,
)
from briefing.job_log import EntryType, RunStatus
from briefing.member_store import SELECT_SQL, PostgresMemberStore
from briefing.sources import CloseRate, Filing
from fake_members import FakeMembers, member
from fake_news import answer_news
from test_daily_review import seed_move
from test_daily_jobs import (
    POST_CLOSE_AT,
    POST_CLOSE_BACKGROUND,
    POST_CLOSE_JOB,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    etf_v2,
    etf_v2_missing,
    post_close_output,
    tick,
)

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
FETCHED = datetime(2026, 10, 20, 16, 30, tzinfo=KST)
FETCHED_TEXT = "2026-10-20T16:30:02+09:00"


# ---- 단위: 멤버 자료 줄이기 ----

def etf_day(symbol, rate="0.50", tickers=()):
    return EtfDay(symbol, "20261020", "9850", "10000", rate, FETCHED, tuple(tickers), False, None, None)


def full_input(**extra):
    etfs = (etf_day("TEST01", "-1.50", ("TESTA",)), etf_day("TEST02", "2.10", ("FAKEB",)), etf_day("TEST03", "0.40"))
    values = dict(
        job_id="job", edition="post-close", range_start=AT - timedelta(days=1), as_of=AT, etfs=etfs,
        etfs_truncated=False,
        close_rates=(CloseRate("TESTA", "20261019", AT, "-1.25", AT), CloseRate("FAKEB", "20261019", AT, "3.00", AT)),
        background=(),
        filings=(Filing("0000000001-26-000001", ("TESTA",), "8-K", (), AT, "https://example.test/f1"),
                 Filing("0000000001-26-000002", ("FAKEB",), "8-K", (), AT, "https://example.test/f2")),
        filings_truncated=False,
        etf_articles=(EtfArticles("TEST01", (), True), EtfArticles("TEST02", (), True), EtfArticles("TEST03", (), True)),
        member_issues=(MemberIssue("TEST01", 1, "TESTA", None, "-1.25", ()),
                       MemberIssue("TEST02", 1, "FAKEB", None, "3.00", ())),
        notables=(NotableCandidate("opposite", ("TEST01", "TEST02")), NotableCandidate("opposite", ("TEST02", "TEST03"))))
    values.update(extra)
    return DailyInput(**values)


def test_member_input_keeps_only_the_members_etfs_and_the_us_data_tied_to_them_and_sets_their_relation():
    mine = member_input(full_input(), member(1, held=("TEST01",), watched=("TEST03",)))
    assert [(e.symbol, e.relation) for e in mine.etfs] == [("TEST01", "HELD"), ("TEST03", "WATCH")]
    assert [r.ticker for r in mine.close_rates] == ["TESTA"]
    assert [f.accession_number for f in mine.filings] == ["0000000001-26-000001"]
    assert [g.symbol for g in mine.etf_articles] == ["TEST01", "TEST03"]
    assert [i.etf for i in mine.member_issues] == ["TEST01"]
    # 두 ETF가 모두 이 멤버 것일 때만 눈에 띄는 점 후보가 남는다.
    assert mine.notables == ()
    both = member_input(full_input(), member(2, held=("TEST01", "TEST02")))
    assert [n.symbols for n in both.notables] == [("TEST01", "TEST02")]


def test_member_input_without_the_etf_response_stays_unknown_and_the_review_is_cut_to_the_members_etfs():
    assert member_input(full_input(etfs=None), member(1, held=("TEST01",))).etfs is None
    assert member_input(full_input(), member(1, held=("TEST09",))).etfs == ()


def test_member_sections_keep_the_evidence_numbers_of_the_whole_board():
    full = full_input()
    sections = build_sections(full)
    day_refs = {e.llm["symbol"]: e.ref for s in sections for e in s.entries if "symbol" in e.llm and "close" in e.llm}
    mine = member_input(full, member(1, held=("TEST02", "TEST03")))
    refs = {e.llm["symbol"]: e.ref for s in member_sections(mine, sections) for e in s.entries
            if "symbol" in e.llm and "close" in e.llm}
    # 멤버 목록에서 TEST02가 첫 줄이 되어도 판 전체의 번호(E2)를 그대로 쓴다.
    assert refs == {"TEST02": day_refs["TEST02"], "TEST03": day_refs["TEST03"]} and refs["TEST02"] == "E2"
    issue_refs = [e.ref for s in member_sections(mine, sections) for e in s.entries if "filingRefs" in e.llm]
    assert issue_refs == [e.ref for s in sections for e in s.entries if e.llm.get("etf") == "TEST02" and "filingRefs" in e.llm]


def test_member_output_drops_sentences_about_other_members_etfs_but_keeps_the_market_wide_ones():
    full = full_input()
    sections = build_sections(full)
    output = DailyOutput(
        points=(Point("하나 이유", ("E1",), None, POINT_ETF, "TEST01"), Point("둘 이유", ("E2",), None, POINT_ETF, "TEST02"),
                Point("하나와 둘이 반대로 움직임", ("E1", "E2"), None, POINT_NOTABLE)),
        insufficient_evidence=False, summary=("요약 첫째", "요약 둘째"),
        issues=(Point("환율 문장", (), None, POINT_FX),))
    mine = member_input(full, member(1, held=("TEST01",)))
    out = member_output(output, mine, member_sections(mine, sections))
    assert [p.statement for p in out.points] == ["하나 이유"]
    assert out.summary == ("요약 첫째", "요약 둘째") and [p.statement for p in out.issues] == ["환율 문장"]
    assert member_output(None, mine, ()) is None


# ---- 저장소: SQL과 묶기 ----

class FakeCursor:
    def __init__(self, rows, log, fail):
        self.rows, self.log, self.fail = rows, log, fail

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("password=secret host=db.internal")
        self.log.append(sql)

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows, log, fail=False):
        self.rows, self.log, self.fail = rows, log, fail

    def cursor(self):
        return FakeCursor(self.rows, self.log, self.fail)

    def close(self):
        pass


def test_the_store_groups_rows_by_member_and_picks_the_receive_column_of_the_edition():
    log = []
    rows = [(1, "ETF", "TEST01", "HELD", None), (1, "ETF", "TEST02", "WATCH", None),
            (1, "STOCK", "TSTK01", "HELD", "TEST 주식 하나"), (3, "ETF", "TEST02", "HELD", None),
            (3, "STOCK", "TSTK02", "WATCH", None), (5, "STOCK", "TSTK01", "HELD", "TEST 주식 하나")]
    store = PostgresMemberStore(lambda: FakeConnection(rows, log))
    # 주식은 (코드, 이름)으로 그 멤버에게 붙고, 국내 ETF가 없는 멤버(5)는 지금처럼 받는 사람이 아니다.
    assert store.daily_members("morning") == (
        MemberPortfolio(1, (("TEST01", "HELD"), ("TEST02", "WATCH")), (("TSTK01", "TEST 주식 하나"),)),
        MemberPortfolio(3, (("TEST02", "HELD"),), (("TSTK02", None),)))
    store.daily_members("post-close")
    assert "mail.receive_morning" in log[0] and "mail.receive_post_close" in log[1]
    # 사용 중인 멤버·메일 줄만, 국내 ETF와 국내 주식만 읽고 메일 주소 열은 읽지 않는다.
    assert "m.enabled" in log[0] and "mail.enabled" in log[0] and "e.market = 'KR'" in log[0]
    assert "s.market = 'KR'" in log[0] and log[0].count("mail.receive_morning") == 2
    assert "SELECT m.id, 'ETF' AS kind, e.symbol, p.relation" in SELECT_SQL and "email" not in SELECT_SQL
    with pytest.raises(ValueError):
        store.daily_members("noon")


def test_the_store_returns_none_without_leaking_connection_details_when_the_read_fails(caplog):
    store = PostgresMemberStore(lambda: FakeConnection([], [], fail=True))
    assert store.daily_members("morning") is None
    assert "secret" not in caplog.text and "db.internal" not in caplog.text


# ---- 가상: 한 판을 멤버마다 ----

REASONS = {"TEST01": "하나 이유 문장입니다.", "TEST02": "둘 이유 문장입니다.", "TEST03": "셋 이유 문장입니다."}


def three_etfs():
    return [etf_v2("TEST01", "TEST 하나", fetched=FETCHED_TEXT, rate="-1.50", tickers=("TESTA",)),
            etf_v2("TEST02", "TEST 둘", fetched=FETCHED_TEXT, rate="2.10", tickers=()),
            etf_v2("TEST03", "TEST 셋", fetched=FETCHED_TEXT, rate="0.40", tickers=()),
            etf_v2_missing("TEST04", name="TEST 넷")]


def run_post_close(members, monkeypatch=None, settings=None, details=None):
    requests, details = Requests(), details if details is not None else Details()
    settings = settings or DailySettings()
    llm = FakeLlm(post_close_output(symbols=("TEST01", "TEST02", "TEST03"), reasons=REASONS))
    core = DailyCore(requests, DailyLlmWriter(llm, settings.llm_max_attempts), details, settings,
                     members=FakeMembers(members))
    core.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    core.on_daily_data_responded(daily_response(etfs=three_etfs(), background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(core, requests, POST_CLOSE_AT)
    return requests.topic("briefing.daily.generated"), details, llm


MEMBERS = (member(1, held=("TEST01",), watched=("TEST02",)), member(2, held=("TEST02", "TEST03")),
           member(7, held=("TEST09",)))


def test_the_llm_is_called_once_for_the_whole_board_no_matter_how_many_members_there_are():
    _, _, llm = run_post_close(MEMBERS)
    assert len(llm.calls) == 1
    _, _, llm = run_post_close(MEMBERS[:1])
    assert len(llm.calls) == 1
    # 한 판의 LLM 입력에는 멤버 번호도 보유·관심도 없다.
    system, user, _ = llm.calls[0]
    assert "memberId" not in user and "HELD" not in user and "WATCH" not in user and "보유" not in user.replace("보유 ETF", "")


def test_each_member_gets_their_own_event_with_only_their_etfs_relation_and_reasons():
    events, _, _ = run_post_close(MEMBERS)
    by_member = {e.value["payload"]["memberId"]: e.value["payload"] for e in events}
    assert sorted(by_member) == [1, 2, 7] and [e.key for e in events] == ["post-close:1", "post-close:2", "post-close:7"]
    one, two = by_member[1]["text"], by_member[2]["text"]
    assert "TEST 하나" in one and "TEST 둘" in one and "TEST 셋" not in one
    assert "하나 이유 문장" in one and "둘 이유 문장" in one and "셋 이유 문장" not in one
    assert "TEST 둘" in two and "TEST 셋" in two and "TEST 하나" not in two
    assert "하나 이유 문장" not in two and "셋 이유 문장" in two
    # 같은 ETF(TEST02)도 멤버마다 보유/관심이 다르다.
    assert "  [보유]\n  - TEST 하나" in one and "  [관심]\n  - TEST 둘" in one
    assert "  [보유]\n  - TEST 둘" in two and "[관심]" not in two
    assert by_member[7]["text"] != one and "TEST 하나" not in by_member[7]["text"]


def test_a_members_html_and_chart_hold_only_that_members_etfs(monkeypatch):
    drawn = []
    monkeypatch.setattr(daily_jobs, "render_day_chart", lambda bars: drawn.append([b.label for b in bars]) or b"\x89PNG\r\n\x1a\n")
    events, _, _ = run_post_close(MEMBERS[:2])
    assert drawn == [["TEST 하나", "TEST 둘 (관심)"], ["TEST 둘", "TEST 셋"]]
    first, second = (e.value["payload"] for e in events)
    assert "TEST 셋" not in first["html"] and "TEST 하나" not in second["html"]
    assert first["images"] and second["images"]


def test_the_event_names_the_member_by_number_only(monkeypatch):
    events, _, _ = run_post_close(MEMBERS)
    for event in events:
        text = json.dumps(event.value, ensure_ascii=False)
        assert event.value["schemaVersion"] == 2 and "@" not in text
        assert set(event.value["payload"]) >= {"memberId", "edition", "text", "generatedBy"}


def test_the_job_log_says_which_member_each_board_was_for_and_the_run_keeps_one_result_line():
    events, details, _ = run_post_close(MEMBERS)
    rows = [e for e in details.entries if e.entry_type == EntryType.MEMBER_BRIEFING]
    assert [(r.data["memberId"], r.data["resultEventId"], r.data["etfCount"]) for r in rows] == [
        (e.value["payload"]["memberId"], e.value["eventId"], n) for e, n in zip(events, (2, 2, 0))]
    [result] = details.results()
    assert result.status == RunStatus.SUCCEEDED and result.result_event_id == events[0].value["eventId"]
    # 작업 기록의 본문은 멤버를 가르지 않은 전체 글이다.
    assert "TEST 하나" in result.final_text and "TEST 셋" in result.final_text and "[보유]" not in result.final_text


def moves_of(payload):
    return payload["text"].split("■ 그날 급변동")[1].split("투자 조언")[0]


def test_a_move_of_a_korean_stock_in_the_members_portfolio_is_listed_with_its_name_for_that_member_only():
    # 그날 급변동이 포트폴리오의 국내 주식에만 있었던 날(#65): 그 주식을 담은 멤버의 목록에 이름과 함께 나오고,
    # 담지 않은 멤버의 목록에는 나오지 않는다. ETF 급변동도 그 ETF를 담은 멤버에게만 나온다.
    details = Details()
    seed_move(details, "TSTK01", datetime(2026, 10, 20, 10, 15, tzinfo=KST), 3.2, True, "TEST-MOVE-S1")
    seed_move(details, "TSTK02", datetime(2026, 10, 20, 11, 0, tzinfo=KST), -2.4, True, "TEST-MOVE-S2")
    seed_move(details, "TEST02", datetime(2026, 10, 20, 13, 20, tzinfo=KST), 1.9, True, "TEST-MOVE-E2")
    members = (member(1, held=("TEST01",), stocks=(("TSTK01", "TEST 주식 하나"),)),
               member(2, held=("TEST02",), stocks=(("TSTK02", None),)))
    events, _, _ = run_post_close(members, details=details)
    by_member = {e.value["payload"]["memberId"]: e.value["payload"] for e in events}
    one, two = moves_of(by_member[1]), moves_of(by_member[2])
    assert "  - 10:15 TEST 주식 하나(TSTK01) +3.20% · 리포트 성공" in one
    assert "TSTK02" not in one and "TEST02" not in one and "  - 없음" not in one
    # 이름을 모르는 주식은 코드만 보인다.
    assert "  - 11:00 TSTK02 −2.40% · 리포트 성공" in two and "  - 13:20 TEST 둘(TEST02) +1.90%" in two
    assert "TSTK01" not in two
    assert "TEST 주식 하나(TSTK01)" in by_member[1]["html"] and "TSTK01" not in by_member[2]["html"]


def test_a_member_whose_etfs_are_not_in_the_response_still_gets_a_board_saying_so():
    events, _, _ = run_post_close(MEMBERS[2:])
    [event] = events
    assert event.value["payload"]["memberId"] == 7 and "받은 ETF가 없음" in event.value["payload"]["text"] + " ".join(
        event.value["payload"]["missingItems"])


def test_no_members_means_no_board_is_sent_and_the_run_still_ends_as_succeeded(caplog):
    with caplog.at_level(logging.WARNING, logger="briefing.daily_jobs"):
        events, details, llm = run_post_close(())
    assert "no member to send to" in caplog.text
    assert events == [] and len(llm.calls) == 1
    [result] = details.results()
    assert result.status == RunStatus.SUCCEEDED and result.result_event_id is None
    assert [e for e in details.entries if e.entry_type == EntryType.MEMBER_BRIEFING] == []


def test_when_the_members_cannot_be_read_nothing_is_sent_and_the_run_fails_with_that_reason():
    events, details, _ = run_post_close(None)
    assert events == []
    [result] = details.results()
    assert (result.status, result.failure_reason) == (RunStatus.FAILED, "members_unreadable")
    assert [e.reason for e in details.failed] == ["members_unreadable"]


def test_one_members_failure_does_not_stop_the_other_members_boards(monkeypatch):
    original = daily_jobs.member_sections

    def broken_for_member_one(mine, sections, settings):
        if [e.symbol for e in mine.etfs] == ["TEST01", "TEST02"]:
            raise RuntimeError("boom")
        return original(mine, sections, settings)

    monkeypatch.setattr(daily_jobs, "member_sections", broken_for_member_one)
    events, details, _ = run_post_close(MEMBERS)
    assert [e.value["payload"]["memberId"] for e in events] == [2, 7]
    assert [(e.reason, e.data["memberId"]) for e in details.failed] == [("member_failed", 1)]


def test_the_members_are_read_for_the_edition_being_built():
    requests, details = Requests(), Details()
    fake = FakeMembers(MEMBERS)
    core = DailyCore(requests, None, details, members=fake)
    core.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    core.on_daily_data_responded(daily_response(etfs=three_etfs(), background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(core, requests, POST_CLOSE_AT)
    assert fake.asked == ["post-close"] and len(requests.topic("briefing.daily.generated")) == 3


def test_a_core_without_a_member_source_does_not_publish():
    requests, details = Requests(), Details()
    core = DailyCore(requests, None, details, members=None)
    core.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    core.on_daily_data_responded(daily_response(etfs=three_etfs(), background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(core, requests, POST_CLOSE_AT)
    assert requests.topic("briefing.daily.generated") == []


def test_when_every_members_board_fails_the_run_fails_and_logs_an_error(monkeypatch, caplog):
    def always_broken(mine, sections, settings):
        raise RuntimeError("boom")

    monkeypatch.setattr(daily_jobs, "member_sections", always_broken)
    with caplog.at_level(logging.ERROR, logger="briefing.daily_jobs"):
        events, details, _ = run_post_close(MEMBERS)
    assert events == []
    [result] = details.results()
    assert (result.status, result.failure_reason) == (RunStatus.FAILED, "member_failed")
    assert [e.reason for e in details.failed] == ["member_failed"] * 3
    assert "every member briefing failed" in caplog.text


def test_a_summary_that_names_an_etf_is_retried_so_other_members_never_read_it():
    requests, details = Requests(), Details()
    bad = post_close_output(symbols=("TEST01", "TEST02", "TEST03"), reasons=REASONS,
                            summary=("TEST 하나가 크게 내렸습니다.", "환율은 올랐습니다."))
    good = post_close_output(symbols=("TEST01", "TEST02", "TEST03"), reasons=REASONS)
    llm = FakeLlm(bad, good)
    core = DailyCore(requests, DailyLlmWriter(llm, 3), details, DailySettings(), members=FakeMembers(MEMBERS))
    core.on_schedule_tick(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT, "post-close")
    core.on_daily_data_responded(daily_response(etfs=three_etfs(), background=POST_CLOSE_BACKGROUND), POST_CLOSE_AT)
    answer_news(core, requests, POST_CLOSE_AT)
    assert len(llm.calls) == 2
    assert all("TEST 하나가" not in e.value["payload"]["text"] for e in requests.topic("briefing.daily.generated"))
