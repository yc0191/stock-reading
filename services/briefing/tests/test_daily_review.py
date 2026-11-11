"""마감 후 판의 아침 예상 대조와 그날 급변동 요약: 맞음·빗나감 판정, 출력 검사, 작업 기록 읽기, 본문 두 칸."""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from briefing.config import DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_report import (
    DailyInput,
    DailyLlmWriter,
    EtfDay,
    InvalidDailyOutput,
    POINT_REVIEW,
    build_sections,
    daily_text,
    parse_post_close_output,
)
from briefing.daily_review import (
    MORNING_READ_FAILED,
    NO_ACTUAL_RATES,
    NO_MORNING_CALLS,
    NOT_COMPARED,
    VERDICT_RIGHT,
    VERDICT_WRONG,
    MorningCall,
    ReviewInput,
    parse_calls,
    review_rows,
    verdict_of,
)
from briefing.job_log import (
    EntryType,
    GeneratedBy,
    JobKind,
    JobLogEntry,
    JobResult,
    JobStart,
    RunStatus,
    move_target,
    parse_move_run,
)
from fake_job_log import FakeJobLog
from fake_news import answer_news
from test_daily_jobs import (
    MORNING_AT,
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
    published,
    tick,
)

KST = timezone(timedelta(hours=9))
FETCHED = "2026-10-20T16:30:02+09:00"
DAY = datetime(2026, 10, 20).date()


# ---- 판정 ----

@pytest.mark.parametrize("direction, rate, expected", [
    # 상승은 +0.30%를 넘을 때만 맞다. 보합 폭 안(경계 +0.30 포함)이면 빗나간다.
    ("상승", "0.31", VERDICT_RIGHT), ("상승", "0.30", VERDICT_WRONG), ("상승", "0.01", VERDICT_WRONG),
    ("상승", "0.00", VERDICT_WRONG), ("상승", "-0.31", VERDICT_WRONG),
    # 하락은 -0.30% 아래일 때만 맞다.
    ("하락", "-0.31", VERDICT_RIGHT), ("하락", "-0.30", VERDICT_WRONG), ("하락", "-0.01", VERDICT_WRONG),
    ("하락", "0.00", VERDICT_WRONG), ("하락", "0.80", VERDICT_WRONG),
    # 보합은 절댓값이 0.30 이하일 때만 맞다(경계 포함).
    ("보합", "0.30", VERDICT_RIGHT), ("보합", "-0.30", VERDICT_RIGHT), ("보합", "0.00", VERDICT_RIGHT),
    ("보합", "0.31", VERDICT_WRONG), ("보합", "-0.31", VERDICT_WRONG),
    ("예상 어려움", "1.00", None), ("모르는 값", "1.00", None),
])
def test_the_verdict_follows_the_flat_band_in_both_directions(direction, rate, expected):
    assert verdict_of(direction, Decimal(rate), 0.3) == expected


def test_the_default_flat_band_is_point_three_percent():
    from briefing.config import DailySettings

    assert DailySettings().flat_band_percent == 0.3


def test_the_flat_band_comes_from_the_setting():
    assert verdict_of("보합", Decimal("0.70"), 0.5) == VERDICT_WRONG
    assert verdict_of("보합", Decimal("0.70"), 1.0) == VERDICT_RIGHT
    # 폭이 넓어지면 같은 상승 폭이 보합 안으로 들어가 상승 예상은 빗나간다.
    assert verdict_of("상승", Decimal("0.70"), 0.5) == VERDICT_RIGHT
    assert verdict_of("상승", Decimal("0.70"), 1.0) == VERDICT_WRONG


def test_the_tally_line_counts_unsure_calls_apart_from_the_graded_ones():
    from briefing.daily_review import Review, review_tally_line

    call = MorningCall("TEST01", "상승", "이유")
    rows = (Review("TEST01", call, Decimal("1.00"), VERDICT_RIGHT), Review("TEST02", call, Decimal("0.10"), VERDICT_WRONG),
            Review("TEST03", call, None, None), Review("TEST04", call, None, None))

    line = review_tally_line(rows, 0.3)

    assert "맞음 1 · 빗나감 1 · 예상 어려움 2개(채점하지 않음)" in line
    assert "±0.3% 안이면 보합" in line and "+0.3% 초과" in line and "-0.3% 미만" in line


def day(symbol, rate):
    return EtfDay(symbol, "20261020", "10000", "10000", rate, None, (), False)


def no_day(symbol):
    return EtfDay(symbol, None, None, None, None, None, (), False, missing_reason="no-daily-price-in-range")


def test_only_etfs_with_a_call_and_a_real_day_are_compared_in_etf_order_and_unsure_is_not_compared():
    calls = (MorningCall("TEST03", "상승", "이유 셋"), MorningCall("TEST01", "상승", "이유 하나"),
             MorningCall("TEST02", "예상 어려움", "이유 둘"), MorningCall("TEST04", "하락", "이유 넷"))
    etfs = (day("TEST01", "1.20"), day("TEST02", "0.40"), day("TEST05", "2.00"), no_day("TEST04"), day("TEST03", "-0.20"))
    rows = review_rows(etfs, calls, 0.5)
    assert [(r.symbol, r.verdict) for r in rows] == [("TEST01", VERDICT_RIGHT), ("TEST02", None),
                                                    ("TEST03", VERDICT_WRONG)]
    assert rows[0].rate == Decimal("1.20")


def test_a_rate_that_is_not_a_number_is_left_out_instead_of_judged():
    assert review_rows((day("TEST01", "abc"),), (MorningCall("TEST01", "상승", "이유"),), 0.5) == ()


def test_the_first_call_for_a_symbol_is_used():
    calls = (MorningCall("TEST01", "상승", "처음"), MorningCall("TEST01", "하락", "다음"))
    [row] = review_rows((day("TEST01", "1.00"),), calls, 0.5)
    assert row.call.reason == "처음" and row.verdict == VERDICT_RIGHT


# ---- 아침 예상 기록 읽기 ----

def record(*outlooks):
    return {"asOf": MORNING_AT.isoformat(), "outlooks": [
        {"symbol": s, "direction": d, "reason": f"{s} 이유", "refs": ["B1"], "weak": False, "shortfall": None}
        for s, d in outlooks]}


def test_the_newest_record_wins_and_broken_items_are_dropped():
    broken = {"outlooks": [{"symbol": "TEST01", "direction": "급등", "reason": "x"}, {"direction": "상승"}, "x",
                           {"symbol": "TEST02", "direction": "보합", "reason": "이유"}]}
    assert parse_calls([broken, record(("TEST09", "상승"))]) == (MorningCall("TEST02", "보합", "이유"),)
    assert parse_calls([]) == () and parse_calls([{"outlooks": "x"}]) == ()


def test_a_newest_record_with_nothing_usable_falls_back_to_the_one_before():
    empty, all_broken = {"outlooks": []}, {"outlooks": [{"symbol": "TEST01", "direction": "급등", "reason": "x"}]}
    assert parse_calls([empty, all_broken, record(("TEST02", "하락"))]) == (MorningCall("TEST02", "하락", "TEST02 이유"),)
    assert parse_calls([empty, all_broken]) == ()


# ---- Claude 출력 검사 ----

ALLOWED = frozenset({"E1", "E2", "B1"})


def output(reviews, etfs=("TEST01", "TEST02")):
    base = json.loads(post_close_output(etfs))
    if reviews is not None:
        base["reviews"] = reviews
    return json.dumps(base, ensure_ascii=False)


def review_item(symbol="TEST01", refs=("B1",), weak=False, reason="미국 지수가 영향을 줬을 수 있습니다."):
    return {"symbol": symbol, "reason": reason, "refs": list(refs), "weak": weak,
            "shortfall": "기사가 더 있어야 합니다." if weak else ""}


def parse(text, required):
    return parse_post_close_output(text, ALLOWED, frozenset(), frozenset({"TEST01", "TEST02"}), {}, True, required)


def test_a_reason_for_every_review_is_kept_with_its_kind_and_refs_count_as_cited():
    out = parse(output([review_item("TEST01"), review_item("TEST02", refs=("E2",))]), frozenset({"TEST01", "TEST02"}))
    assert [(p.symbol, p.kind, p.refs) for p in out.reviews] == [("TEST01", POINT_REVIEW, ("B1",)),
                                                                ("TEST02", POINT_REVIEW, ("E2",))]
    assert "B1" in out.cited_refs()


def test_a_review_missing_for_a_compared_etf_is_rejected():
    with pytest.raises(InvalidDailyOutput, match="no reason"):
        parse(output([review_item("TEST01")]), frozenset({"TEST01", "TEST02"}))
    with pytest.raises(InvalidDailyOutput, match="does not have reviews"):
        parse(output(None), frozenset({"TEST01"}))


def test_a_review_for_an_etf_that_is_not_compared_is_dropped_not_rejected():
    out = parse(output([review_item("TEST01"), review_item("TEST02")]), frozenset({"TEST01"}))
    assert [p.symbol for p in out.reviews] == ["TEST01"]
    # 대조할 ETF가 없으면 reviews 칸이 없거나 깨져 있어도 본문을 버리지 않는다.
    assert parse(output([review_item("TEST01")]), frozenset()).reviews == ()
    assert parse(output(None), frozenset()).reviews == ()


def test_a_duplicate_review_a_foreign_ref_and_an_unmarked_missing_ref_are_rejected():
    required = frozenset({"TEST01"})
    with pytest.raises(InvalidDailyOutput, match="more than once"):
        parse(output([review_item("TEST01"), review_item("TEST01")]), required)
    with pytest.raises(InvalidDailyOutput):
        parse(output([review_item("TEST01", refs=("X9",))]), required)
    with pytest.raises(InvalidDailyOutput, match="not marked weak"):
        parse(output([review_item("TEST01", refs=())]), required)


def test_a_weak_review_may_cite_nothing():
    out = parse(output([review_item("TEST01", refs=(), weak=True)]), frozenset({"TEST01"}))
    [p] = out.reviews
    assert p.refs == () and p.shortfall


def test_an_insufficient_output_skips_the_review_check():
    text = json.dumps({"summary": [], "etfs": [], "notables": [], "insufficientEvidence": True})
    out = parse_post_close_output(text, ALLOWED, frozenset(), frozenset(), {}, True, frozenset({"TEST01"}))
    assert out.insufficient_evidence and out.reviews == ()


# ---- 급변동 한 줄 ----

def move_input(percent=2.1):
    return json.dumps({"payload": {"symbol": "TEST01", "changePercent": percent}})


def test_a_move_row_reads_the_symbol_time_and_percent_from_the_job_run():
    run = parse_move_run("j1", move_input(-1.8), json.dumps(move_target("TEST02", "2026-10-20T13:20:00+09:00")),
                         "failed", None, "test-failure")
    assert (run.symbol, run.move_at, run.change_percent, run.status) == (
        "TEST02", datetime(2026, 10, 20, 13, 20, tzinfo=KST), -1.8, "failed")


def test_a_move_row_without_a_percent_keeps_the_row_and_one_without_a_target_is_dropped():
    target = json.dumps(move_target("TEST01", "2026-10-20T10:15:00+09:00"))
    assert parse_move_run("j1", "{}", target, "running", None, None).change_percent is None
    assert parse_move_run("j1", "not json", target, "running", None, None).change_percent is None
    assert parse_move_run("j1", move_input(), "{}", "running", None, None) is None
    assert parse_move_run("j1", move_input(), json.dumps({"symbol": "TEST01", "moveAt": "no"}), "running", None,
                          None) is None


# ---- 가상: 마감 후 판 한 건을 처음부터 끝까지 ----

def seed_morning(store, outlooks, at=MORNING_AT, job_id="TEST-MORNING-1"):
    store.start(JobStart(job_id, JobKind.MORNING, "{}", {"rangeStart": "x", "rangeEnd": "y"}, at))
    store.record(JobLogEntry(job_id=job_id, entry_type=EntryType.ETF_OUTLOOK, stage="compose",
                             reason="morning_outlook_recorded", occurred_at=at,
                             data={"asOf": at.isoformat(), "outlooks": [
                                 {"symbol": s, "direction": d, "reason": f"{s} 아침 이유", "refs": ["B1"],
                                  "weak": False, "shortfall": None} for s, d in outlooks]}))


def seed_move(store, symbol, at, percent, ok, job_id):
    store.start(JobStart(job_id, JobKind.MOVE, move_input(percent).replace("TEST01", symbol),
                         move_target(symbol, at.isoformat()), at))
    if ok:
        store.finish(JobResult(job_id, RunStatus.SUCCEEDED, at + timedelta(minutes=2), generated_by=GeneratedBy.LLM))
    else:
        store.finish(JobResult(job_id, RunStatus.FAILED, at + timedelta(minutes=2), failure_reason="test-failure"))


ETFS = [etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="1.20"),
        etf_v2("TEST02", "TEST 둘", fetched=FETCHED, rate="-0.60"),
        etf_v2("TEST03", "TEST 셋", fetched=FETCHED, rate="0.40")]
OUTLOOKS = [("TEST01", "상승"), ("TEST02", "보합"), ("TEST03", "예상 어려움")]


def run(store, llm, etfs=None, settings=DailySettings(), at=POST_CLOSE_AT):
    requests = Requests()
    writer = DailyLlmWriter(llm, settings.llm_max_attempts) if llm is not None else None
    core = DailyCore(requests, writer, store, settings)
    core.on_schedule_tick(tick(POST_CLOSE_JOB, at), at, "post-close")
    core.on_daily_data_responded(daily_response(etfs=etfs or ETFS, background=POST_CLOSE_BACKGROUND), at)
    answer_news(core, requests, at)
    [event] = published(requests)
    return event.value["payload"]


def llm_with(reviews, etfs=("TEST01", "TEST02", "TEST03")):
    return FakeLlm(output(reviews, etfs))


def lines_with(text, word):
    return [ln for ln in text.splitlines() if word in ln]


def test_the_morning_call_is_checked_against_the_real_move_and_claude_explains_it_with_a_source():
    store = Details()
    seed_morning(store, OUTLOOKS)
    llm = llm_with([review_item("TEST01"), review_item("TEST02", refs=("E2",), reason="시장이 쉬어 가는 흐름이었을 수 있습니다.")])
    payload = run(store, llm)
    text, html = payload["text"], payload["html"]
    assert "■ 아침 예상 대조" in text and "아침 예상 대조" in html
    # 판정은 코드가 한다: 상승 예상·+1.20% → 맞음, 보합 예상·-0.60% → 빗나감(0.30 초과), 예상 어려움은 대조 안 함.
    assert any("TEST01" in ln and "아침 예상 상승 → 실제 +1.20%: 맞음" in ln for ln in text.splitlines())
    assert any("TEST02" in ln and "아침 예상 보합 → 실제 −0.60%: 빗나감" in ln for ln in text.splitlines())
    assert any("TEST03" in ln and f"→ {NOT_COMPARED}" in ln for ln in text.splitlines())
    assert "맞음" in html and "빗나감" in html and NOT_COMPARED in html
    # 채점 요약은 예상 어려움을 개수로만 보이고 기준(보합 ±0.3%)을 함께 알린다.
    tally = "채점: 맞음 1 · 빗나감 1 · 예상 어려움 1개(채점하지 않음)"
    assert any(tally in ln and "±0.3% 안이면 보합" in ln for ln in text.splitlines()) and tally in html
    assert "시장이 쉬어 가는 흐름이었을 수 있습니다." in text and "시장이 쉬어 가는 흐름이었을 수 있습니다." in html
    # ETF 줄(번호 1~3) 다음에 대조 이유의 새 근거가 4번으로 붙고, 이미 번호가 있는 근거(E2)는 그 번호(2)를 다시 쓴다.
    [first] = [ln for ln in text.splitlines() if "미국 지수가 영향을 줬을 수 있습니다." in ln and "출처 4" in ln]
    assert first.strip().startswith("이유:")
    assert any("시장이 쉬어 가는 흐름이었을 수 있습니다." in ln and ln.endswith("[출처 2]") for ln in text.splitlines())


def test_claude_is_given_the_code_verdicts_but_not_the_etfs_it_cannot_judge():
    store = Details()
    seed_morning(store, OUTLOOKS)
    llm = llm_with([review_item("TEST01"), review_item("TEST02")])
    run(store, llm)
    [(system, user, schema)] = llm.calls
    assert json.loads(user)["reviewEtfs"] == [
        {"symbol": "TEST01", "morningDirection": "상승", "morningReason": "TEST01 아침 이유", "actualRate": "1.20",
         "verdict": "맞음"},
        {"symbol": "TEST02", "morningDirection": "보합", "morningReason": "TEST02 아침 이유", "actualRate": "-0.60",
         "verdict": "빗나감"}]
    assert "reviews" in schema["properties"] and "reviewEtfs" in system


def test_a_call_the_morning_edition_made_for_another_day_is_not_used():
    store = Details()
    seed_morning(store, OUTLOOKS, at=MORNING_AT - timedelta(days=1))
    llm = llm_with([])
    payload = run(store, llm)
    assert "맞음" not in payload["text"] and "빗나감" not in payload["text"] and NO_MORNING_CALLS in payload["text"]
    assert "reviewEtfs" not in json.loads(llm.calls[0][1])


def test_without_a_morning_record_the_review_says_so_and_the_reason_is_kept_in_the_job_log():
    store = Details()
    payload = run(store, llm_with([]))
    assert NO_MORNING_CALLS in payload["text"] and NO_MORNING_CALLS in payload["html"]
    [skipped] = store.of_reason("morning_outlook_missing")
    assert skipped.entry_type == EntryType.MISSING_ITEMS and skipped.data["missingItems"] == [NO_MORNING_CALLS]
    assert skipped.error_type is None


def test_calls_without_any_actual_rate_say_so_and_the_reason_is_kept_apart_from_the_missing_record():
    store = Details()
    seed_morning(store, OUTLOOKS[:2])
    # 아침 예상은 있지만 그 ETF들의 일봉이 하나도 없다. 대조할 ETF 하나는 일봉이 있으나 예상이 없다.
    etfs = [etf_v2_missing("TEST01"), etf_v2_missing("TEST02"), etf_v2_missing("TEST03"),
            etf_v2("TEST09", "TEST 아홉", fetched=FETCHED, rate="0.40")]
    payload = run(store, llm_with([], ("TEST09",)), etfs=etfs)
    assert NO_ACTUAL_RATES in payload["text"] and NO_ACTUAL_RATES in payload["html"]
    assert not store.of_reason("morning_outlook_missing")
    [skipped] = store.of_reason("morning_outlook_no_actual_rates")
    assert skipped.entry_type == EntryType.MISSING_ITEMS and skipped.data["missingItems"] == [NO_ACTUAL_RATES]


def test_when_only_some_etfs_lack_an_actual_rate_only_those_lines_are_left_out():
    store = Details()
    seed_morning(store, OUTLOOKS)
    etfs = [etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="1.20"), etf_v2_missing("TEST02"),
            etf_v2_missing("TEST03")]
    payload = run(store, llm_with([review_item("TEST01")], ("TEST01",)), etfs=etfs)
    assert NO_ACTUAL_RATES not in payload["text"] and not store.of_reason("morning_outlook_no_actual_rates")
    assert any("TEST01" in ln and "맞음" in ln for ln in payload["text"].splitlines())


def test_a_job_log_that_cannot_be_read_still_sends_the_edition_and_says_why():
    store = Details(fail_reads=True)
    payload = run(store, llm_with([]))
    assert payload["generatedBy"] == "llm"
    assert MORNING_READ_FAILED in payload["text"] and "기록을 읽지 못했습니다" in payload["text"]
    [skipped] = store.of_reason("morning_outlook_missing")
    assert skipped.error_type == "ReadFailed"


def test_a_rule_based_edition_still_shows_the_code_verdicts_without_a_reason():
    store = Details()
    seed_morning(store, OUTLOOKS)
    payload = run(store, None)
    assert payload["generatedBy"] == "rule-based"
    assert any("TEST01" in ln and "맞음" in ln for ln in payload["text"].splitlines())
    review = payload["text"].split("■ 아침 예상 대조")[1].split("■ 그날 급변동")[0]
    assert "이유:" not in review


def test_a_missing_review_reason_from_claude_makes_the_attempt_fail_and_the_next_one_is_used():
    store = Details()
    seed_morning(store, OUTLOOKS)
    llm = FakeLlm(output([review_item("TEST01")], ("TEST01", "TEST02", "TEST03")),
                  output([review_item("TEST01"), review_item("TEST02")], ("TEST01", "TEST02", "TEST03")))
    payload = run(store, llm)
    assert len(llm.calls) == 2 and payload["generatedBy"] == "llm"
    [rejected] = store.of_type(EntryType.LLM_REJECTED)
    assert "no reason" in rejected.data["llm"]["rejectReason"]


def test_todays_moves_are_listed_one_line_each_with_the_report_result_and_other_days_are_not():
    store = Details()
    seed_move(store, "TEST01", datetime(2026, 10, 20, 10, 15, tzinfo=KST), 2.1, True, "TEST-MOVE-1")
    seed_move(store, "TEST02", datetime(2026, 10, 20, 13, 20, tzinfo=KST), -1.8, False, "TEST-MOVE-2")
    seed_move(store, "TEST03", datetime(2026, 10, 19, 14, 0, tzinfo=KST), 1.5, True, "TEST-MOVE-3")
    seed_move(store, "TEST03", datetime(2026, 10, 20, 17, 30, tzinfo=KST), 1.7, True, "TEST-MOVE-4")
    payload = run(store, llm_with([]))
    moves = payload["text"].split("■ 그날 급변동")[1].split("투자 조언")[0]
    assert "  - 10:15 TEST 하나(TEST01) +2.10% · 리포트 성공" in moves
    assert "  - 13:20 TEST 둘(TEST02) −1.80% · 리포트 실패" in moves
    # 전날 건과 판 시각 뒤에 시작한 건은 없다.
    assert "TEST03" not in moves and "14:00" not in moves and "17:30" not in moves
    assert "리포트 실패" in payload["html"] and "10:15" in payload["html"]


def test_a_move_from_the_night_before_that_started_after_midnight_is_not_listed():
    store = Details()
    # 변동은 전날 23:58에 났고 작업은 오늘 00:01에 시작했다. 오늘 12:00 변동은 남는다.
    started = datetime(2026, 10, 20, 0, 1, tzinfo=KST)
    store.start(JobStart("TEST-MOVE-N", JobKind.MOVE, move_input(1.9).replace("TEST01", "TEST03"),
                         move_target("TEST03", datetime(2026, 10, 19, 23, 58, tzinfo=KST).isoformat()), started))
    seed_move(store, "TEST01", datetime(2026, 10, 20, 12, 0, tzinfo=KST), 1.1, True, "TEST-MOVE-T")
    moves = run(store, llm_with([]))["text"].split("■ 그날 급변동")[1].split("투자 조언")[0]
    assert "12:00 TEST 하나(TEST01)" in moves and "TEST03" not in moves and "23:58" not in moves


def test_no_moves_and_an_unreadable_move_log_each_say_so_and_extra_moves_are_counted():
    assert "  - 없음" in run(Details(), llm_with([]))["text"].split("■ 그날 급변동")[1]
    store = Details()
    for n in range(3):
        seed_move(store, "TEST01", datetime(2026, 10, 20, 10, 15 + n, tzinfo=KST), 1.0 + n, True, f"TEST-MOVE-{n}")
    capped = run(store, llm_with([]), settings=DailySettings(max_move_lines=2))["text"].split("■ 그날 급변동")[1]
    assert "10:15" in capped and "10:16" in capped and "10:17" not in capped and "외 1건" in capped


def test_a_move_whose_percent_is_unknown_is_still_listed():
    store = Details()
    at = datetime(2026, 10, 20, 11, 0, tzinfo=KST)
    store.start(JobStart("TEST-MOVE-9", JobKind.MOVE, "{}", move_target("TEST01", at.isoformat()), at))
    moves = run(store, llm_with([]))["text"].split("■ 그날 급변동")[1]
    assert "11:00 TEST 하나(TEST01) 자료 없음 · 리포트 진행 중" in moves


def test_before_the_market_opens_the_job_log_is_not_read_and_no_review_is_shown():
    class Spy(FakeJobLog):
        reads = 0

        def morning_outlooks(self, day):
            self.reads += 1
            return super().morning_outlooks(day)

    at = datetime(2026, 10, 20, 8, 40, tzinfo=KST)
    zero = [{**etf_v2(s, fetched="2026-10-20T08:35:00+09:00", rate="0.00"), "date": "20261020", "close": "10000",
             "prevClose": "10000"} for s in ("TEST01", "TEST02")]
    store = Spy()
    payload = run(store, FakeLlm(), etfs=zero, at=at)
    assert store.reads == 0 and "아침 예상 대조" not in payload["text"] and "그날 급변동" not in payload["text"]


def test_the_morning_edition_has_no_review_sections():
    i = DailyInput("j", "morning", MORNING_AT, MORNING_AT, (day("TEST01", "1.00"),), False, None, None, None, False,
                   review=ReviewInput((MorningCall("TEST01", "상승", "이유"),), None, ()))
    # 아침 판 글자판은 대조·급변동 칸을 만들지 않는다.
    assert "아침 예상 대조" not in daily_text(i, build_sections(i), None)


def test_a_move_not_sent_for_lack_of_evidence_is_shown_as_such_not_as_a_success():
    from briefing.daily_post_close import move_line
    from briefing.job_log import MoveRun
    at = datetime(2026, 10, 20, 10, 15, tzinfo=timezone(timedelta(hours=9)))
    sent = MoveRun("j1", "TEST01", at, 2.1, "succeeded", "llm", None)
    skipped = MoveRun("j2", "TEST01", at, 2.1, "succeeded", "llm", "no_evidence")

    assert move_line(sent, {"TEST01": "TEST 하나(TEST01)"}).endswith("· 리포트 성공")
    assert move_line(skipped, {"TEST01": "TEST 하나(TEST01)"}) == "10:15 TEST 하나(TEST01) +2.10% · 근거 없어 보내지 않음"
