"""레벨 상 메일: 짧은 본문(원인 3개까지, 문장 한 칸에 한 문장), 근거 목록 맨 아래 한 번, '확인하지 못한 것' 없음, 오늘 흐름,
장중 차트 그림, HTML과 인라인 그림. 가짜 값과 가짜 LLM으로 돌리며 실제 Kafka·DB·LLM은 쓰지 않는다.

가짜 값으로 만든 메일 본보기 파일(move_mail_high.html·.txt·.png)은 사람이 눈으로 볼 수 있게 두며, 글자판과 HTML은 만든 결과와 같아야 한다.
본보기를 다시 만들려면 BRIEFING_WRITE_MOVE_MAIL_SAMPLES=1 로 이 파일을 돌린다(그림은 글꼴에 따라 바이트가 달라 비교하지 않는다).
"""
import base64
import json
import os
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import test_scenarios as sc
from briefing import main
from briefing.config import CoreSettings
from briefing.job_log import STAGE_CHART_HISTORY, JobKind, JobStart, move_target
from briefing.jobs import ReportInput
from briefing.llm_output import sentence_count
from briefing.move_chart import CHART_CONTENT_ID, chart_input, prev_close_of, render_move_chart
from briefing.move_flow import FlowMove, earlier_moves, flow_input
from briefing.move_mail import CHART_MISSING_ITEM, CHART_OVERSIZED_ITEM, render_move_html
from briefing.queries import QueryKind, QueryResult
from briefing.final_check import MailSentence
from briefing.move_final_check import MoveFinalCheck
from briefing.report import MAX_CAUSES, Report, LlmWriter, InvalidOutput, RuleBasedWriter, parse_output
from briefing.report_stage import ReportStage
from briefing.move_evidence import build_llm_input
from briefing.sentence_research import MoveDraft
from briefing.sources import Article, PricePoint
from contracts import sample, validate
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
T, SEC = sc.T, sc.SEC
SAMPLES = Path(__file__).parent
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
# 브리핑이 쓰는 librdkafka 생산자의 기본 message.max.bytes. 브로커 기본(1048588)보다 작아 이 안에 들면 어디서도 걸리지 않는다.
KAFKA_PRODUCER_MESSAGE_LIMIT = 1_000_000


# ---- 가짜 값 ----

def conflict_cause(**changes):
    cause = {"title": "금리 결정 영향", "detail": "금리 결정이 영향을 줬을 수 있습니다.", "refs": ["a1"],
             "meaning": "같은 시기 금리 기사가 있었습니다.", "meaningRefs": ["a1"], "conflict": "", "conflictRefs": [],
             "weak": False, "shortfall": ""}
    return {**cause, **changes}


def llm_output(*causes):
    return json.dumps({"causes": list(causes), "insufficientEvidence": False}, ensure_ascii=False)


ALLOWED = frozenset({"a1", "a2", "a3"})


# 분(09:00부터) → 가격. 14:40에 전일 종가(10,000) 대비 -5.1%, 15:30에 -7.0%, 변동 시각(16:00)에 -14.0%가 되는 가짜 흐름이다.
ANCHORS = ((0, 10_000), (340, 9_490), (390, 9_300), (420, 8_600), (425, 8_580))


def day_points(through=T + 5 * 60 * SEC):
    """그날 09:00부터 변동 5분 뒤까지 1분 간격 가짜 시세(결정적)."""
    start = T.astimezone(KST).replace(hour=9, minute=0, second=0, microsecond=0)
    points, at, n = [], start, 0
    while at <= through:
        (x0, y0), (x1, y1) = next(pair for pair in zip(ANCHORS, ANCHORS[1:]) if pair[0][0] <= n <= pair[1][0])
        points.append(PricePoint(at, round(y0 + (y1 - y0) * (n - x0) / (x1 - x0))))
        at += timedelta(minutes=1)
        n += 1
    return tuple(points)


def earlier_start(log, at, today, detection_type, job_id=None):
    """작업 기록에 앞선 급변동(그 변동 이벤트가 입력인 작업 한 줄)을 넣는다."""
    event = sample("market.move.sample.json")
    event["payload"].update(symbol=sc.SYMBOL, asOf=at.isoformat(), changePercent=today, todayChangePercent=today,
                            detectionType=detection_type)
    job_id = job_id or str(uuid.uuid4())
    event["eventId"] = job_id
    log.start(JobStart(job_id, JobKind.MOVE, json.dumps(event), move_target(sc.SYMBOL, at.isoformat()),
                       at + 10 * SEC))


def answer_chart(core, points, now=T + SEC, truncated=False):
    """맡겨 둔 장중 차트 시세 조회에 답한다. 차트 조회는 근거용 시세 이력 조회와 같은 종류라 단계 이름으로 찾는다."""
    for index, (query, on_result) in enumerate(core.api.pending):
        if query.stage == STAGE_CHART_HISTORY:
            del core.api.pending[index]
            body = {"jobId": query.payload["jobId"], "seq": query.payload["seq"], "symbol": sc.SYMBOL,
                    "truncated": truncated,
                    "points": [{"asOf": p.as_of.isoformat(), "price": p.price} for p in points]}
            on_result(QueryResult(query, body, 1), now)
            return query
    raise AssertionError("no pending chart query")


def fail_chart(core, now=T + SEC):
    for index, (query, on_result) in enumerate(core.api.pending):
        if query.stage == STAGE_CHART_HISTORY:
            del core.api.pending[index]
            on_result(QueryResult(query, None, 4, "timeout"), now)
            return
    raise AssertionError("no pending chart query")


@pytest.fixture
def rec():
    recorder = sc.Recorder()
    recorder.api.auto_chart = False
    return recorder


@pytest.fixture
def core(rec):
    return sc.BriefingCore(rec, rec.api, rec, job_log=rec.log)


def run_with_articles(core):
    sc.run_us(core, article_items=[sc.article("a1", T - timedelta(minutes=30)),
                                   sc.article("a2", T - timedelta(minutes=90))])


def ready_input(core, rec, chart=day_points(), earlier=((T - 80 * 60 * SEC, -5.1, "prevClose"),
                                                        (T - 30 * 60 * SEC, -7.0, "prevCloseHigh"))):
    for at, today, detection in earlier:
        earlier_start(rec.log, at, today, detection)
    run_with_articles(core)
    if chart is None:
        fail_chart(core)
    else:
        answer_chart(core, chart)
    return rec.readied[0]


FLOW_CAUSES = llm_output(
    conflict_cause(title="금리 결정 영향", conflict="보도에 따라 인하 폭이 0.25%p와 0.5%p로 엇갈립니다.",
                   conflictRefs=["a1", "a2"]),
    conflict_cause(title="자금 유출", detail="자금 유출이 영향을 줬을 수 있습니다.", refs=["a2"],
                   meaning="유출 기사가 변동 30분 전에 나왔습니다.", meaningRefs=["a2"]))


def published(report_input, response=FLOW_CAUSES, settings=None):
    sent, log = sc.Recorder(), FakeJobLog()
    stage = ReportStage(LlmWriter(sc.FakeLlm(response)), sent, log, clock=lambda: T + timedelta(minutes=20),
                        **({"settings": settings} if settings else {}))
    stage.on_ready(report_input)
    return sent.sent, log


def cids_in(html):
    return set(re.findall(r'cid:([^"\'>\s]+)', html))


def assert_cids_match(payload):
    """html이 가리키는 cid:와 images의 contentId가 남김없이 서로 맞는다(빠지거나 남는 그림이 없다)."""
    assert cids_in(payload["html"]) == {image["contentId"] for image in payload.get("images", [])}


# ---- s1: 출력 검증(원인 3개까지, 한 칸 한 문장, 시각 차이) ----

def test_up_to_three_causes_are_accepted_and_a_fourth_is_rejected():
    ok = [conflict_cause(title=f"원인 {n}") for n in range(MAX_CAUSES)]
    assert len(parse_output(llm_output(*ok), ALLOWED).causes) == 3
    with pytest.raises(InvalidOutput, match="more than 3 causes"):
        parse_output(llm_output(*ok, conflict_cause(title="넷째")), ALLOWED)


@pytest.mark.parametrize("field", ["detail", "meaning", "conflict"])
def test_a_sentence_slot_with_two_sentences_is_rejected(field):
    extra = {"conflict": "보도가 엇갈립니다.", "conflictRefs": ["a1"]} if field != "conflict" else {"conflictRefs": ["a1"]}
    cause = conflict_cause(**extra, **{field: "첫 문장입니다. 둘째 문장입니다."})
    with pytest.raises(InvalidOutput, match="more than one sentence"):
        parse_output(llm_output(cause), ALLOWED)


@pytest.mark.parametrize("text", ["수치는 1.05%p 낮았습니다.", "U.S. 종목이 내렸습니다.", "미국 종목이 내렸습니다", "보도에 따라 2천억~3천억 원입니다!"])
def test_numbers_abbreviations_and_missing_end_marks_count_as_one_sentence(text):
    assert sentence_count(text) == 1
    parse_output(llm_output(conflict_cause(detail=text)), ALLOWED)


@pytest.mark.parametrize("field", ["detail", "meaning", "conflict"])
def test_a_cause_slot_over_the_character_limit_is_rejected_and_at_the_limit_passes(field):
    limit = CoreSettings().max_cause_chars
    extra = {"conflictRefs": ["a1"]} if field == "conflict" else {}
    with pytest.raises(InvalidOutput, match=f"longer than {limit} characters"):
        parse_output(llm_output(conflict_cause(**extra, **{field: "가" * (limit + 1)})), ALLOWED)
    parse_output(llm_output(conflict_cause(**extra, **{field: "가" * limit})), ALLOWED)


def test_the_character_limit_counts_spaces_and_leaves_title_alone():
    limit = CoreSettings().max_cause_chars
    with pytest.raises(InvalidOutput, match="characters"):
        parse_output(llm_output(conflict_cause(detail="가 " * (limit // 2 + 1))), ALLOWED)
    (parsed,) = parse_output(llm_output(conflict_cause(title="가" * (limit * 2))), ALLOWED).causes
    assert len(parsed.title) == limit * 2


def test_the_character_limit_counts_decomposed_hangul_as_one_character_each():
    limit = CoreSettings().max_cause_chars
    parse_output(llm_output(conflict_cause(detail=unicodedata.normalize("NFD", "가" * limit))), ALLOWED)


def test_rewriting_a_weak_cause_keeps_the_character_limit(core, rec):
    report_input = ready_input(core, rec)
    long_cause = llm_output(conflict_cause(detail="가" * 21))
    writer = LlmWriter(sc.FakeLlm(long_cause, long_cause, long_cause), settings=CoreSettings(max_cause_chars=20))
    current = parse_output(llm_output(conflict_cause()), ALLOWED).causes[0]
    assert writer.rewrite_cause(report_input, current, 1, ()).cause is None


def test_the_final_check_fix_keeps_the_character_limit(core, rec):
    report_input = ready_input(core, rec)
    writer = LlmWriter(sc.FakeLlm(), settings=CoreSettings(max_cause_chars=20))
    output = parse_output(llm_output(conflict_cause(refs=["a1"], meaningRefs=["a1"])), frozenset({"a1"}))
    draft = MoveDraft(report_input, Report("", "llm", (), output=output))
    sentence = MailSentence(1, "cause", "", ("a1",))
    with pytest.raises(InvalidOutput, match="longer than 20 characters"):
        MoveFinalCheck(writer).parse_fix(draft, sentence, llm_output(conflict_cause(detail="가" * 21)))


def test_the_character_limit_follows_the_setting_and_the_prompt_states_it():
    from briefing.move_prompts import system_prompt
    cause = conflict_cause(detail="가" * 30)
    parse_output(llm_output(cause), ALLOWED)
    with pytest.raises(InvalidOutput, match="longer than 20 characters"):
        parse_output(llm_output(cause), ALLOWED, settings=CoreSettings(max_cause_chars=20))
    assert "20자 이내" in system_prompt(CoreSettings(max_cause_chars=20))
    assert f"{CoreSettings().max_cause_chars}자 이내" in system_prompt()


def test_a_too_long_output_is_retried_by_the_writer(core, rec):
    long = llm_output(conflict_cause(detail="가" * 80))
    ok = llm_output(conflict_cause())
    client = sc.FakeLlm(long, ok)
    report = LlmWriter(client).write(ready_input(core, rec))
    assert len(report.failed_attempts) == 1 and "longer than" in report.failed_attempts[0].rejection and len(client.calls) == 2


def test_a_conflict_line_cites_given_refs_only_and_needs_them():
    cause = conflict_cause(conflict="보도가 엇갈립니다.", conflictRefs=["a1", "a2"])
    (parsed,) = parse_output(llm_output(cause), ALLOWED).causes
    assert parsed.conflict == "보도가 엇갈립니다." and parsed.conflict_refs == ("a1", "a2")
    assert parsed.cited() == ("a1", "a2")
    for bad, reason in ((conflict_cause(conflict="보도가 엇갈립니다.", conflictRefs=[]), "cites no"),
                        (conflict_cause(conflict="보도가 엇갈립니다.", conflictRefs=["zz"]), "were not given"),
                        (conflict_cause(conflict="", conflictRefs=["a1"]), "without conflict")):
        with pytest.raises(InvalidOutput, match=reason):
            parse_output(llm_output(bad), ALLOWED)


def test_output_without_conflict_fields_reads_as_no_conflict():
    cause = conflict_cause()
    del cause["conflict"], cause["conflictRefs"]
    (parsed,) = parse_output(llm_output(cause), ALLOWED).causes
    assert parsed.conflict is None and parsed.conflict_refs == ()


# ---- s1·s2·s3: 글자판 ----

def test_text_has_flow_then_causes_then_one_evidence_list_at_the_bottom_and_no_unchecked_section(core, rec):
    (event,), log = published(ready_input(core, rec))

    text = event.value["payload"]["text"]
    lines = text.splitlines()
    assert lines.index("오늘 흐름:") < lines.index("1. 금리 결정 영향") < lines.index("근거:")
    assert [l for l in lines if l.startswith("- 14:40") or l.startswith("- 15:30")] == [
        "- 14:40 -5.10% (중, 기록만)", "- 15:30 -7.00% (상, 메일 보냄)"]
    # 시각 차이는 그 원인 아래 한 줄이고 코드가 붙인 근거 번호를 인용한다
    assert "- 시각 차이: 보도에 따라 인하 폭이 0.25%p와 0.5%p로 엇갈립니다. [근거 1, 2]" in lines
    # 근거 목록은 한 번뿐이고 그 뒤에는 면책만 있다(다른 절 제목이 없다)
    assert text.count("근거:") == 1 and sum(1 for l in lines if re.match(r"^\s*근거\S*( 목록| 자료)?\s*:\s*$", l)) == 1
    after = lines[lines.index("근거:") + 1:]
    assert not any(l.endswith(":") or "오늘 흐름" in l for l in after if l.strip())
    assert "확인하지 못한 것" not in text


def test_flow_section_is_left_out_when_there_is_no_earlier_move(core, rec):
    (event,), _ = published(ready_input(core, rec, earlier=()))

    assert "오늘 흐름" not in event.value["payload"]["text"] and "오늘 흐름" not in event.value["payload"]["html"]


def test_unchecked_items_stay_in_the_work_log_only(core, rec):
    report_input = ready_input(core, rec)
    (event,), log = published(report_input)

    (entry,) = log.of_reason("published")
    assert entry.entry_type.value == "missing-items" and entry.data["missingItems"]
    assert "확인하지 못한 것" not in json.dumps(event.value, ensure_ascii=False)


def test_a_weak_cause_shows_no_weak_note_in_the_mail_but_its_shortfall_stays_in_the_work_log(core, rec):
    shortfall = "공시 원문이 더 필요합니다."
    weak = conflict_cause(weak=True, shortfall=shortfall)
    (event,), log = published(ready_input(core, rec), response=llm_output(weak))

    payload = event.value["payload"]
    assert "근거 약함" not in payload["text"] and "근거 약함" not in payload["html"]
    assert shortfall not in payload["text"] and shortfall not in payload["html"]
    assert any(shortfall in json.dumps(e.data, ensure_ascii=False) for e in log.entries)


# ---- s3: 오늘 흐름 ----

def run(at, today, level="mid", job_id="j", symbol=sc.SYMBOL):
    from briefing.job_log import MoveRun
    return MoveRun(job_id, symbol, at, today, "succeeded", None, None, today, level)


def test_earlier_moves_keep_same_symbol_before_this_move_in_time_order():
    runs = [run(T - timedelta(minutes=10), -6.0, "high", "b"), run(T - timedelta(minutes=50), -5.0, "mid", "a"),
            run(T, -7.0, "high", "same-time"), run(T + timedelta(minutes=3), -8.0, "high", "later"),
            run(T - timedelta(minutes=5), -1.0, "mid", "other-symbol", symbol="TEST99"),
            run(T - timedelta(minutes=20), -2.0, "mid", "this-job")]
    moves = earlier_moves(runs, sc.SYMBOL, T, "this-job")
    assert [(m.rate, m.level) for m in moves] == [(-5.0, "mid"), (-6.0, "high")]
    assert earlier_moves(None, sc.SYMBOL, T, "x") == () and earlier_moves([], sc.SYMBOL, T, "x") == ()


def test_flow_comes_from_the_work_log_and_goes_into_the_llm_input(core, rec):
    report_input = ready_input(core, rec)

    assert [(m.at, m.rate, m.level) for m in report_input.earlier_moves] == [
        (T - 80 * 60 * SEC, -5.1, "mid"), (T - 30 * 60 * SEC, -7.0, "high")]
    data = build_llm_input(report_input)
    assert data["todayFlow"] == [{"at": "14:40", "todayChangePercent": -5.1, "level": "mid"},
                                 {"at": "15:30", "todayChangePercent": -7.0, "level": "high"}]
    assert data["todayFlow"] == flow_input(report_input.earlier_moves)


def test_the_llm_input_has_no_flow_key_without_earlier_moves(core, rec):
    assert "todayFlow" not in build_llm_input(ready_input(core, rec, earlier=()))


def test_an_unreadable_work_log_leaves_the_flow_empty_and_the_mail_still_goes(core, rec):
    rec.log.fail_reads = True
    run_with_articles(core)
    answer_chart(core, day_points())

    assert rec.readied[0].earlier_moves == ()


# ---- s4: 장중 차트 ----

def test_chart_history_is_asked_from_nine_to_five_minutes_after_the_move_every_minute(core, rec):
    sc.start(core)

    chart = [q for q, _ in core.api.pending if q.stage == STAGE_CHART_HISTORY]
    (query,) = chart
    assert query.kind is QueryKind.HISTORY
    payload = query.payload
    assert (payload["since"], payload["until"], payload["intervalSeconds"], payload["maxPoints"]) == (
        T.replace(hour=9, minute=0, second=0).isoformat(), (T + sc.S.delta).isoformat(), 60, 500)
    # 근거용 시세 이력은 그대로 따로 나간다
    assert [q.payload["intervalSeconds"] for q, _ in core.api.pending if q.stage == "market.history"] == [10]


def test_the_report_waits_for_the_chart_history_and_goes_without_a_chart_when_it_fails(core, rec):
    run_with_articles(core)
    assert rec.readied == []
    fail_chart(core)

    (report_input,) = rec.readied
    assert report_input.chart_points == () and rec.log.failed == []
    # 차트가 없으면 글자판+HTML만 가고 작업 기록에 사유가 남는다
    (event,), log = published(report_input)
    payload = event.value["payload"]
    assert "images" not in payload and "cid:" not in payload["html"]
    (entry,) = log.of_reason("published")
    assert CHART_MISSING_ITEM in entry.data["missingItems"]


def test_chart_input_needs_two_points_and_the_previous_close_comes_from_todays_rate(core, rec):
    report_input = ready_input(core, rec)
    c = chart_input(report_input)
    assert [p[0] for p in c.points] == sorted(p[0] for p in c.points) and len(c.points) == len(day_points())
    assert [m.rate for m in c.earlier] == [-5.1, -7.0]
    assert chart_input(ReportInput_with(report_input, chart_points=day_points()[:1])) is None
    assert chart_input(ReportInput_with(report_input, chart_points=())) is None
    # 전일 종가: 현재가 / (1 + 오늘 등락률). 전일 종가 대비 유형은 이벤트의 기준가가 전일 종가다.
    r = ReportInput_with(report_input, price=9_300, today_change_percent=-7.0, detection_type="prevCloseHigh")
    assert prev_close_of(r) == pytest.approx(10_000)
    assert prev_close_of(ReportInput_with(report_input, detection_type="prevClose", previous_price=9_999)) == 9_999
    assert prev_close_of(ReportInput_with(report_input, today_change_percent=None, detection_type="window")) is None


def ReportInput_with(report_input, **changes):
    from dataclasses import replace
    return replace(report_input, **changes)


def test_the_chart_renders_a_png_for_a_full_day_and_for_the_bare_minimum(core, rec):
    report_input = ready_input(core, rec)
    png = render_move_chart(chart_input(report_input))
    assert png.startswith(PNG_SIGNATURE)
    two = chart_input(ReportInput_with(report_input, chart_points=day_points()[:2], earlier_moves=()))
    assert render_move_chart(two).startswith(PNG_SIGNATURE)


# ---- s5: html·images를 이벤트에 담기 ----

def test_the_event_carries_text_html_and_one_inline_chart_that_match(core, rec):
    (event,), _ = published(ready_input(core, rec))

    assert validate("briefing.generated.v1.schema.json", event.value) == []
    payload = event.value["payload"]
    assert payload["text"] and payload["html"].startswith("<html")
    (image,) = payload["images"]
    assert (image["contentId"], image["contentType"]) == (CHART_CONTENT_ID, "image/png")
    assert base64.b64decode(image["data"]).startswith(PNG_SIGNATURE)
    assert_cids_match(payload)
    # 글자판과 HTML이 같은 내용이다: 오늘 흐름·원인·시각 차이·근거가 모두 있다
    for part in ("오늘 흐름", "금리 결정 영향", "시각 차이", "근거:", "기사 a1", "기사 a2"):
        assert part in payload["html"]
    assert "확인하지 못한 것" not in payload["html"]


def test_a_percent_point_difference_is_not_painted_as_a_rate():
    from briefing.daily_html import _rate_spans
    painted = _rate_spans("코스피보다 0.25%p 높고 -1.05%p 낮으며 -2.10% 내렸습니다")
    assert "0.25%p 높고 -1.05%p 낮으며" in painted and painted.count("<span") == 1
    assert ">-2.10%</span>" in painted


def test_html_escapes_model_text_and_links_only_http_urls():
    from briefing.move_view import MoveView, MoveViewCause, MoveViewEvidence
    view = MoveView("[TEST01] <b>-7.0%</b>", ("09:10 -5.00% (중, 기록만)",), (),
                    (MoveViewCause("<script>x</script>", "a & b", (1,), "c", (1,)),),
                    evidence=(MoveViewEvidence(1, "제목 <i>", "javascript:alert(1)"),
                              MoveViewEvidence(2, "둘째", "https://news.example.com/a")), closing="면책")
    html = render_move_html(view, None)
    assert "<script>" not in html and "<i>" not in html and "&lt;script&gt;" in html
    assert "javascript:" not in html and 'href="https://news.example.com/a"' in html
    assert "<img" not in html


def test_an_oversized_chart_is_dropped_but_the_html_does_not_point_to_it(core, rec):
    small = CoreSettings(max_mail_image_bytes=1_000)
    (event,), log = published(ready_input(core, rec), settings=small)

    payload = event.value["payload"]
    assert "images" not in payload and "cid:" not in payload["html"]
    assert_cids_match(payload)
    (entry,) = log.of_reason("published")
    assert CHART_OVERSIZED_ITEM in entry.data["missingItems"]


def test_a_chart_that_keeps_the_event_over_its_limit_is_dropped_too(core, rec):
    tight = CoreSettings(max_mail_event_bytes=20_000)
    (event,), log = published(ready_input(core, rec), settings=tight)

    payload = event.value["payload"]
    assert "images" not in payload and payload["text"] and payload["html"]
    assert_cids_match(payload)
    assert CHART_OVERSIZED_ITEM in log.of_reason("published")[0].data["missingItems"]


def test_rule_based_reports_also_carry_html_and_the_chart(core, rec):
    sent, _ = sc.Recorder(), None
    stage = ReportStage(RuleBasedWriter(), sent, FakeJobLog(), clock=lambda: T + timedelta(minutes=20))
    stage.on_ready(ready_input(core, rec))

    (event,) = sent.sent
    assert_cids_match(event.value["payload"])
    assert validate("briefing.generated.v1.schema.json", event.value) == []
    assert "확인하지 못한 것" not in event.value["payload"]["text"]


# ---- 이벤트 크기: 차트 PNG를 담은 이벤트가 Kafka 메시지 한도 안에 든다 ----

def test_a_full_day_chart_event_fits_the_kafka_message_limit_with_room(core, rec):
    (event,), _ = published(ready_input(core, rec))

    size = len(json.dumps(event.value, ensure_ascii=False).encode("utf-8"))
    image = event.value["payload"]["images"][0]
    png_size = len(base64.b64decode(image["data"]))
    # 한 장 상한(300KiB)과 이벤트 상한(900KB, 데일리 판과 같은 값)이 생산자 한도(1,000,000바이트)보다 작고 실제 크기는 그보다 훨씬 작다
    assert png_size < CoreSettings().max_mail_image_bytes
    assert size < CoreSettings().max_mail_event_bytes < KAFKA_PRODUCER_MESSAGE_LIMIT
    assert size < KAFKA_PRODUCER_MESSAGE_LIMIT // 4


def test_the_worst_case_event_is_still_under_the_producer_limit_and_the_producer_keeps_its_default():
    s = CoreSettings()
    # 그림 한 장이 상한까지 크고 base64로 4/3배가 되어도, 본문 여유(64KiB)를 더해 생산자 한도 안이다.
    worst = s.max_mail_image_bytes * 4 // 3 + 64 * 1024
    assert worst < s.max_mail_event_bytes < KAFKA_PRODUCER_MESSAGE_LIMIT
    # 생산자 설정은 message.max.bytes를 건드리지 않아 librdkafka 기본(약 1MB)이다.
    assert "message.max.bytes" not in main.COMMON


# ---- s6: 가짜 값 메일 본보기와 가짜 LLM 리허설 ----

def sample_mail(core, rec):
    # 상 기준가 9,300(15:30에 낸 상 이후의 기준가) 대비 -7.53%, 전일 종가(10,000) 대비 -14.0%인 두 번째 상으로 본다.
    report_input = ReportInput_with(ready_input(core, rec), previous_price=9_300, price=8_600, change_percent=-7.53,
                                    today_change_percent=-14.0)
    (event,), _ = published(report_input)
    return event.value["payload"]


def test_the_fake_value_mail_files_match_what_the_code_makes(core, rec):
    payload = sample_mail(core, rec)
    assert (payload["todayChangePercent"], payload["baseKind"]) == (-14.0, "earlierMove")
    if os.environ.get("BRIEFING_WRITE_MOVE_MAIL_SAMPLES") == "1":
        (SAMPLES / "move_mail_high.txt").write_text(payload["text"] + "\n", encoding="utf-8")
        (SAMPLES / "move_mail_high.html").write_text(payload["html"] + "\n", encoding="utf-8")
        (SAMPLES / "move_mail_high.png").write_bytes(base64.b64decode(payload["images"][0]["data"]))
    assert (SAMPLES / "move_mail_high.txt").read_text(encoding="utf-8") == payload["text"] + "\n"
    assert (SAMPLES / "move_mail_high.html").read_text(encoding="utf-8") == payload["html"] + "\n"
    assert (SAMPLES / "move_mail_high.png").read_bytes().startswith(PNG_SIGNATURE)


def test_fake_llm_rehearsal_from_move_event_to_one_html_mail(core, rec):
    """변동 이벤트 → 자료 수집 → 가짜 LLM → 발행. 레벨 상 한 건이 글자판·HTML·그림이 든 메일 1통이 된다."""
    report_input = ready_input(core, rec)
    llm = sc.FakeLlm(FLOW_CAUSES)
    sent = sc.Recorder()
    ReportStage(LlmWriter(llm), sent, FakeJobLog(), clock=lambda: T + timedelta(minutes=20)).on_ready(report_input)

    (event,) = sent.sent
    # LLM은 한 번만 불렸고 오늘 흐름을 입력으로 받았으며 근거 번호는 코드가 붙인 것만 인용했다
    assert len(llm.calls) == 1
    assert json.loads(llm.calls[0][1])["todayFlow"][0]["at"] == "14:40"
    assert [e["articleId"] for e in event.value["payload"]["evidence"]] == ["a1", "a2"]
    payload = event.value["payload"]
    assert payload["text"] and payload["html"] and len(payload["images"]) == 1
    assert_cids_match(payload)
