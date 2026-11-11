import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from briefing.jobs import ReportInput
from briefing.ports import FailureReason
from fake_job_log import FakeJobLog
from briefing.llm import DEFAULT_MODEL, LlmCallError
from briefing.report import (
    GENERATED_BY_LLM,
    GENERATED_BY_RULE,
    LLM_DISCLAIMER,
    LLM_INSUFFICIENT,
    OUTPUT_SCHEMA,
    SYSTEM_PROMPT,
    LlmWriter,
    ReportStage,
    RuleBasedWriter,
    build_llm_input,
    build_report_writer,
    unchecked_items,
)
from briefing.sources import Article, Background, EtfInfo, PricePoint
from contracts import validate
from fake_anthropic import FAKE_KEY, fake_sdk  # noqa: F401 - 픽스처

KST = timezone(timedelta(hours=9))
AS_OF = datetime(2026, 10, 20, 16, 0, 0, tzinfo=KST)
NOW = datetime(2026, 10, 20, 16, 1, 0, tzinfo=KST)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
# 실제 키 형태가 아닌 가짜 값. 로그·예외에 이 문자열이 나오면 안 된다.


def article(article_id, minutes_before, title="Sample headline"):
    return Article(article_id, title, f"{title} summary", AS_OF - timedelta(minutes=minutes_before),
                   "sample", f"https://news.example.com/articles/{article_id}")


ARTICLES = (article("a1", 90, "Rate decision"), article("a2", 30, "Fund outflow"), article("a3", -3, "Late note"))


def report_input(articles=ARTICLES, etf=None):
    return ReportInput(
        job_id=JOB_ID, symbol="TEST01", correlation_id="collect-1", as_of=AS_OF,
        previous_price=10_000, price=9_850, change_percent=-1.5,
        price_points=(PricePoint(AS_OF - timedelta(minutes=1), 10_000), PricePoint(AS_OF, 9_850)),
        articles=articles, etf=etf)


def cause(title, detail, refs, meaning, meaning_refs):
    return {"title": title, "detail": detail, "refs": refs, "meaning": meaning, "meaningRefs": meaning_refs}


def output(causes, insufficient=False):
    """(문장, 근거) 묶음은 같은 문장·근거로 구체 문장과 뜻 문장을 채운다. 5개짜리 묶음은 그대로 쓴다."""
    items = [cause(s, s, ids, s, ids) if len(c) == 2 else cause(*c) for c in causes for s, ids in [c[:2]]]
    return json.dumps({"causes": items, "insufficientEvidence": insufficient}, ensure_ascii=False)


VALID = output([("금리 결정", "금리 결정이 영향을 줬을 수 있습니다.", ["a1"], "같은 시기 금리 기사가 있었습니다.", ["a1"]),
                ("자금 유출", "자금 유출이 영향을 줬을 수 있습니다.", ["a2", "a1"], "유출 기사가 변동 30분 전에 나왔습니다.",
                 ["a2"])])


class FakeLlm:
    """정해 둔 응답을 차례로 돌려준다. 실제 API를 부르지 않는다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.schemas = []

    def complete(self, system, user, output_schema):
        self.calls.append((system, user))
        self.schemas.append(output_schema)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class Recorder:
    def __init__(self):
        self.items = []

    def send(self, event):
        self.items.append(event)

def stage(writer):
    published, job_log = Recorder(), FakeJobLog()
    return ReportStage(writer, published, job_log, clock=lambda: NOW), published.items, job_log


# ---- 정상 응답 ----

def test_valid_llm_output_is_published_with_code_written_headline_and_disclaimer():
    s, published, failures = stage(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input())

    assert failures.failed == []
    [event] = published
    assert event.topic == "briefing.generated" and event.key == "TEST01"
    payload = event.value["payload"]
    lines = payload["text"].splitlines()
    assert lines[0] == "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)"
    assert lines[2] == "1. 금리 결정"
    assert lines[3] == "- 금리 결정이 영향을 줬을 수 있습니다. [근거 1]"
    assert lines[4] == "- 같은 시기 금리 기사가 있었습니다. [근거 1]"
    assert lines[5].startswith("  근거 1. Rate decision — sample, 발행 10-20 14:30")
    assert lines[7] == "2. 자금 유출"
    assert lines[8] == "- 자금 유출이 영향을 줬을 수 있습니다. [근거 2, 1]"
    assert lines[9] == "- 유출 기사가 변동 30분 전에 나왔습니다. [근거 2]"
    assert lines[-1] == LLM_DISCLAIMER
    assert payload["generatedBy"] == GENERATED_BY_LLM == "llm"


def test_evidence_holds_only_cited_articles_with_id_and_url_and_passes_the_schema():
    s, published, _ = stage(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input())

    event = published[0].value
    assert [e["articleId"] for e in event["payload"]["evidence"]] == ["a1", "a2"]
    assert event["payload"]["evidence"][0]["url"] == "https://news.example.com/articles/a1"
    assert validate("briefing.generated.v1.schema.json", event) == []


def test_same_job_gets_the_same_briefing_event_id():
    first, _, _ = stage(LlmWriter(FakeLlm(VALID)))
    second, _, _ = stage(LlmWriter(FakeLlm(VALID)))
    a, b = Recorder(), Recorder()
    first.publisher, second.publisher = a, b

    first.on_ready(report_input())
    second.on_ready(report_input())

    assert a.items[0].value["eventId"] == b.items[0].value["eventId"]


# ---- 검증과 재호출 ----

def test_unknown_article_id_is_rejected_and_the_llm_is_called_again():
    llm = FakeLlm(output([("지어낸 기사가 영향을 줬을 수 있습니다.", ["a9"])]), VALID)
    s, published, failures = stage(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(llm.calls) == 2
    assert failures.failed == []
    payload = published[0].value["payload"]
    assert [e["articleId"] for e in payload["evidence"]] == ["a1", "a2"]
    assert "지어낸 기사" not in payload["text"]


def test_broken_json_is_rejected_and_the_llm_is_called_again():
    llm = FakeLlm('{"causes": [', VALID)
    s, published, failures = stage(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(llm.calls) == 2
    assert len(published) == 1 and failures.failed == []


@pytest.mark.parametrize("bad", [
    '[]',
    '{"causes": []}',
    '{"causes": [{"statement": "영향을 줬을 수 있습니다."}], "insufficientEvidence": false}',
    '{"causes": [{"statement": "영향을 줬을 수 있습니다.", "refs": []}], "insufficientEvidence": false}',
    '{"causes": [], "insufficientEvidence": false}',
    '{"causes": [], "insufficientEvidence": "no"}',
])
def test_malformed_shapes_are_rejected(bad):
    llm = FakeLlm(bad, VALID)
    s, published, _ = stage(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(llm.calls) == 2 and len(published) == 1


def test_three_failed_attempts_record_llm_error_and_publish_nothing():
    llm = FakeLlm("not json", output([("x", ["a9"])]), LlmCallError("LLM call failed: APIConnectionError"))
    s, published, failures = stage(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(llm.calls) == 3
    assert published == []
    [failure] = failures.failed
    assert failure.reason == FailureReason.LLM_ERROR
    assert (failure.job_id, failure.attempts) == (JOB_ID, 3)
    assert "APIConnectionError" in failure.data["detail"]


# ---- 근거 부족 ----

def test_insufficient_evidence_publishes_a_report_without_causes_or_evidence():
    llm = FakeLlm(output([], insufficient=True))
    s, published, failures = stage(LlmWriter(llm))

    s.on_ready(report_input(articles=()))

    assert failures.failed == []
    payload = published[0].value["payload"]
    assert payload["text"].splitlines()[1:] == [
        "", LLM_INSUFFICIENT, "", "확인하지 못한 것:", "- 기사: 기간 안에 받은 자료 없음",
        "- 미국 공시·미국 종목 등락률: 변동 종목에 미국 종목이 없어 조회하지 않음", LLM_DISCLAIMER]
    assert payload["evidence"] == []
    assert validate("briefing.generated.v1.schema.json", published[0].value) == []


def test_statements_sent_with_insufficient_evidence_are_not_used():
    llm = FakeLlm(output([("억지 원인이 영향을 줬을 수 있습니다.", ["a1"])], insufficient=True))
    s, published, _ = stage(LlmWriter(llm))

    s.on_ready(report_input())

    payload = published[0].value["payload"]
    assert "억지 원인" not in payload["text"]
    assert payload["evidence"] == []


# ---- LLM 입력 ----

def test_llm_input_has_only_public_move_and_article_fields():
    data = build_llm_input(report_input())

    assert set(data) == {"symbol", "direction", "changePercent", "moveAt", "priceHistoryStatus", "priceHistory",
                         "articlesStatus", "articles", "filingsStatus", "filings", "overseasCloseRatesStatus",
                         "overseasCloseRates", "overseasCloseRatesMissing", "background", "figures"}
    # 배경 자료가 없으면 꺼낸 숫자와 비교 숫자는 모두 없음이고 번호가 붙지 않는다.
    assert all(f == {"name": f["name"], "status": "없음"} for f in data["figures"])
    assert data["direction"] == "하락" and data["changePercent"] == -1.5
    assert data["moveAt"] == "2026-10-20T16:00:00+09:00"
    assert data["priceHistory"] == [{"at": "2026-10-20T15:59:00+09:00", "price": 10_000},
                                    {"at": "2026-10-20T16:00:00+09:00", "price": 9_850}]
    assert set(data["articles"][0]) == {"ref", "articleId", "title", "summary", "source", "publishedAt",
                                        "minutesFromMove"}
    assert data["articles"][0]["ref"] == data["articles"][0]["articleId"] == "a1"
    # 개인 금액·수량, 계좌 같은 값은 키 이름으로도 들어가지 않는다.
    text = json.dumps(data).lower()
    for word in ("quantity", "amount", "holding", "account", "position", "email", "url"):
        assert word not in text


def test_minutes_from_move_are_integers_negative_before_and_positive_after():
    late = article("a4", -1, "One second late")
    late = Article(late.article_id, late.title, late.summary, AS_OF + timedelta(seconds=1), late.publisher, late.url)
    early = Article("a5", "Just before", "s", AS_OF - timedelta(seconds=1), "sample", "https://news.example.com/a5")

    data = build_llm_input(report_input(articles=ARTICLES + (late, early)))

    minutes = {a["articleId"]: a["minutesFromMove"] for a in data["articles"]}
    assert minutes == {"a1": -90, "a2": -30, "a3": 3, "a4": 0, "a5": -1}
    assert all(type(m) is int for m in minutes.values())


def test_llm_receives_the_input_json_and_the_rules():
    llm = FakeLlm(VALID)
    LlmWriter(llm).write(report_input())

    system, user = llm.calls[0]
    assert json.loads(user) == build_llm_input(report_input())
    assert system == SYSTEM_PROMPT
    # 공통 클라이언트는 출력 형식을 정하지 않는다. 급변동 형식은 작성기가 넘긴다.
    assert llm.schemas == [OUTPUT_SCHEMA]


def test_system_prompt_forbids_certainty_forced_causes_and_investment_advice():
    assert "단정하지 않습니다" in SYSTEM_PROMPT
    assert "영향을 줬을 수 있다" in SYSTEM_PROMPT
    assert "insufficientEvidence를 true" in SYSTEM_PROMPT
    assert "억지로 원인을 만들지 않습니다" in SYSTEM_PROMPT
    assert "투자 조언" in SYSTEM_PROMPT


# ---- 키가 없을 때 ----

@pytest.mark.parametrize("env", [{}, {"ANTHROPIC_API_KEY": ""}, {"ANTHROPIC_API_KEY": "change-me"}])
def test_missing_or_placeholder_key_falls_back_to_rule_based(env):
    def must_not_build(*_args):
        raise AssertionError("LLM client must not be built without a key")

    assert isinstance(build_report_writer(env, client_factory=must_not_build), RuleBasedWriter)


def test_without_a_key_startup_warns_and_uses_rule_based_reports(caplog):
    def must_not_build(*_args):
        raise AssertionError("LLM client must not be built without a key")

    with caplog.at_level(logging.WARNING, logger="briefing.report"):
        writer = build_report_writer({"ANTHROPIC_API_KEY": "  "}, client_factory=must_not_build)

    assert isinstance(writer, RuleBasedWriter)
    assert "ANTHROPIC_API_KEY is not set" in caplog.text
    s, published, _ = stage(writer)
    s.on_ready(report_input())
    payload = published[0].value["payload"]
    assert payload["generatedBy"] == GENERATED_BY_RULE == "rule-based"
    assert payload["text"].splitlines()[0] == "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)"
    assert "근거 후보:" in payload["text"]
    assert validate("briefing.generated.v1.schema.json", published[0].value) == []


def test_rule_based_report_lists_the_latest_articles_first():
    report = RuleBasedWriter(limit=2).write(report_input())

    assert [a.article_id for a in report.evidence] == ["a3", "a2"]


def test_model_comes_from_env_with_sonnet_default():
    built = []

    def factory(key, model):
        built.append(model)
        return FakeLlm()

    build_report_writer({"ANTHROPIC_API_KEY": FAKE_KEY}, client_factory=factory)
    build_report_writer({"ANTHROPIC_API_KEY": FAKE_KEY, "BRIEFING_LLM_MODEL": "claude-opus-5-5"}, client_factory=factory)

    assert built == [DEFAULT_MODEL, "claude-opus-5-5"]
    assert DEFAULT_MODEL == "claude-sonnet-5-5"


# ---- 키 노출 ----

def test_key_never_appears_in_logs_errors_or_failure_records(fake_sdk, caplog):
    with caplog.at_level(logging.DEBUG):
        writer = build_report_writer({"ANTHROPIC_API_KEY": FAKE_KEY})
        s, published, failures = stage(writer)
        s.on_ready(report_input())

    assert published == []
    [failure] = failures.failed
    assert failure.reason == FailureReason.LLM_ERROR and failure.attempts == 3
    assert FAKE_KEY not in caplog.text
    assert FAKE_KEY not in failure.data["detail"]
    assert FAKE_KEY not in repr(writer)


# ---- 원인 묶음 본문 ----

def report_input_with(**changes):
    from dataclasses import replace
    return replace(report_input(), **changes)


def test_each_cause_shows_title_detail_meaning_and_the_cited_article_link():
    s, published, _ = stage(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input())

    text = published[0].value["payload"]["text"]
    assert "https://news.example.com/articles/a1" in text and "https://news.example.com/articles/a2" in text
    # 두 원인 모두 a1을 인용하므로 a1은 원인마다 다시 보이고, 번호는 리포트 전체에서 같다.
    assert text.count("  근거 1. Rate decision") == 2
    assert text.count("  근거 2. Fund outflow") == 1
    # 숫자만 나열하는 칸은 없다.
    for heading in ("요약", "가격 흐름", "시장 상황", "직전 장"):
        assert f"{heading}:" not in text


def test_report_ends_with_what_could_not_be_checked_then_the_disclaimer():
    s, published, _ = stage(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input_with(history_truncated=True, articles_truncated=True))

    lines = published[0].value["payload"]["text"].splitlines()
    unchecked = lines.index("확인하지 못한 것:")
    assert lines[unchecked + 1:] == [
        "- 시세 이력: 상한 때문에 일부만 받음",
        "- 기사: 상한 때문에 일부만 받음",
        "- 미국 공시·미국 종목 등락률: 변동 종목에 미국 종목이 없어 조회하지 않음",
        LLM_DISCLAIMER,
    ]


def test_unchecked_world_index_items_use_continent_names_and_keep_unknown_codes():
    background = tuple(Background("world-index", code, "missing") for code in ("C", "S", "E", "X"))

    items = unchecked_items(report_input_with(background=background))

    for line in ("세계지수 아메리카: 기간 안에 받은 자료 없음", "세계지수 아시아: 기간 안에 받은 자료 없음",
                 "세계지수 유럽: 기간 안에 받은 자료 없음", "세계지수 X: 기간 안에 받은 자료 없음"):
        assert line in items
    for code in ("C", "S", "E"):
        assert f"세계지수 {code}:" not in " ".join(items)


@pytest.mark.parametrize("detected_seconds,detection,expected", [
    (185, "window", "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850), 3분 사이"),
    (20, "window", "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850), 1분 안에"),
    (600, "prevClose", "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850), 전일 종가 대비"),
    (None, None, "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)"),
])
def test_headline_says_how_many_minutes_the_move_took(detected_seconds, detection, expected):
    detected = AS_OF + timedelta(seconds=detected_seconds) if detected_seconds is not None else None
    s, published, _ = stage(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input_with(detected_at=detected, detection_type=detection))

    assert published[0].value["payload"]["text"].splitlines()[0] == expected


@pytest.mark.parametrize("field,value", [
    ("title", "  "), ("detail", ""), ("meaning", " "), ("refs", []), ("meaningRefs", []),
    ("meaningRefs", ["a9"]), ("meaning", None),
])
def test_empty_sentences_and_uncited_or_unknown_refs_are_rejected(field, value):
    bad = json.loads(VALID)
    bad["causes"][0][field] = value
    llm = FakeLlm(json.dumps(bad, ensure_ascii=False), VALID)
    s, published, _ = stage(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(llm.calls) == 2 and len(published) == 1


def test_output_schema_asks_for_cause_bundles_with_refs_for_each_sentence():
    item = OUTPUT_SCHEMA["properties"]["causes"]["items"]
    assert set(item["required"]) == {"title", "detail", "refs", "meaning", "meaningRefs", "weak", "shortfall"}
    assert OUTPUT_SCHEMA["properties"]["insufficientEvidence"] == {"type": "boolean"}


def test_system_prompt_asks_for_concrete_details_from_the_material_and_meaning_of_given_numbers():
    assert '"자료에 없음"' in SYSTEM_PROMPT
    assert "기사 제목·요약과 공시" in SYSTEM_PROMPT
    assert "figures" in SYSTEM_PROMPT and "differencePoint" in SYSTEM_PROMPT
    assert "숫자를 새로 계산하지 않습니다" in SYSTEM_PROMPT
    assert "meaningRefs" in SYSTEM_PROMPT


def test_rule_based_report_has_the_same_shape_with_links_unchecked_items_and_disclaimer():
    report = RuleBasedWriter().write(report_input())

    lines = report.text.splitlines()
    assert lines[0] == "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)"
    assert "원인은 추정하지 않고" in lines[2]
    assert "https://news.example.com/articles/a3" in report.text
    assert "확인하지 못한 것:" in lines
    assert lines[-1].startswith("※ 투자 조언이 아니며")


# ---- 작업 기록 (LLM) ----

def stage_with_log(writer, job_log=None):
    published, job_log = Recorder(), job_log or FakeJobLog()
    s = ReportStage(writer, published, job_log, clock=lambda: NOW)
    return s, published.items, job_log


def filled(record, allowed_empty=("error_type", "attempts", "sentence_no", "round", "before", "after",
                                   "failure_reason", "generated_by", "final_text", "result_event_id", "weak_count",
                                   "research_count")):
    from dataclasses import fields
    return all(getattr(record, f.name) not in (None, "", {}, []) for f in fields(record)
               if f.name not in allowed_empty)


def test_rejected_llm_attempts_are_recorded_even_when_the_report_finally_succeeds():
    from briefing.job_log import REASON_LLM_REJECTED, STAGE_LLM, EntryType, JobLogEntry
    bad = output([("지어낸 기사가 영향을 줬을 수 있습니다.", ["a9"])])
    llm = FakeLlm(bad, VALID)
    s, published, job_log = stage_with_log(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(published) == 1 and job_log.failed == []
    (record,) = job_log.of_type(EntryType.LLM_REJECTED)
    assert isinstance(record, JobLogEntry) and filled(record)
    assert (record.job_id, record.stage, record.reason, record.attempts) == (
        JOB_ID, STAGE_LLM, REASON_LLM_REJECTED, 1)
    assert record.sentence_no is None and record.round is None
    assert record.error_type == "InvalidOutput"
    assert record.data["llmInput"] == llm.calls[0][1]
    assert record.data["llmResponse"] == bad
    assert "not given" in record.data["rejection"]


def test_the_accepted_llm_attempt_is_recorded_with_its_input_and_original_response():
    from briefing.job_log import REASON_LLM_ACCEPTED, EntryType
    bad = output([("지어낸 기사가 영향을 줬을 수 있습니다.", ["a9"])])
    llm = FakeLlm(bad, VALID)
    s, _, job_log = stage_with_log(LlmWriter(llm))

    s.on_ready(report_input())

    (accepted,) = job_log.of_type(EntryType.REPORT_WRITTEN)
    assert (accepted.reason, accepted.attempts, accepted.error_type) == (REASON_LLM_ACCEPTED, 2, None)
    assert accepted.data["llmInput"] == llm.calls[1][1] and accepted.data["llmResponse"] == VALID
    assert accepted.data["rejection"] is None


def test_a_published_report_leaves_one_result_line_with_text_evidence_missing_items_and_generation_method():
    from briefing.job_log import EntryType, GeneratedBy, RunStatus
    s, published, job_log = stage_with_log(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input())

    (result,) = job_log.results()
    event = published[0]
    payload = event.value["payload"]
    # 작업 한 줄(job_run)에는 끝 상태, 최종 본문, 작성 방식, 메일의 브리핑 ID가 적힌다.
    assert (result.job_id, result.status, result.generated_by) == (JOB_ID, RunStatus.SUCCEEDED, GeneratedBy.LLM)
    assert result.final_text == payload["text"] and result.result_event_id == event.value["eventId"]
    assert (result.weak_count, result.research_count, result.failure_reason) == (0, 0, None)
    # 빠진 자료와 근거 목록은 일 줄 하나로 남는다.
    (missing,) = job_log.of_type(EntryType.MISSING_ITEMS)
    assert missing.data["evidence"] == payload["evidence"]
    assert missing.data["missingItems"] == ["미국 공시·미국 종목 등락률: 변동 종목에 미국 종목이 없어 조회하지 않음"]


def test_rule_based_report_also_leaves_a_result_line_but_no_llm_success_line():
    from briefing.job_log import EntryType, GeneratedBy
    s, published, job_log = stage_with_log(RuleBasedWriter())

    s.on_ready(report_input())

    (result,) = job_log.results()
    assert result.generated_by == GeneratedBy.RULE_BASED and len(published) == 1
    assert job_log.of_type(EntryType.REPORT_WRITTEN) == []


def test_three_failed_llm_attempts_give_one_record_per_attempt_a_final_failure_and_a_failed_result():
    from briefing.job_log import REASON_LLM_CALL_FAILED, REASON_LLM_REJECTED, EntryType, RunStatus
    llm = FakeLlm("not json", output([("x", ["a9"])]), LlmCallError("LLM call failed: APIConnectionError"))
    s, published, job_log = stage_with_log(LlmWriter(llm))

    s.on_ready(report_input())

    assert published == []
    assert [r.reason for r in job_log.entries] == [REASON_LLM_REJECTED, REASON_LLM_REJECTED, REASON_LLM_CALL_FAILED,
                                                   "llm_error"]
    assert job_log.entries[-1].entry_type == EntryType.STEP_FAILED
    assert all(filled(r) for r in job_log.entries)
    assert job_log.entries[2].data["llmResponse"].startswith("(응답 없음")
    final = job_log.entries[3]
    assert final.attempts == 3 and len(final.data["failedAttempts"]) == 3
    assert final.data["missingItems"] == ["미국 공시·미국 종목 등락률: 변동 종목에 미국 종목이 없어 조회하지 않음"]
    # 작업 한 줄은 실패로 끝난다.
    (result,) = job_log.results()
    assert (result.status, result.failure_reason) == (RunStatus.FAILED, "llm_error")


def test_report_is_not_published_when_its_log_could_not_be_saved(caplog):
    s, published, job_log = stage_with_log(LlmWriter(FakeLlm(VALID)), FakeJobLog(fail=True))

    with caplog.at_level(logging.ERROR, logger="briefing.job_log"):
        s.on_ready(report_input())

    assert published == []
    assert "result was not saved; result event not published" in caplog.text
    assert "TEST01" not in caplog.text


def test_report_is_not_published_when_an_earlier_line_of_the_job_was_lost():
    class LosesFirstLine(FakeJobLog):
        def record(self, entry):
            if not self.entries and not self._failed:
                self._failed.add(entry.job_id)
                return False
            return super().record(entry)

    bad = output([("지어낸 기사가 영향을 줬을 수 있습니다.", ["a9"])])
    s, published, job_log = stage_with_log(LlmWriter(FakeLlm(bad, VALID)), LosesFirstLine())

    s.on_ready(report_input())

    assert published == [] and len(job_log.results()) == 1  # 작업 끝 줄은 남기되 이벤트는 내보내지 않는다


def test_failure_records_and_their_logs_carry_no_api_key(fake_sdk, caplog):
    with caplog.at_level(logging.DEBUG):
        writer = build_report_writer({"ANTHROPIC_API_KEY": FAKE_KEY})
        s, _, job_log = stage_with_log(writer)
        s.on_ready(report_input())

    # 시도 세 줄과 끝내 실패한 줄, 그리고 작업 끝 줄
    assert len(job_log.entries) == 4 and len(job_log.results()) == 1
    assert FAKE_KEY not in repr(job_log.entries) + repr(job_log.results())
    assert "TEST01" not in caplog.text


# ---- 종목 이름 ----

def etf_named(name):
    return EtfInfo(members=(), members_truncated=False, name=name)


@pytest.mark.parametrize("name, headline, event_name", [
    ("TEST 샘플 ETF", "[TEST 샘플 ETF(TEST01)] 16:00 기준 하락 -1.50% (10,000 → 9,850)", "TEST 샘플 ETF"),
    ("  TEST 샘플 ETF ", "[TEST 샘플 ETF(TEST01)] 16:00 기준 하락 -1.50% (10,000 → 9,850)", "TEST 샘플 ETF"),
    (None, "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)", None),
    ("", "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)", None),
    ("   ", "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)", None),
])
@pytest.mark.parametrize("writer", [lambda: LlmWriter(FakeLlm(VALID)), RuleBasedWriter])
def test_headline_and_event_carry_the_etf_name_only_when_known(writer, name, headline, event_name):
    s, published, _ = stage(writer())

    s.on_ready(report_input(etf=etf_named(name)))

    payload = published[0].value["payload"]
    assert payload["text"].splitlines()[0] == headline
    assert payload.get("name") == event_name
    assert ("name" in payload) == (event_name is not None)
    assert validate("briefing.generated.v1.schema.json", published[0].value) == []


def test_headline_uses_the_symbol_alone_when_the_response_has_no_etf_info():
    s, published, _ = stage(RuleBasedWriter())

    s.on_ready(report_input(etf=None))

    payload = published[0].value["payload"]
    assert payload["text"].startswith("[TEST01] 16:00")
    assert "name" not in payload
