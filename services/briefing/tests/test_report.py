import json
import logging
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from briefing.jobs import Article, FailureReason, PricePoint, ReportInput
from briefing.report import (
    DEFAULT_MODEL,
    GENERATED_BY_LLM,
    GENERATED_BY_RULE,
    LLM_DISCLAIMER,
    LLM_INSUFFICIENT,
    SYSTEM_PROMPT,
    AnthropicClient,
    LlmCallError,
    LlmWriter,
    ReportStage,
    RuleBasedWriter,
    build_llm_input,
    build_report_writer,
)
from contracts import validate

KST = timezone(timedelta(hours=9))
AS_OF = datetime(2026, 10, 20, 16, 0, 0, tzinfo=KST)
NOW = datetime(2026, 10, 20, 16, 1, 0, tzinfo=KST)
JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13"
# 실제 키 형태가 아닌 가짜 값. 로그·예외에 이 문자열이 나오면 안 된다.
FAKE_KEY = "fake-anthropic-key-for-tests-0123456789"


def article(article_id, minutes_before, title="Sample headline"):
    return Article(article_id, title, f"{title} summary", AS_OF - timedelta(minutes=minutes_before),
                   "sample", f"https://news.example.com/articles/{article_id}")


ARTICLES = (article("a1", 90, "Rate decision"), article("a2", 30, "Fund outflow"), article("a3", -3, "Late note"))


def report_input(articles=ARTICLES):
    return ReportInput(
        job_id=JOB_ID, symbol="TEST01", correlation_id="collect-1", as_of=AS_OF,
        previous_price=10_000, price=9_850, change_percent=-1.5,
        price_points=(PricePoint(AS_OF - timedelta(minutes=1), 10_000), PricePoint(AS_OF, 9_850)),
        articles=articles)


def output(causes, insufficient=False):
    return json.dumps({"causes": [{"statement": s, "refs": ids} for s, ids in causes],
                       "insufficientEvidence": insufficient}, ensure_ascii=False)


VALID = output([("금리 결정이 영향을 줬을 수 있습니다.", ["a1"]), ("자금 유출이 영향을 줬을 수 있습니다.", ["a2", "a1"])])


class FakeLlm:
    """정해 둔 응답을 차례로 돌려준다. 실제 API를 부르지 않는다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, system, user):
        self.calls.append((system, user))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class Recorder:
    def __init__(self):
        self.items = []

    def send(self, event):
        self.items.append(event)

    def record(self, failure):
        self.items.append(failure)


def stage(writer):
    published, failures = Recorder(), Recorder()
    return ReportStage(writer, published, failures, clock=lambda: NOW), published.items, failures.items


# ---- 정상 응답 ----

def test_valid_llm_output_is_published_with_code_written_headline_and_disclaimer():
    s, published, failures = stage(LlmWriter(FakeLlm(VALID)))

    s.on_ready(report_input())

    assert failures == []
    [event] = published
    assert event.topic == "briefing.generated" and event.key == "TEST01"
    payload = event.value["payload"]
    lines = payload["text"].splitlines()
    assert lines[0] == "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)"
    assert lines[1] == "- 금리 결정이 영향을 줬을 수 있습니다. [근거 1]"
    assert lines[2] == "- 자금 유출이 영향을 줬을 수 있습니다. [근거 2, 1]"
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
    assert failures == []
    payload = published[0].value["payload"]
    assert [e["articleId"] for e in payload["evidence"]] == ["a1", "a2"]
    assert "지어낸 기사" not in payload["text"]


def test_broken_json_is_rejected_and_the_llm_is_called_again():
    llm = FakeLlm('{"causes": [', VALID)
    s, published, failures = stage(LlmWriter(llm))

    s.on_ready(report_input())

    assert len(llm.calls) == 2
    assert len(published) == 1 and failures == []


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
    [failure] = failures
    assert failure.reason is FailureReason.LLM_ERROR
    assert (failure.job_id, failure.symbol, failure.attempts) == (JOB_ID, "TEST01", 3)
    assert "APIConnectionError" in failure.detail


# ---- 근거 부족 ----

def test_insufficient_evidence_publishes_a_report_without_causes_or_evidence():
    llm = FakeLlm(output([], insufficient=True))
    s, published, failures = stage(LlmWriter(llm))

    s.on_ready(report_input(articles=()))

    assert failures == []
    payload = published[0].value["payload"]
    assert payload["text"].splitlines()[1:] == [LLM_INSUFFICIENT, LLM_DISCLAIMER]
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
                         "overseasCloseRates", "overseasCloseRatesMissing", "background"}
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

@pytest.fixture
def fake_sdk(monkeypatch):
    """anthropic 모듈을 가짜로 바꿔 실제 API를 부르지 않는다. 예외 문구에 일부러 키를 넣는다."""
    module = types.ModuleType("anthropic")

    class APIError(Exception):
        pass

    class APIConnectionError(APIError):
        pass

    class Messages:
        def __init__(self, behaviour):
            self.behaviour = behaviour
            self.requests = []

        def create(self, **kwargs):
            self.requests.append(kwargs)
            return self.behaviour(kwargs)

    class Anthropic:
        def __init__(self, api_key):
            self.api_key = api_key
            self.messages = Messages(module.behaviour)

    def fail(_kwargs):
        raise APIConnectionError(f"connection failed with x-api-key: {FAKE_KEY}")

    module.APIError = APIError
    module.Anthropic = Anthropic
    module.behaviour = fail
    monkeypatch.setitem(sys.modules, "anthropic", module)
    return module


def test_key_never_appears_in_logs_errors_or_failure_records(fake_sdk, caplog):
    with caplog.at_level(logging.DEBUG):
        writer = build_report_writer({"ANTHROPIC_API_KEY": FAKE_KEY})
        s, published, failures = stage(writer)
        s.on_ready(report_input())

    assert published == []
    [failure] = failures
    assert failure.reason is FailureReason.LLM_ERROR and failure.attempts == 3
    assert FAKE_KEY not in caplog.text
    assert FAKE_KEY not in failure.detail
    assert FAKE_KEY not in repr(writer)


def test_sdk_error_is_replaced_without_message_or_cause(fake_sdk):
    client = AnthropicClient(FAKE_KEY, DEFAULT_MODEL)

    with pytest.raises(LlmCallError) as caught:
        client.complete("system", "user")

    assert FAKE_KEY not in str(caught.value)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


def test_client_sends_model_system_and_json_schema_and_reads_the_text(fake_sdk):
    def ok(_kwargs):
        block = types.SimpleNamespace(type="text", text=VALID)
        return types.SimpleNamespace(stop_reason="end_turn", content=[types.SimpleNamespace(type="thinking"), block])

    fake_sdk.behaviour = ok
    client = AnthropicClient(FAKE_KEY, "claude-sonnet-5-5")

    assert client.complete("system", "user") == VALID
    [request] = client._client.messages.requests
    assert request["model"] == "claude-sonnet-5-5"
    assert request["system"] == "system"
    assert request["messages"] == [{"role": "user", "content": "user"}]
    assert request["output_config"]["format"]["type"] == "json_schema"


def test_client_treats_refusal_or_truncation_as_a_failed_attempt(fake_sdk):
    fake_sdk.behaviour = lambda _kwargs: types.SimpleNamespace(stop_reason="refusal", content=[])
    client = AnthropicClient(FAKE_KEY, DEFAULT_MODEL)

    with pytest.raises(LlmCallError, match="refusal"):
        client.complete("system", "user")
