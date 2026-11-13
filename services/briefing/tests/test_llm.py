import types

import pytest

import logging

from briefing.llm import DEFAULT_MODEL, AnthropicClient, LlmCallError, LlmConfig, LlmUsage, llm_config
from fake_anthropic import FAKE_KEY, fake_sdk  # noqa: F401 - 픽스처

SCHEMA = {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}
VALID = '{"text": "ok"}'


# ---- 키·모델 판단 ----

@pytest.mark.parametrize("env", [{}, {"ANTHROPIC_API_KEY": ""}, {"ANTHROPIC_API_KEY": "  "},
                                 {"ANTHROPIC_API_KEY": "change-me"}])
def test_missing_or_placeholder_key_means_no_llm(env):
    assert llm_config(env) is None


def test_model_comes_from_env_with_sonnet_default():
    assert llm_config({"ANTHROPIC_API_KEY": FAKE_KEY}) == LlmConfig(FAKE_KEY, DEFAULT_MODEL)
    assert llm_config({"ANTHROPIC_API_KEY": FAKE_KEY, "BRIEFING_LLM_MODEL": " claude-opus-5-5 "}).model \
        == "claude-opus-5-5"
    assert DEFAULT_MODEL == "claude-sonnet-5-5"


def test_config_repr_hides_the_key():
    assert FAKE_KEY not in repr(llm_config({"ANTHROPIC_API_KEY": FAKE_KEY}))


# ---- 호출 ----

def test_sdk_error_is_replaced_without_message_or_cause(fake_sdk):
    client = AnthropicClient(FAKE_KEY, DEFAULT_MODEL)

    with pytest.raises(LlmCallError) as caught:
        client.complete("system", "user", SCHEMA)

    assert FAKE_KEY not in str(caught.value)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


def test_client_sends_model_system_and_the_given_json_schema_and_reads_the_text(fake_sdk):
    def ok(_kwargs):
        block = types.SimpleNamespace(type="text", text=VALID)
        return types.SimpleNamespace(stop_reason="end_turn", content=[types.SimpleNamespace(type="thinking"), block])

    fake_sdk.behaviour = ok
    client = AnthropicClient(FAKE_KEY, "claude-sonnet-5-5")

    assert client.complete("system", "user", SCHEMA).text == VALID
    [request] = client._client.messages.requests
    assert request["model"] == "claude-sonnet-5-5"
    assert request["system"] == [{"type": "text", "text": "system", "cache_control": {"type": "ephemeral"}}]
    assert request["messages"] == [{"role": "user", "content": "user"}]
    assert request["output_config"] == {"format": {"type": "json_schema", "schema": SCHEMA}}


def test_client_treats_refusal_or_truncation_as_a_failed_attempt(fake_sdk):
    fake_sdk.behaviour = lambda _kwargs: types.SimpleNamespace(stop_reason="refusal", content=[])
    client = AnthropicClient(FAKE_KEY, DEFAULT_MODEL)

    with pytest.raises(LlmCallError, match="refusal"):
        client.complete("system", "user", SCHEMA)


def test_client_repr_hides_the_key(fake_sdk):
    assert FAKE_KEY not in repr(AnthropicClient(FAKE_KEY, DEFAULT_MODEL))


def test_rewrite_addendum_goes_in_a_second_block_so_the_first_block_matches_the_original_call(fake_sdk):
    fake_sdk.behaviour = lambda _kwargs: types.SimpleNamespace(
        stop_reason="end_turn", content=[types.SimpleNamespace(type="text", text=VALID)])
    client = AnthropicClient(FAKE_KEY, DEFAULT_MODEL)

    client.complete("system", "user", SCHEMA)
    client.complete("system", "user", SCHEMA, "addendum")
    first, rewrite = client._client.messages.requests

    assert rewrite["system"][0] == first["system"][0]
    assert rewrite["system"][1] == {"type": "text", "text": "addendum"}
    assert len(rewrite["system"]) == 2


def test_search_query_call_also_carries_the_cache_mark(fake_sdk):
    from briefing.research import QUERY_SYSTEM_PROMPT

    fake_sdk.behaviour = lambda _kwargs: types.SimpleNamespace(
        stop_reason="end_turn", content=[types.SimpleNamespace(type="text", text=VALID)])
    client = AnthropicClient(FAKE_KEY, DEFAULT_MODEL)

    client.complete(QUERY_SYSTEM_PROMPT.format(max_queries=3), "user", SCHEMA)

    [request] = client._client.messages.requests
    assert [b.get("cache_control") for b in request["system"]] == [{"type": "ephemeral"}]


# ---- 사용량 ----

def _response(stop_reason="end_turn", usage=None, text=VALID):
    return types.SimpleNamespace(stop_reason=stop_reason, usage=usage,
                                 content=[types.SimpleNamespace(type="text", text=text)])


def _usage(write=None, read=None, input=0, output=0):
    return types.SimpleNamespace(cache_creation_input_tokens=write, cache_read_input_tokens=read,
                                 input_tokens=input, output_tokens=output)


def test_reply_carries_the_four_usage_counts_and_none_cache_counts_become_zero(fake_sdk):
    fake_sdk.behaviour = lambda _kwargs: _response(usage=_usage(write=1200, read=None, input=30, output=400))
    reply = AnthropicClient(FAKE_KEY, DEFAULT_MODEL).complete("system", "user", SCHEMA)

    assert reply.text == VALID
    assert reply.usage == LlmUsage(cache_write=1200, cache_read=0, input=30, output=400)
    assert reply.usage.as_record() == {"cacheWrite": 1200, "cacheRead": 0, "input": 30, "output": 400}


def test_response_without_usage_gives_none(fake_sdk):
    fake_sdk.behaviour = lambda _kwargs: _response()

    assert AnthropicClient(FAKE_KEY, DEFAULT_MODEL).complete("system", "user", SCHEMA).usage is None


def test_every_call_logs_one_line_with_model_and_usage_but_no_key_or_input(fake_sdk, caplog):
    fake_sdk.behaviour = lambda _kwargs: _response(usage=_usage(write=5, read=6, input=7, output=8))
    client = AnthropicClient(FAKE_KEY, "claude-sonnet-5-5")

    with caplog.at_level(logging.INFO, logger="briefing.llm"):
        client.complete("지시문 원문", "입력 원문 TEST-SECRET", SCHEMA)

    [line] = [r.getMessage() for r in caplog.records if r.name == "briefing.llm"]
    assert "claude-sonnet-5-5" in line and "cache_write=5" in line and "cache_read=6" in line
    assert "input=7" in line and "output=8" in line
    assert FAKE_KEY not in line and "TEST-SECRET" not in line and "지시문 원문" not in line


def test_stopped_response_still_reports_the_tokens_it_used(fake_sdk):
    fake_sdk.behaviour = lambda _kwargs: _response("max_tokens", _usage(write=0, read=9, input=3, output=16000))

    with pytest.raises(LlmCallError) as caught:
        AnthropicClient(FAKE_KEY, DEFAULT_MODEL).complete("system", "user", SCHEMA)

    assert caught.value.usage == LlmUsage(0, 9, 3, 16000)
