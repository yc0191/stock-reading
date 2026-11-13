"""Claude 호출과 키·모델 판단. 무엇을 묻고(지시문, 출력 형식) 어떻게 검증할지는 부르는 쪽이 정한다."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-sonnet-5-5"
MODEL_ENV = "BRIEFING_LLM_MODEL"
API_KEY_ENV = "ANTHROPIC_API_KEY"
KEY_PLACEHOLDERS = frozenset({"", "change-me"})


@dataclass(frozen=True)
class LlmUsage:
    """응답 하나의 토큰 사용량. 캐시가 듣는지 작업 기록에서 바로 볼 수 있게 4개를 따로 둔다."""

    cache_write: int
    cache_read: int
    input: int
    output: int

    def as_record(self) -> dict[str, int]:
        return {"cacheWrite": self.cache_write, "cacheRead": self.cache_read, "input": self.input,
                "output": self.output}


def usage_record(usage: LlmUsage | None) -> dict[str, int] | None:
    """작업 기록에 넣을 값. 응답을 받지 못해 사용량이 없으면 None."""
    return usage.as_record() if usage is not None else None


@dataclass(frozen=True)
class LlmReply:
    text: str
    usage: LlmUsage | None


class LlmClient(Protocol):
    def complete(self, system: str, user: str, output_schema: dict[str, Any], addendum: str = "") -> LlmReply: ...


@dataclass(frozen=True)
class AcceptedAttempt:
    """받아들인 LLM 시도 하나. 작업 기록에 입력과 원래 응답을 그대로 남긴다."""

    attempt: int
    llm_input: str
    response: str
    usage: LlmUsage | None = None


class LlmCallError(Exception):
    """LLM 호출 실패. 키가 섞이지 않게 원래 예외의 종류 이름만 담는다."""

    def __init__(self, message: str, usage: LlmUsage | None = None) -> None:
        super().__init__(message)
        # 응답은 받았지만 쓸 수 없는 경우(중단·글 없음)에는 토큰이 이미 쓰였으므로 사용량을 함께 담는다.
        self.usage = usage


def attempt_usage(usage: LlmUsage | None, error: Exception) -> LlmUsage | None:
    # 출력 검사 오류처럼 사용량이 없는 예외도 받아야 하므로 속성이 없으면 None으로 본다.
    return usage or getattr(error, "usage", None)


def _read_usage(response: Any) -> LlmUsage | None:
    raw = getattr(response, "usage", None)
    if raw is None:
        return None

    def count(name: str) -> int:
        # SDK는 캐시 항목을 쓰지 않은 호출에서 None으로 줄 수 있다.
        return getattr(raw, name, None) or 0

    return LlmUsage(count("cache_creation_input_tokens"), count("cache_read_input_tokens"),
                    count("input_tokens"), count("output_tokens"))


@dataclass(frozen=True)
class LlmConfig:
    # 로그나 예외에 객체가 찍혀도 키가 나오지 않게 repr에서 뺀다.
    api_key: str = field(repr=False)
    model: str


def llm_config(env: Mapping[str, str]) -> LlmConfig | None:
    """키가 없으면 None. 부르는 쪽이 규칙 기반 대체로 바꾼다."""
    api_key = env.get(API_KEY_ENV, "").strip()
    # .env.example 의 자리표시 값을 그대로 복사해도 키가 없는 것으로 본다. 그대로 쓰면 모든 호출이 실패한다.
    if api_key in KEY_PLACEHOLDERS:
        return None
    return LlmConfig(api_key, env.get(MODEL_ENV, "").strip() or DEFAULT_MODEL)


class AnthropicClient:
    """공식 SDK로 Claude를 부른다. 키는 SDK 클라이언트 안에만 두고 이 객체의 필드나 repr에 남기지 않는다."""

    def __init__(self, api_key: str, model: str, max_tokens: int = 16000) -> None:
        import anthropic

        self._anthropic = anthropic
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def __repr__(self) -> str:
        return f"AnthropicClient(model={self.model!r})"

    def complete(self, system: str, user: str, output_schema: dict[str, Any], addendum: str = "") -> LlmReply:
        # 캐싱 표시는 지시문 블록에만 붙인다. 다시 쓰기 덧붙임은 별도 블록이라, 같은 종류의 처음 작성 호출과
        # 첫 블록이 같아 그 캐시를 그대로 읽는다.
        blocks: list[dict[str, Any]] = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
        if addendum:
            blocks.append({"type": "text", "text": addendum})
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=blocks,
                messages=[{"role": "user", "content": user}],
                output_config={"format": {"type": "json_schema", "schema": output_schema}},
            )
        except self._anthropic.APIError as error:
            # SDK 예외 문구에는 요청 정보가 섞일 수 있어 종류 이름만 남기고 원래 예외도 잇지 않는다.
            raise LlmCallError(f"LLM call failed: {type(error).__name__}") from None
        usage = _read_usage(response)
        # 모델과 사용량만 남긴다. 키와 입력 원문은 찍지 않는다.
        log.info("LLM call: model=%s stop=%s cache_write=%s cache_read=%s input=%s output=%s", self.model,
                 response.stop_reason, *(usage.as_record().values() if usage else ("n/a",) * 4))
        if response.stop_reason != "end_turn":
            raise LlmCallError(f"LLM stopped with {response.stop_reason}", usage)
        texts = [block.text for block in response.content if block.type == "text"]
        if not texts:
            raise LlmCallError("LLM returned no text", usage)
        return LlmReply(texts[0], usage)
