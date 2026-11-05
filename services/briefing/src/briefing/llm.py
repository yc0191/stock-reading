"""Claude 호출과 키·모델 판단. 무엇을 묻고(지시문, 출력 형식) 어떻게 검증할지는 부르는 쪽이 정한다."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

DEFAULT_MODEL = "claude-sonnet-5-5"
MODEL_ENV = "BRIEFING_LLM_MODEL"
API_KEY_ENV = "ANTHROPIC_API_KEY"
KEY_PLACEHOLDERS = frozenset({"", "change-me"})


class LlmClient(Protocol):
    def complete(self, system: str, user: str, output_schema: dict[str, Any]) -> str: ...


@dataclass(frozen=True)
class AcceptedAttempt:
    """받아들인 LLM 시도 하나. 작업 기록에 입력과 원래 응답을 그대로 남긴다."""

    attempt: int
    llm_input: str
    response: str


class LlmCallError(Exception):
    """LLM 호출 실패. 키가 섞이지 않게 원래 예외의 종류 이름만 담는다."""


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

    def complete(self, system: str, user: str, output_schema: dict[str, Any]) -> str:
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
                output_config={"format": {"type": "json_schema", "schema": output_schema}},
            )
        except self._anthropic.APIError as error:
            # SDK 예외 문구에는 요청 정보가 섞일 수 있어 종류 이름만 남기고 원래 예외도 잇지 않는다.
            raise LlmCallError(f"LLM call failed: {type(error).__name__}") from None
        if response.stop_reason != "end_turn":
            raise LlmCallError(f"LLM stopped with {response.stop_reason}")
        texts = [block.text for block in response.content if block.type == "text"]
        if not texts:
            raise LlmCallError("LLM returned no text")
        return texts[0]
