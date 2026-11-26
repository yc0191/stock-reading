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


class CachedUser(str):
    """앞부분을 캐시할 수 있는 사용자 입력. 글은 그대로 하나의 문자열(JSON)이고 breaks는 블록을 나누는 글자 위치다.

    같은 작업의 호출들이 앞부분(처음 작성의 근거 입력, 찾은 순서대로 덧붙인 재검색 기사)을 똑같이 보내므로, 그 경계에서
    블록을 나눠 캐시 표시를 붙이면 앞 호출이 쓴 캐시를 다음 호출이 읽는다. str을 이어 받아 가짜 클라이언트·작업 기록은
    지금처럼 글 하나로 다룬다(잘라 붙이면 보통 str이 되어 캐시 표시 없이 보낸다).
    """

    breaks: tuple[int, ...]

    def __new__(cls, text: str, breaks: tuple[int, ...] = ()) -> CachedUser:
        made = super().__new__(cls, text)
        made.breaks = tuple(b for b in breaks if 0 < b < len(text))
        return made


def user_blocks(user: str, addendum: str = "") -> list[dict[str, Any]]:
    """CachedUser를 경계마다 블록으로 나눈다. 캐시 표시는 첫 경계(처음 작성의 근거 입력 끝)와 마지막 경계에만 붙인다.

    캐시 표시는 요청당 4개까지이고(지시문에 하나), 표시 사이의 경계는 다음 호출이 앞 호출의 캐시를 찾는 자리로만 쓴다.
    덧붙임은 근거 뒤 마지막 블록으로 보낸다. 지시문 블록 뒤에 두면 처음 작성과 앞부분이 달라져 근거 캐시를 읽지 못한다.
    """
    breaks = getattr(user, "breaks", ())
    cuts = (0, *breaks, len(user))
    blocks: list[dict[str, Any]] = []
    for n, (start, end) in enumerate(zip(cuts, cuts[1:])):
        block: dict[str, Any] = {"type": "text", "text": str(user[start:end])}
        if breaks and n in (0, len(breaks) - 1):
            block["cache_control"] = {"type": "ephemeral"}
        blocks.append(block)
    if addendum:
        blocks.append({"type": "text", "text": addendum})
    return blocks


class LlmClient(Protocol):
    def complete(self, system: str, user: str, output_schema: dict[str, Any], addendum: str = "") -> LlmReply: ...


@dataclass(frozen=True)
class AcceptedAttempt:
    """받아들인 LLM 시도 하나. 작업 기록에 입력과 원래 응답을 그대로 남긴다."""

    attempt: int
    llm_input: str
    response: str
    usage: LlmUsage | None = None
    # 입력에 넣은 같은 사건 기사 묶음 수(article_groups.GroupedArticles.record). 기사를 넣지 않는 호출은 None.
    article_groups: dict[str, int] | None = None


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
        # 캐싱 표시는 지시문 블록에 붙인다. 다시 쓰기 덧붙임은 별도 블록이라, 같은 종류의 처음 작성 호출과
        # 첫 블록이 같아 그 캐시를 그대로 읽는다. 앞부분을 캐시할 입력(CachedUser)이면 근거도 블록으로 나눠 캐시하고
        # 덧붙임은 근거 뒤에 둔다.
        blocks: list[dict[str, Any]] = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
        content: str | list[dict[str, Any]] = str(user)
        if getattr(user, "breaks", ()):
            content = user_blocks(user, addendum)
        elif addendum:
            blocks.append({"type": "text", "text": addendum})
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=blocks,
                messages=[{"role": "user", "content": content}],
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
