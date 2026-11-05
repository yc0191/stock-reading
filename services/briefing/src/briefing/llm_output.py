"""LLM 출력 검사. 급변동 보고서와 데일리 판이 함께 쓴다. 출력 칸 이름과 사유 문구는 부르는 쪽이 정한다."""
from __future__ import annotations

import json
from typing import Any


class InvalidLlmOutput(ValueError):
    """검사에 걸린 LLM 출력. 부르는 쪽이 자기 이름의 하위 예외를 넘겨 구분한다."""


def parse_output_object(text: str, items_key: str, error: type[InvalidLlmOutput]) -> dict[str, Any]:
    """JSON 형식과 최상위 칸(항목 목록, 근거 부족 표시)이 맞는지 확인하고 파싱한 값을 돌려준다."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as cause:
        raise error("output is not valid JSON") from cause
    if not isinstance(data, dict) or not isinstance(data.get(items_key), list) \
            or not isinstance(data.get("insufficientEvidence"), bool):
        raise error(f"output does not have {items_key} and insufficientEvidence")
    return data


def output_item(item: Any, noun: str, error: type[InvalidLlmOutput]) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise error(f"{noun} is not an object")
    return item


def output_sentence(item: dict, name: str, noun: str, error: type[InvalidLlmOutput]) -> str:
    value = item.get(name)
    if not isinstance(value, str) or not value.strip():
        raise error(f"{noun} has no {name}")
    return value.strip()


def output_cited_refs(item: dict, name: str, noun: str, allowed: frozenset[str],
                      error: type[InvalidLlmOutput]) -> tuple[str, ...]:
    """근거 번호 목록이 비어 있지 않고 모두 준 목록 안에 있는지 확인한다. 사유에는 번호 값을 넣지 않는다."""
    ids = item.get(name)
    if not isinstance(ids, list) or not ids or not all(isinstance(x, str) for x in ids):
        raise error(f"{noun} cites no {name}")
    unknown = [x for x in ids if x not in allowed]
    if unknown:
        raise error(f"{noun} cites {len(unknown)} ref(s) that were not given")
    return tuple(dict.fromkeys(ids))


def check_items_present(data: dict, items_key: str, noun: str, error: type[InvalidLlmOutput]) -> bool:
    """근거 부족이 아니라고 하면서 항목이 하나도 없으면 거부한다. 근거 부족 표시를 돌려준다."""
    insufficient = data["insufficientEvidence"]
    if not insufficient and not data[items_key]:
        raise error(f"no {noun}s although evidence was not marked insufficient")
    return insufficient


# 다시 써도 약한 항목에 본문이 붙이는 표시. 뒤에 부족한 점이 따른다.
WEAK_NOTE_PREFIX = "근거 약함: "


def output_weak_shortfall(item: dict, noun: str, error: type[InvalidLlmOutput]) -> str | None:
    """항목의 근거 약함 표시를 읽는다. 약하면 부족한 점(문장)을, 아니면 None을 돌려준다.

    weak 칸이 없는 출력은 약하지 않은 것으로 읽는다(출력 형식이 칸을 요구하므로 실제 응답에는 항상 있다).
    약하다고 하면서 부족한 점이 비어 있으면 재검색할 단서가 없으므로 거부한다.
    """
    weak = item.get("weak", False)
    if not isinstance(weak, bool):
        raise error(f"{noun} weak is not a boolean")
    return output_sentence(item, "shortfall", noun, error) if weak else None
