import json

import pytest

from briefing.daily_report import InvalidDailyOutput
from briefing.daily_report import parse_output as parse_daily
from briefing.llm_output import (
    InvalidLlmOutput,
    check_items_present,
    output_cited_refs,
    output_sentence,
    parse_output_object,
)
from briefing.report import InvalidOutput
from briefing.report import parse_output as parse_move


class _Invalid(InvalidLlmOutput):
    pass


def test_both_reports_reject_with_exceptions_of_the_shared_base():
    assert issubclass(InvalidOutput, InvalidLlmOutput)
    assert issubclass(InvalidDailyOutput, InvalidLlmOutput)


def test_non_json_and_missing_top_level_fields_are_rejected():
    with pytest.raises(_Invalid, match="valid JSON"):
        parse_output_object("not json", "items", _Invalid)
    with pytest.raises(_Invalid, match="items and insufficientEvidence"):
        parse_output_object(json.dumps({"items": []}), "items", _Invalid)


def test_empty_sentence_is_rejected_and_the_rest_is_trimmed():
    assert output_sentence({"a": " 문장 "}, "a", "item", _Invalid) == "문장"
    with pytest.raises(_Invalid, match="item has no a"):
        output_sentence({"a": "  "}, "a", "item", _Invalid)


def test_refs_outside_the_given_list_are_rejected_without_naming_them():
    allowed = frozenset({"A1"})
    assert output_cited_refs({"refs": ["A1", "A1"]}, "refs", "item", allowed, _Invalid) == ("A1",)
    with pytest.raises(_Invalid) as error:
        output_cited_refs({"refs": ["Z9"]}, "refs", "item", allowed, _Invalid)
    assert "Z9" not in str(error.value)
    with pytest.raises(_Invalid, match="cites no refs"):
        output_cited_refs({"refs": []}, "refs", "item", allowed, _Invalid)


def test_no_items_is_rejected_unless_evidence_is_marked_insufficient():
    with pytest.raises(_Invalid, match="no items"):
        check_items_present({"items": [], "insufficientEvidence": False}, "items", "item", _Invalid)
    check_items_present({"items": [], "insufficientEvidence": True}, "items", "item", _Invalid)


def test_move_and_daily_use_the_same_insufficient_evidence_check():
    with pytest.raises(InvalidOutput, match="no causes"):
        parse_move(json.dumps({"causes": [], "insufficientEvidence": False}), frozenset())
    with pytest.raises(InvalidDailyOutput, match="no points"):
        parse_daily(json.dumps({"points": [], "insufficientEvidence": False}), frozenset())
    assert parse_move(json.dumps({"causes": [], "insufficientEvidence": True}), frozenset()).insufficient_evidence
    assert parse_daily(json.dumps({"points": [], "insufficientEvidence": True}), frozenset()).insufficient_evidence
