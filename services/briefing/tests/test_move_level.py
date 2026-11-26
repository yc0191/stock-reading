"""급변동 레벨 판단(상·중)과 레벨 상의 머리말·비교 숫자. 가짜 응답과 규칙 기반 작성기로 변동 1건을 끝까지 돌린다.

레벨은 market.move의 감지 유형만 본다: prevCloseHigh면 상, 그 밖(window, prevClose, 감지 유형 없는 이전 형식)은 중이다.
중이 대표 기사를 고르는 흐름은 test_jobs.py가, 대표 기사 고르기 규칙은 test_move_representative.py가 본다.
"""
import json

import pytest

from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.job_log import LEVEL_HIGH, LEVEL_MID, level_of, parse_move_run
from briefing.jobs import parse_move
from briefing.report import _today_first_entry, build_llm_input, comparison_facts
from test_move_compare import comparison, move, rule_based_text, run


@pytest.mark.parametrize("detection_type, level", [
    ("prevCloseHigh", LEVEL_HIGH), ("window", LEVEL_MID), ("prevClose", LEVEL_MID), (None, LEVEL_MID),
    # 모르는 값도 중이다. 리포트와 메일은 상으로 정해진 값에만 나간다.
    ("somethingNew", LEVEL_MID), ("", LEVEL_MID)])
def test_only_the_prev_close_high_detection_type_is_level_high(detection_type, level):
    assert level_of(detection_type) == level


def test_a_move_event_gets_its_level_from_the_detection_type():
    assert parse_move(move(detection="prevCloseHigh"), None).level == LEVEL_HIGH
    assert parse_move(move(detection="window"), None).level == LEVEL_MID
    # 감지 유형·움직임 시작 시각이 없는 이전 형식은 중이다.
    assert parse_move(move(detection=None), None).level == LEVEL_MID


def test_the_job_run_row_reads_the_same_level_from_the_stored_input():
    target = json.dumps({"symbol": "TEST01", "moveAt": "2026-10-20T14:39:00+09:00"})
    for detection, level in (("prevCloseHigh", LEVEL_HIGH), ("window", LEVEL_MID), (None, LEVEL_MID)):
        payload = {"symbol": "TEST01", "changePercent": 7.1, **({"detectionType": detection} if detection else {})}
        row = parse_move_run("j1", json.dumps({"payload": payload}), target, "succeeded", None, None)
        assert row.level == level
    # 입력을 읽을 수 없는 줄은 중으로 본다(리포트를 만들지 않은 줄처럼 보인다).
    assert parse_move_run("j2", "{}", target, "succeeded", None, None).level == LEVEL_MID


def test_the_first_high_move_headline_starts_with_the_today_change_only():
    # 첫 상은 기준가가 전일 종가라 기준가 대비 변동률과 오늘 등락률이 같다.
    ready = run(move(change=-7.2, previous=10_000, price=9_280, detection="prevCloseHigh", today=-7.2))

    first = rule_based_text(ready)["text"].split("\n")[0]

    assert first == "[TEST01] 14:39 기준 오늘 -7.20% 하락 (10,000 → 9,280)"
    assert _today_first_entry(ready, DEFAULT_SETTINGS) == {"todayChangePercent": -7.2, "baseKind": "prevClose"}


def test_a_later_high_move_headline_adds_the_change_against_the_base_price():
    ready = run(move(change=-7.53, previous=9_300, price=8_600, detection="prevCloseHigh", today=-14.0))

    first = rule_based_text(ready)["text"].split("\n")[0]

    assert first == "[TEST01] 14:39 기준 오늘 -14.00% · 기준가 대비 -7.53% 하락 (9,300 → 8,600)"
    assert _today_first_entry(ready, DEFAULT_SETTINGS) == {"todayChangePercent": -14.0, "baseKind": "earlierMove"}


def test_a_high_move_that_turns_against_the_day_is_a_rebound_and_a_missing_today_change_keeps_the_old_headline():
    rebound = run(move(change=7.1, previous=10_000, price=10_710, detection="prevCloseHigh", today=-2.0))
    unknown = run(move(change=7.1, previous=10_000, price=10_710, detection="prevCloseHigh", today=None))

    assert rule_based_text(rebound)["text"].split("\n")[0] == (
        "[TEST01] 14:39 기준 오늘 -2.00% · 기준가 대비 +7.10% 반등 (10,000 → 10,710)")
    assert rule_based_text(unknown)["text"].split("\n")[0] == "[TEST01] 14:39 기준 상승 +7.10% (10,000 → 10,710), 기준가 대비"
    assert _today_first_entry(unknown, DEFAULT_SETTINGS) == {}


def test_a_high_move_reports_the_base_price_move_but_compares_the_today_change_with_the_market():
    ready = run(move(change=-7.2, previous=10_000, price=9_280, detection="prevCloseHigh", today=-2.0))

    facts = comparison_facts(ready)

    # 기준가 대비 -7.20%는 코스피 하루 등락과 기간이 달라 빼지 않는다. 오늘 등락 -2.00에서 코스피 -1.45를 뺀다.
    assert "오늘 등락률이 코스피종합 등락률보다 0.55%p 낮습니다 (차이 -0.55%p, 시세 자료로 코드가 계산)." in facts
    assert not any("5.75" in fact for fact in facts)
    assert build_llm_input(ready)["todayChangePercent"] == -2.0
    assert build_llm_input(ready)["changePercent"] == -7.2


def test_a_high_move_without_the_today_change_makes_no_market_comparison():
    facts = comparison_facts(run(move(change=7.1, previous=10_000, price=10_710, detection="prevCloseHigh",
                                      today=None)))

    assert not any("코스피종합 등락률" in fact or "코스닥종합 등락률" in fact for fact in facts)


def test_the_report_pause_settings_do_not_exist_any_more():
    names = [f for f in CoreSettings.__dataclass_fields__ if "pause" in f]
    assert names == []
