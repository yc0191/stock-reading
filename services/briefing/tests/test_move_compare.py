"""짧은 시간 변동(window) 급변동의 시장 비교와 첫 줄(#66).

짧은 변동률(몇 분 사이)을 코스피 하루 등락과 빼면 기간이 달라 뜻이 없다. 짧은 시간 변동은 그 종목의 오늘 등락(전일 종가 대비,
market.move의 todayChangePercent)을 코스피 하루 등락과 비교하고, 첫 줄에 오늘 등락을 먼저 보여 준다.
가짜 응답과 규칙 기반 작성기로 변동 1건을 끝까지 돌린다. 실제 Kafka·DB·LLM은 쓰지 않는다.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from briefing.config import CoreSettings
from briefing.jobs import BriefingCore, parse_move
from briefing.report import ReportStage, RuleBasedWriter, build_llm_input, comparison_facts, figure_entries
from contracts import sample, validate
from fake_job_log import FakeJobLog

KST = timezone(timedelta(hours=9))
# 운영 예(11-13 14:39): 1분 새 +1.00%였지만 그날 전일 대비로는 약 -2%였던 종목
T = datetime(2026, 10, 20, 14, 39, 0, tzinfo=KST)
S = CoreSettings()
SEC = timedelta(seconds=1)
JOB_ID = "6d3f9b52-8e4a-4c7b-b2da-1f5a3c9e7b24"
SYMBOL = "TEST01"


class Recorder:
    def __init__(self):
        self.sent, self.readied = [], []
        self.log = FakeJobLog()

    def send(self, event):
        self.sent.append(event)

    def on_ready(self, report_input):
        self.readied.append(report_input)


def move(change=1.0, previous=12_000, price=12_120, detection="window", today=-2.0, detected=T + 60 * SEC):
    event = sample("market.move.sample.json")
    event["eventId"] = JOB_ID
    event["occurredAt"] = detected.isoformat()
    payload = event["payload"]
    payload.update(symbol=SYMBOL, previousPrice=previous, price=price, changePercent=change, asOf=T.isoformat(),
                   moveStartedAt=T.isoformat())
    payload.pop("todayChangePercent", None)
    if today is not None:
        payload["todayChangePercent"] = today
    if detection is None:
        payload.pop("detectionType")
        payload.pop("moveStartedAt")
    else:
        payload["detectionType"] = detection
    assert validate("market.move.v1.schema.json", event) == []
    return json.dumps(event)


def market_raw(kospi="  -1.45", kospi_direction="5", kosdaq="   0.30", kosdaq_direction="2"):
    rows = [{"indx_nm": "KOSPI종합", "now_indx_p2": "  2222.22", "bdy_cmpr_p2": "    32.10", "up_dwn_r_p2": kospi,
             "bdy_cmpr_ccd": kospi_direction},
            {"indx_nm": "KOSDAQ종합", "now_indx_p2": "   777.77", "bdy_cmpr_p2": "     2.33", "up_dwn_r_p2": kosdaq,
             "bdy_cmpr_ccd": kosdaq_direction}]
    return json.dumps({"dataBody": {"out2": rows}}, ensure_ascii=False)


def run(move_json, market_fetched_at=T - timedelta(minutes=5)):
    """변동 1건을 응답이 모두 오는 경우로 끝까지 돌려 리포트 입력을 돌려준다. 미국 종목이 없어 공시는 요청하지 않는다."""
    rec = Recorder()
    core = BriefingCore(rec, rec, job_log=rec.log)
    core.on_move(move_json, T)
    completed = sample("collection.completed.sample.json")
    completed["payload"].update(jobType="collect-news", startedAt=(T + S.delta).isoformat(), incompleteSymbols=[])
    core.on_collection_completed(json.dumps(completed), T)
    history = sample("market.history.responded.sample.json")
    history["payload"].update(jobId=JOB_ID, seq=1, symbol=SYMBOL, truncated=False,
                              points=[{"asOf": T.isoformat(), "price": 12_000}])
    core.on_history_responded(json.dumps(history), T + SEC)
    context = sample("market.context.responded.sample.json")
    context["payload"].update(jobId=JOB_ID, seq=1, symbol=SYMBOL, tickers=[], truncated=False, closeRates=[],
                              background=[{"apiType": "market-summary", "target": None, "status": "ok",
                                           "response": market_raw(), "fetchedAt": market_fetched_at.isoformat()}])
    # 이름 표시는 이 파일의 관심이 아니어서 코드만 보이게 한다.
    context["payload"].pop("name")
    context["payload"].pop("etf")
    core.on_context_responded(json.dumps(context), T + SEC)
    for seq, at in ((1, T + SEC), (2, T + SEC + S.recheck_interval)):
        articles = sample("news.articles.responded.sample.json")
        articles["payload"].update(jobId=JOB_ID, seq=seq, symbol=SYMBOL, truncated=False, articles=[{
            "articleId": "a1", "title": "기사 a1", "summary": "", "publisher": "news.example.com",
            "publishedAt": (T - timedelta(minutes=30)).isoformat(), "url": "https://news.example.com/a1"}])
        if seq == 2:
            core.on_tick(at)
        core.on_articles_responded(json.dumps(articles), at)
    (ready,) = rec.readied
    return ready


def rule_based_text(report_input):
    published = Recorder()
    stage = ReportStage(RuleBasedWriter(), published, FakeJobLog(), clock=lambda: T + timedelta(minutes=10))
    stage.on_ready(report_input)
    (event,) = published.sent
    assert validate("briefing.generated.v1.schema.json", event.value) == []
    return event.value["payload"]


def comparison(report_input, name):
    (entry,) = [e for e in figure_entries(report_input) if e.name.endswith(name)]
    return entry


# ---- 시장 대비 차이: 같은 기간끼리 ----

def test_window_move_compares_today_change_with_the_kospi_daily_change_not_the_short_change():
    ready = run(move(change=1.0, today=-2.0))

    facts = comparison_facts(ready)

    # 오늘 -2.00 − 코스피 -1.45 = -0.55. 짧은 변동 +1.00에서 빼면 +2.45가 되어 '시장보다 더 올랐다'로 틀린다.
    assert "오늘 등락률이 코스피종합 등락률보다 0.55%p 낮습니다 (차이 -0.55%p, 시세 자료로 코드가 계산)." in facts
    assert not any("2.45" in fact for fact in facts)
    kospi = comparison(ready, "코스피종합 등락률")
    assert kospi.name == "오늘 등락률 - 코스피종합 등락률" and kospi.values["differencePoint"] == "-0.55"


def test_window_move_without_today_change_makes_no_market_comparison():
    ready = run(move(change=1.0, today=None))

    facts = comparison_facts(ready)

    assert not any("코스피종합 등락률" in fact or "코스닥종합 등락률" in fact for fact in facts)
    for name in ("코스피종합 등락률", "코스닥종합 등락률"):
        entry = comparison(ready, name)
        assert entry.ref is None and entry.values["differencePoint"] == "없음"


@pytest.mark.parametrize("detection", ["prevClose", None])
def test_prev_close_and_older_moves_still_compare_the_move_change(detection):
    ready = run(move(change=-1.5, previous=10_000, price=9_850, detection=detection, today=-1.5))

    facts = comparison_facts(ready)

    # -1.50 − (-1.45) = -0.05
    assert "변동률이 코스피종합 등락률보다 0.05%p 낮습니다 (차이 -0.05%p, 시세 자료로 코드가 계산)." in facts
    assert comparison(ready, "코스피종합 등락률").name == "변동률 - 코스피종합 등락률"


def test_kospi_received_more_than_ten_minutes_before_the_move_names_its_received_time():
    ready = run(move(change=1.0, today=-2.0), market_fetched_at=T - timedelta(minutes=27))

    facts = comparison_facts(ready)

    assert ("오늘 등락률이 코스피종합 등락률보다 0.55%p 낮습니다 "
            "(차이 -0.55%p, 코스피종합은 14:12에 받은 값, 시세 자료로 코드가 계산).") in facts
    kospi = comparison(ready, "코스피종합 등락률")
    assert kospi.values["indexReceivedAt"] == "14:12"
    assert "지수 받은 시각 14:12" in kospi.title
    figures = {f["name"]: f for f in build_llm_input(ready)["figures"]}
    assert figures["오늘 등락률 - 코스피종합 등락률"]["indexReceivedAt"] == "14:12"


@pytest.mark.parametrize("minutes_before", [10, 3])
def test_kospi_received_within_ten_minutes_of_the_move_does_not_name_its_time(minutes_before):
    ready = run(move(change=1.0, today=-2.0), market_fetched_at=T - timedelta(minutes=minutes_before))

    facts = comparison_facts(ready)

    assert "오늘 등락률이 코스피종합 등락률보다 0.55%p 낮습니다 (차이 -0.55%p, 시세 자료로 코드가 계산)." in facts
    assert "indexReceivedAt" not in comparison(ready, "코스피종합 등락률").values


def test_stale_limit_comes_from_the_settings():
    ready = run(move(change=1.0, today=-2.0), market_fetched_at=T - timedelta(minutes=5))

    facts = comparison_facts(ready, CoreSettings(market_stale_after=timedelta(minutes=2)))

    assert any("코스피종합은 14:34에 받은 값" in fact for fact in facts)


def test_llm_input_carries_today_change_only_for_a_window_move_that_has_it():
    assert build_llm_input(run(move(change=1.0, today=-2.0)))["todayChangePercent"] == -2.0
    assert "todayChangePercent" not in build_llm_input(run(move(change=1.0, today=None)))
    assert "todayChangePercent" not in build_llm_input(
        run(move(change=-1.5, previous=10_000, price=9_850, detection="prevClose", today=-1.5)))


# ---- 첫 줄과 메일 제목 칸 ----

@pytest.mark.parametrize("change,previous,price,today,first_line", [
    # 오늘 등락과 짧은 변동 방향이 반대면 반등/반락
    (1.0, 12_000, 12_120, -2.0, "[TEST01] 14:39 기준 오늘 -2.00% · 1분 새 +1.00% 반등 (12,000 → 12,120)"),
    (-1.0, 12_000, 11_880, 1.2, "[TEST01] 14:39 기준 오늘 +1.20% · 1분 새 -1.00% 반락 (12,000 → 11,880)"),
    # 같은 방향이거나 오늘 등락이 0.00이면 상승/하락
    (-1.0, 12_000, 11_880, -2.0, "[TEST01] 14:39 기준 오늘 -2.00% · 1분 새 -1.00% 하락 (12,000 → 11,880)"),
    (1.0, 12_000, 12_120, 0.0, "[TEST01] 14:39 기준 오늘 +0.00% · 1분 새 +1.00% 상승 (12,000 → 12,120)"),
])
def test_window_first_line_shows_today_change_first(change, previous, price, today, first_line):
    payload = rule_based_text(run(move(change=change, previous=previous, price=price, today=today)))

    assert payload["text"].splitlines()[0] == first_line
    assert payload["todayChangePercent"] == today and payload["moveMinutes"] == 1


def test_window_first_line_counts_the_minutes_from_the_move_to_its_detection():
    payload = rule_based_text(run(move(today=-2.0, detected=T + 185 * SEC)))

    assert payload["text"].splitlines()[0] == "[TEST01] 14:39 기준 오늘 -2.00% · 3분 새 +1.00% 반등 (12,000 → 12,120)"
    assert payload["moveMinutes"] == 3


def test_window_first_line_says_one_minute_when_detected_within_a_minute():
    payload = rule_based_text(run(move(today=-2.0, detected=T + 20 * SEC)))

    assert payload["text"].splitlines()[0] == "[TEST01] 14:39 기준 오늘 -2.00% · 1분 새 +1.00% 반등 (12,000 → 12,120)"


def test_window_move_redetected_long_after_its_move_time_says_short_time_without_minutes():
    # 이어지는 변동을 다시 보내면 변동 시각이 처음 시각에 고정돼 감지까지 35분이 벌어진다. 변동률은 구간(5분) 안의 값이라
    # '35분 새'는 거짓이므로 분을 쓰지 않는다.
    payload = rule_based_text(run(move(today=-2.0, detected=T + timedelta(minutes=35))))

    assert payload["text"].splitlines()[0] == "[TEST01] 14:39 기준 오늘 -2.00% · 짧은 시간 +1.00% 반등 (12,000 → 12,120)"
    assert payload["todayChangePercent"] == -2.0 and "moveMinutes" not in payload


def test_window_move_detected_exactly_at_the_window_limit_still_shows_its_minutes():
    payload = rule_based_text(run(move(today=-2.0, detected=T + timedelta(minutes=S.window_move_max_minutes))))

    assert payload["text"].splitlines()[0] == "[TEST01] 14:39 기준 오늘 -2.00% · 5분 새 +1.00% 반등 (12,000 → 12,120)"
    assert payload["moveMinutes"] == 5


def test_window_move_without_today_change_keeps_the_previous_first_line_and_no_title_fields():
    payload = rule_based_text(run(move(change=1.0, today=None)))

    assert payload["text"].splitlines()[0] == "[TEST01] 14:39 기준 상승 +1.00% (12,000 → 12,120), 1분 사이"
    assert "todayChangePercent" not in payload and "moveMinutes" not in payload


def test_prev_close_move_keeps_its_first_line_and_sends_no_title_fields():
    payload = rule_based_text(run(move(change=-1.5, previous=10_000, price=9_850, detection="prevClose", today=-1.5)))

    assert payload["text"].splitlines()[0] == "[TEST01] 14:39 기준 하락 -1.50% (10,000 → 9,850), 전일 종가 대비"
    assert "todayChangePercent" not in payload and "moveMinutes" not in payload


def test_today_change_is_read_from_the_move_and_older_moves_have_none():
    assert parse_move(move(today=-2.0), T).today_change_percent == -2.0
    assert parse_move(move(today=None), T).today_change_percent is None
