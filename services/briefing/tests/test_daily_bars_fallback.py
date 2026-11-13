"""일봉 없음(#64): 마감 후 판이 시세처리기가 장중 시세로 계산한 하루 등락 대체값을 쓰고 기준 시각과 함께 밝힌다."""
import json
import logging

from briefing.daily_jobs import DailyCore
from briefing.daily_report import NO_DATA, DailyLlmWriter
from briefing.queries import QueryKind
from contracts import validate_payload
from fake_news import answer_news
from test_daily_jobs import (
    OPEN_AT,
    POST_CLOSE_AT,
    POST_CLOSE_BACKGROUND,
    POST_CLOSE_JOB,
    Details,
    FakeLlm,
    Requests,
    daily_response,
    etf_v2,
    etf_v2_missing,
    filings_response,
    post_close_output,
    published,
    tick,
)
from test_daily_review import llm_with, review_item, seed_morning
from test_rehearsal_direct_calls import rehearsal  # noqa: F401 - pytest fixture

FETCHED = "2026-10-20T16:30:02+09:00"
FALLBACK_AT = "2026-10-20T15:29:58+09:00"
NOTE = "(장중 시세 15:29 기준, 일봉 없음)"


def fallback(symbol="TEST02", name="TEST 둘", rate="-2.95", received_at=FALLBACK_AT,
             reason="no-daily-price-in-range"):
    """일봉이 없어 시세처리기가 판 시각 전 마지막 장중 시세로 계산해 준 하루 등락(가짜 값)."""
    item = etf_v2_missing(symbol, reason, name=name)
    item.update(prevClose="10000", rate=rate, intradayFallback={"price": "9705", "receivedAt": received_at})
    return item


def run(etfs, store=None, llm=None, at=POST_CLOSE_AT):
    requests = Requests()
    core = DailyCore(requests, requests.api, DailyLlmWriter(llm) if llm is not None else None, store or Details())
    core.on_collection_ready(tick(POST_CLOSE_JOB, at), at)
    core.api.reply(daily_response(etfs=etfs, background=POST_CLOSE_BACKGROUND), at)
    answer_news(core, requests, at)
    [event] = published(requests)
    return event.value["payload"]


# #64 재현: 일봉이 없으면 장중 시세가 있어도 '자료 없음'이 됐다.
def test_post_close_edition_shows_the_intraday_fallback_rate_with_its_time_instead_of_no_data():
    payload = run([etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="1.20"), fallback()])
    assert f"TEST 둘(TEST02) -2.95% {NOTE}" in payload["text"]
    assert f"TEST 둘(TEST02)" in payload["html"] and NOTE in payload["html"]
    assert f"{NO_DATA}: TEST 둘" not in payload["text"]
    assert not any("TEST 둘" in item for item in payload["missingItems"])
    # 일봉 값(종가·일자)은 없으므로 쓰지 않는다.
    assert "종가 None" not in payload["text"] and "None" not in payload["text"]


def test_a_fallback_from_an_unreadable_daily_price_is_shown_the_same_way():
    payload = run([fallback(reason="daily-price-unreadable", rate="0.40")])
    assert f"TEST 둘(TEST02) +0.40% {NOTE}" in payload["text"]


def test_the_llm_gets_the_fallback_as_a_day_entry_with_its_basis_and_can_cite_it():
    llm = FakeLlm(llm_with([], ("TEST01", "TEST02")).outputs[0])
    payload = run([etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="1.20"), fallback()], llm=llm)
    system, user, _ = llm.calls[0]
    item = next(i for i in json.loads(user)["items"] if i["name"] == "보유 ETF별 하루 등락")
    [entry] = [e for e in item["entries"] if e["symbol"] == "TEST02"]
    assert entry["basis"] == NOTE and entry["rate"] == "-2.95" and entry["price"] == "9705"
    assert entry["receivedAt"] == FALLBACK_AT
    assert "date" not in entry and "close" not in entry
    assert "missing" not in item or all(m["symbol"] != "TEST02" for m in item["missing"])
    assert "basis" in system
    # 출처 줄에도 대체값임을 밝힌다.
    assert f"TEST02 하루 등락 -2.95% {NOTE}" in payload["text"]


def test_the_morning_call_is_checked_against_the_fallback_and_the_line_says_so():
    store = Details()
    seed_morning(store, [("TEST01", "상승"), ("TEST02", "하락")])
    llm = llm_with([review_item("TEST01"), review_item("TEST02")], ("TEST01", "TEST02"))
    payload = run([etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="1.20"), fallback()], store=store, llm=llm)
    lines = payload["text"].splitlines()
    assert any("TEST 둘(TEST02) 아침 예상 하락 → 실제 −2.95% (장중 시세 15:29 기준, 일봉 없음): 맞음" in ln
               for ln in lines)
    # 일봉이 있는 ETF 줄에는 표시가 붙지 않는다.
    assert any("TEST 하나(TEST01) 아침 예상 상승 → 실제 +1.20%: 맞음" in ln for ln in lines)
    _, user, _ = llm.calls[0]
    reviews = {r["symbol"]: r for r in json.loads(user)["reviewEtfs"]}
    assert reviews["TEST02"]["actualRate"] == "-2.95" and reviews["TEST02"]["actualBasis"] == NOTE
    assert "actualBasis" not in reviews["TEST01"]


def test_a_fallback_received_after_the_edition_or_before_its_range_is_dropped_back_to_no_data(caplog):
    with caplog.at_level(logging.WARNING, logger="briefing.daily_jobs"):
        late = run([fallback(received_at="2026-10-20T17:00:01+09:00")])
        early = run([fallback(received_at="2026-10-19T16:59:59+09:00")])
    for payload in (late, early):
        assert f"{NO_DATA}: TEST 둘, 범위 안에 받은 일봉이 없음" in payload["text"]
        assert "-2.95" not in payload["text"]
    assert "intradayFallback outside the range" in caplog.text
    # 판 기준 시각에 받은 것은 범위 안이다.
    assert "TEST 둘(TEST02) -2.95% (장중 시세 17:00 기준, 일봉 없음)" in run(
        [fallback(received_at="2026-10-20T17:00:00+09:00")])["text"]


def test_the_morning_edition_does_not_use_a_fallback(caplog):
    requests = Requests()
    core = DailyCore(requests, requests.api, None, Details())
    core.on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    with caplog.at_level(logging.WARNING, logger="briefing.daily_jobs"):
        core.api.reply(daily_response(etfs=[fallback(received_at="2026-10-20T09:05:00+09:00")]), OPEN_AT)
    core.api.reply(filings_response(), OPEN_AT)
    answer_news(core, requests, OPEN_AT)
    [event] = published(requests)
    assert "일봉 없음)" not in event.value["payload"]["text"]
    assert "dropped 1 intradayFallback" in caplog.text


def test_only_the_post_close_request_asks_the_market_for_the_fallback():
    requests = Requests()
    core = DailyCore(requests, requests.api, None, Details())
    core.on_collection_ready(tick(POST_CLOSE_JOB, POST_CLOSE_AT), POST_CLOSE_AT)
    [post] = requests.api.of(QueryKind.DAILY)
    assert post.payload["includeIntradayFallback"] is True
    assert validate_payload("market.daily.requested.v1.schema.json", post.payload) == []

    morning_requests = Requests()
    DailyCore(morning_requests, morning_requests.api, None, Details()).on_collection_ready(tick(at=OPEN_AT), OPEN_AT)
    [morning] = morning_requests.api.of(QueryKind.DAILY)
    assert "includeIntradayFallback" not in morning.payload


# ---- 가짜 리허설: 가짜 시세처리기 HTTP 서버가 대체값을 답하고 마감 후 판 메일까지 ----

def test_rehearsal_a_fallback_from_the_daily_query_api_reaches_the_post_close_mail(rehearsal, monkeypatch):
    import test_rehearsal_direct_calls as stage
    from test_runner import FakeMessage, sent
    from contracts import validate

    body = daily_response(etfs=[etf_v2("TEST01", "TEST 하나", fetched=FETCHED, rate="1.20"), fallback()],
                          background=POST_CLOSE_BACKGROUND)
    monkeypatch.setattr(stage, "daily_response", lambda: body)
    # 리허설 기본 LLM은 아침 판 출력이라 마감 후 판 출력으로 바꾼다.
    monkeypatch.setattr(stage, "good_output", lambda *_: post_close_output(("TEST01", "TEST02")))
    runner, consumer, producer, _, services, _ = rehearsal(fail_first={"/internal/market/daily": 1})

    consumer.queue.append(FakeMessage("daily.collection.completed", tick(POST_CLOSE_JOB, POST_CLOSE_AT)))
    stage.run_until(runner, lambda: sent(producer, "briefing.daily.generated"))

    (_, mail), = sent(producer, "briefing.daily.generated")
    assert validate("briefing.daily.generated.v2.schema.json", mail) == []
    # 첫 조회가 503이어도 다시 불러 받은 대체값이 메일 본문에 기준 시각과 함께 나온다.
    assert services.paths()[:2] == ["/internal/market/daily", "/internal/market/daily"]
    assert f"TEST 둘(TEST02) -2.95% {NOTE}" in mail["payload"]["text"]
    assert NOTE in mail["payload"]["html"]
