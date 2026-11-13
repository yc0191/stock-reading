"""README의 마감 후 판 메일 그림(docs/images/mail-post-close.png)에 쓸 HTML을 가짜 값으로 만든다.

브리핑의 실제 판 작성 코드(DailyCore)와 테스트용 가짜(시세처리기·뉴스 응답, LLM, 멤버, 작업 기록)를 써서 실제 메일과 같은 HTML이 나온다.
값은 모두 가짜(TEST 접두 종목, example.test 링크)다.
실행: services/briefing/.venv/bin/python docs/images/mail_example.py <출력 폴더>
그림은 docs/images/mail_figure.sh가 이 HTML을 브라우저로 폭 720px에 찍어 만든다. README에는 '그날 급변동'까지만 담고,
그 아래 안내 문구와 출처 목록은 잘라 낸다.
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services/briefing/tests"))
sys.path.insert(0, str(ROOT / "services/briefing/src"))

from briefing.daily_jobs import DailyCore
from briefing.daily_report import DailyLlmWriter
from fake_members import FakeMembers, member
from fake_news import answer_news, article
from test_daily_jobs import POST_CLOSE_JOB, Details, FakeLlm, Requests, daily_response, etf_v2, etf_v2_missing, \
    published, tick, WORLD_C, EXCHANGE, MARKET
from test_daily_review import seed_morning, seed_move
from test_move_figures import index_row, fx_row, response
from briefing.move_figures import KOSPI_NAME, KOSDAQ_NAME

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)
FETCHED = "2026-10-20T16:30:02+09:00"


def etf(symbol, name, rate, close, members=()):
    item = etf_v2(symbol, name, fetched=FETCHED, rate=rate)
    item.update(date="20261020", close=close, prevClose="10000")
    item["etf"]["members"] = [{"market": m[0], "symbol": m[1], "rank": i + 1, "name": m[2], "weight": m[3]}
                              for i, m in enumerate(members)]
    return item


def fallback(symbol, name, rate, price):
    """일봉을 받지 못해 시세처리기가 장중 마지막 시세로 계산해 준 하루 등락."""
    item = etf_v2_missing(symbol, "no-daily-price-in-range", name=name)
    item.update(prevClose="10000", rate=rate, intradayFallback={"price": price, "receivedAt": "2026-10-20T15:29:58+09:00"})
    return item


# 반도체·기술 ETF 셋이 함께 2% 넘게 올라 '눈에 띄는 점'(같은 방향 묶음)이 되고, 미국 기술 ETF는 비중 상위 미국 종목의
# 직전 장 등락이 커서 구성 종목 줄이 붙는다. 채권 ETF는 일봉을 받지 못한 경우다.
ETFS = [etf("TEST01", "TEST 반도체", "2.40", "10240", [("KR", "TEST900", "가짜반도체", "25.0"),
                                                        ("KR", "TEST901", "가짜메모리", "18.0")]),
        etf("TEST04", "TEST 반도체장비", "2.10", "10210", [("KR", "TEST902", "가짜장비", "21.0")]),
        etf("TEST02", "TEST 미국기술", "2.05", "10205", [("US", "FAKEA", "FAKE CHIP CORP", "12.0"),
                                                         ("US", "FAKEB", "FAKE CLOUD INC", "9.5")]),
        fallback("TEST03", "TEST 채권", "-0.80", "9920")]

POST_CLOSE_OUT = json.dumps({
    "summary": ["코스피가 1% 넘게 오른 가운데 반도체·기술 ETF가 함께 2% 넘게 올랐습니다.",
                "전날 미국 기술주 강세와 원/달러 환율 하락이 함께 있었고, 채권 ETF만 내렸습니다."],
    "etfs": [
        {"symbol": "TEST01", "reason": "비중 1위 가짜반도체가 수출 호조 보도와 함께 강세였습니다. 수출 증가 폭은 보도에 따라 "
                                       "20%~25%로 엇갈립니다.", "refs": ["a1", "a4", "E1"], "weak": False, "shortfall": ""},
        {"symbol": "TEST04", "reason": "구성 종목 가짜장비가 대형 수주를 따냈다는 기사와 함께 올랐습니다.", "refs": ["a5", "E2"],
         "weak": False, "shortfall": ""},
        {"symbol": "TEST02", "reason": "비중 상위 미국 종목 둘이 전날 미국 장에서 3% 넘게 올랐고 미국 기술주 지수도 강세였습니다. "
                                       "원/달러 환율 하락은 원화 가격 상승 폭을 줄였습니다.", "refs": ["M1", "M2", "B2", "B3"],
         "weak": False, "shortfall": ""},
        {"symbol": "TEST03", "reason": "국채 금리가 소폭 올랐다는 기사가 있어 채권 가격이 내렸을 수 있습니다.",
         "refs": ["a3"], "weak": False, "shortfall": ""}],
    "notables": [{"statement": "반도체·기술 ETF 세 개가 함께 2% 넘게 올라, 한 종목의 일이라기보다 업종 전체의 흐름이었습니다.",
                  "refs": ["E1", "E2", "E3"], "weak": False, "shortfall": ""}],
    "reviews": [
        {"symbol": "TEST01", "reason": "예상대로 반도체 수출 기사가 상승을 이끌었습니다.", "refs": ["a1"], "weak": False, "shortfall": ""},
        {"symbol": "TEST02", "reason": "예상대로 전날 미국 기술주 강세가 이어졌습니다.", "refs": ["M1", "B2"], "weak": False,
         "shortfall": ""},
        {"symbol": "TEST03", "reason": "아침에는 금리 소식이 없었지만 오후에 국채 금리가 올라 예상보다 더 내렸습니다.", "refs": ["a3"],
         "weak": False, "shortfall": ""}],
    "fxIssue": {"statement": "원/달러 환율이 내려 달러 자산 ETF의 원화 환산 수익이 줄었습니다.", "refs": ["B3"], "weak": False,
                "shortfall": ""},
    "rateIssue": {"statement": "국채 금리가 소폭 올랐다는 기사가 있었습니다.", "refs": ["a3"], "weak": False, "shortfall": ""},
    "insufficientEvidence": False}, ensure_ascii=False)


def post_close_html():
    requests, details = Requests(), Details()
    seed_morning(details, [("TEST01", "상승"), ("TEST02", "상승"), ("TEST03", "보합")])
    # 그날 급변동: 보유 ETF 하나와 포트폴리오의 국내 주식 하나
    seed_move(details, "TEST01", datetime(2026, 10, 20, 10, 12, tzinfo=KST), 1.6, True, "TEST-MOVE-1")
    seed_move(details, "TEST900", datetime(2026, 10, 20, 13, 47, tzinfo=KST), 3.1, True, "TEST-MOVE-2")
    members = FakeMembers((member(1, ("TEST01", "TEST04", "TEST02", "TEST03"), stocks=(("TEST900", "가짜반도체"),)),))
    core = DailyCore(requests, requests.api, DailyLlmWriter(FakeLlm(POST_CLOSE_OUT), 1), details, members=members)
    core.on_collection_ready(tick(POST_CLOSE_JOB, AT), AT)
    background = [WORLD_C,
                  {**EXCHANGE, "response": response([fx_row(value="     1385.2000", change="       -5.3000",
                                                            rate="     -0.38")])},
                  {**MARKET, "fetchedAt": "2026-10-20T16:30:05+09:00",
                   "response": response([index_row(KOSPI_NAME, "  2620.50", "    28.51", "   1.10", "2"),
                                         index_row(KOSDAQ_NAME, "   812.40", "     6.85", "   0.85", "2")])}]
    close_rates = [{"ticker": t, "date": "20261019", "closeAt": "2026-10-20T05:00:00+09:00", "rate": r,
                    "fetchedAt": "2026-10-20T08:30:02+09:00"} for t, r in (("FAKEA", "3.10"), ("FAKEB", "3.40"))]
    core.api.reply(daily_response(etfs=ETFS, close_rates=close_rates, background=background), AT)
    articles = {
        "TEST01": [article("a1", "TEST 반도체 수출 사상 최대…가짜반도체 강세", "2026-10-20T10:00:00+09:00"),
                   article("a4", "반도체 수출 20% 늘어…업계 \"예상보다 빠른 회복\"", "2026-10-20T10:20:00+09:00",
                           publisher="TEST 경제")],
        "TEST04": [article("a5", "가짜장비, 대형 반도체 장비 수주…주가 강세", "2026-10-20T11:30:00+09:00", publisher="TEST 경제")],
        "TEST02": [article("a2", "TEST 미국기술 지수 상승, FAKE CHIP CORP 실적 기대", "2026-10-20T07:00:00+09:00")],
        "TEST03": [article("a3", "TEST 채권 지수 약세…국채 금리 소폭 상승", "2026-10-20T14:00:00+09:00")]}
    answer_news(core, requests, AT, articles=articles)
    [event] = published(requests)
    payload = event.value["payload"]
    body = payload.get("html") or ""
    for image in payload.get("images", []):
        body = body.replace(f"cid:{image['contentId']}", f"data:{image['contentType']};base64,{image['data']}")
    # README에는 그날 급변동까지만 보인다(안내 문구와 출처 목록은 뺌).
    cut = body.rfind("<p", 0, body.index("※ 투자 조언"))
    return body[:cut] + "</div></body></html>"


if __name__ == "__main__":
    Path(sys.argv[1], "mail-post-close.html").write_text(post_close_html(), encoding="utf-8")
