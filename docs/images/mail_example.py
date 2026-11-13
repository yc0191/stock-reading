"""README의 마감 후 판 메일 그림(docs/images/mail-post-close.png)에 쓸 HTML을 가짜 값으로 만든다.

브리핑의 실제 판 작성 코드(DailyCore)와 테스트용 가짜(시세처리기·뉴스 응답, LLM, 멤버)를 그대로 써서 실제 메일과 같은 HTML이 나온다.
값은 모두 가짜(TEST 접두 종목, example.test 링크)다.
실행: services/briefing/.venv/bin/python docs/images/mail_example.py <출력.html>
그림은 이 HTML을 브라우저로 열어 폭 720px로 화면을 찍고, '아침 예상 대조' 제목 위에서 자른다.
"""
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services/briefing/tests"))
sys.path.insert(0, str(ROOT / "services/briefing/src"))
from datetime import datetime, timedelta, timezone
from briefing.daily_jobs import DailyCore
from briefing.daily_report import DailyLlmWriter
from fake_news import answer_news, article
from briefing.daily_prompts import NO_REASON_STATEMENT as NO_REASON
from test_daily_review import seed_morning
from test_move_figures import index_row, fx_row, response
from briefing.move_figures import KOSPI_NAME, KOSDAQ_NAME
from test_daily_jobs import post_close_output, FakeLlm, POST_CLOSE_JOB, Details, Requests, daily_response, etf, published, tick, WORLD_C, EXCHANGE, MARKET

KST = timezone(timedelta(hours=9))
AT = datetime(2026, 10, 20, 17, 0, tzinfo=KST)

def item(sym, name, tracks, rate, members, close="10240", prev="10000"):
    e = etf(sym, tickers=tuple(m[1] for m in members if m[0]=="US"), rate=rate, fetched="2026-10-20T16:30:02+09:00")
    e["date"]="20261020"; e["close"]=close; e["prevClose"]=prev
    e["etf"]={"name": name, "tracks": tracks, "members":[{"market":m[0],"symbol":m[1],"rank":i+1,"name":m[2],"weight":m[3]} for i,m in enumerate(members)], "membersTruncated": False}
    return e

ETFS=[item("TEST01","TEST 반도체","TEST 반도체 지수","2.40",[("KR","TEST900","가짜반도체","25.0")]),
      item("TEST02","TEST 미국기술","TEST 미국기술 지수","0.60",[("US","FAKEA","FAKE CHIP CORP","12.0")],"10060"),
      item("TEST03","TEST 채권","TEST 채권 지수","-0.80",[],"9920")]
out=json.dumps({
 "summary":["반도체 수출 호조 기사와 함께 반도체 ETF가 가장 크게 올랐습니다.","원/달러 환율이 내려 미국 자산 ETF의 원화 가격은 덜 올랐습니다."],
 "etfs":[{"symbol":"TEST01","reason":"반도체 수출이 크게 늘었다는 기사와 함께 구성 종목이 강세였습니다.","refs":["a1","E1"],"weak":False,"shortfall":""},
         {"symbol":"TEST02","reason":"담긴 미국 종목이 전날 올랐지만 원/달러 환율이 내려 원화 가격 상승 폭이 줄었습니다.","refs":["a2","M1","B3"],"weak":False,"shortfall":""},
         {"symbol":"TEST03","reason":"국채 금리가 소폭 올랐다는 기사가 있어 채권 가격이 내렸을 수 있습니다.","refs":["a3"],"weak":False,"shortfall":""}],
 "notables":[],
 "reviews":[{"symbol":"TEST01","reason":"예상대로 반도체 수출 기사가 상승을 이끌었습니다.","refs":["a1"],"weak":False,"shortfall":""},
            {"symbol":"TEST02","reason":"미국 종목은 올랐지만 환율 하락으로 오름폭이 작았습니다.","refs":["M1","B3"],"weak":False,"shortfall":""},
            {"symbol":"TEST03","reason":"아침에는 금리 소식이 없었지만 오후에 국채 금리가 올라 예상보다 더 내렸습니다.","refs":["a3"],"weak":False,"shortfall":""}],
 "fxIssue":{"statement":"원/달러 환율이 내려 달러 자산 ETF의 원화 환산 수익이 줄었습니다.","refs":["B3"],"weak":False,"shortfall":""},
 "rateIssue":{"statement":"국채 금리가 소폭 올랐다는 기사가 있었습니다.","refs":["a3"],"weak":False,"shortfall":""},
 "insufficientEvidence":False}, ensure_ascii=False)
llm=FakeLlm(out)
req=Requests(); from fake_members import FakeMembers
details=Details()
seed_morning(details, [("TEST01","상승"),("TEST02","상승"),("TEST03","보합")])
core=DailyCore(req, DailyLlmWriter(llm,1), details, members=FakeMembers())
core.on_collection_ready(tick(POST_CLOSE_JOB, AT), AT)  # tick: 수집기의 판 시작 알림(daily.collection.completed)을 만드는 테스트 보조
cr=[{"ticker":"FAKEA","date":"20261019","closeAt":"2026-10-20T05:00:00+09:00","rate":"3.10","fetchedAt":"2026-10-20T08:30:02+09:00"}]
core.on_daily_data_responded(daily_response(etfs=ETFS, close_rates=cr, background=[WORLD_C, {**EXCHANGE,"response":response([fx_row(value="     1385.2000", change="       -5.3000", rate="     -0.38")])}, {**MARKET,"fetchedAt":"2026-10-20T16:30:05+09:00","response":response([index_row(KOSPI_NAME,"  2620.50","    28.51","   1.10","2"), index_row(KOSDAQ_NAME,"   812.40","     6.85","   0.85","2")])}]), AT)
arts={"TEST01":[article("a1","TEST 반도체 수출 사상 최대…가짜반도체 강세","2026-10-20T10:00:00+09:00")],
      "TEST03":[article("a3","TEST 채권 지수 약세…국채 금리 소폭 상승","2026-10-20T14:00:00+09:00")],
      "TEST02":[article("a2","TEST 미국기술 지수 상승, FAKE CHIP CORP 실적 기대","2026-10-20T07:00:00+09:00")]}
answer_news(core, req, AT, articles=arts)
answer_news(core, req, AT, silent=('TEST01','TEST02','TEST03'))
[ev]=published(req); pl=ev.value["payload"]
html=pl.get("html") or ""
for img in pl.get("images",[]):
    html=html.replace(f"cid:{img['contentId']}", f"data:{img['contentType']};base64,{img['data']}")
Path(sys.argv[1]).write_text(html, encoding="utf-8")

