"""데일리 브리핑 수동 확인 스크립트(scripts/manual-daily-briefing.sh)의 도우미.

하위 명령:
- fake-llm <포트 파일>: 로컬 루프백에 가짜 Claude(메시지 API) 서버를 연다. 받은 입력의 첫 근거 번호를 인용한 요약 한 줄을
  돌려주고, 근거가 없으면 근거 부족으로 답한다. 고른 포트를 파일에 쓰고, 받은 호출 수는 /__stats 로 알려 준다.
- seed: 가짜 수집 자료(market.insight 국내 일봉·세계지수·환율종합·시장종합·해외 일봉, filing.collected 공시)를
  계약 스키마로 검사한 뒤 일회용 Kafka 에 발행한다. 값은 모두 TEST·FAKE 접두의 가짜다.
- ticks: 아침 판·마감 후 판 시작 신호(schedule.tick) 한 건씩을 발행한다.
- check: briefing.daily.generated 를 처음부터 읽어 이번 실행의 두 판을 찾고, 계약 검사와 함께 결과 파일에 본문을 쓴다.

설정은 환경변수로만 받는다(값은 manual-daily-briefing.sh 가 넣음).
  DAILY_E2E_KAFKA     일회용 Kafka 주소
  DAILY_E2E_RUN_ID    이 실행의 이름(추적 ID 에 씀)
  DAILY_E2E_NOW       시작 신호의 예정 시각(RFC 3339). 수집 자료의 받은 시각은 이 시각보다 앞에 둔다
  DAILY_E2E_RESULT    check 가 본문을 쓰는 파일(저장소 밖)
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

KST = timezone(timedelta(hours=9))
NEW_YORK = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts" / "events"

INSIGHT_TOPIC = "market.insight"
FILING_TOPIC = "filing.collected"
TICK_TOPIC = "schedule.tick"
DAILY_TOPIC = "briefing.daily.generated"
JOB_TYPES = {"morning": "daily-briefing-morning", "post-close": "daily-briefing-post-close"}

# 시세처리기 ETF_US_HOLDINGS 와 같은 형식. TEST02 는 미국 종목이 없는 ETF 로 둔다.
HOLDINGS = {"TEST01": ("TESTA", "FAKEB"), "TEST02": ()}
US_TICKERS = ("TESTA", "FAKEB")
FAKE_STATEMENT = "[가짜 LLM] 시험용 요약 문장입니다."
COMMON = {"broker.address.family": "v4"}


def holdings_setting() -> str:
    """ETF_US_HOLDINGS 값. 미국 종목이 없는 ETF 는 대응표에 넣지 않는다(일봉만으로 보유 ETF 가 된다)."""
    return ";".join(f"{etf}={','.join(tickers)}" for etf, tickers in HOLDINGS.items() if tickers)


def holdings_file_json() -> str:
    """시세처리기 보유 종목 파일(HOLDINGS_FILE) 내용. holdings_setting 과 같은 가짜 대응표를 파일 모양으로 옮긴다."""
    items = [{"kind": "ETF", "market": "KR", "symbol": etf, "relation": "HELD",
              "members": [{"market": "US", "symbol": ticker, "rank": rank}
                          for rank, ticker in enumerate(tickers, start=1)]}
             for etf, tickers in HOLDINGS.items()]
    return json.dumps(items)


def _ts(value: datetime) -> str:
    return value.astimezone(KST).isoformat(timespec="milliseconds")


def _envelope(topic: str, occurred: datetime, correlation_id: str, payload: dict) -> dict:
    return {"eventId": str(uuid.uuid4()), "eventType": topic, "schemaVersion": 1, "occurredAt": _ts(occurred),
            "producedAt": _ts(occurred), "correlationId": correlation_id, "payload": payload}


def last_ny_close(before: datetime) -> date:
    """before 이하로 마감한 가장 최근 미국 정규장(평일 뉴욕 16:00)의 일자. 휴장일은 보지 않는다."""
    day = before.astimezone(NEW_YORK).date()
    while True:
        close = datetime.combine(day, time(16, 0), NEW_YORK)
        if day.weekday() < 5 and close <= before:
            return day
        day -= timedelta(days=1)


def _previous_weekday(day: date) -> date:
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def _ymd(day: date) -> str:
    return day.strftime("%Y%m%d")


def _insight(api_type: str, target: str | None, response: dict, fetched: datetime, run_id: str) -> dict:
    payload = {"apiType": api_type, "target": target, "response": json.dumps(response, ensure_ascii=False),
               "scheduledFor": _ts(fetched.replace(microsecond=0)), "fetchedAt": _ts(fetched)}
    return _envelope(INSIGHT_TOPIC, fetched, f"daily-e2e-{run_id}", payload)


def _daily_price(day: date, close: int, prev_close: int) -> dict:
    rate = f"{(close - prev_close) / prev_close * 100:6.2f}"
    older = _previous_weekday(day)
    # 시세처리기는 일자가 가장 최근인 레코드를 고른다. 순서를 섞어 위치로 고르지 않는지도 함께 본다.
    return {"dataHeader": {"resultCode": "200"}, "dataBody": {"out2": [
        {"dt": _ymd(older), "cls_prc_p2": f" {prev_close}", "bdy_cls_prc_p2": f" {prev_close}", "up_dwn_r_p2": "   0.00"},
        {"dt": _ymd(day), "cls_prc_p2": f" {close}", "bdy_cls_prc_p2": f" {prev_close}", "up_dwn_r_p2": rate},
    ]}}


def _world_index(day: date, closes: dict[str, str]) -> dict:
    # 장이 끝난 지수처럼 원문의 전일대비·등락률은 0.00 으로 둔다. 등락률은 시세처리기가 직전 행 종가로 계산한다.
    return {"dataHeader": {"resultCode": "200"}, "dataBody": {"out2": [
        {"is_cd": f"{code}  ", "is_cd_nm": f"{code} INDEX", "bsnss_dy": _ymd(day), "ttm": "9999",
         "cls_prc_p2": f"  {close}", "bdy_cmpr_p2": "    0.00", "bdy_cmpr_r_p2": "    0.00"}
        for code, close in closes.items()
    ]}}


def _overseas(day: date, rate: str) -> dict:
    return {"dataHeader": {"resultCode": "200"}, "dataBody": {"up_dwn_r_p2": "   9.99", "out2": [
        {"dt": _ymd(day), "tm": "000000", "up_dwn_r_p2": rate},
        {"dt": _ymd(_previous_weekday(day)), "tm": "000000", "up_dwn_r_p2": "   0.10"},
    ]}}


def seed_events(now: datetime, run_id: str) -> list[tuple[str, str | None, dict]]:
    """(토픽, 메시지 키, 이벤트) 목록. 받은 시각은 모두 now 보다 앞이고 직전 평일 같은 시각보다 뒤다."""
    fetched = now - timedelta(minutes=30)
    previous = now - timedelta(hours=2)
    kr_day = fetched.astimezone(KST).date()
    us_day = last_ny_close(fetched)
    events: list[tuple[str, str | None, dict]] = []

    def insight(api_type: str, target: str | None, response: dict, at: datetime) -> None:
        events.append((INSIGHT_TOPIC, target or api_type, _insight(api_type, target, response, at, run_id)))

    insight("daily-price", "TEST01", _daily_price(kr_day, 10150, 10000), fetched)
    insight("daily-price", "TEST02", _daily_price(kr_day, 4950, 5000), fetched)
    # 직전 영업일 행을 먼저 받은 것으로 두어 세계지수 등락률이 계산되게 한다.
    insight("world-index", "C", _world_index(_previous_weekday(us_day), {"TESTIX": "100.00", "FAKEIX": "50.00"}),
            previous)
    insight("world-index", "C", _world_index(us_day, {"TESTIX": "102.00", "FAKEIX": "49.00"}), fetched)
    insight("exchange-rate-summary", None, {"dataHeader": {"resultCode": "200"},
                                            "dataBody": {"out2": [{"cur_nm": "TEST CURRENCY", "cls_prc_p4": " 1000.0000"}]}},
            fetched)
    insight("market-summary", None, {"dataHeader": {"resultCode": "200"},
                                     "dataBody": {"out1": [{"indx_nm": "TEST INDEX", "now_indx_p2": " 2500.00",
                                                            "up_dwn_r_p2": "   0.40"}]}}, fetched)
    insight("overseas-daily-price", "TESTA", _overseas(us_day, "   1.25"), fetched)
    insight("overseas-daily-price", "FAKEB", _overseas(us_day, "  -0.80"), fetched)

    accepted = (now - timedelta(hours=3)).astimezone(timezone.utc).replace(microsecond=0)
    accession = f"0000000001-26-{int(hashlib.sha1(run_id.encode()).hexdigest(), 16) % 1000000:06d}"
    filing = {"tickers": ["TESTA"], "cik": "0000000001", "accessionNumber": accession, "form": "8-K",
              "items": ["2.02"], "acceptedAt": accepted.isoformat().replace("+00:00", "Z"),
              "filingDate": accepted.date().isoformat(),
              "url": "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/test-8k.htm"}
    events.append((FILING_TOPIC, accession, _envelope(FILING_TOPIC, accepted, f"daily-e2e-{run_id}", filing)))
    return events


def tick_events(now: datetime) -> list[tuple[str, str, dict]]:
    scheduled = now.astimezone(KST).replace(microsecond=0)
    out = []
    for job_type in JOB_TYPES.values():
        stamp = scheduled.isoformat()
        event = {"eventId": str(uuid.uuid4()), "eventType": TICK_TOPIC, "schemaVersion": 1, "occurredAt": stamp,
                 "producedAt": stamp, "correlationId": f"{job_type}-{stamp}",
                 "payload": {"jobType": job_type, "scheduledFor": stamp}}
        out.append((TICK_TOPIC, job_type, event))
    return out


def schema_errors(topic: str, event: dict) -> list[str]:
    from jsonschema import Draft202012Validator, FormatChecker

    schema = json.loads((CONTRACTS / f"{topic}.v1.schema.json").read_text(encoding="utf-8"))
    return [e.message for e in Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(event)]


# ---- 가짜 LLM ----

def fake_llm_output(user: str) -> str:
    """입력의 첫 근거 번호를 인용한 요약 한 줄. 근거가 없으면 근거 부족."""
    try:
        refs = [e["ref"] for item in json.loads(user).get("items", []) for e in item.get("entries", []) if "ref" in e]
    except (ValueError, AttributeError, TypeError):
        refs = []
    if not refs:
        return json.dumps({"points": [], "insufficientEvidence": True})
    return json.dumps({"points": [{"statement": FAKE_STATEMENT, "refs": [refs[0]]}], "insufficientEvidence": False},
                      ensure_ascii=False)


def fake_message(request: dict) -> dict:
    """메시지 API 응답 모양. SDK 가 읽는 칸만 채운다."""
    messages = request.get("messages") or [{}]
    content = messages[-1].get("content", "")
    if isinstance(content, list):
        content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
    return {"id": f"msg_fake_{uuid.uuid4().hex[:12]}", "type": "message", "role": "assistant",
            "model": request.get("model", "fake"), "stop_reason": "end_turn", "stop_sequence": None,
            "content": [{"type": "text", "text": fake_llm_output(content)}],
            "usage": {"input_tokens": 1, "output_tokens": 1}}


def serve_fake_llm(port_file: str) -> None:
    calls = {"count": 0}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: dict) -> None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802 - http.server 의 이름 규칙
            if self.path == "/__stats":
                with lock:
                    self._send(200, {"calls": calls["count"]})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            try:
                request = json.loads(self.rfile.read(length) or b"{}")
            except ValueError:
                self._send(400, {"type": "error", "error": {"type": "invalid_request_error", "message": "bad json"}})
                return
            if not self.path.startswith("/v1/messages"):
                self._send(404, {"type": "error", "error": {"type": "not_found_error", "message": "not found"}})
                return
            with lock:
                calls["count"] += 1
            self._send(200, fake_message(request))

        def log_message(self, fmt: str, *args) -> None:  # 요청 내용(본문)을 로그에 남기지 않는다
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    Path(port_file).write_text(str(server.server_address[1]), encoding="utf-8")
    server.serve_forever()


# ---- Kafka ----

def _now() -> datetime:
    return datetime.fromisoformat(os.environ["DAILY_E2E_NOW"])


def _produce(events) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": os.environ["DAILY_E2E_KAFKA"], "acks": "all", **COMMON})
    for topic, key, event in events:
        errors = schema_errors(topic, event)
        if errors:
            raise SystemExit(f"{topic} 이벤트가 계약에 맞지 않습니다: {errors[:3]}")
        producer.produce(topic, key=key, value=json.dumps(event, ensure_ascii=False))
    if producer.flush(10):
        raise SystemExit("이벤트를 모두 보내지 못했습니다")


def seed() -> None:
    events = seed_events(_now(), os.environ["DAILY_E2E_RUN_ID"])
    _produce(events)
    counts: dict[str, int] = {}
    for topic, _, event in events:
        name = event["payload"].get("apiType", topic)
        counts[name] = counts.get(name, 0) + 1
    print("발행: " + ", ".join(f"{name} {n}건" for name, n in counts.items()))


def ticks() -> None:
    events = tick_events(_now())
    _produce(events)
    print("발행: " + ", ".join(e["payload"]["jobType"] for _, _, e in events))


def read_dailies(bootstrap: str, correlation_ids: set[str], timeout_seconds: float = 15) -> list[dict]:
    """briefing.daily.generated 를 처음부터 읽어 이번 실행의 추적 ID 를 가진 이벤트만 돌려준다. 그룹을 만들지 않는다."""
    from confluent_kafka import Consumer, TopicPartition
    import time as clock

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"daily-e2e-reader-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False, **COMMON})
    metadata = consumer.list_topics(DAILY_TOPIC, timeout=10).topics[DAILY_TOPIC]
    consumer.assign([TopicPartition(DAILY_TOPIC, p, 0) for p in metadata.partitions])
    found: list[dict] = []
    deadline = clock.monotonic() + timeout_seconds
    while clock.monotonic() < deadline and len(found) < len(correlation_ids):
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        event = json.loads(message.value())
        if event.get("correlationId") in correlation_ids:
            found.append(event)
    consumer.close()
    return found


def check() -> None:
    scheduled = _now().astimezone(KST).replace(microsecond=0).isoformat()
    wanted = {f"{job_type}-{scheduled}" for job_type in JOB_TYPES.values()}
    dailies = read_dailies(os.environ["DAILY_E2E_KAFKA"], wanted)
    lines: list[str] = []
    ok = True
    by_edition = {d["payload"]["edition"]: d for d in dailies}
    for edition in JOB_TYPES:
        daily = by_edition.get(edition)
        if daily is None:
            print(f"{edition}: briefing.daily.generated 없음")
            ok = False
            continue
        errors = schema_errors(DAILY_TOPIC, daily)
        payload = daily["payload"]
        leaked = [t for t in US_TICKERS if t in payload["text"]]
        print(f"{edition}: generatedBy={payload['generatedBy']} missingItems={payload['missingItems']} "
              f"계약 위반 {len(errors)}건, 본문의 미국 티커 {len(leaked)}건")
        ok = ok and not errors and not leaked
        lines += [f"===== {edition} (generatedBy={payload['generatedBy']})", payload["text"], ""]
    Path(os.environ["DAILY_E2E_RESULT"]).write_text("\n".join(lines), encoding="utf-8")
    if not ok:
        raise SystemExit(1)


def main(argv: list[str]) -> None:
    if len(argv) >= 2 and argv[1] == "fake-llm" and len(argv) == 3:
        serve_fake_llm(argv[2])
    elif len(argv) == 2 and argv[1] in ("seed", "ticks", "check"):
        {"seed": seed, "ticks": ticks, "check": check}[argv[1]]()
    else:
        raise SystemExit("사용법: daily_e2e.py fake-llm <포트 파일> | seed | ticks | check")


if __name__ == "__main__":
    main(sys.argv)
