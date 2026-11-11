"""시세 흐름 실제 테스트의 확인 단계: 실제 시세 조회 → Kafka → market 이 테스트 전용 데이터베이스에 저장한 것을 다시 읽는다.

scripts/test-all.sh --live 가 일회용 Kafka 와 collector·market 을 띄우고 tick 을 보낸 뒤 아래 환경변수를 넣어 이 파일만 돌린다.
환경변수가 없으면(평소 pytest) 건너뛴다. 종목 코드와 시세 값은 읽지도 출력하지도 않고 행 개수만 센다.

- QUOTE_FLOW_LIVE_DB_URL·QUOTE_FLOW_LIVE_DB_USER·QUOTE_FLOW_LIVE_DB_PASSWORD: 테스트 전용 데이터베이스(psycopg 주소)
- QUOTE_FLOW_LIVE_EXPECTED: 저장돼야 하는 스냅샷 수(조회한 종목 수). 기본 1
- QUOTE_FLOW_LIVE_WAIT_SECONDS: 저장을 기다리는 최대 시간. 기본 60
- QUOTE_FLOW_LIVE_COUNTS_FILE: 행 개수를 남길 파일(호출 횟수 계산용). 없으면 남기지 않음
- QUOTE_FLOW_LIVE_SINCE: tick 을 보내기 직전의 DB 시각. 이 시각 뒤에 저장된 행만 센다(필수)
"""
import json
import os
import time

import pytest

_VARS = ("QUOTE_FLOW_LIVE_DB_URL", "QUOTE_FLOW_LIVE_DB_USER", "QUOTE_FLOW_LIVE_DB_PASSWORD")

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="시세 흐름 실제 테스트: scripts/test-all.sh --live 로만 실행",
)


def _counts(conn, since):
    # 같은 테스트 DB 에 앞 단계(실제 DB 테스트)가 넣은 가짜 행이 남아 있으므로 이 실행이 저장한 행만 센다.
    # 가짜 행에는 미래 날짜(예: 2033년)도 있어 시작 시각 뒤라는 조건만으로는 섞이므로 지금 이전으로도 자른다.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE raw IS NOT NULL AND raw::text NOT IN ('null', '{}', '\"\"'))"
                    " FROM market.price_snapshot WHERE received_at >= %s::timestamptz AND received_at <= now()", (since,))
        snapshots, with_raw = cur.fetchone()
        cur.execute("SELECT failure_type, count(*) FROM market.quote_failure WHERE received_at >= %s::timestamptz AND received_at <= now()"
                    " GROUP BY failure_type", (since,))
        failures = {row[0]: row[1] for row in cur.fetchall()}
        # 원인을 가르는 값만 읽는다. 종목 코드와 응답 원문은 읽지 않는다(처리 코드는 수집기가 형식을 확인한 값).
        cur.execute("SELECT failure_type, http_status, response_code, exception_type FROM market.quote_failure"
                    " WHERE received_at >= %s::timestamptz AND received_at <= now() ORDER BY received_at", (since,))
        details = [dict(zip(("type", "http", "code", "exception"), row)) for row in cur.fetchall()]
    return {"snapshots": snapshots, "with_raw": with_raw, "failures": failures, "failure_details": details}


def test_quote_reaches_test_database_through_kafka():
    import psycopg
    from psycopg.conninfo import make_conninfo

    since = os.environ["QUOTE_FLOW_LIVE_SINCE"]
    expected = int(os.environ.get("QUOTE_FLOW_LIVE_EXPECTED", "1"))
    deadline = time.monotonic() + int(os.environ.get("QUOTE_FLOW_LIVE_WAIT_SECONDS", "60"))
    conninfo = make_conninfo(os.environ["QUOTE_FLOW_LIVE_DB_URL"], user=os.environ["QUOTE_FLOW_LIVE_DB_USER"],
                             password=os.environ["QUOTE_FLOW_LIVE_DB_PASSWORD"])
    with psycopg.connect(conninfo, autocommit=True) as conn:
        counts = _counts(conn, since)
        # 스냅샷이든 실패 기록이든 종목 수만큼 도착하면 더 기다리지 않는다.
        while counts["snapshots"] + sum(counts["failures"].values()) < expected and time.monotonic() < deadline:
            time.sleep(1)
            counts = _counts(conn, since)

    counts_file = os.environ.get("QUOTE_FLOW_LIVE_COUNTS_FILE")
    if counts_file:
        with open(counts_file, "w", encoding="utf-8") as f:
            json.dump(counts, f)

    assert counts["failures"] == {}, f"조회 실패로 기록됨(실패 종류별 개수): {counts['failures']}, 원인: {counts['failure_details']}"
    assert counts["snapshots"] == expected, f"저장된 스냅샷 {counts['snapshots']}개, 기대 {expected}개"
    assert counts["with_raw"] == expected, f"원문이 비어 있지 않은 스냅샷 {counts['with_raw']}개, 기대 {expected}개"
