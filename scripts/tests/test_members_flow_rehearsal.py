"""구성 종목 리허설의 확인 단계: 가짜 운용사 서버 → 수집기 → Kafka(etf.members.fetched) → 시세처리기 → 테스트 전용 데이터베이스.

scripts/test-all.sh 가 실제 DB 단계 뒤에 일회용 Kafka 와 collector·market·가짜 운용사 서버(fake_fund_site.py)를 띄우고
수동 실행 명령 collect-etf-members 한 건을 보낸 뒤 아래 환경변수를 넣어 이 파일만 돌린다. 환경변수가 없으면(평소 pytest) 건너뛴다.

- MEMBERS_FLOW_DB_URL·MEMBERS_FLOW_DB_USER·MEMBERS_FLOW_DB_PASSWORD: 테스트 전용 데이터베이스(psycopg 주소, 시세처리기 계정)
- MEMBERS_FLOW_REQUEST_LOG: 가짜 운용사 서버의 요청 기록 파일
- MEMBERS_FLOW_WAIT_SECONDS: 반영을 기다리는 최대 시간. 기본 60
"""
import os
import time
from pathlib import Path

import pytest

import fake_fund_site as site

_VARS = ("MEMBERS_FLOW_DB_URL", "MEMBERS_FLOW_DB_USER", "MEMBERS_FLOW_DB_PASSWORD", "MEMBERS_FLOW_REQUEST_LOG")
# test-all.sh 가 넣는 리허설 ETF(가짜 종목코드 RHRS 접두어)와 출처 번호
OK_ETF, DOWN_ETF = "RHRS01", "RHRS02"

needs_rehearsal = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="구성 종목 리허설: scripts/test-all.sh 의 실제 DB 단계 뒤에서만 실행",
)


def _state(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT e.symbol, r.http_status, r.body, r.event_id IS NOT NULL FROM market.etf_stock_raw r"
                    " JOIN market.etf e ON e.id = r.etf_id WHERE e.symbol IN (%s, %s) ORDER BY e.symbol, r.id",
                    (OK_ETF, DOWN_ETF))
        raws = cur.fetchall()
        cur.execute("SELECT e.symbol, h.members FROM market.etf_stock_history h JOIN market.etf e ON e.id = h.etf_id"
                    " WHERE e.symbol IN (%s, %s) ORDER BY e.symbol, h.id", (OK_ETF, DOWN_ETF))
        history = cur.fetchall()
        cur.execute("SELECT s.market, s.symbol, m.rank FROM market.etf_stock_map m JOIN market.etf e ON e.id = m.etf_id"
                    " JOIN market.stock s ON s.id = m.stock_id WHERE e.symbol = %s ORDER BY m.rank", (OK_ETF,))
        mapped = [tuple(row) for row in cur.fetchall()]
    return raws, history, mapped


@needs_rehearsal
def test_members_fetched_by_the_collector_reach_the_market_tables_through_kafka():
    import psycopg
    from psycopg.conninfo import make_conninfo

    deadline = time.monotonic() + int(os.environ.get("MEMBERS_FLOW_WAIT_SECONDS", "60"))
    conninfo = make_conninfo(os.environ["MEMBERS_FLOW_DB_URL"], user=os.environ["MEMBERS_FLOW_DB_USER"],
                             password=os.environ["MEMBERS_FLOW_DB_PASSWORD"])
    with psycopg.connect(conninfo, autocommit=True) as conn:
        raws, history, mapped = _state(conn)
        # 두 ETF의 원문이 모두 저장되고 성공한 쪽 구성이 반영될 때까지 기다린다.
        while (len(raws) < 2 or not mapped) and time.monotonic() < deadline:
            time.sleep(1)
            raws, history, mapped = _state(conn)

    # 원문은 앞뒤 공백까지 가공 없이 저장되고, 실패한 응답도 원문이 남는다. 둘 다 이벤트 번호가 있다.
    assert raws == [(OK_ETF, 200, site.OK_BODY, True), (DOWN_ETF, 503, site.DOWN_BODY, True)]
    # 성공한 ETF만 이력 한 줄과 구성 연결 표가 생기고, 실패한 ETF는 그대로다.
    assert [symbol for symbol, _ in history] == [OK_ETF]
    assert [(m["market"], m["symbol"], m["rank"]) for m in history[0][1]] == site.OK_MEMBERS
    assert mapped == site.OK_MEMBERS
    # ETF마다 운용사 사이트 요청은 한 번이다.
    requests = Path(os.environ["MEMBERS_FLOW_REQUEST_LOG"]).read_text(encoding="utf-8").split()
    assert requests.count(site.OK_FUND) == 1
    assert requests.count(site.DOWN_FUND) == 1


def test_fake_site_answers_by_fund_id():
    # 가짜 서버의 응답 표 자체는 늘 확인한다(환경변수와 무관).
    assert site.response_for(site.OK_FUND) == (200, site.OK_BODY)
    assert site.response_for(site.DOWN_FUND) == (503, site.DOWN_BODY)
    assert site.response_for("OTHER") == (404, "")
