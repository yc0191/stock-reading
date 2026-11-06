"""브리핑·메일 실제 테스트의 확인 단계: 가짜 변동 → 근거 조회(market·news) → 실제 LLM 리포트 → briefing.generated → 실제 메일 1통.

scripts/test-all.sh --live 가 일회용 Kafka 와 news·notifier·briefing 을 띄우고 scripts/live_briefing.py seed 로
변동·기사·수집 완료를 넣은 뒤 아래 환경변수를 넣어 이 파일만 돌린다. 환경변수가 없으면(평소 pytest) 건너뛴다.
리포트 본문은 로컬 파일(권한 600)에만 쓰고, 화면에는 개수와 생성 방식만 남긴다(본문에 종목 코드·가격이 있음).

- BRIEFING_FLOW_LIVE_KAFKA·BRIEFING_FLOW_LIVE_RUN_ID: 일회용 Kafka 주소, 이 실행의 이름
- BRIEFING_FLOW_LIVE_DB_URL·BRIEFING_FLOW_LIVE_DB_USER·BRIEFING_FLOW_LIVE_DB_PASSWORD: 테스트 전용 데이터베이스(브리핑 계정)
- BRIEFING_FLOW_LIVE_BRIEFING_LOG·BRIEFING_FLOW_LIVE_NOTIFIER_LOG: 이 실행이 띄운 브리핑·notifier 의 로그 파일
- BRIEFING_FLOW_LIVE_REPORT_FILE: 리포트 본문을 남길 파일
- BRIEFING_FLOW_LIVE_COUNTS_FILE: 개수를 남길 파일(호출 횟수 계산용). 없으면 남기지 않음
- BRIEFING_FLOW_LIVE_WAIT_SECONDS: 리포트를 기다리는 최대 시간. 기본 240
- BRIEFING_FLOW_LIVE_SINCE: 이 단계를 시작하기 직전의 DB 시각. 이 시각 뒤에 기록된 실패만 본다(필수)
"""
import json
import os
import time
from pathlib import Path

import pytest

import live_briefing as lb

_VARS = ("BRIEFING_FLOW_LIVE_KAFKA", "BRIEFING_FLOW_LIVE_RUN_ID", "BRIEFING_FLOW_LIVE_DB_URL",
         "BRIEFING_FLOW_LIVE_DB_USER", "BRIEFING_FLOW_LIVE_DB_PASSWORD", "BRIEFING_FLOW_LIVE_BRIEFING_LOG",
         "BRIEFING_FLOW_LIVE_NOTIFIER_LOG", "BRIEFING_FLOW_LIVE_REPORT_FILE")
# 리포트를 받은 뒤 메일 발송 성공 줄을 기다리는 시간. 발송이 실패하면 notifier 가 3초 간격으로 다시 시도한다.
MAIL_WAIT_SECONDS = 60
# 마지막으로 늦게 오는 중복 브리핑·메일이 없는지 보는 시간.
SETTLE_SECONDS = 5

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(name, "").strip() for name in _VARS),
    reason="브리핑·메일 실제 테스트: scripts/test-all.sh --live 로만 실행",
)


def _read(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def _failed_jobs(conn, since: str) -> list:
    with conn.cursor() as cur:
        # 끝내 실패한 줄의 사유(정해진 값)만 읽는다. 관련 데이터에는 값이 섞일 수 있어 읽지 않는다.
        # 앞 단계(실제 DB 테스트)가 같은 DB 에 남긴 행은 빼고 이 실행이 기록한 것만 본다.
        cur.execute("SELECT reason FROM briefing.job_log WHERE entry_type = 'step-failed' "
                    "AND created_at >= %s::timestamptz ORDER BY created_at",
                    (since,))
        return [row[0] for row in cur.fetchall()]


def _poll_briefings(consumer, messages: list, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        messages.append(json.loads(message.value()))


def test_llm_report_is_published_and_one_real_email_is_sent():
    import psycopg
    from confluent_kafka import Consumer
    from psycopg.conninfo import make_conninfo

    env = os.environ
    run_id = env["BRIEFING_FLOW_LIVE_RUN_ID"]
    since = env["BRIEFING_FLOW_LIVE_SINCE"]
    wait = int(env.get("BRIEFING_FLOW_LIVE_WAIT_SECONDS", "240"))
    conninfo = make_conninfo(env["BRIEFING_FLOW_LIVE_DB_URL"], user=env["BRIEFING_FLOW_LIVE_DB_USER"],
                             password=env["BRIEFING_FLOW_LIVE_DB_PASSWORD"])
    consumer = Consumer({"bootstrap.servers": env["BRIEFING_FLOW_LIVE_KAFKA"], "group.id": f"test-all-verify-{run_id}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False, **lb.COMMON})
    consumer.subscribe([lb.BRIEFING_TOPIC])
    messages: list = []
    failed: list = []
    try:
        with psycopg.connect(conninfo, autocommit=True) as conn:
            deadline = time.monotonic() + wait
            # 리포트가 나오거나 실패가 기록될 때까지 기다린다.
            while time.monotonic() < deadline:
                _poll_briefings(consumer, messages, 2)
                failed = _failed_jobs(conn, since)
                if lb.run_briefings(messages, run_id)[0] or failed:
                    break
            mail_deadline = time.monotonic() + MAIL_WAIT_SECONDS
            while lb.run_briefings(messages, run_id)[0] and time.monotonic() < mail_deadline:
                if lb.count_mail(_read(env["BRIEFING_FLOW_LIVE_NOTIFIER_LOG"]))[0]:
                    break
                time.sleep(1)
            _poll_briefings(consumer, messages, SETTLE_SECONDS)
            failed = _failed_jobs(conn, since)
    finally:
        consumer.close()

    mine, others = lb.run_briefings(messages, run_id)
    generated_by = mine[0]["payload"].get("generatedBy", "") if mine else ""
    mail, dry_run = lb.count_mail(_read(env["BRIEFING_FLOW_LIVE_NOTIFIER_LOG"]))
    counts = {
        "briefings": len(mine),
        "otherBriefings": others,
        "generatedBy": generated_by,
        "llm": lb.count_llm_calls(_read(env["BRIEFING_FLOW_LIVE_BRIEFING_LOG"]), published=bool(mine)),
        "mail": mail,
        "dryRun": dry_run,
        "failedJobs": len(failed),
    }
    counts_file = env.get("BRIEFING_FLOW_LIVE_COUNTS_FILE")
    if counts_file:
        Path(counts_file).write_text(json.dumps(counts), encoding="utf-8")
    if mine:
        lb.write_private(Path(env["BRIEFING_FLOW_LIVE_REPORT_FILE"]), lb.report_text(mine[0]))

    assert failed == [], f"브리핑 실패로 기록됨(이유): {failed}"
    assert len(mine) == 1, f"이 실행의 브리핑 {len(mine)}건, 기대 1건({wait}초 기다림)"
    assert others == 0, f"이 실행이 넣지 않은 브리핑 {others}건"
    assert generated_by == lb.GENERATED_BY_LLM, f"생성 방식 {generated_by!r}, 기대 'llm'(키가 없거나 자리표시 값이면 규칙 기반이 됨)"
    assert dry_run == 0, f"드라이런 {dry_run}건(발송 스위치가 테스트 notifier 에 들어가지 않음)"
    assert mail == 1, f"메일 발송 성공 {mail}건, 기대 1건"
