"""전체 실행 스크립트(scripts/test-all.sh --live)의 브리핑·메일 실제 단계 도우미.

- seed: 시세 흐름 실제 단계가 테스트 전용 데이터베이스에 저장한 기준 종목 스냅샷 1행을 읽어, 그 가격을 기준으로 한
  가짜 변동(market.move) 1건과 가짜 근거 기사 1건, 수집 완료(collection.completed) 1건을 넣는다.
  기사는 news 스키마에 직접 넣고, 두 이벤트는 일회용 Kafka 에 발행한다.
- 나머지 함수는 확인 테스트(scripts/tests/test_briefing_flow_live.py)가 쓰는 계산이다(로그 세기, 리포트 파일 쓰기).

종목 코드와 실제 가격은 DB 에서 읽어 기사·이벤트에 넣을 뿐 출력하지 않는다. 이벤트는 일회용 Kafka 안에만 있다.
seed 는 아래 환경변수로만 설정을 받는다(값은 test-all.sh 가 실행할 때 넣음).

  LIVE_BRIEFING_DB_URL                                     테스트 전용 데이터베이스(psycopg 주소)
  LIVE_BRIEFING_MARKET_USER·LIVE_BRIEFING_MARKET_PASSWORD  스냅샷을 읽는 market 서비스 계정
  LIVE_BRIEFING_NEWS_USER·LIVE_BRIEFING_NEWS_PASSWORD      기사를 넣는 news 서비스 계정
  LIVE_BRIEFING_KAFKA                                      일회용 Kafka 주소
  LIVE_BRIEFING_RUN_ID                                     이 실행의 이름(추적 ID 에 씀)
  LIVE_BRIEFING_SINCE                                      시세 흐름 단계가 tick 을 보내기 직전의 DB 시각
"""
from __future__ import annotations

import hashlib
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
MOVE_TOPIC = "market.move"
COMPLETION_TOPIC = "collection.completed"
BRIEFING_TOPIC = "briefing.generated"
COLLECT_NEWS = "collect-news"
GENERATED_BY_LLM = "llm"
# 브리핑 임계값(1%)을 넘는 가짜 변동. 실제 가격을 이 비율만큼 오른 것으로 둔다.
MOVE_RATIO = 1.015
THRESHOLD_PERCENT = 1.0
# 브리핑은 startedAt 이 변동 시각 + Δ(5분) 이후인 수집 완료만 쓴다. 여유를 두어 6분 뒤로 둔다.
COMPLETION_AFTER = timedelta(minutes=6)
# 근거 범위(변동 24시간 전 ~ 변동 + Δ) 안쪽.
ARTICLE_BEFORE = timedelta(hours=1)
ARTICLE_TITLE = "[테스트] 전체 실행 점검용 가상 기사"
ARTICLE_SUMMARY = "실제 사건이 아닌 시험용 문장입니다. 전체 실행 스크립트가 브리핑 흐름을 확인하려고 넣었습니다."
ARTICLE_PUBLISHER = "test-all.invalid"

# 서비스 로그에서 세는 줄. 브리핑 report.py 의 재시도 경고, notifier SmtpMailSender·LoggingMailSender 의 로그와 같아야 한다.
LLM_RETRY_LOG = "LLM report attempt "
MAIL_SENT_LOG = "email sent to="
MAIL_DRY_RUN_LOG = "DRY-RUN (not sent)"

COMMON = {"broker.address.family": "v4"}


def correlation_id(run_id: str) -> str:
    return f"test-all-{run_id}"


def _ts(value: datetime) -> str:
    return value.astimezone(KST).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Snapshot:
    symbol: str
    price: int
    as_of: datetime


@dataclass(frozen=True)
class FakeArticle:
    event_id: str
    article_key: str
    title: str
    summary: str
    url: str
    publisher: str
    published_at: datetime


def move_event(snapshot: Snapshot, run_id: str, now: datetime, event_id: str | None = None) -> dict:
    """실제 스냅샷 가격을 지금 가격으로, 그보다 1.5% 낮은 값을 이전 가격으로 둔 가짜 변동."""
    if snapshot.price <= 0:
        raise ValueError("스냅샷 가격이 0 이하라 변동을 만들 수 없습니다")
    previous = round(snapshot.price / MOVE_RATIO)
    if previous <= 0 or previous == snapshot.price:
        raise ValueError("스냅샷 가격이 너무 작아 변동을 만들 수 없습니다")
    as_of = _ts(snapshot.as_of)
    return {
        "eventId": event_id or str(uuid.uuid4()),
        "eventType": "market.move",
        "schemaVersion": 1,
        "occurredAt": as_of,
        "producedAt": _ts(now),
        "correlationId": correlation_id(run_id),
        "payload": {
            "symbol": snapshot.symbol,
            "previousPrice": previous,
            "price": snapshot.price,
            "changePercent": round((snapshot.price - previous) / previous * 100, 2),
            "thresholdPercent": THRESHOLD_PERCENT,
            "asOf": as_of,
            "detectionType": "window",
            "moveStartedAt": as_of,
        },
    }


def completion_event(snapshot: Snapshot, run_id: str, now: datetime, event_id: str | None = None) -> dict:
    """변동 시각 + 6분에 시작한 것으로 둔 가짜 수집 완료. 가짜이므로 미래 시각이어도 된다."""
    started = snapshot.as_of + COMPLETION_AFTER
    finished = started + timedelta(seconds=1)
    return {
        "eventId": event_id or str(uuid.uuid4()),
        "eventType": "collection.completed",
        "schemaVersion": 1,
        "occurredAt": _ts(finished),
        "producedAt": _ts(now),
        "correlationId": correlation_id(run_id),
        "payload": {
            "jobType": COLLECT_NEWS,
            "scheduledFor": _ts(started),
            "startedAt": _ts(started),
            "finishedAt": _ts(finished),
            "publishedCount": 1,
            "incompleteSymbols": [],
        },
    }


def fake_article(snapshot: Snapshot, run_id: str, event_id: str | None = None) -> FakeArticle:
    url = f"https://{ARTICLE_PUBLISHER}/articles/{run_id}"
    return FakeArticle(
        event_id=event_id or str(uuid.uuid4()),
        article_key=hashlib.sha256(url.encode("utf-8")).hexdigest(),
        title=ARTICLE_TITLE,
        summary=ARTICLE_SUMMARY,
        url=url,
        publisher=ARTICLE_PUBLISHER,
        published_at=snapshot.as_of - ARTICLE_BEFORE,
    )


# ---------------------------------------------------------------- 확인 테스트가 쓰는 계산

def count_llm_calls(briefing_log: str, published: bool) -> int:
    """실제로 부른 LLM 횟수 = 재시도 경고 수 + (리포트를 만들었으면 성공한 마지막 호출 1)."""
    return briefing_log.count(LLM_RETRY_LOG) + (1 if published else 0)


def count_mail(notifier_log: str) -> tuple[int, int]:
    """(실제 발송 성공 수, 드라이런 수)."""
    return notifier_log.count(MAIL_SENT_LOG), notifier_log.count(MAIL_DRY_RUN_LOG)


def run_briefings(messages: list, run_id: str) -> tuple[list, int]:
    """(이 실행의 추적 ID 를 가진 브리핑 목록, 다른 브리핑 수)."""
    mine = [m for m in messages if m.get("correlationId") == correlation_id(run_id)]
    return mine, len(messages) - len(mine)


def report_text(event: dict) -> str:
    payload = event.get("payload", {})
    lines = [payload.get("text", ""), "", f"생성 방식: {payload.get('generatedBy', '')}",
             f"브리핑 ID: {event.get('eventId', '')}", "근거:"]
    for item in payload.get("evidence", []):
        lines.append(f"  - {item.get('title', '')} ({item.get('source', '')}, {item.get('publishedAt', '')}) {item.get('url', '')}")
    return "\n".join(lines) + "\n"


def write_private(path: Path, text: str) -> None:
    """본인만 읽는 파일(600)로 쓴다. 본문에 종목 코드·가격이 들어 있다."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(path, 0o600)


# ---------------------------------------------------------------- seed 실행부

def _env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} 가 없습니다")
    return value


def _connect(user_var: str, password_var: str):
    import psycopg
    from psycopg.conninfo import make_conninfo

    return psycopg.connect(make_conninfo(_env("LIVE_BRIEFING_DB_URL"), user=_env(user_var),
                                         password=os.environ.get(password_var, "")), autocommit=True)


def read_snapshot() -> Snapshot:
    with _connect("LIVE_BRIEFING_MARKET_USER", "LIVE_BRIEFING_MARKET_PASSWORD") as conn, conn.cursor() as cur:
        # 같은 DB 에 실제 DB 테스트가 넣은 가짜 스냅샷이 남아 있으므로 이 실행의 시세 흐름이 저장한 행만 본다.
        # 가짜 스냅샷에는 미래 날짜도 있어 지금 이전으로도 자른다.
        since = _env("LIVE_BRIEFING_SINCE")
        cur.execute("SELECT count(*) FROM market.price_snapshot WHERE received_at >= %s::timestamptz AND received_at <= now()", (since,))
        (count,) = cur.fetchone()
        if count != 1:
            raise RuntimeError(f"이번 실행이 저장한 스냅샷이 {count}행입니다(1행이어야 함)")
        cur.execute("SELECT symbol, price, as_of FROM market.price_snapshot WHERE received_at >= %s::timestamptz AND received_at <= now()",
                    (since,))
        symbol, price, as_of = cur.fetchone()
    return Snapshot(symbol, int(price), as_of)


def insert_article(article: FakeArticle, symbol: str) -> None:
    with _connect("LIVE_BRIEFING_NEWS_USER", "LIVE_BRIEFING_NEWS_PASSWORD") as conn, conn.transaction(), \
            conn.cursor() as cur:
        cur.execute(
            "INSERT INTO news.news_article (event_id, article_key, title, summary, url, publisher, published_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (uuid.UUID(article.event_id), article.article_key, article.title, article.summary, article.url,
             article.publisher, article.published_at))
        (article_id,) = cur.fetchone()
        cur.execute("INSERT INTO news.news_article_symbol (article_id, symbol) VALUES (%s, %s)", (article_id, symbol))


def existing_moves(bootstrap: str, run_id: str) -> int:
    """market.move 에 이미 쌓인 메시지 수. market 이 스스로 낸 변동이 있으면 리포트가 2건이 되어 LLM 을 더 부른다."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"test-all-seed-{run_id}", **COMMON})
    try:
        topic = consumer.list_topics(MOVE_TOPIC, timeout=10).topics.get(MOVE_TOPIC)
        if topic is None or topic.error is not None:
            return 0
        total = 0
        for partition in topic.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(MOVE_TOPIC, partition), timeout=10)
            total += high - low
        return total
    finally:
        consumer.close()


def publish(bootstrap: str, events: list) -> None:
    import json

    from confluent_kafka import Producer

    errors = []

    def on_delivery(error, _message):
        if error is not None:
            errors.append(str(error.code()))

    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", **COMMON})
    for topic, key, value in events:
        producer.produce(topic, key=key, value=json.dumps(value, ensure_ascii=False), on_delivery=on_delivery)
    remaining = producer.flush(15)
    if remaining or errors:
        raise RuntimeError(f"이벤트 발행 실패(남은 {remaining}건, 오류 {', '.join(errors) or '없음'})")


def seed() -> int:
    run_id = _env("LIVE_BRIEFING_RUN_ID")
    bootstrap = _env("LIVE_BRIEFING_KAFKA")
    snapshot = read_snapshot()
    moves = existing_moves(bootstrap, run_id)
    if moves:
        raise RuntimeError(f"market.move 에 이미 {moves}건이 있어 넣지 않았습니다(리포트가 여러 건이 되어 LLM 을 더 부름)")
    now = datetime.now(KST)
    insert_article(fake_article(snapshot, run_id), snapshot.symbol)
    move = move_event(snapshot, run_id, now)
    completion = completion_event(snapshot, run_id, now)
    # 변동을 먼저 보낸다. 브리핑은 완료 알림이 먼저 와도 보관했다가 쓰지만, 순서를 고정해 흐름을 단순하게 둔다.
    publish(bootstrap, [(MOVE_TOPIC, snapshot.symbol, move), (COMPLETION_TOPIC, COLLECT_NEWS, completion)])
    print("넣음: 가짜 근거 기사 1건, 변동 1건, 수집 완료 1건")
    return 0


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv != ["seed"]:
        print("사용법: live_briefing.py seed", file=sys.stderr)
        return 64
    try:
        return seed()
    except RuntimeError as error:
        print(f"넣기 실패: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 - DB·Kafka 예외 문구에 종목 코드 같은 값이 섞일 수 있어 종류 이름만 남긴다
        print(f"넣기 실패: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
