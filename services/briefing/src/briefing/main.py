"""Kafka 실행부: market.move -> 시세·기사 조회 -> briefing.generated. 실행: python -m briefing.main

Kafka를 import하는 곳은 이 파일뿐이다. 근거 조회(jobs), 리포트(report), 실패 기록(failure_store)은
메시지와 시각을 인자로 받기만 하고, 여기서 받은 메시지를 넘기고 그쪽이 낸 이벤트를 발행한다.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from datetime import datetime
from typing import Callable, Mapping, Protocol

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from briefing.compose import KST
from briefing.config import DEFAULT_SETTINGS, CoreSettings
from briefing.events import (
    ARTICLES_REQUESTED_TOPIC,
    BRIEFING_GENERATED_TOPIC,
    CONTEXT_REQUESTED_TOPIC,
    FILINGS_REQUESTED_TOPIC,
    HISTORY_REQUESTED_TOPIC,
    OutboundEvent,
)
from briefing.failure_store import PostgresFailureStore, psycopg_connector
from briefing.jobs import BriefingCore, Failure
from briefing.report import ReportStage, ReportWriter, build_report_writer

log = logging.getLogger("briefing")

MOVE_TOPIC = "market.move"
COLLECTION_COMPLETED_TOPIC = "collection.completed"
HISTORY_RESPONDED_TOPIC = "market.history.responded"
ARTICLES_RESPONDED_TOPIC = "news.articles.responded"
CONTEXT_RESPONDED_TOPIC = "market.context.responded"
FILINGS_RESPONDED_TOPIC = "news.filings.responded"

# 같은 토픽을 선언하는 다른 서비스(Topics.java)와 파티션 수가 같아야 한다. 어긋나면 일부 종목이 오류 없이 누락된다.
TOPIC_PARTITIONS = {
    MOVE_TOPIC: 3,                   # market
    COLLECTION_COMPLETED_TOPIC: 1,   # collector
    HISTORY_REQUESTED_TOPIC: 3,      # market
    HISTORY_RESPONDED_TOPIC: 3,      # market
    ARTICLES_REQUESTED_TOPIC: 3,     # news
    ARTICLES_RESPONDED_TOPIC: 3,     # news
    CONTEXT_REQUESTED_TOPIC: 3,      # market
    CONTEXT_RESPONDED_TOPIC: 3,      # market
    FILINGS_REQUESTED_TOPIC: 3,      # news
    FILINGS_RESPONDED_TOPIC: 3,      # news
    BRIEFING_GENERATED_TOPIC: 3,     # notifier
}
SUBSCRIBED_TOPICS = (MOVE_TOPIC, COLLECTION_COMPLETED_TOPIC, HISTORY_RESPONDED_TOPIC, ARTICLES_RESPONDED_TOPIC)

# 로컬 브로커는 IPv4만 받는다. 이 설정이 없으면 클라이언트가 먼저 ::1로 접속을 시도하며 연결 오류를 로그에 남긴다.
COMMON = {"broker.address.family": "v4"}
# 제한 시간이 없어도 이 간격으로는 깨어나 멈추기 요청을 확인한다.
MAX_POLL_SECONDS = 1.0
FLUSH_SECONDS = 10

# 실패 기록 DB. 다른 서비스(application.yml)와 같은 로컬 주소를 쓰고, 계정은 .env.example의 키를 따른다.
DB_HOST = "localhost"
DB_PORT = 5432
DEFAULT_DB_NAME = "stock"
DEFAULT_DB_USER = "briefing_svc"


def ensure_topics(bootstrap: str) -> None:
    """이 서비스가 쓰는 토픽을 선언해서 시작 순서와 무관하게 동작하게 한다(브로커 자동 생성은 꺼져 있다)."""
    admin = AdminClient({"bootstrap.servers": bootstrap, **COMMON})
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)
                                   for name, partitions in TOPIC_PARTITIONS.items()])
    for name, future in futures.items():
        try:
            future.result()
            log.info("created topic %s", name)
        except Exception as error:  # noqa: BLE001 - confluent는 코드가 담긴 KafkaException을 던진다
            code = error.args[0].code() if error.args and hasattr(error.args[0], "code") else None
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


class FailureStore(Protocol):
    def create_table(self) -> None: ...
    def record(self, failure: Failure) -> None: ...


class KafkaPublisher:
    """RequestPort 구현. 이벤트 키(종목코드)를 메시지 키로 쓴다."""

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def send(self, event: OutboundEvent) -> None:
        self._producer.produce(event.topic, key=event.key,
                               value=json.dumps(event.value, ensure_ascii=False), on_delivery=_on_delivery)
        # 요청은 응답 제한 시간을 재므로 버퍼에 묵히지 않고 바로 내보낸다.
        remaining = self._producer.flush(FLUSH_SECONDS)
        if remaining:
            log.error("%d message(s) not delivered within %ds (topic=%s)", remaining, FLUSH_SECONDS, event.topic)
        elif event.topic == BRIEFING_GENERATED_TOPIC:
            log.info("published briefing for %s (correlationId=%s)", event.key, event.value["correlationId"])


def _on_delivery(error, message) -> None:
    if error is not None:
        log.error("delivery failed for %s[%s]: %s", message.topic(), message.key(), error)


class BriefingRunner:
    """Kafka 수신·발행 실행부. start()로 시작하고 stop()으로 멈춘다.

    수신 루프는 자기 스레드 하나에서만 돌고 BriefingCore도 그 스레드에서만 부르므로 잠금이 필요 없다.
    main()뿐 아니라 다른 프로그램(예: 어드민 화면)도 같은 실행부를 시작·멈출 수 있게 분리했다.
    """

    def __init__(self, bootstrap: str, writer: ReportWriter, failures: FailureStore, *,
                 settings: CoreSettings = DEFAULT_SETTINGS,
                 clock: Callable[[], datetime] = lambda: datetime.now(KST),
                 consumer_factory: Callable[[dict], Consumer] = Consumer,
                 producer_factory: Callable[[dict], Producer] = Producer,
                 declare_topics: Callable[[str], None] = ensure_topics) -> None:
        self.bootstrap = bootstrap
        self.writer = writer
        self.failures = failures
        self.settings = settings
        self.clock = clock
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._declare_topics = declare_topics
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None
        self.core: BriefingCore | None = None
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self.error: BaseException | None = None

    # ---- 시작·멈추기 ----

    def start(self) -> None:
        """토픽과 테이블을 준비하고 수신 루프를 백그라운드 스레드로 시작한다. 준비가 실패하면 예외로 알린다."""
        if self._thread is not None:
            raise RuntimeError("runner already started")
        self.open()
        self._running.set()
        self._thread = threading.Thread(target=self._loop, name="briefing-kafka", daemon=True)
        self._thread.start()
        log.info("briefing started: %s -> %s", ", ".join(SUBSCRIBED_TOPICS),
                 ", ".join((HISTORY_REQUESTED_TOPIC, ARTICLES_REQUESTED_TOPIC, BRIEFING_GENERATED_TOPIC)))

    def request_stop(self) -> None:
        """루프에 멈추라고만 알린다. 시그널 처리기에서 부를 수 있도록 기다리지 않는다."""
        self._running.clear()

    def stop(self, timeout: float = 15.0) -> None:
        self.request_stop()
        self.wait(timeout)

    def wait(self, timeout: float | None = None) -> None:
        """루프가 끝날 때까지 기다린다. 시그널을 받을 수 있게 짧게 나눠 기다린다."""
        thread = self._thread
        if thread is None:
            return
        deadline = None if timeout is None else time.monotonic() + timeout
        while thread.is_alive() and (deadline is None or time.monotonic() < deadline):
            thread.join(0.5)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ---- 한 단계씩 (테스트도 이것을 직접 부른다) ----

    def open(self) -> None:
        self._declare_topics(self.bootstrap)
        self.failures.create_table()
        self._producer = self._producer_factory({"bootstrap.servers": self.bootstrap, "acks": "all", **COMMON})
        publisher = KafkaPublisher(self._producer)
        stage = ReportStage(self.writer, publisher, self.failures, clock=self.clock)
        self.core = BriefingCore(publisher, self.failures, stage, self.settings)
        self._consumer = self._consumer_factory({
            "bootstrap.servers": self.bootstrap,
            "group.id": "briefing",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # 읽은 위치는 handle_message에서 직접 확정한다
            **COMMON,
        })
        self._consumer.subscribe(list(SUBSCRIBED_TOPICS))

    def run_once(self) -> None:
        """다음 제한 시각까지만 메시지를 기다리고, 받은 메시지와 지난 제한 시간을 처리한다."""
        message = self._consumer.poll(self._poll_timeout())
        if message is not None:
            if message.error():
                log.error("consumer error: %s", message.error())
            else:
                self.handle_message(message)
        self._tick()

    def handle_message(self, message) -> None:
        # 작업 상태는 메모리에만 두므로, 읽은 위치를 처리 전에 바로 확정한다. 브리핑이 재시작되면
        # 처리 중이던 변동은 다시 읽지 않고 사라지는데, 이를 감수하기로 결정했다. 처리 뒤에 확정하면
        # 재시작 때 같은 변동을 다시 받아 같은 eventId 작업을 또 만들고, 이미 지난 응답을 기다리게 된다.
        # 응답·완료 알림도 메모리 상태에만 쓰이므로 같은 이유로 바로 확정한다.
        self._consumer.commit(message=message, asynchronous=False)
        handler = {
            MOVE_TOPIC: self.core.on_move,
            COLLECTION_COMPLETED_TOPIC: self.core.on_collection_completed,
            HISTORY_RESPONDED_TOPIC: self.core.on_history_responded,
            ARTICLES_RESPONDED_TOPIC: self.core.on_articles_responded,
        }.get(message.topic())
        if handler is None:
            log.warning("message from unexpected topic %s ignored", message.topic())
            return
        try:
            handler(message.value(), self.clock())
        except ValueError as error:
            log.error("skipping malformed message at %s[%d]@%d: %s",
                      message.topic(), message.partition(), message.offset(), error)

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._producer is not None:
            self._producer.flush(5)
            self._producer = None

    # ---- 내부 ----

    def _poll_timeout(self) -> float:
        deadline = self.core.next_deadline()
        if deadline is None:
            return MAX_POLL_SECONDS
        seconds = (deadline - self.clock()).total_seconds()
        return max(0.0, min(MAX_POLL_SECONDS, seconds))

    def _tick(self) -> None:
        now = self.clock()
        deadline = self.core.next_deadline()
        if deadline is not None and now >= deadline:
            self.core.on_tick(now)

    def _loop(self) -> None:
        try:
            while self._running.is_set():
                self.run_once()
        except BaseException as error:  # noqa: BLE001 - 루프가 죽은 이유를 남기고 wait()가 끝나게 한다
            self.error = error
            log.exception("briefing loop stopped by an error")
        finally:
            self._running.clear()
            self.close()
            log.info("briefing stopped")


def db_conninfo(env: Mapping[str, str]) -> str:
    from psycopg.conninfo import make_conninfo

    return make_conninfo(host=DB_HOST, port=DB_PORT,
                         dbname=env.get("POSTGRES_DB", "").strip() or DEFAULT_DB_NAME,
                         user=env.get("BRIEFING_DB_USER", "").strip() or DEFAULT_DB_USER,
                         password=env.get("BRIEFING_DB_PASSWORD", ""))


def build_runner(env: Mapping[str, str]) -> BriefingRunner:
    """환경변수로 실제 구성(Kafka, Postgres 실패 기록, LLM 또는 규칙 기반 작성기)을 만든다."""
    bootstrap = env.get("KAFKA_BOOTSTRAP", "localhost:9092")
    store = PostgresFailureStore(psycopg_connector(db_conninfo(env)))
    return BriefingRunner(bootstrap, build_report_writer(env), store)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    runner = build_runner(os.environ)

    signal.signal(signal.SIGINT, lambda *_: runner.request_stop())
    signal.signal(signal.SIGTERM, lambda *_: runner.request_stop())

    runner.start()
    runner.wait()
    if runner.error is not None:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
