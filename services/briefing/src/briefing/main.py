"""Kafka 실행부: market.move -> 시세 이력·배경 자료·기사·공시 조회 -> briefing.generated,
schedule.tick(데일리 판) -> 데일리 자료·공시 조회 -> briefing.daily.generated. 실행: python -m briefing.main

Kafka를 import하는 곳은 이 파일뿐이다. 근거 조회(jobs), 리포트(report), 작업 기록(job_log_store)은
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
from briefing.config import DEFAULT_DAILY_SETTINGS, DEFAULT_SETTINGS, CoreSettings, DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_report import DailyLlmWriter, build_daily_writer
from briefing.events import (
    ARTICLES_REQUESTED_TOPIC,
    BRIEFING_GENERATED_TOPIC,
    CONTEXT_REQUESTED_TOPIC,
    DAILY_BRIEFING_GENERATED_TOPIC,
    EDITION_MORNING,
    EDITION_POST_CLOSE,
    FILINGS_REQUESTED_TOPIC,
    HISTORY_REQUESTED_TOPIC,
    RESEARCH_REQUESTED_TOPIC,
    OutboundEvent,
)
from briefing.job_log import JobLogPort
from briefing.job_log_store import PostgresJobLog, psycopg_connector
from briefing.jobs import BriefingCore
from briefing.llm import AnthropicClient
from briefing.report import ReportStage, ReportWriter, build_report_writer
from briefing.research import QueryWriter, build_query_writer
from briefing.sentence_research import ResearchReportStage, SentenceResearch
from briefing.status import StatusPort, parse_collection
from briefing.status_store import PostgresStatusStore

log = logging.getLogger("briefing")

MOVE_TOPIC = "market.move"
COLLECTION_COMPLETED_TOPIC = "collection.completed"
HISTORY_RESPONDED_TOPIC = "market.history.responded"
ARTICLES_RESPONDED_TOPIC = "news.articles.responded"
CONTEXT_RESPONDED_TOPIC = "market.context.responded"
FILINGS_RESPONDED_TOPIC = "news.filings.responded"
DAILY_DATA_REQUESTED_TOPIC = "market.daily.requested"
DAILY_DATA_RESPONDED_TOPIC = "market.daily.responded"
SCHEDULE_TICK_TOPIC = "schedule.tick"

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
    RESEARCH_REQUESTED_TOPIC: 3,     # collector
    BRIEFING_GENERATED_TOPIC: 3,     # notifier
    DAILY_BRIEFING_GENERATED_TOPIC: 1,  # notifier
    DAILY_DATA_REQUESTED_TOPIC: 3,   # market
    DAILY_DATA_RESPONDED_TOPIC: 3,   # market
    SCHEDULE_TICK_TOPIC: 1,          # collector
}
SUBSCRIBED_TOPICS = (MOVE_TOPIC, COLLECTION_COMPLETED_TOPIC, HISTORY_RESPONDED_TOPIC, ARTICLES_RESPONDED_TOPIC,
                     CONTEXT_RESPONDED_TOPIC, FILINGS_RESPONDED_TOPIC, DAILY_DATA_RESPONDED_TOPIC)
# schedule.tick 은 파티션이 1개라 같은 그룹이면 한쪽 판만 tick 을 받아 다른 판이 오류 없이 빠진다. 판마다 그룹을 따로 둔다.
TICK_CONSUMER_GROUPS = {
    EDITION_MORNING: {"group.id": "briefing-daily-morning"},
    EDITION_POST_CLOSE: {"group.id": "briefing-daily-post-close"},
}

# 로컬 브로커는 IPv4만 받는다. 이 설정이 없으면 클라이언트가 먼저 ::1로 접속을 시도하며 연결 오류를 로그에 남긴다.
COMMON = {"broker.address.family": "v4"}
# 제한 시간이 없어도 이 간격으로는 깨어나 멈추기 요청을 확인한다.
MAX_POLL_SECONDS = 1.0
FLUSH_SECONDS = 10
# 한 번에 이어서 읽을 메시지 수 상한. 메시지가 계속 쌓여도 tick과 제한 시각 확인이 밀리지 않게 한다.
MAX_MESSAGES_PER_RUN = 50
# 쌓인 메시지를 읽는 데 쓸 시간 상한. 메시지마다 LLM 호출로 10초 넘게 걸려도 tick 소비자 poll(기본 300초 안에 필요)과
# 멈추기 요청 확인이 이 안에 돌게 하되, 약한 문장 5개의 다시 쓰기(약 70초)는 한 번에 처리할 만큼 넉넉히 둔다.
MAX_READ_SECONDS = 120

# 작업 기록 DB. 다른 서비스(application.yml)와 같은 로컬 주소를 쓰고, 계정은 .env.example의 키를 따른다.
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


class JobLogStore(JobLogPort, Protocol):
    def create_table(self) -> None: ...


class StatusStore(StatusPort, Protocol):
    def create_table(self) -> None: ...


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
        elif event.topic == DAILY_BRIEFING_GENERATED_TOPIC:
            log.info("published daily briefing %s (correlationId=%s)", event.key, event.value["correlationId"])


def _on_delivery(error, message) -> None:
    if error is not None:
        log.error("delivery failed for %s[%s]: %s", message.topic(), message.key(), error)


class BriefingRunner:
    """Kafka 수신·발행 실행부. start()로 시작하고 stop()으로 멈춘다.

    수신 루프는 자기 스레드 하나에서만 돌고 BriefingCore도 그 스레드에서만 부르므로 잠금이 필요 없다.
    main()뿐 아니라 다른 프로그램(예: 어드민 화면)도 같은 실행부를 시작·멈출 수 있게 분리했다.
    """

    def __init__(self, bootstrap: str, writer: ReportWriter, job_log: JobLogStore, *,
                 settings: CoreSettings = DEFAULT_SETTINGS,
                 clock: Callable[[], datetime] = lambda: datetime.now(KST),
                 consumer_factory: Callable[[dict], Consumer] = Consumer,
                 producer_factory: Callable[[dict], Producer] = Producer,
                 declare_topics: Callable[[str], None] = ensure_topics,
                 query_writer: QueryWriter | None = None,
                 daily_writer: DailyLlmWriter | None = None,
                 daily_settings: DailySettings = DEFAULT_DAILY_SETTINGS,
                 status_store: StatusStore | None = None) -> None:
        self.bootstrap = bootstrap
        self.writer = writer
        self.job_log = job_log
        self.settings = settings
        self.clock = clock
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._declare_topics = declare_topics
        self.daily_writer = daily_writer
        self.daily_settings = daily_settings
        self.status_store = status_store
        self.daily: DailyCore | None = None
        self.tick_consumers: dict[str, Consumer] = {}
        self.query_writer = query_writer
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None
        self.core: BriefingCore | None = None
        self.research: ResearchReportStage | None = None
        self.sentences: SentenceResearch | None = None
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
                 ", ".join((HISTORY_REQUESTED_TOPIC, CONTEXT_REQUESTED_TOPIC, ARTICLES_REQUESTED_TOPIC,
                            FILINGS_REQUESTED_TOPIC, RESEARCH_REQUESTED_TOPIC, BRIEFING_GENERATED_TOPIC,
                            DAILY_DATA_REQUESTED_TOPIC, DAILY_BRIEFING_GENERATED_TOPIC)))

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
        self.job_log.create_table()
        if self.status_store is not None:
            self.status_store.create_table()
        self._producer = self._producer_factory({"bootstrap.servers": self.bootstrap, "acks": "all", **COMMON})
        publisher = KafkaPublisher(self._producer)
        stage = ReportStage(self.writer, publisher, self.job_log, clock=self.clock)
        # 급변동과 데일리 판이 문장 단위 재검색 흐름 하나를 함께 쓴다.
        self.sentences = SentenceResearch(self.query_writer, publisher, self.job_log, self.settings)
        self.research = ResearchReportStage(stage, self.sentences)
        self.core = BriefingCore(publisher, self.research, self.settings, self.job_log)
        self.daily = DailyCore(publisher, self.daily_writer, self.job_log, self.daily_settings,
                               sentences=self.sentences, status=self.status_store)
        for edition, group in TICK_CONSUMER_GROUPS.items():
            consumer = self._consumer_factory({
                "bootstrap.servers": self.bootstrap,
                **group,
                # 그룹이 처음 생길 때 지난 tick 을 처음부터 읽으면 지난 판이 한꺼번에 다시 나간다. 그룹이 생긴 뒤의 tick 만 받는다.
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                **COMMON,
            })
            consumer.subscribe([SCHEDULE_TICK_TOPIC])
            self.tick_consumers[edition] = consumer
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
        started = self.clock()
        # 메시지 하나를 처리하는 데 LLM 호출로 오래 걸릴 수 있다. 그동안 이미 도착한 다른 응답을 읽지 않고 제한 시간부터
        # 보면 응답을 받아 두고도 무응답으로 처리하므로, 쌓인 메시지를 먼저 읽은 뒤 제한 시간을 본다.
        for _ in range(MAX_MESSAGES_PER_RUN):
            if message is None:
                break
            if message.error():
                log.error("consumer error: %s", message.error())
            else:
                self.handle_message(message)
            if (self.clock() - started).total_seconds() >= MAX_READ_SECONDS:
                break
            message = self._consumer.poll(0)
        for edition, consumer in self.tick_consumers.items():
            tick = consumer.poll(0)
            if tick is not None:
                if tick.error():
                    log.error("tick consumer error (%s): %s", edition, tick.error())
                else:
                    self.handle_tick(edition, consumer, tick)
        self._tick()

    def handle_message(self, message) -> None:
        # 작업 상태는 메모리에만 두므로, 읽은 위치를 처리 전에 바로 확정한다. 브리핑이 재시작되면
        # 처리 중이던 변동은 다시 읽지 않고 사라지는데, 이를 감수하기로 결정했다. 처리 뒤에 확정하면
        # 재시작 때 같은 변동을 다시 받아 같은 eventId 작업을 또 만들고, 이미 지난 응답을 기다리게 된다.
        # 응답·완료 알림도 메모리 상태에만 쓰이므로 같은 이유로 바로 확정한다.
        self._consumer.commit(message=message, asynchronous=False)
        handler = {
            MOVE_TOPIC: self.core.on_move,
            # 재검색 완료 알림과 재검색 기사 응답은 재검색 쪽이 먼저 가져가고, 아니면 근거 조회 작업으로 넘긴다.
            COLLECTION_COMPLETED_TOPIC: self._on_collection_completed,
            HISTORY_RESPONDED_TOPIC: self.core.on_history_responded,
            ARTICLES_RESPONDED_TOPIC: self._on_articles_responded,
            CONTEXT_RESPONDED_TOPIC: self.core.on_context_responded,
            FILINGS_RESPONDED_TOPIC: self._on_filings_responded,
            DAILY_DATA_RESPONDED_TOPIC: self.daily.on_daily_data_responded,
        }.get(message.topic())
        if handler is None:
            log.warning("message from unexpected topic %s ignored", message.topic())
            return
        try:
            handler(message.value(), self.clock())
        except ValueError as error:
            log.error("skipping malformed message at %s[%d]@%d: %s",
                      message.topic(), message.partition(), message.offset(), error)

    def handle_tick(self, edition: str, consumer: Consumer, message) -> None:
        # 판 작업도 메모리에만 두므로 급변동 알림과 같은 이유로 읽은 위치를 처리 전에 확정한다.
        consumer.commit(message=message, asynchronous=False)
        try:
            self.daily.on_schedule_tick(message.value(), self.clock(), edition)
        except ValueError as error:
            log.error("skipping malformed tick at %s[%d]@%d: %s",
                      message.topic(), message.partition(), message.offset(), error)

    def _on_collection_completed(self, message: str | bytes, now: datetime) -> None:
        # 서버 상태 절이 센다. 저장은 eventId로 멱등하다. 저장에 실패해도 근거 조회 흐름은 막지 않는다.
        if self.status_store is not None:
            try:
                record = parse_collection(message)
            except ValueError as error:
                # 상태용 해석이 틀려도 근거 조회는 계속 받아야 한다. 원문과 외부 값은 로그에 남기지 않는다.
                log.warning("collection.completed not saved for server status: %s", type(error).__name__)
            else:
                self.status_store.save_collection(record)
        self.sentences.on_collection_completed(message, now) or self.core.on_collection_completed(message, now)

    def _on_articles_responded(self, message: str | bytes, now: datetime) -> None:
        # 재검색 기사 응답은 재검색 쪽이 먼저 가져가고, 아니면 급변동 작업과 마감 후 판 작업이 각자 모르는 jobId는 넘긴다.
        if self.sentences.on_articles_responded(message, now):
            return
        self.core.on_articles_responded(message, now)
        self.daily.on_articles_responded(message, now)

    def _on_filings_responded(self, message: str | bytes, now: datetime) -> None:
        # 급변동 작업과 데일리 판 작업이 같은 토픽으로 공시 응답을 받는다. 각자 모르는 jobId는 넘긴다.
        self.core.on_filings_responded(message, now)
        self.daily.on_filings_responded(message, now)

    def close(self) -> None:
        for consumer in self.tick_consumers.values():
            consumer.close()
        self.tick_consumers = {}
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._producer is not None:
            self._producer.flush(5)
            self._producer = None

    # ---- 내부 ----

    def _next_deadline(self) -> datetime | None:
        deadlines = [d for d in (self.core.next_deadline(), self.sentences.next_deadline(),
                                 self.daily.next_deadline()) if d is not None]
        return min(deadlines) if deadlines else None

    def _poll_timeout(self) -> float:
        deadline = self._next_deadline()
        if deadline is None:
            return MAX_POLL_SECONDS
        seconds = (deadline - self.clock()).total_seconds()
        return max(0.0, min(MAX_POLL_SECONDS, seconds))

    def _tick(self) -> None:
        now = self.clock()
        deadline = self.core.next_deadline()
        if deadline is not None and now >= deadline:
            self.core.on_tick(now)
        deadline = self.sentences.next_deadline()
        if deadline is not None and now >= deadline:
            self.sentences.on_tick(now)
        deadline = self.daily.next_deadline()
        if deadline is not None and now >= deadline:
            self.daily.on_tick(now)

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
    """환경변수로 실제 구성(Kafka, Postgres 작업 기록, LLM 또는 규칙 기반 작성기)을 만든다."""
    bootstrap = env.get("KAFKA_BOOTSTRAP", "localhost:9092")
    connect = psycopg_connector(db_conninfo(env))
    store = PostgresJobLog(connect)
    return BriefingRunner(bootstrap, build_report_writer(env), store, daily_writer=build_daily_writer(env),
                          query_writer=build_query_writer(env, AnthropicClient),
                          status_store=PostgresStatusStore(connect))


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
