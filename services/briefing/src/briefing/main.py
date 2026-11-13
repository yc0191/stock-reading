"""Kafka 실행부: market.move + 저장 끝 알림(news.stored) -> 시세 이력·배경 자료·기사·공시 조회 -> briefing.generated,
daily.collection.completed(데일리 판 시작 알림) 또는 한도 시각 -> 데일리 자료·기사·공시 조회 -> 멤버마다 briefing.daily.generated.
실행: python -m briefing.main

Kafka를 import하는 곳은 이 파일뿐이다. 알림(이벤트)은 Kafka로 받고 내며, 시세처리기·뉴스처리기의 자료는 내부 조회 API를
직접 부른다(http_queries). 근거 조회(jobs), 리포트(report), 작업 기록(job_log_store)은 메시지·조회 결과와 시각을 인자로 받기만
하고, 여기서 받은 메시지와 끝난 조회의 결과를 넘기고 그쪽이 낸 이벤트를 발행한다.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from collections import OrderedDict
from dataclasses import replace
from datetime import datetime
from typing import Callable, Mapping, Protocol

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from briefing.compose import KST
from briefing.config import DEFAULT_DAILY_SETTINGS, DEFAULT_SETTINGS, CoreSettings, DailySettings
from briefing.daily_jobs import DailyCore
from briefing.daily_members import MemberPort
from briefing.daily_final_check import daily_final_check
from briefing.daily_report import DailyLlmWriter, build_daily_writer
from briefing.events import (
    BRIEFING_GENERATED_TOPIC,
    DAILY_BRIEFING_GENERATED_TOPIC,
    NEWS_STORED,
    RESEARCH_REQUESTED_TOPIC,
    OutboundEvent,
)
from briefing.http_queries import HttpQueries, base_urls
from briefing.job_log import JobLogPort
from briefing.job_log_store import PostgresJobLog, psycopg_connector
from briefing.job_step import StepRecordingLlm, job_scope
from briefing.jobs import BriefingCore
from briefing.llm import AnthropicClient
from briefing.member_store import PostgresMemberStore
from briefing.move_group import MoveGrouper
from briefing.move_merge import MorningMerge
from briefing.move_final_check import move_final_check
from briefing.queries import OnResult, QueryPort, QueryResult
from briefing.report import ReportStage, ReportWriter, build_report_writer
from briefing.research import QueryWriter, build_query_writer
from briefing.sentence_research import ResearchReportStage, SentenceResearch
from briefing.status import StatusPort, parse_collection
from briefing.status_store import PostgresStatusStore
from briefing.trace import configure_logging, correlation_context, from_event, from_headers, kafka_headers

log = logging.getLogger("briefing")

MOVE_TOPIC = "market.move"
COLLECTION_COMPLETED_TOPIC = "collection.completed"
NEWS_STORED_TOPIC = NEWS_STORED
DAILY_COLLECTION_COMPLETED_TOPIC = "daily.collection.completed"

# 같은 토픽을 선언하는 다른 서비스(Topics.java)와 파티션 수가 같아야 한다. 어긋나면 일부 종목이 오류 없이 누락된다.
TOPIC_PARTITIONS = {
    MOVE_TOPIC: 3,                   # market
    COLLECTION_COMPLETED_TOPIC: 1,   # collector
    NEWS_STORED_TOPIC: 1,            # news
    RESEARCH_REQUESTED_TOPIC: 3,     # collector
    BRIEFING_GENERATED_TOPIC: 3,     # notifier
    DAILY_BRIEFING_GENERATED_TOPIC: 1,  # notifier
    DAILY_COLLECTION_COMPLETED_TOPIC: 1,  # collector
}
SUBSCRIBED_TOPICS = (MOVE_TOPIC, COLLECTION_COMPLETED_TOPIC, NEWS_STORED_TOPIC, DAILY_COLLECTION_COMPLETED_TOPIC)

# 로컬 브로커는 IPv4만 받는다. 이 설정이 없으면 클라이언트가 먼저 ::1로 접속을 시도하며 연결 오류를 로그에 남긴다.
COMMON = {"broker.address.family": "v4"}
# 제한 시간이 없어도 이 간격으로는 깨어나 멈추기 요청을 확인한다.
MAX_POLL_SECONDS = 1.0
FLUSH_SECONDS = 10
# 한 번에 이어서 읽을 메시지 수 상한. 메시지가 계속 쌓여도 제한 시각과 한도 시각 확인이 밀리지 않게 한다.
MAX_MESSAGES_PER_RUN = 50
# 쌓인 메시지를 읽는 데 쓸 시간 상한. 메시지마다 LLM 호출로 10초 넘게 걸려도 소비자 poll(기본 300초 안에 필요)과
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


JobLogStore = JobLogPort
StatusStore = StatusPort


class QueryRunner(QueryPort, Protocol):
    """수신 루프가 끝난 조회의 결과를 꺼내 가는 조회 구현(HttpQueries)."""

    def drain(self) -> list[tuple[OnResult, QueryResult]]: ...

    def close(self) -> None: ...


class KafkaPublisher:
    """RequestPort 구현. 이벤트 키(종목코드)를 메시지 키로 쓴다."""

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def send(self, event: OutboundEvent) -> None:
        # 받는 쪽이 본문을 풀지 않고도 추적 ID를 로그 문맥에 걸 수 있게 헤더에 같은 값을 싣는다.
        self._producer.produce(event.topic, key=event.key,
                               value=json.dumps(event.value, ensure_ascii=False), headers=kafka_headers(event.value),
                               on_delivery=_on_delivery)
        # 재검색 요청은 저장 끝을 기다리는 상한을 재므로 버퍼에 묵히지 않고 바로 내보낸다.
        remaining = self._producer.flush(FLUSH_SECONDS)
        if remaining:
            log.error("%d message(s) not delivered within %ds (topic=%s)", remaining, FLUSH_SECONDS, event.topic)
        elif event.topic == BRIEFING_GENERATED_TOPIC:
            log.info("published briefing for %s (correlationId=%s)", event.key, event.value["correlationId"])
        elif event.topic == DAILY_BRIEFING_GENERATED_TOPIC:
            log.info("published daily briefing %s (correlationId=%s)", event.key, event.value["correlationId"])


def message_correlation_id(message) -> str | None:
    """받은 메시지의 추적 ID. 헤더를 먼저 보고, 헤더가 없는 메시지(헤더를 싣기 전에 쌓인 메시지)는 본문에서 읽는다."""
    return from_headers(message.headers()) or from_event(message.value())


def _event_id(message: str | bytes) -> str:
    """알림의 eventId. 없으면 ValueError(형식이 잘못된 메시지로 건너뛴다)."""
    try:
        event_id = json.loads(message)["eventId"]
    except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"event without eventId: {error!r}") from error
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event without eventId")
    return event_id


def _on_delivery(error, message) -> None:
    if error is not None:
        log.error("delivery failed for %s[%s]: %s", message.topic(), message.key(), error)


class BriefingRunner:
    """Kafka 수신·발행 실행부. start()로 시작하고 stop()으로 멈춘다.

    수신 루프는 자기 스레드 하나에서만 돌고 BriefingCore도 그 스레드에서만 부르므로 잠금이 필요 없다. 내부 조회 API 호출은
    조회 구현의 작업 스레드에서 돌고, 끝난 결과는 수신 루프가 꺼내 이 스레드에서 작업에 넘긴다.
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
                 status_store: StatusStore | None = None,
                 member_store: MemberPort | None = None,
                 api_factory: Callable[[], QueryRunner] | None = None) -> None:
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
        self.member_store = member_store
        self._api_factory = api_factory
        self.api: QueryRunner | None = None
        # 저장 끝 알림은 같은 eventId로 다시 올 수 있어(뉴스처리기 재시작·재시도) 처리한 eventId를 기억한다.
        self._stored_ids: OrderedDict[str, None] = OrderedDict()
        self.daily: DailyCore | None = None
        self.query_writer = query_writer
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None
        self.core: BriefingCore | None = None
        self.grouper: MoveGrouper | None = None
        self.merge: MorningMerge | None = None
        self.research: ResearchReportStage | None = None
        self.sentences: SentenceResearch | None = None
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self.error: BaseException | None = None

    # ---- 시작·멈추기 ----

    def start(self) -> None:
        """토픽을 선언하고, 이전 실행이 남긴 작업을 정리하고, 수신 루프를 백그라운드 스레드로 시작한다. 준비가 실패하면 예외로 알린다."""
        if self._thread is not None:
            raise RuntimeError("runner already started")
        self.open()
        self._running.set()
        self._thread = threading.Thread(target=self._loop, name="briefing-kafka", daemon=True)
        self._thread.start()
        log.info("briefing started: %s -> %s", ", ".join(SUBSCRIBED_TOPICS),
                 ", ".join((RESEARCH_REQUESTED_TOPIC, BRIEFING_GENERATED_TOPIC, DAILY_BRIEFING_GENERATED_TOPIC)))

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
        # 테이블은 Flyway가 브리핑보다 먼저 만들어 둔다. 이전 실행에서 끝나지 않은 작업은 다시 실행하지 않고 실패로만 표시한다.
        self.job_log.fail_unfinished(self.clock())
        self._producer = self._producer_factory({"bootstrap.servers": self.bootstrap, "acks": "all", **COMMON})
        publisher = KafkaPublisher(self._producer)
        # 조회 시도마다 작업 기록 저장소의 job_step에 한 줄씩 남긴다.
        self.api = self._api_factory() if self._api_factory is not None else HttpQueries(
            base_urls(os.environ), self.job_log, self.clock)
        stage = ReportStage(self.writer, publisher, self.job_log, clock=self.clock)
        # 급변동과 데일리 판이 문장 단위 재검색 흐름 하나를 함께 쓴다.
        self.sentences = SentenceResearch(self.query_writer, publisher, self.api, self.job_log, self.settings)
        # LLM 본문은 발행 직전에 마지막 검사(코드 검사, 필요하면 LLM 검토와 고침)를 거친다.
        self.research = ResearchReportStage(stage, self.sentences, move_final_check(self.writer, self.job_log))
        self.core = BriefingCore(publisher, self.api, self.research, self.settings, self.job_log)
        # 같은 종목의 급변동은 모아서 작업 하나로 넘긴다.
        self.grouper = MoveGrouper(self.core.on_move, self.job_log, self.settings)
        # 장 시작부터 아침 판 시작 사이의 급변동은 따로 리포트를 만들지 않고 아침 판에 합친다(실패하면 리포트로 넘긴다).
        self.merge = MorningMerge(self.grouper.on_move, self.job_log, self.daily_settings)
        self.daily = DailyCore(publisher, self.api, self.daily_writer, self.job_log, self.daily_settings,
                               sentences=self.sentences, status=self.status_store, members=self.member_store,
                               on_morning_result=self.merge.on_morning_result,
                               final=daily_final_check(self.daily_writer, self.job_log))
        self.merge.morning_in_progress = self.daily.morning_in_progress
        self._consumer = self._consumer_factory({
            "bootstrap.servers": self.bootstrap,
            "group.id": "briefing",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # 읽은 위치는 handle_message에서 직접 확정한다
            **COMMON,
        })
        self._consumer.subscribe(list(SUBSCRIBED_TOPICS))

    def run_once(self) -> None:
        """다음 제한 시각까지만 메시지를 기다리고, 받은 메시지·끝난 조회의 결과와 지난 제한 시간을 처리한다."""
        message = self._consumer.poll(self._poll_timeout())
        started = self.clock()
        # 메시지 하나를 처리하는 데 LLM 호출로 오래 걸릴 수 있다. 그동안 이미 도착한 알림을 읽지 않고 제한 시간부터
        # 보면 알림을 받아 두고도 기다림 한도로 처리하므로, 쌓인 메시지를 먼저 읽은 뒤 제한 시간을 본다.
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
        self.deliver_results()
        self._tick()

    def deliver_results(self) -> None:
        """끝난 조회의 결과를 그 조회를 맡긴 작업에 넘긴다. 결과를 받은 작업이 리포트·판을 쓰면 LLM을 부를 수 있다."""
        for on_result, result in self.api.drain():
            query = result.query
            # 결과 처리 중 로그 줄에 그 작업의 추적 ID를, LLM 호출 기록(job_step)에 그 작업 id를 건다.
            with correlation_context(query.correlation_id), job_scope(query.job_id):
                try:
                    on_result(result, self.clock())
                except ValueError as error:
                    log.error("skipping %s result for job %s: %s", query.kind.value, query.job_id, error)

    def handle_message(self, message) -> None:
        # 작업 상태와 모으는 중인 변동은 메모리에만 두므로, 읽은 위치를 처리 전에 바로 확정한다. 브리핑이 재시작되면
        # 처리 중이던 변동은 다시 읽지 않는다. 정상 종료 때는 모으는 중이던 변동과 진행 중이던 작업을 실패로 남기고(시작할 때
        # 진행 중이던 작업도 실패로 표시한다), 강제 종료만 흔적 없이 사라지는데 이를 감수하기로 결정했다. 처리 뒤에 확정하면
        # 재시작 때 같은 변동을 다시 받아 같은 eventId 작업을 또 만들고, 이미 지난 응답을 기다리게 된다.
        # 응답·완료 알림도 메모리 상태에만 쓰이므로 같은 이유로 바로 확정한다.
        self._consumer.commit(message=message, asynchronous=False)
        handler = {
            MOVE_TOPIC: self._on_move,
            COLLECTION_COMPLETED_TOPIC: self._on_collection_completed,
            # 재검색의 저장 끝은 재검색 쪽이 먼저 가져가고, 아니면 근거 조회 작업으로 넘긴다.
            NEWS_STORED_TOPIC: self._on_news_stored,
            DAILY_COLLECTION_COMPLETED_TOPIC: self.daily.on_collection_ready,
        }.get(message.topic())
        with correlation_context(message_correlation_id(message)):
            if handler is None:
                log.warning("message from unexpected topic %s ignored", message.topic())
                return
            try:
                handler(message.value(), self.clock())
            except ValueError as error:
                log.error("skipping malformed message at %s[%d]@%d: %s",
                          message.topic(), message.partition(), message.offset(), error)

    def _on_move(self, message: str | bytes, now: datetime) -> None:
        # 장 시작~아침 판 시작 사이 변동은 아침 판이 담으므로 모아 두고, 그 밖의 변동은 급변동 리포트로 간다.
        if not self.daily_settings.morning_merge_enabled or not self.merge.on_move(message, now):
            self.grouper.on_move(message, now)

    def _on_collection_completed(self, message: str | bytes, now: datetime) -> None:
        # 서버 상태 절이 센다. 저장은 eventId로 멱등하다. 근거 조회는 이 알림이 아니라 뉴스처리기의 저장 끝 알림으로 시작한다.
        if self.status_store is None:
            return
        try:
            record = parse_collection(message)
        except ValueError as error:
            # 원문과 외부 값은 로그에 남기지 않는다.
            log.warning("collection.completed not saved for server status: %s", type(error).__name__)
        else:
            self.status_store.save_collection(record)

    def _on_news_stored(self, message: str | bytes, now: datetime) -> None:
        # 같은 수집 완료의 저장 끝은 같은 eventId로 다시 올 수 있다. 두 번째부터는 근거 조회를 다시 시작하지 않는다.
        event_id = _event_id(message)
        if event_id in self._stored_ids:
            log.info("duplicate %s %s ignored", NEWS_STORED, event_id)
            return
        # T+5분 재검색의 저장 끝은 급변동 작업이 먼저 가져간다. 문장 단위 재검색의 것이면 그쪽이 가져가고, 아니면 정기 수집이다.
        (self.core.on_research_stored(message, now) or self.sentences.on_stored(message, now)
         or self.core.on_news_stored(message, now))
        # 해석에 실패한 알림(ValueError)은 기억하지 않는다. 처리한 알림만 기억한다.
        self._stored_ids[event_id] = None
        while len(self._stored_ids) > self.settings.remembered_jobs:
            self._stored_ids.popitem(last=False)

    def close(self) -> None:
        if self.api is not None:
            self.api.close()
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._producer is not None:
            self._producer.flush(5)
            self._producer = None

    # ---- 내부 ----

    def _next_deadline(self) -> datetime | None:
        deadlines = [d for d in (self.grouper.next_deadline(), self.merge.next_deadline(), self.core.next_deadline(),
                                 self.sentences.next_deadline()) if d is not None]
        return min(deadlines) if deadlines else None

    def _poll_timeout(self) -> float:
        deadline = self._next_deadline()
        if deadline is None:
            return MAX_POLL_SECONDS
        seconds = (deadline - self.clock()).total_seconds()
        return max(0.0, min(MAX_POLL_SECONDS, seconds))

    def _tick(self) -> None:
        now = self.clock()
        # 아침 판에 합치려고 모아 둔 변동 중 때가 지난 것을 먼저 리포트 흐름으로 넘기고, 모은 변동을 내보낸다.
        # 그래야 방금 만든 작업의 제한 시간을 같은 순서에서 볼 수 있다.
        deadline = self.merge.next_deadline()
        if deadline is not None and now >= deadline:
            self.merge.on_tick(now)
        deadline = self.grouper.next_deadline()
        if deadline is not None and now >= deadline:
            self.grouper.on_tick(now)
        deadline = self.core.next_deadline()
        if deadline is not None and now >= deadline:
            self.core.on_tick(now)
        deadline = self.sentences.next_deadline()
        if deadline is not None and now >= deadline:
            self.sentences.on_tick(now)
        # 판 시작 알림의 한도 시각은 루프가 최대 MAX_POLL_SECONDS마다 깨어나므로 그 간격으로 본다.
        self.daily.check_deadlines(now)

    def _abandon_pending_moves(self) -> None:
        # 읽은 위치는 이미 확정했으므로 모으는 중이던 변동은 다시 오지 않는다. 사라지지 않고 실패 기록으로 남긴다.
        # 기록이 실패해도 종료는 막지 않는다(저장 재시도와 실패 로그는 저장소가 한다).
        if self.grouper is None:
            return
        try:
            abandoned = self.grouper.abandon_pending(self.clock())
            if self.merge is not None:
                abandoned += self.merge.abandon_pending(self.clock())
        except Exception:  # noqa: BLE001 - 종료를 막지 않는다
            log.exception("could not record pending market.move groups at shutdown")
            return
        if abandoned:
            log.warning("shutdown marked %d pending market.move group(s) as interrupted: %s",
                        len(abandoned), ", ".join(abandoned))

    def _loop(self) -> None:
        try:
            while self._running.is_set():
                self.run_once()
        except BaseException as error:  # noqa: BLE001 - 루프가 죽은 이유를 남기고 wait()가 끝나게 한다
            self.error = error
            log.exception("briefing loop stopped by an error")
        finally:
            self._running.clear()
            self._abandon_pending_moves()
            self.close()
            log.info("briefing stopped")


def db_conninfo(env: Mapping[str, str]) -> str:
    from psycopg.conninfo import make_conninfo

    return make_conninfo(host=DB_HOST, port=DB_PORT,
                         dbname=env.get("POSTGRES_DB", "").strip() or DEFAULT_DB_NAME,
                         user=env.get("BRIEFING_DB_USER", "").strip() or DEFAULT_DB_USER,
                         password=env.get("BRIEFING_DB_PASSWORD", ""))


def build_runner(env: Mapping[str, str]) -> BriefingRunner:
    """환경변수로 실제 구성(Kafka, 내부 조회 API 주소, Postgres 작업 기록, LLM 또는 규칙 기반 작성기)을 만든다."""
    bootstrap = env.get("KAFKA_BOOTSTRAP", "localhost:9092")
    connect = psycopg_connector(db_conninfo(env))
    store = PostgresJobLog(connect)
    # 아침 판 합침과 한도 시각 안전장치는 수집기의 판 시작 알림을 켜는 값과 같은 환경변수로 켠다. 알림을 꺼 둔 곳에서는
    # 합칠 판이 없고, 평일마다 판이 저절로 나가면 안 된다.
    daily_on = env.get("COLLECTOR_DAILY_BRIEFING_ENABLED", "").strip().lower() == "true"
    daily_settings = replace(DEFAULT_DAILY_SETTINGS, morning_merge_enabled=daily_on, deadline_enabled=daily_on)
    # 같은 사건 기사 묶기 기준값은 급변동·데일리 작성기와 서비스가 같은 CoreSettings를 쓴다.
    settings = DEFAULT_SETTINGS
    def clock() -> datetime:
        return datetime.now(KST)

    def llm(api_key: str, model: str) -> StepRecordingLlm:
        # LLM 호출마다 job_step 한 줄을 남긴다(시세처리기·뉴스처리기 조회는 HttpQueries가 남긴다).
        return StepRecordingLlm(AnthropicClient(api_key, model), store, clock)

    return BriefingRunner(bootstrap, build_report_writer(env, llm, settings=settings), store, settings=settings,
                          clock=clock,
                          daily_writer=build_daily_writer(env, llm, core=settings),
                          daily_settings=daily_settings,
                          query_writer=build_query_writer(env, llm),
                          status_store=PostgresStatusStore(connect), member_store=PostgresMemberStore(connect),
                          api_factory=lambda: HttpQueries(base_urls(env), store, clock))


def main() -> None:
    configure_logging(logging.INFO)
    runner = build_runner(os.environ)

    signal.signal(signal.SIGINT, lambda *_: runner.request_stop())
    signal.signal(signal.SIGTERM, lambda *_: runner.request_stop())

    runner.start()
    runner.wait()
    if runner.error is not None:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
