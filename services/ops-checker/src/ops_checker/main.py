"""Kafka 실행부: daily.collection.completed(데일리 판 시작 알림) -> 기준 파일 지표 계산 -> ops 장부와 S3 결과 파일.
실행: python -m ops_checker.main

Kafka를 import하는 곳은 이 파일뿐이다. 알림 처리(flow)는 메시지를 인자로 받기만 한다.
"""
from __future__ import annotations

import logging
import signal
import threading

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from ops_checker.config import DAILY_COLLECTION_COMPLETED_PARTITIONS, DAILY_COLLECTION_COMPLETED_TOPIC, Settings
from ops_checker.flow import Checker, MalformedNotification
from ops_checker.results import S3ResultStore
from ops_checker.store import PostgresRunStore, PostgresScalarQuery, connector

log = logging.getLogger("ops-checker")

# 로컬 브로커는 IPv4만 받는다. 이 설정이 없으면 클라이언트가 먼저 ::1로 접속을 시도하며 연결 오류를 로그에 남긴다.
COMMON = {"broker.address.family": "v4"}
MAX_POLL_SECONDS = 1.0
# 처리에 실패한 알림(DB·S3 일시 장애)을 다시 시도하는 횟수와 간격. 끝내 안 되면 건너뛰고 오류 로그를 남긴다.
ATTEMPTS = 3
RETRY_SECONDS = 10


def ensure_topics(bootstrap: str) -> None:
    """이 서비스가 받는 토픽을 선언해 시작 순서와 무관하게 동작하게 한다. 파티션 수는 수집기와 같아야 한다."""
    admin = AdminClient({"bootstrap.servers": bootstrap, **COMMON})
    futures = admin.create_topics([NewTopic(DAILY_COLLECTION_COMPLETED_TOPIC,
                                            num_partitions=DAILY_COLLECTION_COMPLETED_PARTITIONS,
                                            replication_factor=1)])
    for future in futures.values():
        try:
            future.result()
        except Exception as error:  # noqa: BLE001 - confluent는 코드가 담긴 KafkaException을 던진다
            code = error.args[0].code() if error.args and hasattr(error.args[0], "code") else None
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def run(settings: Settings, stop: threading.Event) -> None:
    connect = connector(settings.db_host, settings.db_port, settings.db_name, settings.db_user, settings.db_password)
    checker = Checker(PostgresRunStore(connect), PostgresScalarQuery(connect, settings.deploy_version),
                      S3ResultStore(settings.results_bucket), settings.criteria_dir, settings.deploy_version)
    ensure_topics(settings.kafka_bootstrap)
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": "ops-checker",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False, **COMMON})
    consumer.subscribe([DAILY_COLLECTION_COMPLETED_TOPIC])
    try:
        while not stop.is_set():
            message = consumer.poll(MAX_POLL_SECONDS)
            if message is None:
                continue
            if message.error():
                log.error("kafka error: %s", message.error().code())
                continue
            if _handle(checker, message.value(), stop):
                consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


def _handle(checker: Checker, value: bytes, stop: threading.Event, retry_seconds: float = RETRY_SECONDS) -> bool:
    """알림을 처리한다. 끝났거나(건너뜀 포함) 포기했으면 True, 멈추기 요청으로 못 끝냈으면 False(오프셋을 남기지 않는다)."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            checker.handle(value)
            return True
        except MalformedNotification as error:
            log.error("skip malformed notification: %s", error)
            return True
        except Exception as error:  # noqa: BLE001 - DB·S3 일시 장애는 종류만 남기고 다시 시도한다
            log.error("check failed (attempt %d/%d): %s", attempt, ATTEMPTS, type(error).__name__)
            if attempt < ATTEMPTS and stop.wait(retry_seconds):
                return False
    log.error("giving up on notification after %d attempts", ATTEMPTS)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    run(settings, stop)


if __name__ == "__main__":
    main()
