"""Kafka 루프: market.move -> briefing.generated (+ 브리핑 출력). 실행: python -m briefing.main"""
from __future__ import annotations

import logging
import os
import signal
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from briefing.compose import KST
from briefing.evidence import load_sample_news
from briefing.service import handle_move

log = logging.getLogger("briefing")

IN_TOPIC = "market.move"
OUT_TOPIC = "briefing.generated"
PARTITIONS = 3  # 생산자(시세처리기)의 선언과 같아야 한다
SAMPLE_NEWS = Path(__file__).resolve().parents[2] / "data" / "sample_news.json"
# 로컬 브로커는 IPv4만 받는다. 이 설정이 없으면 클라이언트가 먼저 ::1로 접속을 시도하며 연결 오류를 로그에 남긴다.
COMMON = {"broker.address.family": "v4"}


def ensure_topics(bootstrap: str) -> None:
    """이 서비스가 쓰는 토픽을 선언해서 시작 순서와 무관하게 동작하게 한다(브로커 자동 생성은 꺼져 있다)."""
    admin = AdminClient({"bootstrap.servers": bootstrap, **COMMON})
    futures = admin.create_topics([NewTopic(name, num_partitions=PARTITIONS, replication_factor=1) for name in (IN_TOPIC, OUT_TOPIC)])
    for name, future in futures.items():
        try:
            future.result()
            log.info("created topic %s", name)
        except Exception as error:  # noqa: BLE001 - confluent는 코드가 담긴 KafkaException을 던진다
            code = error.args[0].code() if error.args and hasattr(error.args[0], "code") else None
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    news = load_sample_news(SAMPLE_NEWS, anchor=datetime.now(KST))

    ensure_topics(bootstrap)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "briefing",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # 브리핑을 생산한 뒤에만 커밋한다(최소 한 번 처리)
        **COMMON,
    })
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", **COMMON})
    consumer.subscribe([IN_TOPIC])

    running = True

    def stop(*_args) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    log.info("briefing started: %s -> %s (%d sample news items)", IN_TOPIC, OUT_TOPIC, len(news))
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                log.error("consumer error: %s", message.error())
                continue
            try:
                result = handle_move(message.value(), news, datetime.now(KST))
            except ValueError as error:
                log.error("skipping malformed message at %s[%d]@%d: %s",
                          message.topic(), message.partition(), message.offset(), error)
                consumer.commit(message=message, asynchronous=False)
                continue
            producer.produce(OUT_TOPIC, key=result.key, value=result.value)
            producer.flush(10)
            consumer.commit(message=message, asynchronous=False)
            print(result.text, flush=True)
            print("-" * 60, flush=True)
    finally:
        consumer.close()
        producer.flush(5)
        log.info("briefing stopped")


if __name__ == "__main__":
    main()
