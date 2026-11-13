import re
from concurrent.futures import Future
from pathlib import Path

from briefing import main


class FakeAdminClient:
    created: list = []

    def __init__(self, _config):
        pass

    def create_topics(self, new_topics):
        FakeAdminClient.created = new_topics
        futures = {}
        for topic in new_topics:
            future = Future()
            future.set_result(None)
            futures[topic.topic] = future
        return futures


def test_declares_both_topics_with_the_partition_count_the_other_services_use(monkeypatch):
    monkeypatch.setattr(main, "AdminClient", FakeAdminClient)

    main.ensure_topics("localhost:9092")

    declared = {topic.topic: topic.num_partitions for topic in FakeAdminClient.created}
    # 시세처리기, 발송기의 Topics.java와 같은 3이어야 한다. 어긋나면 일부 종목이 오류 없이 누락된다.
    assert declared["market.move"] == 3
    assert declared["briefing.generated"] == 3


def test_declares_every_topic_it_reads_or_writes_with_the_partition_count_the_other_services_use(monkeypatch):
    monkeypatch.setattr(main, "AdminClient", FakeAdminClient)

    main.ensure_topics("localhost:9092")

    declared = {topic.topic: topic.num_partitions for topic in FakeAdminClient.created}
    # 수집기(collection.completed 1, daily.collection.completed 1, news.research.requested 3), 뉴스처리기(news.stored 1),
    # 발송기(briefing.daily.generated 1), 시세처리기·발송기(나머지 3)의 Topics.java와 같아야 한다.
    # 근거 자료 조회는 내부 조회 API라 요청·응답 토픽이 없다.
    assert declared == {
        "market.move": 3,
        "collection.completed": 1,
        "news.stored": 1,
        "news.research.requested": 3,
        "briefing.generated": 3,
        "briefing.daily.generated": 1,
        "daily.collection.completed": 1,
    }


def test_declares_the_daily_briefing_topic_with_the_partition_count_the_notifier_uses(monkeypatch):
    monkeypatch.setattr(main, "AdminClient", FakeAdminClient)

    main.ensure_topics("localhost:9092")

    declared = {topic.topic: topic.num_partitions for topic in FakeAdminClient.created}
    # 발송기 Topics.java 의 DAILY_PARTITIONS 와 같은 1이어야 한다.
    assert declared["briefing.daily.generated"] == 1


def test_declares_the_stored_notice_topic_with_the_partition_count_news_uses(monkeypatch):
    monkeypatch.setattr(main, "AdminClient", FakeAdminClient)

    main.ensure_topics("localhost:9092")

    declared = {topic.topic: topic.num_partitions for topic in FakeAdminClient.created}
    # 뉴스처리기 Topics.java 의 COMPLETION_PARTITIONS 와 같은 1이어야 한다.
    assert declared["news.stored"] == 1


JAVA_SERVICES = Path(__file__).resolve().parents[2]
_DECLARATION = re.compile(r'TopicBuilder\.name\((\w+)\)\.partitions\((\w+)\)')


def java_declarations() -> dict[str, dict[str, int]]:
    """각 Java 서비스의 Topics.java에서 (토픽 -> 파티션 수)를 읽는다."""
    result = {}
    for path in JAVA_SERVICES.glob("*/src/main/java/**/Topics.java"):
        text = path.read_text(encoding="utf-8")
        constants = dict(re.findall(r'(\w+)\s*=\s*"([^"]+)"', text))
        numbers = {name: int(value) for name, value in re.findall(r'(\w+)\s*=\s*(\d+)\s*;', text)}
        topics = {}
        for name, partitions in _DECLARATION.findall(text):
            topics[constants[name]] = int(partitions) if partitions.isdigit() else numbers[partitions]
        result[path.parts[len(JAVA_SERVICES.parts)]] = topics
    return result


def test_partition_counts_match_every_java_service_that_declares_the_same_topic(monkeypatch):
    monkeypatch.setattr(main, "AdminClient", FakeAdminClient)
    main.ensure_topics("localhost:9092")
    declared = {topic.topic: topic.num_partitions for topic in FakeAdminClient.created}

    java = java_declarations()
    shared = {(service, topic) for service, topics in java.items() for topic in topics if topic in declared}

    # 브리핑이 쓰는 토픽이 모두 어느 Java 서비스에서 함께 선언되어야 비교가 의미 있다.
    assert {topic for _, topic in shared} == set(declared)
    for service, topic in shared:
        assert java[service][topic] == declared[topic], f"{topic}: briefing {declared[topic]} != {service}"
