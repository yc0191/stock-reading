from concurrent.futures import Future

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
    assert declared == {"market.move": 3, "briefing.generated": 3}
