import pytest
from src.infra.kafka_client import KafkaManager


@pytest.fixture
def kafka_mgr():
    return KafkaManager(bootstrap_servers="localhost:9092")


def test_create_topics(kafka_mgr):
    topics = ["raw.alerts", "aggregated.alerts", "k8s.events"]
    kafka_mgr.create_topics(topics)
    existing = kafka_mgr.list_topics()
    for t in topics:
        assert t in existing


def test_produce_and_consume(kafka_mgr):
    topic = "raw.alerts"
    test_msg = {"alert_id": "test-001", "severity": "critical"}
    kafka_mgr.produce(topic, test_msg)
    messages = kafka_mgr.consume(topic, max_messages=1, timeout=5)
    assert len(messages) == 1
    assert messages[0]["alert_id"] == "test-001"
