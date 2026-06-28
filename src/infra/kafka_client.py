import json
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


class KafkaManager:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def _get_admin(self):
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def create_topics(self, topics: list[str], num_partitions: int = 3):
        admin = self._get_admin()
        new_topics = [NewTopic(t, num_partitions, 1) for t in topics]
        try:
            admin.create_topics(new_topics)
        except TopicAlreadyExistsError:
            pass
        admin.close()

    def list_topics(self) -> set[str]:
        admin = self._get_admin()
        topics = admin.list_topics()
        admin.close()
        return set(topics)

    def produce(self, topic: str, message: dict):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, message)
        producer.flush()
        producer.close()

    def consume(self, topic: str, max_messages: int = 10, timeout: int = 10) -> list[dict]:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=timeout * 1000,
        )
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= max_messages:
                break
        consumer.close()
        return messages
