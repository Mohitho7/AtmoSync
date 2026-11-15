"""Opt-in integration test for publishing to the local Kafka broker."""

from __future__ import annotations

import json
import os
import time
import unittest
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition

from atmo_sync.contracts import validate_sensor_event
from atmo_sync.producer import KafkaSensorProducer
from atmo_sync.simulator import SensorSimulator


@unittest.skipUnless(
    os.environ.get("ATMOSYNC_RUN_KAFKA_INTEGRATION") == "1",
    "set ATMOSYNC_RUN_KAFKA_INTEGRATION=1 to run the local Kafka integration test",
)
class KafkaProducerIntegrationTests(unittest.TestCase):
    def test_event_is_read_back_from_sensor_events(self):
        bootstrap_server = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        topic = os.environ.get("ATMOSYNC_KAFKA_TOPIC", "sensor-events")
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_server,
                "group.id": f"atmosync-producer-test-{uuid.uuid4()}",
                "enable.auto.commit": False,
            }
        )
        producer = None
        try:
            _, starting_offset = consumer.get_watermark_offsets(
                TopicPartition(topic, 0), timeout=10
            )
            consumer.assign([TopicPartition(topic, 0, starting_offset)])
            event = SensorSimulator(seed=804).generate_events(steps=1)[0]
            producer = KafkaSensorProducer(bootstrap_server, topic)
            producer.publish(event)
            self.assertEqual(producer.close(), 1)

            deadline = time.monotonic() + 15
            received = None
            while time.monotonic() < deadline:
                message = consumer.poll(0.5)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.fail(f"Kafka consumer error: {message.error()}")
                if message.key().decode("utf-8") == event["event_id"]:
                    received = message.value()
                    break

            self.assertIsNotNone(received, "published event was not read back from Kafka")
            decoded = json.loads(received.decode("utf-8"))
            validate_sensor_event(decoded)
            self.assertEqual(decoded, event)
        finally:
            if producer is not None:
                producer.close()
            consumer.close()


if __name__ == "__main__":
    unittest.main()
