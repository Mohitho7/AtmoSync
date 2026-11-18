"""Opt-in live Kafka tests with a Snowflake test double."""

from __future__ import annotations

import json
import os
import time
import unittest
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition

from atmo_sync.consumers.base import ConsumerConfig
from atmo_sync.consumers.snowflake import SnowflakeIngestionConsumer
from atmo_sync.simulator import SensorSimulator


@unittest.skipUnless(
    os.environ.get("ATMOSYNC_RUN_KAFKA_INTEGRATION") == "1",
    "set ATMOSYNC_RUN_KAFKA_INTEGRATION=1 to run local Kafka integration tests",
)
class SnowflakeConsumerKafkaIntegrationTests(unittest.TestCase):
    def test_ingestion_commit_restart_and_partition_rebalance(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        topic = "sensor-events-validated"
        group_id = f"atmosync-snowflake-integration-{uuid.uuid4()}"
        partitions = self._seed_group_at_current_end(bootstrap, group_id, topic)
        shared_rows: set[tuple[str, int, int]] = set()
        first_event = self._event(771)
        self._produce(bootstrap, topic, first_event, partitions[0])

        config = ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id=group_id,
            topic=topic,
        )
        results_a = _FakeSnowflake(shared_rows)
        member_a = SnowflakeIngestionConsumer(
            config,
            snowflake_client=results_a,
        )
        member_b = None
        restarted = None
        try:
            self._wait_for_assignment([member_a], set(partitions))
            self._consume_event([member_a], first_event["event_id"])
            self.assertEqual(member_a.metrics.rows_inserted, 1)
            self.assertEqual(member_a.metrics.offset_commits, 1)
            source = results_a.calls[0][1]
            committed = member_a.consumer.committed(
                [
                    TopicPartition(
                        topic,
                        int(source["kafka_partition"]),
                    )
                ],
                timeout=10,
            )
            self.assertEqual(
                committed[0].offset,
                int(source["kafka_offset"]) + 1,
            )

            results_b = _FakeSnowflake(shared_rows)
            member_b = SnowflakeIngestionConsumer(config, snowflake_client=results_b)
            assignments = self._wait_for_assignment(
                [member_a, member_b], set(partitions)
            )
            self.assertTrue(assignments[0])
            self.assertTrue(assignments[1])
            self.assertFalse(assignments[0] & assignments[1])

            second_event = self._event(772)
            self._produce(bootstrap, topic, second_event, partitions[0])
            self._consume_event([member_a, member_b], second_event["event_id"])
            self.assertEqual(len(shared_rows), 2)

            member_a.close()
            member_b.close()
            member_a = None
            member_b = None

            restarted = SnowflakeIngestionConsumer(
                config, snowflake_client=_FakeSnowflake(shared_rows)
            )
            self._wait_for_assignment([restarted], set(partitions))
            for _ in range(10):
                restarted.process_one(timeout=0.1)
            self.assertEqual(restarted.metrics.messages_consumed, 0)
        finally:
            if member_a is not None:
                member_a.close()
            if member_b is not None:
                member_b.close()
            if restarted is not None:
                restarted.close()

    def _seed_group_at_current_end(
        self, bootstrap: str, group_id: str, topic: str
    ) -> list[int]:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "isolation.level": "read_committed",
            }
        )
        try:
            metadata = consumer.list_topics(topic, timeout=10)
            partitions = sorted(metadata.topics[topic].partitions)
            offsets = []
            for partition in partitions:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )
                offsets.append(TopicPartition(topic, partition, high))
            consumer.assign(offsets)
            result = consumer.commit(offsets=offsets, asynchronous=False)
            self.assertFalse(
                any(getattr(item, "error", None) for item in result or ())
            )
            return partitions
        finally:
            consumer.close()

    def _produce(
        self, bootstrap: str, topic: str, event: dict, partition: int
    ) -> None:
        producer = Producer({"bootstrap.servers": bootstrap})
        producer.produce(
            topic,
            partition=partition,
            key=event["event_id"].encode("utf-8"),
            value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
        )
        self.assertEqual(producer.flush(15), 0)

    def _event(self, seed: int) -> dict:
        event = SensorSimulator(seed=seed).generate_events(steps=1)[0]
        event["event_id"] = str(uuid.uuid4())
        return event

    def _wait_for_assignment(
        self,
        consumers: list[SnowflakeIngestionConsumer],
        expected: set[int],
        timeout: float = 20,
    ) -> list[set[int]]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for consumer in consumers:
                consumer.process_one(timeout=0.1)
            assignments = [
                {
                    item.partition
                    for item in consumer.consumer.assignment()
                    if item.topic == "sensor-events-validated"
                }
                for consumer in consumers
            ]
            if (
                set().union(*assignments) == expected
                and sum(map(len, assignments)) == len(expected)
                and all(assignments)
            ):
                return assignments
        self.fail("Snowflake consumer group did not reach expected partition assignment")

    def _consume_event(
        self, consumers: list[SnowflakeIngestionConsumer], event_id: str
    ) -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            for consumer in consumers:
                consumer.process_one(timeout=0.1)
            if any(
                call[0]["event_id"] == event_id
                for consumer in consumers
                for call in consumer.snowflake.calls
            ):
                return
        self.fail(f"event {event_id} was not persisted by a consumer")


class _FakeSnowflake:
    def __init__(self, rows: set[tuple[str, int, int]]) -> None:
        self.config = type("Config", (), {"database": "ATMOSYNC", "schema": "RAW"})()
        self.rows = rows
        self.calls: list[tuple[dict, dict]] = []

    def check_connection(self) -> bool:
        return True

    def insert_raw_event(self, event: dict, **source: object) -> bool:
        self.calls.append((event, source))
        identity = (
            str(source["kafka_topic"]),
            int(source["kafka_partition"]),
            int(source["kafka_offset"]),
        )
        if identity in self.rows:
            return False
        self.rows.add(identity)
        return True

    def close(self) -> None:
        return None


if __name__ == "__main__":
    unittest.main()
