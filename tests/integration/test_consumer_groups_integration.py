"""Opt-in live tests for independent and shared Kafka consumer groups."""

from __future__ import annotations

import json
import os
import time
import unittest
import uuid
from collections import Counter

from confluent_kafka import Consumer, Producer, TopicPartition

from atmo_sync.consumers.base import (
    ConsumerConfig,
    ConsumerOutcome,
    DEFAULT_VALIDATED_TOPIC,
)
from atmo_sync.consumers.validated import ValidatedSensorConsumer
from atmo_sync.simulator import SensorSimulator


@unittest.skipUnless(
    os.environ.get("ATMOSYNC_RUN_KAFKA_INTEGRATION") == "1",
    "set ATMOSYNC_RUN_KAFKA_INTEGRATION=1 to run local Kafka integration tests",
)
class KafkaConsumerGroupIntegrationTests(unittest.TestCase):
    def test_independent_groups_and_same_group_members_read_committed_events(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        group_a = f"atmosync-validated-a-{uuid.uuid4()}"
        group_b = f"atmosync-analytics-b-{uuid.uuid4()}"
        shared_group = f"atmosync-shared-group-{uuid.uuid4()}"
        for group_id in (group_a, group_b, shared_group):
            self._seed_group_at_current_end(bootstrap, group_id)

        aborted_event = SensorSimulator(seed=2041).generate_events(steps=1)[0]
        aborted_event["event_id"] = str(uuid.uuid4())
        event = SensorSimulator(seed=2042).generate_events(steps=1)[0]
        event["event_id"] = str(uuid.uuid4())
        second_event = SensorSimulator(seed=2043).generate_events(steps=1)[0]
        second_event["event_id"] = str(uuid.uuid4())
        self._produce_transactional(bootstrap, aborted_event, commit=False)
        self._produce_transactional(bootstrap, event, commit=True, partition=0)
        self._produce_transactional(bootstrap, second_event, commit=True, partition=5)
        expected_ids = {event["event_id"], second_event["event_id"]}

        independent_results: dict[str, list[dict]] = {"a": [], "b": []}
        consumer_a = ValidatedSensorConsumer(
            ConsumerConfig(bootstrap_servers=bootstrap, group_id=group_a),
            event_handler=independent_results["a"].append,
        )
        consumer_b = ValidatedSensorConsumer(
            ConsumerConfig(bootstrap_servers=bootstrap, group_id=group_b),
            event_handler=independent_results["b"].append,
        )
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and (
                len(independent_results["a"]) < 2
                or len(independent_results["b"]) < 2
            ):
                if len(independent_results["a"]) < 2:
                    consumer_a.process_one(timeout=0.1)
                if len(independent_results["b"]) < 2:
                    consumer_b.process_one(timeout=0.1)
            self.assertEqual(
                {item["event_id"] for item in independent_results["a"]},
                expected_ids,
            )
            self.assertEqual(len(independent_results["a"]), 2)
            self.assertEqual(
                {item["event_id"] for item in independent_results["b"]},
                expected_ids,
            )
            self.assertEqual(len(independent_results["b"]), 2)
        finally:
            consumer_a.close()
            consumer_b.close()

        same_group_results_a: list[dict] = []
        same_group_results_b: list[dict] = []
        shared_config = ConsumerConfig(
            bootstrap_servers=bootstrap, group_id=shared_group
        )
        member_a = ValidatedSensorConsumer(
            shared_config, event_handler=same_group_results_a.append
        )
        member_b = ValidatedSensorConsumer(
            shared_config, event_handler=same_group_results_b.append
        )
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and (
                len(same_group_results_a) + len(same_group_results_b) < 2
            ):
                member_a.process_one(timeout=0.1)
                member_b.process_one(timeout=0.1)
            same_group_results = same_group_results_a + same_group_results_b
            self.assertEqual(
                {item["event_id"] for item in same_group_results}, expected_ids
            )
            self.assertEqual(len(same_group_results), 2)
            self.assertLessEqual(len(same_group_results_a), 2)
            self.assertLessEqual(len(same_group_results_b), 2)
            member_a.process_one(timeout=0.2)
            member_b.process_one(timeout=0.2)
            self.assertEqual(
                len(same_group_results_a) + len(same_group_results_b), 2
            )
        finally:
            member_a.close()
            member_b.close()

        restarted_result: list[dict] = []
        restarted = ValidatedSensorConsumer(
            ConsumerConfig(bootstrap_servers=bootstrap, group_id=group_a),
            event_handler=restarted_result.append,
        )
        try:
            self._wait_for_assignments([restarted], set(range(6)))
            self.assertIsNone(restarted.process_one(timeout=0.5))
            self.assertEqual(restarted_result, [])
        finally:
            restarted.close()

    def test_same_group_rebalance_assigns_both_members_without_loss_or_duplicates(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        shared_group = f"atmosync-controlled-rebalance-{uuid.uuid4()}"
        self._seed_group_at_current_end(bootstrap, shared_group)
        events = SensorSimulator(seed=2044).generate_events(steps=6)
        for event in events:
            event["event_id"] = str(uuid.uuid4())
        before_rebalance = events[:6]
        after_rebalance = events[6:]
        results_a: list[dict] = []
        results_b: list[dict] = []
        config = ConsumerConfig(bootstrap_servers=bootstrap, group_id=shared_group)
        member_a = ValidatedSensorConsumer(
            config, event_handler=results_a.append
        )
        member_b: ValidatedSensorConsumer | None = None
        restarted: ValidatedSensorConsumer | None = None
        try:
            initial_assignments = self._wait_for_assignments(
                [member_a], set(range(6))
            )
            initial_member_a_assignment = initial_assignments[0]
            self.assertEqual(initial_member_a_assignment, set(range(6)))

            for index, event in enumerate(before_rebalance):
                self._produce_transactional(
                    bootstrap, event, commit=True, partition=index
                )
            self._consume_until(
                [member_a],
                [results_a],
                {event["event_id"] for event in before_rebalance},
            )
            self.assertEqual(
                Counter(event["event_id"] for event in results_a),
                Counter(event["event_id"] for event in before_rebalance),
            )

            member_b = ValidatedSensorConsumer(
                config, event_handler=results_b.append
            )
            rebalanced_assignments = self._wait_for_assignments(
                [member_a, member_b], set(range(6))
            )
            self.assertNotEqual(
                rebalanced_assignments[0], initial_member_a_assignment
            )
            self.assertTrue(rebalanced_assignments[0])
            self.assertTrue(rebalanced_assignments[1])
            self.assertFalse(
                rebalanced_assignments[0] & rebalanced_assignments[1]
            )

            for index, event in enumerate(after_rebalance):
                self._produce_transactional(
                    bootstrap, event, commit=True, partition=index % 6
                )
            all_expected_ids = {event["event_id"] for event in events}
            self._consume_until(
                [member_a, member_b],
                [results_a, results_b],
                all_expected_ids,
            )

            combined_results = results_a + results_b
            self.assertEqual(
                Counter(event["event_id"] for event in combined_results),
                Counter(all_expected_ids),
            )
            self.assertTrue(results_a)
            self.assertTrue(results_b)
        finally:
            member_a.close()
            if member_b is not None:
                member_b.close()

        restarted_results: list[dict] = []
        restarted = ValidatedSensorConsumer(
            config, event_handler=restarted_results.append
        )
        try:
            self._wait_for_assignments([restarted], set(range(6)))
            self.assertIsNone(restarted.process_one(timeout=0.5))
            self.assertEqual(restarted_results, [])
        finally:
            restarted.close()

    def _wait_for_assignments(
        self,
        consumers: list[ValidatedSensorConsumer],
        expected_partitions: set[int],
        timeout: float = 15,
    ) -> list[set[int]]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for consumer in consumers:
                consumer.process_one(timeout=0.1)
            assignments = [
                {
                    partition.partition
                    for partition in consumer.consumer.assignment()
                    if partition.topic == DEFAULT_VALIDATED_TOPIC
                }
                for consumer in consumers
            ]
            assigned_partitions = set().union(*assignments)
            if (
                all(assignments)
                and assigned_partitions == expected_partitions
                and sum(map(len, assignments)) == len(expected_partitions)
            ):
                return assignments
        self.fail(
            "consumer group did not assign the expected partitions to all members"
        )

    def _consume_until(
        self,
        consumers: list[ValidatedSensorConsumer],
        result_lists: list[list[dict]],
        expected_ids: set[str],
        timeout: float = 30,
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            observed = {
                event["event_id"]
                for results in result_lists
                for event in results
            }
            if observed == expected_ids:
                return
            for consumer in consumers:
                consumer.process_one(timeout=0.1)
        observed = {
            event["event_id"]
            for results in result_lists
            for event in results
        }
        self.assertEqual(observed, expected_ids)

    def _seed_group_at_current_end(self, bootstrap: str, group_id: str) -> None:
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
            offsets = []
            for partition in range(6):
                _, end = consumer.get_watermark_offsets(
                    TopicPartition(DEFAULT_VALIDATED_TOPIC, partition), timeout=10
                )
                offsets.append(TopicPartition(DEFAULT_VALIDATED_TOPIC, partition, end))
            consumer.assign(offsets)
            committed = consumer.commit(offsets=offsets, asynchronous=False)
            self.assertFalse(
                any(getattr(partition, "error", None) for partition in committed or ())
            )
        finally:
            consumer.close()

    def _produce_transactional(
        self, bootstrap: str, event: dict, *, commit: bool, partition: int = 0
    ) -> None:
        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "transactional.id": f"atmosync-consumer-group-test-{uuid.uuid4()}",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        producer.init_transactions(10)
        try:
            producer.begin_transaction()
            producer.produce(
                DEFAULT_VALIDATED_TOPIC,
                partition=partition,
                key=event["event_id"].encode("utf-8"),
                value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
            )
            if commit:
                producer.commit_transaction(20)
            else:
                producer.abort_transaction(20)
        finally:
            producer.flush(10)


if __name__ == "__main__":
    unittest.main()
