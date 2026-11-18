"""Unit tests for Kafka-to-Snowflake ordering and recovery."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from types import SimpleNamespace

from atmo_sync.consumers.base import ConsumerError, ConsumerOutcome
from atmo_sync.consumers.snowflake import (
    DEFAULT_SNOWFLAKE_GROUP_ID,
    SnowflakeIngestionConsumer,
)
from atmo_sync.snowflake import SnowflakeClientError


ROOT = Path(__file__).resolve().parents[1]


class FakeMessage:
    def __init__(self, value: bytes, *, offset=12, partition=3, error=None):
        self._value = value
        self._offset = offset
        self._partition = partition
        self._error = error

    def value(self):
        return self._value

    def topic(self):
        return "sensor-events-validated"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, messages=(), *, commit_results=(), actions=None):
        self.messages = list(messages)
        self.commit_results = list(commit_results)
        self.actions = actions if actions is not None else []
        self.subscribed = None
        self.callbacks = {}
        self.commits = []
        self.closed = False

    def subscribe(self, topics, **callbacks):
        self.subscribed = topics
        self.callbacks = callbacks

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous):
        self.commits.append((message, asynchronous))
        self.actions.append("commit")
        if self.commit_results:
            result = self.commit_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        return []

    def close(self):
        self.closed = True


class FakeSnowflake:
    def __init__(self, *, persisted=None, outcomes=(), actions=None):
        self.config = SimpleNamespace(database="ATMOSYNC", schema="RAW")
        self.persisted = persisted if persisted is not None else set()
        self.outcomes = list(outcomes)
        self.calls = []
        self.actions = actions if actions is not None else []
        self.close_count = 0

    def check_connection(self):
        self.actions.append("checked")
        return True

    def insert_raw_event(self, event, **source):
        identity = (
            source["kafka_topic"],
            source["kafka_partition"],
            source["kafka_offset"],
        )
        self.calls.append((event, source))
        self.actions.append(("persist", identity))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return bool(outcome)
        if identity in self.persisted:
            return False
        self.persisted.add(identity)
        return True

    def close(self):
        self.close_count += 1
        self.actions.append("closed")


def sample_event():
    with (ROOT / "data" / "sample_sensor_event.json").open(encoding="utf-8") as file:
        return json.load(file)


def make_consumer(event, *, snowflake=None, consumer=None, **kwargs):
    message = FakeMessage(json.dumps(event).encode("utf-8"))
    actions = []
    kafka = consumer or FakeConsumer([message], actions=actions)
    warehouse = snowflake or FakeSnowflake()
    if not isinstance(warehouse, FakeSnowflake):
        warehouse.actions = actions
    elif consumer is None:
        warehouse.actions = actions
    kwargs.setdefault("sleep", lambda _delay: None)
    ingestion = SnowflakeIngestionConsumer(
        snowflake_client=warehouse,
        consumer_client=kafka,
        **kwargs,
    )
    return ingestion, warehouse, kafka, message


class SnowflakeIngestionConsumerTests(unittest.TestCase):
    def test_uses_dedicated_group_and_passes_message_coordinates(self):
        event = sample_event()
        ingestion, warehouse, kafka, message = make_consumer(event)

        self.assertEqual(ingestion.config.group_id, DEFAULT_SNOWFLAKE_GROUP_ID)
        self.assertEqual(kafka.subscribed, ["sensor-events-validated"])
        self.assertEqual(ingestion.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(
            warehouse.calls,
            [
                (
                    event,
                    {
                        "kafka_topic": message.topic(),
                        "kafka_partition": message.partition(),
                        "kafka_offset": message.offset(),
                    },
                )
            ],
        )
        self.assertEqual(kafka.actions, ["checked", ("persist", (
            "sensor-events-validated", 3, 12
        )), "commit"])
        self.assertEqual(kafka.commits, [(message, False)])
        self.assertEqual(ingestion.metrics.rows_inserted, 1)
        self.assertEqual(ingestion.metrics.offset_commits, 1)

    def test_commit_occurs_only_after_successful_persistence(self):
        event = sample_event()
        kafka = FakeConsumer([FakeMessage(json.dumps(event).encode("utf-8"))])
        warehouse = FakeSnowflake(
            outcomes=[SnowflakeClientError("temporary outage")]
        )
        ingestion = SnowflakeIngestionConsumer(
            snowflake_client=warehouse,
            consumer_client=kafka,
            retry_attempts=1,
        )

        with self.assertRaisesRegex(ConsumerError, "offset was not committed"):
            ingestion.process_one(0.01)

        self.assertEqual(kafka.commits, [])
        self.assertEqual(ingestion.metrics.failed_write_attempts, 1)
        self.assertEqual(ingestion.metrics.offset_commits, 0)

    def test_transient_failure_reconnects_with_bounded_backoff(self):
        event = sample_event()
        delays = []
        warehouse = FakeSnowflake(
            outcomes=[
                SnowflakeClientError("transient", retryable=True),
                True,
            ]
        )
        ingestion, _, kafka, _ = make_consumer(
            event,
            snowflake=warehouse,
            retry_attempts=3,
            retry_backoff_seconds=0.25,
            sleep=delays.append,
        )

        self.assertEqual(ingestion.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(delays, [0.25])
        self.assertEqual(warehouse.close_count, 1)
        self.assertEqual(len(warehouse.calls), 2)
        self.assertEqual(len(kafka.commits), 1)

    def test_permanent_failure_is_not_retried(self):
        event = sample_event()
        warehouse = FakeSnowflake(
            outcomes=[SnowflakeClientError("invalid SQL", retryable=False)]
        )
        ingestion, _, kafka, _ = make_consumer(
            event, snowflake=warehouse, retry_attempts=3
        )

        with self.assertRaises(ConsumerError):
            ingestion.process_one(0.01)

        self.assertEqual(len(warehouse.calls), 1)
        self.assertEqual(kafka.commits, [])

    def test_exhausted_transient_retries_are_bounded_and_uncommitted(self):
        event = sample_event()
        delays = []
        warehouse = FakeSnowflake(
            outcomes=[
                SnowflakeClientError("temporary", retryable=True),
                SnowflakeClientError("temporary", retryable=True),
                SnowflakeClientError("temporary", retryable=True),
            ]
        )
        ingestion, _, kafka, _ = make_consumer(
            event,
            snowflake=warehouse,
            retry_attempts=3,
            retry_backoff_seconds=0.2,
            sleep=delays.append,
        )

        with self.assertRaises(ConsumerError):
            ingestion.process_one(0.01)

        self.assertEqual(len(warehouse.calls), 3)
        self.assertEqual(delays, [0.2, 0.4])
        self.assertEqual(kafka.commits, [])
        self.assertEqual(ingestion.metrics.failed_write_attempts, 3)

    def test_duplicate_delivery_and_restart_are_replay_safe(self):
        event = sample_event()
        shared_rows = set()
        kafka = FakeConsumer(
            [
                FakeMessage(json.dumps(event).encode("utf-8")),
                FakeMessage(json.dumps(event).encode("utf-8")),
            ]
        )
        warehouse = FakeSnowflake(persisted=shared_rows)
        ingestion = SnowflakeIngestionConsumer(
            snowflake_client=warehouse, consumer_client=kafka
        )

        self.assertEqual(ingestion.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(ingestion.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(len(warehouse.calls), 2)
        self.assertEqual(ingestion.metrics.duplicate_messages, 1)
        self.assertEqual(len(kafka.commits), 2)

        restarted_kafka = FakeConsumer(
            [FakeMessage(json.dumps(event).encode("utf-8"))]
        )
        restarted_warehouse = FakeSnowflake(persisted=shared_rows)
        restarted = SnowflakeIngestionConsumer(
            snowflake_client=restarted_warehouse,
            consumer_client=restarted_kafka,
        )
        self.assertEqual(
            restarted.process_one(0.01), ConsumerOutcome.PROCESSED
        )
        self.assertEqual(restarted.metrics.rows_inserted, 0)
        self.assertEqual(restarted.metrics.duplicate_messages, 1)
        self.assertEqual(len(restarted_kafka.commits), 1)

    def test_same_event_id_at_distinct_kafka_offsets_is_not_suppressed(self):
        event = sample_event()
        kafka = FakeConsumer(
            [
                FakeMessage(json.dumps(event).encode("utf-8"), offset=12),
                FakeMessage(json.dumps(event).encode("utf-8"), offset=13),
            ]
        )
        warehouse = FakeSnowflake()
        ingestion = SnowflakeIngestionConsumer(
            snowflake_client=warehouse,
            consumer_client=kafka,
        )

        self.assertEqual(ingestion.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(ingestion.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(len(warehouse.calls), 2)
        self.assertEqual(
            [call[1]["kafka_offset"] for call in warehouse.calls], [12, 13]
        )
        self.assertEqual(ingestion.metrics.rows_inserted, 2)

    def test_commit_failure_after_persistence_replays_through_idempotent_store(self):
        event = sample_event()
        shared_rows = set()
        kafka = FakeConsumer(
            [FakeMessage(json.dumps(event).encode("utf-8"))],
            commit_results=[[SimpleNamespace(error=RuntimeError("commit failed"))]],
        )
        warehouse = FakeSnowflake(persisted=shared_rows)
        ingestion = SnowflakeIngestionConsumer(
            snowflake_client=warehouse, consumer_client=kafka
        )
        with self.assertRaisesRegex(ConsumerError, "offset commit failed"):
            ingestion.process_one(0.01)
        self.assertEqual(len(shared_rows), 1)

        restarted_kafka = FakeConsumer(
            [FakeMessage(json.dumps(event).encode("utf-8"))]
        )
        restarted_warehouse = FakeSnowflake(persisted=shared_rows)
        restarted = SnowflakeIngestionConsumer(
            snowflake_client=restarted_warehouse,
            consumer_client=restarted_kafka,
        )
        self.assertEqual(
            restarted.process_one(0.01), ConsumerOutcome.PROCESSED
        )
        self.assertEqual(len(shared_rows), 1)
        self.assertEqual(restarted.metrics.duplicate_messages, 1)

    def test_invalid_event_and_rebalance_callbacks_never_commit_unpersisted_work(self):
        event = sample_event()
        event["readings"]["humidity_pct"] = 101
        kafka = FakeConsumer([FakeMessage(json.dumps(event).encode("utf-8"))])
        warehouse = FakeSnowflake()
        ingestion = SnowflakeIngestionConsumer(
            snowflake_client=warehouse, consumer_client=kafka
        )

        with self.assertRaisesRegex(ConsumerError, "failed DE-02 validation"):
            ingestion.process_one(0.01)
        self.assertEqual(kafka.commits, [])
        self.assertIn("on_revoke", kafka.callbacks)
        self.assertIn("on_lost", kafka.callbacks)
        kafka.callbacks["on_revoke"](
            kafka, [SimpleNamespace(topic="sensor-events-validated", partition=3)]
        )
        kafka.callbacks["on_lost"](
            kafka, [SimpleNamespace(topic="sensor-events-validated", partition=3)]
        )
        self.assertEqual(kafka.commits, [])

    def test_close_releases_kafka_and_snowflake_resources(self):
        ingestion, warehouse, kafka, _ = make_consumer(sample_event())
        ingestion.process_one(0.01)
        ingestion.close()
        self.assertTrue(kafka.closed)
        self.assertEqual(warehouse.close_count, 1)
        self.assertGreater(ingestion.metrics.processing_duration_seconds, 0)


if __name__ == "__main__":
    unittest.main()
