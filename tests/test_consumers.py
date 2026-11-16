"""Unit tests for DE-04E downstream Kafka consumer groups."""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from atmo_sync.consumers.analytics import AnalyticsAggregationConsumer
from atmo_sync.consumers.base import (
    ConsumerConfig,
    ConsumerError,
    ConsumerOutcome,
    DEFAULT_ANALYTICS_GROUP_ID,
    DEFAULT_MONITORING_GROUP_ID,
    DEFAULT_VALIDATED_GROUP_ID,
)
from atmo_sync.consumers.monitoring import OperationalMetricsConsumer
from atmo_sync.consumers.validated import ValidatedSensorConsumer
from atmo_sync.simulator import SensorSimulator


def sample_event() -> dict:
    return SensorSimulator(seed=42).generate_events(steps=1)[0]


class FakeMessage:
    def __init__(self, value: bytes | None, *, offset=0, error=None):
        self._value = value
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def topic(self):
        return "sensor-events-validated"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.subscribed = None
        self.commits = []
        self.closed = False
        self.commit_results = []

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous):
        self.commits.append((message, asynchronous))
        if self.commit_results:
            return self.commit_results.pop(0)
        return []

    def close(self):
        self.closed = True


class ConsumerConfigTests(unittest.TestCase):
    def test_independent_consumers_have_distinct_groups_and_read_committed_configs(self):
        self.assertEqual(
            len(
                {
                    DEFAULT_VALIDATED_GROUP_ID,
                    DEFAULT_ANALYTICS_GROUP_ID,
                    DEFAULT_MONITORING_GROUP_ID,
                }
            ),
            3,
        )
        with patch("atmo_sync.consumers.base.Consumer") as factory:
            consumer = AnalyticsAggregationConsumer()
        config = factory.call_args.args[0]
        self.assertEqual(config["group.id"], DEFAULT_ANALYTICS_GROUP_ID)
        self.assertFalse(config["enable.auto.commit"])
        self.assertFalse(config["enable.auto.offset.store"])
        self.assertEqual(config["isolation.level"], "read_committed")
        self.assertEqual(config["auto.offset.reset"], "earliest")
        self.assertEqual(factory.return_value.subscribe.call_args.args[0], ["sensor-events-validated"])
        consumer.close()

    def test_consumer_configuration_rejects_invalid_values(self):
        with self.assertRaisesRegex(ValueError, "group_id"):
            ConsumerConfig(group_id="")
        with self.assertRaisesRegex(ValueError, "auto_offset_reset"):
            ConsumerConfig(auto_offset_reset="invalid")
        with self.assertRaisesRegex(ValueError, "poll_timeout"):
            ConsumerConfig(poll_timeout=0)


class ConsumerProcessingTests(unittest.TestCase):
    def test_validated_consumer_handles_before_manual_offset_commit(self):
        event = sample_event()
        client = FakeConsumer(
            [FakeMessage(json.dumps(event).encode("utf-8"), offset=17)]
        )
        actions = []
        consumer = ValidatedSensorConsumer(
            ConsumerConfig(group_id="validated-test"),
            consumer_client=client,
            event_handler=lambda value: actions.append(("handled", value)),
        )
        original_commit = client.commit

        def commit_after_handler(*, message, asynchronous):
            actions.append(("committed", message.offset()))
            return original_commit(message=message, asynchronous=asynchronous)

        client.commit = commit_after_handler

        self.assertEqual(consumer.process_one(0.01), ConsumerOutcome.PROCESSED)
        self.assertEqual(actions[0], ("handled", event))
        self.assertEqual(actions[1], ("committed", 17))
        self.assertEqual(client.commits[0][1], False)

    def test_invalid_validated_record_is_not_committed(self):
        event = sample_event()
        event["readings"]["humidity_pct"] = 101
        client = FakeConsumer(
            [FakeMessage(json.dumps(event).encode("utf-8"), offset=22)]
        )
        consumer = ValidatedSensorConsumer(
            ConsumerConfig(group_id="invalid-test"), consumer_client=client
        )

        with self.assertRaisesRegex(ConsumerError, "failed DE-02 validation"):
            consumer.process_one(0.01)

        self.assertEqual(client.commits, [])

    def test_handler_failure_does_not_commit_offset(self):
        event = sample_event()
        client = FakeConsumer(
            [FakeMessage(json.dumps(event).encode("utf-8"), offset=25)]
        )

        def fail(_event):
            raise RuntimeError("handler unavailable")

        consumer = ValidatedSensorConsumer(
            ConsumerConfig(group_id="failure-test"),
            consumer_client=client,
            event_handler=fail,
        )
        with self.assertRaisesRegex(RuntimeError, "handler unavailable"):
            consumer.process_one(0.01)

        self.assertEqual(client.commits, [])

    def test_failed_commit_redelivery_does_not_repeat_local_handler(self):
        event = sample_event()
        record = FakeMessage(json.dumps(event).encode("utf-8"), offset=30)
        client = FakeConsumer([record, record])
        client.commit_results = [
            [SimpleNamespace(error=RuntimeError("commit failed"))],
            [],
        ]
        handled = []
        consumer = ValidatedSensorConsumer(
            ConsumerConfig(group_id="commit-failure-test"),
            consumer_client=client,
            event_handler=handled.append,
        )

        with self.assertRaisesRegex(ConsumerError, "offset commit failed"):
            consumer.process_one(0.01)
        self.assertEqual(consumer.process_one(0.01), ConsumerOutcome.DUPLICATE)
        self.assertEqual(handled, [event])
        self.assertEqual(len(client.commits), 2)

    def test_analytics_consumer_aggregates_by_container(self):
        event = sample_event()
        client = FakeConsumer([FakeMessage(json.dumps(event).encode("utf-8"))])
        consumer = AnalyticsAggregationConsumer(
            ConsumerConfig(group_id="analytics-test"), consumer_client=client
        )

        self.assertEqual(consumer.process_one(0.01), ConsumerOutcome.PROCESSED)
        aggregate = consumer.aggregates[event["container_id"]]
        self.assertEqual(aggregate["event_count"], 1)
        self.assertEqual(aggregate["temperature_c_sum"], event["readings"]["temperature_c"])
        self.assertEqual(aggregate["humidity_pct_sum"], event["readings"]["humidity_pct"])

    def test_monitoring_consumer_tracks_count_by_container(self):
        event = sample_event()
        client = FakeConsumer([FakeMessage(json.dumps(event).encode("utf-8"))])
        consumer = OperationalMetricsConsumer(
            ConsumerConfig(group_id="monitoring-test"), consumer_client=client
        )

        self.assertEqual(consumer.process_one(0.01), ConsumerOutcome.PROCESSED)
        metrics = consumer.metrics
        self.assertEqual(metrics["processed_count"], 1)
        self.assertEqual(metrics["counts_by_container"][event["container_id"]], 1)
        self.assertGreater(metrics["events_per_second"], 0)

    def test_close_is_graceful_and_processing_after_close_is_rejected(self):
        client = FakeConsumer()
        consumer = ValidatedSensorConsumer(
            ConsumerConfig(group_id="close-test"), consumer_client=client
        )
        consumer.close()
        consumer.close()
        self.assertTrue(client.closed)
        with self.assertRaisesRegex(ConsumerError, "closed"):
            consumer.process_one(0.01)


if __name__ == "__main__":
    unittest.main()
