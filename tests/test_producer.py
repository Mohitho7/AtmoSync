"""Unit tests for the DE-04B Kafka sensor producer."""

from __future__ import annotations

import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from atmo_sync.producer import (
    KafkaSensorProducer,
    MAX_SENSOR_KAFKA_RECORD_BYTES,
    ProducerError,
    _parse_args,
    main,
    publish_finite,
)
from atmo_sync.simulator import SensorSimulator, main as simulator_main


def sample_event() -> dict:
    return SensorSimulator(seed=42).generate_events(steps=1)[0]


def delivery_message(topic: str = "sensor-events") -> SimpleNamespace:
    return SimpleNamespace(
        topic=lambda: topic,
        partition=lambda: 0,
        offset=lambda: 17,
    )


class KafkaSensorProducerTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.flush.return_value = 0
        self.producer = KafkaSensorProducer(client=self.client)

    def test_serializes_valid_event_as_utf8_json_without_changing_fields(self):
        event = sample_event()
        event["commodity"] = "mang\u00f3"
        self.producer.publish(event)
        call = self.client.produce.call_args
        self.assertEqual(call.args[0], "sensor-events")
        self.assertEqual(json.loads(call.kwargs["value"].decode("utf-8")), event)
        self.assertIn(b"mang\xc3\xb3", call.kwargs["value"])

    def test_uses_event_id_as_utf8_message_key_and_selects_topic(self):
        event = sample_event()
        producer = KafkaSensorProducer("broker:9092", "custom-topic", client=self.client)
        producer.publish(event)
        call = self.client.produce.call_args
        self.assertEqual(call.args[0], "custom-topic")
        self.assertEqual(call.kwargs["key"], event["event_id"].encode("utf-8"))

    def test_successful_delivery_callback_confirms_delivery(self):
        self.producer.publish(sample_event())
        callback = self.client.produce.call_args.kwargs["on_delivery"]
        callback(None, delivery_message())
        self.assertEqual(self.producer.close(), 1)

    def test_failed_delivery_callback_is_reported_at_shutdown(self):
        event = sample_event()
        self.producer.publish(event)
        callback = self.client.produce.call_args.kwargs["on_delivery"]
        callback(RuntimeError("broker rejected message"), delivery_message())
        with self.assertRaisesRegex(ProducerError, "failed delivery"):
            self.producer.close()

    def test_queue_full_polls_then_retries_without_losing_event(self):
        event = sample_event()
        self.client.produce.side_effect = [BufferError("full"), None]
        self.producer.publish(event)
        self.assertEqual(self.client.produce.call_count, 2)
        self.client.poll.assert_any_call(0)
        callback = self.client.produce.call_args.kwargs["on_delivery"]
        callback(None, delivery_message())
        self.assertEqual(self.producer.close(), 1)

    def test_persistent_queue_full_is_reported(self):
        self.client.produce.side_effect = BufferError("full")
        producer = KafkaSensorProducer(
            client=self.client,
            queue_full_timeout=0.000001,
        )
        with self.assertRaisesRegex(ProducerError, "queue remained full"):
            producer.publish(sample_event())
        self.client.flush.return_value = 0
        self.assertEqual(producer.close(), 0)

    def test_flush_reports_messages_that_remain_undelivered(self):
        self.client.flush.return_value = 2
        with self.assertRaisesRegex(ProducerError, "2 message"):
            self.producer.close()
        self.client.flush.assert_called_once()

    def test_shutdown_flushes_once_and_rejects_later_events(self):
        self.producer.publish(sample_event())
        self.producer.close()
        self.producer.close()
        self.client.flush.assert_called_once()
        with self.assertRaisesRegex(ProducerError, "closed"):
            self.producer.publish(sample_event())

    def test_serialization_failure_is_explicit(self):
        event = sample_event()
        event["readings"]["temperature_c"] = Decimal("12.5")
        with self.assertRaisesRegex(ProducerError, "could not serialize"):
            self.producer.publish(event)
        self.client.produce.assert_not_called()

    def test_producer_configuration_enables_idempotent_acknowledged_delivery(self):
        with patch("atmo_sync.producer.Producer") as producer_factory:
            KafkaSensorProducer("broker:9092", "sensor-events")
        configuration = producer_factory.call_args.args[0]
        self.assertEqual(configuration["bootstrap.servers"], "broker:9092")
        self.assertEqual(
            configuration["message.max.bytes"], MAX_SENSOR_KAFKA_RECORD_BYTES
        )
        self.assertTrue(configuration["enable.idempotence"])
        self.assertEqual(configuration["acks"], "all")
        self.assertGreater(configuration["message.send.max.retries"], 0)

    def test_finite_publishing_reuses_existing_simulator(self):
        simulator = SensorSimulator(seed=42)
        publish_finite(simulator, self.producer, steps=2)
        self.assertEqual(self.client.produce.call_count, 6)
        published = [
            json.loads(call.kwargs["value"].decode("utf-8"))
            for call in self.client.produce.call_args_list
        ]
        self.assertEqual(
            {event["container_id"] for event in published},
            {"CONT-001", "CONT-002", "CONT-003"},
        )
        self.assertTrue(
            all(
                sum(event["container_id"] == container_id for event in published) == 2
                for container_id in ("CONT-001", "CONT-002", "CONT-003")
            )
        )
        for call, event in zip(self.client.produce.call_args_list, published):
            self.assertEqual(call.args[0], "sensor-events")
            self.assertEqual(call.kwargs["key"], event["event_id"].encode("utf-8"))

    def test_existing_simulator_cli_runs_without_a_kafka_broker(self):
        output = io.StringIO()
        with redirect_stdout(output):
            simulator_main(["--steps", "1"])
        self.assertEqual(len(json.loads(output.getvalue())), 3)

    def test_environment_overrides_and_argument_validation(self):
        with patch.dict(
            "os.environ",
            {
                "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
                "ATMOSYNC_KAFKA_TOPIC": "override-topic",
            },
        ):
            args = _parse_args(["--steps", "2"])
        self.assertEqual(args.bootstrap_server, "broker:9092")
        self.assertEqual(args.topic, "override-topic")

        with self.assertRaises(SystemExit) as result:
            _parse_args(["--continuous", "--steps", "1"])
        self.assertEqual(result.exception.code, 2)

        with self.assertRaises(SystemExit) as result:
            _parse_args(["--flush-timeout", "nan"])
        self.assertEqual(result.exception.code, 2)

    def test_continuous_mode_handles_keyboard_interrupt_and_flushes(self):
        event = sample_event()

        class InterruptingSimulator:
            def stream(self):
                yield event
                raise KeyboardInterrupt

        with (
            patch("atmo_sync.producer.SensorSimulator", return_value=InterruptingSimulator()),
            patch("atmo_sync.producer.KafkaSensorProducer") as producer_factory,
        ):
            producer_factory.return_value.close.return_value = 1
            result = main(["--continuous"])
        self.assertEqual(result, 0)
        producer_factory.return_value.publish.assert_called_once_with(event)
        producer_factory.return_value.close.assert_called_once()

    def test_invalid_sensor_event_is_not_queued(self):
        event = copy.deepcopy(sample_event())
        event["readings"]["humidity_pct"] = 101
        with self.assertRaisesRegex(ValueError, "between 0 and 100"):
            self.producer.publish(event)
        self.client.produce.assert_not_called()


if __name__ == "__main__":
    unittest.main()
