"""Unit tests for DE-04C transactional validation and deduplication."""

from __future__ import annotations

import base64
import copy
import json
import time
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from confluent_kafka import KafkaException

from atmo_sync.processor import (
    DEFAULT_TRANSACTIONAL_ID,
    MAX_DLQ_KAFKA_RECORD_BYTES,
    MAX_DLQ_MESSAGE_BYTES,
    MAX_RAW_MESSAGE_BYTES,
    ProcessorConfig,
    ProcessingOutcome,
    ProcessorError,
    TransactionOutcomeUnknown,
    TransactionalValidationProcessor,
    payload_sha256,
    restore_deduplication_ledger,
)
from atmo_sync.simulator import SensorSimulator
from atmo_sync.retry import (
    DEFAULT_RETRY_TOPIC,
    MAX_RETRY_KAFKA_RECORD_BYTES,
    RetryEnvelope,
    TransientProcessingError,
    utc_timestamp,
)


def sample_event() -> dict:
    return SensorSimulator(seed=42).generate_events(steps=1)[0]


def ledger_marker(event_id: str, payload_hash: str) -> bytes:
    return json.dumps(
        {"schema_version": "1.0", "event_id": event_id, "payload_sha256": payload_hash},
        separators=(",", ":"),
    ).encode("utf-8")


class FakeMessage:
    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        value: bytes | None,
        key: bytes | None = None,
        error=None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._error = error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return self._error


class FakeTransactionalProducer:
    def __init__(self):
        self.calls = []
        self.current = None
        self.committed = []
        self.aborted = []
        self.fail_produce = None
        self.fail_commit = None
        self.fail_begin = None

    def init_transactions(self, timeout):
        self.calls.append(("init_transactions", timeout))

    def begin_transaction(self):
        self.calls.append(("begin_transaction",))
        if self.fail_begin is not None:
            raise self.fail_begin
        self.current = {"outputs": [], "offsets": []}

    def produce(self, topic, *, key, value):
        if self.fail_produce is not None:
            raise self.fail_produce
        self.current["outputs"].append((topic, key, value))
        self.calls.append(("produce", topic, key, value))

    def send_offsets_to_transaction(self, offsets, metadata, timeout):
        self.current["offsets"].extend(offsets)
        self.calls.append(("send_offsets", offsets, metadata, timeout))

    def commit_transaction(self, timeout):
        self.calls.append(("commit_transaction", timeout))
        if self.fail_commit is not None:
            raise self.fail_commit
        self.committed.append(self.current)
        self.current = None

    def abort_transaction(self, timeout):
        self.calls.append(("abort_transaction", timeout))
        self.aborted.append(self.current)
        self.current = None

    def flush(self, timeout):
        self.calls.append(("flush", timeout))
        return 0


class FakeInputConsumer:
    def __init__(self):
        self.subscribed = None
        self.closed = False
        self.group_metadata = object()
        self.messages = []
        self.paused = []
        self.resumed = []

    def list_topics(self, timeout):
        topics = {
            "sensor-events": 1,
            "sensor-events-validated": 6,
            "sensor-events-dlq": 3,
            "sensor-event-dedup": 1,
            "sensor-events-retry": 1,
        }
        return SimpleNamespace(
            topics={
                name: SimpleNamespace(
                    error=None,
                    partitions={index: object() for index in range(count)},
                )
                for name, count in topics.items()
            }
        )

    def subscribe(self, topics):
        self.subscribed = topics

    def consumer_group_metadata(self):
        return self.group_metadata

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def pause(self, partitions):
        self.paused.extend(partitions)

    def resume(self, partitions):
        self.resumed.extend(partitions)

    def close(self):
        self.closed = True


class FakeLedgerConsumer:
    def __init__(self, *, low=0, end=0, messages=()):
        self.low = low
        self.end = end
        self.messages = list(messages)
        self.current_offset = low
        self.closed = False

    def get_watermark_offsets(self, partition, timeout, cached):
        return self.low, self.end

    def assign(self, partitions):
        self.current_offset = partitions[0].offset

    def poll(self, _timeout):
        if self.messages:
            message = self.messages.pop(0)
            self.current_offset = message.offset() + 1
            return message
        self.current_offset = self.end
        return None

    def position(self, _partitions):
        return [SimpleNamespace(offset=self.current_offset, error=None)]

    def close(self):
        self.closed = True


class FakeAdminFuture:
    def __init__(self, config):
        self.config = config

    def result(self, timeout):
        return self.config


class FakeAdminClient:
    def __init__(
        self,
        *,
        input_max_bytes=MAX_RAW_MESSAGE_BYTES,
        dlq_max_bytes=MAX_DLQ_KAFKA_RECORD_BYTES,
        retry_max_bytes=4 * 1024 * 1024,
        cleanup_policy="compact",
        retry_cleanup_policy="delete",
    ):
        self.input_max_bytes = input_max_bytes
        self.dlq_max_bytes = dlq_max_bytes
        self.retry_max_bytes = retry_max_bytes
        self.cleanup_policy = cleanup_policy
        self.retry_cleanup_policy = retry_cleanup_policy

    def describe_configs(self, resources):
        configs = {
            "sensor-events": {
                "max.message.bytes": SimpleNamespace(
                    value=str(self.input_max_bytes)
                )
            },
            "sensor-events-dlq": {
                "max.message.bytes": SimpleNamespace(value=str(self.dlq_max_bytes))
            },
            "sensor-event-dedup": {
                "max.message.bytes": SimpleNamespace(
                    value=str(MAX_RAW_MESSAGE_BYTES)
                ),
                "cleanup.policy": SimpleNamespace(value=self.cleanup_policy),
            },
            "sensor-events-retry": {
                "max.message.bytes": SimpleNamespace(
                    value=str(self.retry_max_bytes)
                ),
                "cleanup.policy": SimpleNamespace(
                    value=self.retry_cleanup_policy
                ),
            },
        }
        return {
            resource: FakeAdminFuture(configs[resource.name])
            for resource in resources
        }


def make_processor(
    ledger_records=(), *, admin_client=None, initialize=True, config=None
):
    producer = FakeTransactionalProducer()
    consumer = FakeInputConsumer()
    high = max((record.offset() + 1 for record in ledger_records), default=0)
    restore_client = FakeLedgerConsumer(end=high, messages=ledger_records)
    restore_configs = []

    def restore_factory(config):
        restore_configs.append(config)
        return restore_client

    processor = TransactionalValidationProcessor(
        config or ProcessorConfig(
            operation_timeout=0.1,
            transaction_timeout=0.2,
            ledger_restore_timeout=0.1,
            poll_timeout=0.01,
        ),
        producer_client=producer,
        consumer_client=consumer,
        ledger_consumer_factory=restore_factory,
        admin_client=FakeAdminClient() if admin_client is None else admin_client,
    )
    if initialize:
        processor.initialize()
    return processor, producer, consumer, restore_client, restore_configs


def raw_message(event: dict, *, offset=12, key: bytes | None = None) -> FakeMessage:
    return FakeMessage(
        "sensor-events",
        0,
        offset,
        json.dumps(event, separators=(",", ":")).encode("utf-8"),
        key,
    )


class PayloadHashTests(unittest.TestCase):
    def test_hash_is_deterministic_and_independent_of_object_key_order(self):
        event = sample_event()
        reordered = dict(reversed(list(event.items())))
        self.assertEqual(payload_sha256(event), payload_sha256(reordered))
        self.assertEqual(payload_sha256(event), payload_sha256(copy.deepcopy(event)))

    def test_hash_canonicalizes_case_variants_of_a_valid_uuid(self):
        event = sample_event()
        event["event_id"] = "abcdefab-cdef-4abc-8def-abcdefabcdef"
        uppercase = dict(event, event_id=event["event_id"].upper())
        self.assertEqual(payload_sha256(event), payload_sha256(uppercase))


class LedgerRestoreTests(unittest.TestCase):
    def test_restores_latest_values_and_applies_tombstones_in_offset_order(self):
        original = sample_event()
        current_hash = payload_sha256(original)
        stale_hash = "0" * 64
        removed_id = "removed-event"
        messages = [
            FakeMessage(
                "sensor-event-dedup", 0, 5,
                ledger_marker(original["event_id"], stale_hash),
                original["event_id"].encode(),
            ),
            FakeMessage(
                "sensor-event-dedup", 0, 6,
                ledger_marker(original["event_id"], current_hash),
                original["event_id"].encode(),
            ),
            FakeMessage(
                "sensor-event-dedup", 0, 7,
                ledger_marker(removed_id, stale_hash),
                removed_id.encode(),
            ),
            FakeMessage(
                "sensor-event-dedup", 0, 9, None, removed_id.encode()
            ),
        ]
        client = FakeLedgerConsumer(low=5, end=10, messages=messages)
        configs = []

        def factory(config):
            configs.append(config)
            return client

        ledger = restore_deduplication_ledger(
            factory,
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            timeout=0.1,
        )
        self.assertEqual(ledger, {original["event_id"]: current_hash})
        self.assertTrue(client.closed)
        self.assertEqual(configs[0]["isolation.level"], "read_committed")
        self.assertFalse(configs[0]["enable.auto.commit"])
        self.assertEqual(configs[0]["auto.offset.reset"], "earliest")

    def test_empty_ledger_returns_without_waiting(self):
        client = FakeLedgerConsumer(low=0, end=0)
        ledger = restore_deduplication_ledger(
            lambda _config: client,
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            timeout=0.1,
        )
        self.assertEqual(ledger, {})
        self.assertTrue(client.closed)

    def test_rejects_ledger_marker_with_unsupported_schema_version(self):
        client = FakeLedgerConsumer(
            end=1,
            messages=[
                FakeMessage(
                    "sensor-event-dedup",
                    0,
                    0,
                    json.dumps(
                        {
                            "schema_version": "2.0",
                            "event_id": "event-1",
                            "payload_sha256": "0" * 64,
                        }
                    ).encode(),
                    b"event-1",
                )
            ],
        )
        with self.assertRaisesRegex(ProcessorError, "unsupported schema_version"):
            restore_deduplication_ledger(
                lambda _config: client,
                bootstrap_servers="localhost:9092",
                group_id="test-group",
                timeout=0.1,
            )
        self.assertTrue(client.closed)


class TransactionalProcessorTests(unittest.TestCase):
    def test_new_valid_event_routes_output_and_ledger_in_one_transaction(self):
        processor, producer, consumer, _, _ = make_processor()
        event = sample_event()
        outcome = processor.process_message(raw_message(event, offset=31))

        self.assertEqual(outcome, ProcessingOutcome.VALIDATED)
        transaction = producer.committed[0]
        self.assertEqual(len(transaction["outputs"]), 2)
        validated, marker = transaction["outputs"]
        self.assertEqual(validated[0], "sensor-events-validated")
        self.assertEqual(validated[1], event["container_id"].encode("utf-8"))
        self.assertEqual(json.loads(validated[2]), event)
        self.assertEqual(marker[0], "sensor-event-dedup")
        self.assertEqual(marker[1], event["event_id"].encode("utf-8"))
        marker_value = json.loads(marker[2])
        self.assertEqual(marker_value["payload_sha256"], payload_sha256(event))
        self.assertEqual(processor.ledger[event["event_id"]], payload_sha256(event))
        self.assertEqual(transaction["offsets"][0].offset, 32)
        self.assertIs(producer.calls[-1][0], "commit_transaction")
        self.assertIsNotNone(consumer.group_metadata)

    def test_same_id_and_hash_suppresses_output_and_commits_offset(self):
        event = sample_event()
        processor, producer, _, _, _ = make_processor(
            [
                FakeMessage(
                    "sensor-event-dedup",
                    0,
                    0,
                    ledger_marker(event["event_id"], payload_sha256(event)),
                    event["event_id"].encode(),
                )
            ]
        )
        outcome = processor.process_message(raw_message(event, offset=4))
        self.assertEqual(outcome, ProcessingOutcome.DUPLICATE)
        self.assertEqual(producer.committed[0]["outputs"], [])
        self.assertEqual(producer.committed[0]["offsets"][0].offset, 5)

    def test_case_variant_uuid_is_suppressed_as_duplicate(self):
        event = sample_event()
        event["event_id"] = "abcdefab-cdef-4abc-8def-abcdefabcdef"
        uppercase = dict(event, event_id=event["event_id"].upper())
        processor, producer, _, _, _ = make_processor()
        self.assertEqual(
            processor.process_message(raw_message(event, offset=10)),
            ProcessingOutcome.VALIDATED,
        )
        self.assertEqual(
            processor.process_message(raw_message(uppercase, offset=11)),
            ProcessingOutcome.DUPLICATE,
        )
        self.assertEqual(len(producer.committed[0]["outputs"]), 2)
        self.assertEqual(producer.committed[1]["outputs"], [])

    def test_conflict_goes_to_dlq_without_overwriting_ledger(self):
        original = sample_event()
        original_hash = payload_sha256(original)
        conflicting = copy.deepcopy(original)
        conflicting["readings"]["temperature_c"] += 1
        processor, producer, _, _, _ = make_processor(
            [
                FakeMessage(
                    "sensor-event-dedup",
                    0,
                    0,
                    ledger_marker(original["event_id"], original_hash),
                    original["event_id"].encode(),
                )
            ]
        )
        outcome = processor.process_message(raw_message(conflicting, offset=7))
        self.assertEqual(outcome, ProcessingOutcome.CONFLICT)
        transaction = producer.committed[0]
        self.assertEqual(len(transaction["outputs"]), 1)
        topic, key, value = transaction["outputs"][0]
        envelope = json.loads(value)
        self.assertEqual(topic, "sensor-events-dlq")
        self.assertEqual(key, original["event_id"].encode())
        self.assertEqual(envelope["error_code"], "EVENT_ID_PAYLOAD_CONFLICT")
        self.assertEqual(envelope["original_event"], conflicting)
        self.assertEqual(envelope["source_offset"], 7)
        self.assertEqual(processor.ledger[original["event_id"]], original_hash)
        self.assertEqual(transaction["offsets"][0].offset, 8)

    def test_invalid_sensor_event_goes_to_dlq_with_source_metadata(self):
        invalid = sample_event()
        invalid["readings"]["humidity_pct"] = 101
        raw = json.dumps(invalid).encode("utf-8")
        processor, producer, _, _, _ = make_processor()
        outcome = processor.process_message(
            FakeMessage("sensor-events", 0, 17, raw, b"invalid-key")
        )
        self.assertEqual(outcome, ProcessingOutcome.INVALID)
        transaction = producer.committed[0]
        topic, key, value = transaction["outputs"][0]
        envelope = json.loads(value)
        self.assertEqual(topic, "sensor-events-dlq")
        self.assertEqual(key, invalid["event_id"].encode())
        self.assertEqual(envelope["error_code"], "INVALID_SENSOR_EVENT")
        self.assertIn("between 0 and 100", envelope["error_message"])
        self.assertEqual(envelope["original_event"], invalid)
        self.assertEqual(envelope["source_partition"], 0)
        self.assertEqual(envelope["source_offset"], 17)
        self.assertEqual(
            base64.b64decode(envelope["raw_payload_base64"]),
            raw,
        )
        self.assertEqual(processor.ledger, {})

    def test_malformed_json_is_preserved_in_dlq_without_crashing(self):
        raw = b"\xff{not-json"
        processor, producer, _, _, _ = make_processor()
        outcome = processor.process_message(
            FakeMessage("sensor-events", 0, 23, raw, b"raw-key")
        )
        self.assertEqual(outcome, ProcessingOutcome.MALFORMED)
        transaction = producer.committed[0]
        topic, key, value = transaction["outputs"][0]
        envelope = json.loads(value)
        self.assertEqual(topic, "sensor-events-dlq")
        self.assertEqual(key, b"raw-key")
        self.assertEqual(envelope["error_code"], "MALFORMED_JSON")
        self.assertEqual(envelope["source_offset"], 23)
        self.assertEqual(base64.b64decode(envelope["raw_payload_base64"]), raw)
        self.assertNotIn("original_event", envelope)
        self.assertEqual(processor.ledger, {})

    def test_non_object_json_values_are_dead_lettered_without_contract_call(self):
        processor, producer, _, _, _ = make_processor()
        payloads = (b"null", b"[]", b'"text"', b"17", b"false")
        with patch("atmo_sync.processor.validate_sensor_event") as validator:
            for offset, raw in enumerate(payloads, start=30):
                outcome = processor.process_message(
                    FakeMessage("sensor-events", 0, offset, raw, b"non-object")
                )
                self.assertEqual(outcome, ProcessingOutcome.INVALID)

        validator.assert_not_called()
        self.assertEqual(len(producer.committed), len(payloads))
        for transaction, raw, offset in zip(producer.committed, payloads, range(30, 35)):
            self.assertEqual(transaction["offsets"][0].offset, offset + 1)
            topic, key, value = transaction["outputs"][0]
            envelope = json.loads(value)
            self.assertEqual(topic, "sensor-events-dlq")
            self.assertEqual(key, b"non-object")
            self.assertEqual(envelope["error_code"], "JSON_OBJECT_REQUIRED")
            self.assertEqual(envelope["error_message"], "sensor event JSON must be a JSON object")
            self.assertEqual(envelope["source_topic"], "sensor-events")
            self.assertEqual(envelope["source_partition"], 0)
            self.assertEqual(envelope["source_offset"], offset)
            self.assertEqual(envelope["original_event"], json.loads(raw))
            self.assertEqual(
                base64.b64decode(envelope["raw_payload_base64"]),
                raw,
            )

    def test_raw_payload_near_input_limit_preserves_full_dlq_envelope(self):
        event = sample_event()
        event["readings"]["humidity_pct"] = 101
        event["shipment_id"] = "S"
        target_size = MAX_RAW_MESSAGE_BYTES - 512
        initial = json.dumps(event, separators=(",", ":")).encode("utf-8")
        event["shipment_id"] += "x" * (target_size - len(initial))
        raw = json.dumps(event, separators=(",", ":")).encode("utf-8")
        self.assertEqual(len(raw), target_size)

        processor, producer, _, _, _ = make_processor()
        outcome = processor.process_message(
            FakeMessage(
                "sensor-events",
                0,
                251,
                raw,
                event["event_id"].encode("utf-8"),
            )
        )

        self.assertEqual(outcome, ProcessingOutcome.INVALID)
        transaction = producer.committed[0]
        self.assertEqual(transaction["offsets"][0].offset, 252)
        _, _, value = transaction["outputs"][0]
        envelope = json.loads(value)
        self.assertEqual(envelope["original_event"], event)
        self.assertEqual(
            base64.b64decode(envelope["raw_payload_base64"]),
            raw,
        )
        self.assertLessEqual(len(value), MAX_DLQ_MESSAGE_BYTES)

    def test_dlq_envelope_limit_includes_base64_and_parsed_event(self):
        raw = b"raw-payload"
        event = {"padding": ""}
        message = FakeMessage("sensor-events", 2, 55, raw, b"source-key")
        envelope = {
            "error_code": "INVALID_SENSOR_EVENT",
            "error_message": "invalid",
            "source_topic": "sensor-events",
            "source_partition": 2,
            "source_offset": 55,
            "raw_payload_base64": base64.b64encode(raw).decode("ascii"),
            "original_event": event,
        }
        base_size = len(
            json.dumps(
                envelope,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        event["padding"] = "x" * (MAX_DLQ_MESSAGE_BYTES - base_size)

        processor, _, _, _, _ = make_processor()
        output = processor._dlq_output(
            message,
            "INVALID_SENSOR_EVENT",
            "invalid",
            raw,
            event,
        )

        self.assertEqual(len(output[0][2]), MAX_DLQ_MESSAGE_BYTES)
        encoded = json.loads(output[0][2])
        self.assertEqual(encoded["original_event"], event)
        self.assertEqual(
            base64.b64decode(encoded["raw_payload_base64"]),
            raw,
        )

    def test_oversized_dlq_envelope_does_not_start_or_commit_transaction(self):
        processor, producer, _, _, _ = make_processor()
        raw = b"x" * 3_200_000
        with self.assertRaisesRegex(ProcessorError, "maximum is"):
            processor.process_message(
                FakeMessage("sensor-events", 0, 252, raw, b"source-key")
            )

        self.assertEqual(producer.committed, [])
        self.assertFalse(any(call[0] == "begin_transaction" for call in producer.calls))
        self.assertFalse(any(call[0] == "send_offsets" for call in producer.calls))

    def test_unavailable_broker_does_not_commit_input_offset_or_retry(self):
        processor, producer, _, _, _ = make_processor(
            config=ProcessorConfig(
                max_retry_attempts=2,
                retry_base_delay_seconds=0,
                retry_max_delay_seconds=1,
                retry_jitter_fraction=0,
                operation_timeout=0.1,
                transaction_timeout=0.2,
                ledger_restore_timeout=0.1,
                poll_timeout=0.01,
            )
        )
        producer.fail_begin = KafkaException("broker unavailable")
        with patch.object(
            processor,
            "_validate_for_processing",
            side_effect=TransientProcessingError("temporary failure"),
        ):
            with self.assertRaisesRegex(ProcessorError, "could not begin"):
                processor.process_message(raw_message(sample_event(), offset=105))

        self.assertEqual(producer.committed, [])
        self.assertEqual(producer.aborted, [])
        self.assertFalse(any(call[0] == "send_offsets" for call in producer.calls))

    def test_deep_json_and_nonfinite_values_are_dead_lettered_as_raw_bytes(self):
        processor, producer, _, _, _ = make_processor()
        event = sample_event()
        event["readings"]["temperature_c"] = "__OVERFLOW__"
        overflow_json = (
            json.dumps(event, separators=(",", ":"))
            .replace('"__OVERFLOW__"', "1e10000")
            .encode("utf-8")
        )
        deep_json = ("[" * 2000 + "0" + "]" * 2000).encode("ascii")

        for offset, raw in enumerate((overflow_json, deep_json), start=200):
            outcome = processor.process_message(
                FakeMessage("sensor-events", 0, offset, raw, b"bad-json")
            )
            self.assertEqual(outcome, ProcessingOutcome.MALFORMED)
            envelope = json.loads(producer.committed[-1]["outputs"][0][2])
            self.assertEqual(envelope["error_code"], "MALFORMED_JSON")
            self.assertEqual(
                base64.b64decode(envelope["raw_payload_base64"]),
                raw,
            )
            self.assertNotIn("original_event", envelope)

    def test_dlq_payload_amplification_stays_below_configured_limit(self):
        event = sample_event()
        event["shipment_id"] = "S" + "x" * 500_000
        event["readings"]["humidity_pct"] = 101
        raw = json.dumps(event, separators=(",", ":")).encode("utf-8")
        processor, producer, _, _, _ = make_processor()
        outcome = processor.process_message(
            FakeMessage("sensor-events", 0, 250, raw)
        )
        self.assertEqual(outcome, ProcessingOutcome.INVALID)
        self.assertLessEqual(
            len(producer.committed[0]["outputs"][0][2]),
            MAX_DLQ_MESSAGE_BYTES,
        )

    def test_transaction_abort_does_not_commit_input_offset_or_ledger(self):
        processor, producer, _, _, _ = make_processor()
        producer.fail_produce = RuntimeError("simulated produce failure")
        event = sample_event()
        with self.assertRaisesRegex(RuntimeError, "simulated produce failure"):
            processor.process_message(raw_message(event))
        self.assertEqual(producer.committed, [])
        self.assertEqual(len(producer.aborted), 1)
        self.assertEqual(producer.aborted[0]["offsets"], [])
        self.assertNotIn(event["event_id"], processor.ledger)

    def test_queue_full_aborts_transaction_and_reports_without_losing_offset(self):
        processor, producer, _, _, _ = make_processor()
        producer.fail_produce = BufferError("producer queue full")
        event = sample_event()
        with self.assertRaisesRegex(ProcessorError, "queue is full"):
            processor.process_message(raw_message(event))
        self.assertEqual(producer.committed, [])
        self.assertEqual(len(producer.aborted), 1)
        self.assertNotIn(event["event_id"], processor.ledger)

    def test_successful_transaction_commits_next_input_offset(self):
        processor, producer, _, _, _ = make_processor()
        processor.process_message(raw_message(sample_event(), offset=100))
        committed_offsets = producer.committed[0]["offsets"]
        self.assertEqual(len(committed_offsets), 1)
        self.assertEqual(committed_offsets[0].topic, "sensor-events")
        self.assertEqual(committed_offsets[0].partition, 0)
        self.assertEqual(committed_offsets[0].offset, 101)

    def test_commit_outcome_unknown_does_not_update_ledger_or_assume_abort(self):
        processor, producer, _, _, _ = make_processor()
        producer.fail_commit = RuntimeError("commit response lost")
        event = sample_event()
        with self.assertRaisesRegex(TransactionOutcomeUnknown, "outcome is uncertain"):
            processor.process_message(raw_message(event))
        self.assertNotIn(event["event_id"], processor.ledger)
        self.assertEqual(producer.aborted, [])
        processor.close()
        self.assertFalse(any(call[0] == "flush" for call in producer.calls))

    def test_abortable_commit_error_aborts_transaction_without_ledger_update(self):
        processor, producer, _, _, _ = make_processor()
        event = sample_event()
        with patch.object(
            processor,
            "_kafka_error_flag",
            side_effect=lambda _error, flag_name: flag_name == "txn_requires_abort",
        ):
            producer.fail_commit = KafkaException("simulated abortable commit error")
            with self.assertRaisesRegex(ProcessorError, "was aborted"):
                processor.process_message(raw_message(event))
        self.assertEqual(len(producer.aborted), 1)
        self.assertNotIn(event["event_id"], processor.ledger)

    def test_restart_restores_durable_ledger_before_raw_subscription(self):
        event = sample_event()
        digest = payload_sha256(event)
        processor, _, consumer, _, _ = make_processor(
            [
                FakeMessage(
                    "sensor-event-dedup",
                    0,
                    11,
                    ledger_marker(event["event_id"], digest),
                    event["event_id"].encode(),
                )
            ]
        )
        self.assertEqual(processor.ledger[event["event_id"]], digest)
        self.assertEqual(
            consumer.subscribed,
            ["sensor-events", "sensor-events-retry"],
        )

    def test_producer_and_consumer_configs_are_transactional_and_read_committed(self):
        with (
            patch("atmo_sync.processor.Producer") as producer_factory,
            patch("atmo_sync.processor.Consumer") as consumer_factory,
            patch("atmo_sync.processor.AdminClient") as admin_factory,
        ):
            TransactionalValidationProcessor(ProcessorConfig())
        producer_config = producer_factory.call_args.args[0]
        consumer_config = consumer_factory.call_args.args[0]
        self.assertEqual(
            producer_config["transactional.id"], DEFAULT_TRANSACTIONAL_ID
        )
        self.assertEqual(
            producer_config["message.max.bytes"], MAX_DLQ_KAFKA_RECORD_BYTES
        )
        self.assertTrue(producer_config["enable.idempotence"])
        self.assertEqual(producer_config["acks"], "all")
        self.assertEqual(consumer_config["group.id"], "atmosync-validation-processor")
        self.assertFalse(consumer_config["enable.auto.commit"])
        self.assertFalse(consumer_config["enable.auto.offset.store"])
        self.assertEqual(consumer_config["isolation.level"], "read_committed")
        self.assertEqual(consumer_config["auto.offset.reset"], "earliest")
        self.assertEqual(
            consumer_config["max.partition.fetch.bytes"],
            2 * 4 * 1024 * 1024,
        )
        self.assertEqual(
            consumer_config["fetch.max.bytes"],
            2 * 4 * 1024 * 1024,
        )
        admin_factory.assert_called_once_with(
            {"bootstrap.servers": "localhost:9092"}
        )

    def test_topic_verification_rejects_non_compacted_ledger(self):
        processor, _, _, _, _ = make_processor(
            admin_client=FakeAdminClient(cleanup_policy="delete"),
            initialize=False,
        )
        with self.assertRaisesRegex(ProcessorError, "cleanup.policy must include compact"):
            processor._verify_topics()

    def test_topic_verification_rejects_undersized_dlq_topic(self):
        processor, _, _, _, _ = make_processor(
            admin_client=FakeAdminClient(
                dlq_max_bytes=MAX_DLQ_KAFKA_RECORD_BYTES - 1
            ),
            initialize=False,
        )
        with self.assertRaisesRegex(ProcessorError, "expected at least"):
            processor._verify_topics()

    def test_topic_verification_rejects_undersized_retry_topic(self):
        processor, _, _, _, _ = make_processor(
            admin_client=FakeAdminClient(
                retry_max_bytes=MAX_RETRY_KAFKA_RECORD_BYTES - 1
            ),
            initialize=False,
        )
        with self.assertRaisesRegex(ProcessorError, "expected at least"):
            processor._verify_topics()

    def test_topic_verification_rejects_compacted_retry_topic(self):
        processor, _, _, _, _ = make_processor(
            admin_client=FakeAdminClient(retry_cleanup_policy="compact"),
            initialize=False,
        )
        with self.assertRaisesRegex(ProcessorError, "delete-only"):
            processor._verify_topics()

    def test_topic_verification_rejects_oversized_raw_topic(self):
        processor, _, _, _, _ = make_processor(
            admin_client=FakeAdminClient(input_max_bytes=MAX_RAW_MESSAGE_BYTES + 1),
            initialize=False,
        )
        with self.assertRaisesRegex(ProcessorError, "expected at most"):
            processor._verify_topics()

    def test_close_does_not_commit_offsets_outside_transaction(self):
        processor, producer, consumer, _, _ = make_processor()
        processor.close()
        self.assertTrue(consumer.closed)
        self.assertFalse(any(call[0] == "commit" for call in producer.calls))
        self.assertTrue(any(call[0] == "flush" for call in producer.calls))

    def test_config_rejects_reused_topic_names(self):
        with self.assertRaisesRegex(ValueError, "must be distinct"):
            ProcessorConfig(validated_topic="sensor-events")


class RetryProcessingTests(unittest.TestCase):
    def retry_config(self, **overrides):
        values = {
            "operation_timeout": 0.1,
            "transaction_timeout": 0.2,
            "ledger_restore_timeout": 0.1,
            "poll_timeout": 0.01,
            "max_retry_attempts": 2,
            "retry_base_delay_seconds": 0,
            "retry_max_delay_seconds": 1,
            "retry_jitter_fraction": 0,
        }
        values.update(overrides)
        return ProcessorConfig(**values)

    def retry_message(self, producer, *, offset=8, transaction_index=-1):
        topic, key, value = producer.committed[transaction_index]["outputs"][0]
        self.assertEqual(topic, DEFAULT_RETRY_TOPIC)
        return FakeMessage(DEFAULT_RETRY_TOPIC, 0, offset, value, key)

    def test_transient_failure_retries_then_commits_validated_event_and_offsets(self):
        event = sample_event()
        raw = json.dumps(event, separators=(",", ":")).encode("utf-8")
        processor, producer, _, _, _ = make_processor(
            config=self.retry_config()
        )
        with patch.object(
            processor,
            "_validate_for_processing",
            side_effect=[
                TransientProcessingError("temporary dependency failure"),
                None,
            ],
        ):
            first = processor.process_message(
                FakeMessage("sensor-events", 0, 12, raw, event["event_id"].encode())
            )
            retry_record = self.retry_message(producer)
            second = processor.process_message(retry_record)

        self.assertEqual(first, ProcessingOutcome.RETRY_SCHEDULED)
        self.assertEqual(second, ProcessingOutcome.VALIDATED)
        retry_envelope = RetryEnvelope.from_bytes(
            producer.committed[0]["outputs"][0][2]
        )
        self.assertEqual(retry_envelope.raw_payload, raw)
        self.assertEqual(retry_envelope.source_topic, "sensor-events")
        self.assertEqual(retry_envelope.source_partition, 0)
        self.assertEqual(retry_envelope.source_offset, 12)
        self.assertEqual(retry_envelope.attempt_number, 1)
        self.assertEqual(
            producer.committed[0]["offsets"][0].offset,
            13,
        )
        self.assertEqual(
            producer.committed[1]["offsets"][0].topic,
            DEFAULT_RETRY_TOPIC,
        )
        self.assertEqual(producer.committed[1]["offsets"][0].offset, 9)
        self.assertEqual(
            [output[0] for output in producer.committed[1]["outputs"]],
            ["sensor-events-validated", "sensor-event-dedup"],
        )

    def test_retries_are_bounded_and_exhaustion_routes_to_dlq(self):
        event = sample_event()
        processor, producer, _, _, _ = make_processor(
            config=self.retry_config(max_retry_attempts=2)
        )
        with patch.object(
            processor,
            "_validate_for_processing",
            side_effect=TransientProcessingError("temporary failure"),
        ):
            first = processor.process_message(raw_message(event, offset=20))
            second = processor.process_message(
                self.retry_message(producer, offset=40, transaction_index=0)
            )
            third = processor.process_message(
                self.retry_message(producer, offset=41, transaction_index=1)
            )

        self.assertEqual(first, ProcessingOutcome.RETRY_SCHEDULED)
        self.assertEqual(second, ProcessingOutcome.RETRY_SCHEDULED)
        self.assertEqual(third, ProcessingOutcome.RETRY_EXHAUSTED)
        retry_outputs = [
            output
            for transaction in producer.committed
            for output in transaction["outputs"]
            if output[0] == DEFAULT_RETRY_TOPIC
        ]
        dlq_outputs = [
            output
            for transaction in producer.committed
            for output in transaction["outputs"]
            if output[0] == "sensor-events-dlq"
        ]
        self.assertEqual(len(retry_outputs), 2)
        self.assertEqual(len(dlq_outputs), 1)
        envelope = json.loads(dlq_outputs[0][2])
        self.assertEqual(envelope["error_code"], "RETRY_EXHAUSTED")
        self.assertEqual(envelope["original_event"], event)
        self.assertEqual(envelope["source_offset"], 20)
        self.assertEqual(envelope["retry_metadata"]["attempt_number"], 2)
        self.assertEqual(
            envelope["retry_metadata"]["failure_class"], "transient_exhausted"
        )
        self.assertEqual(
            [
                attempt["attempt_number"]
                for attempt in envelope["retry_metadata"]["attempt_history"]
            ],
            [0, 1, 2],
        )
        self.assertTrue(
            all(
                attempt["failure_reason"] == "temporary failure"
                for attempt in envelope["retry_metadata"]["attempt_history"]
            )
        )
        self.assertEqual(producer.committed[-1]["offsets"][0].topic, DEFAULT_RETRY_TOPIC)
        self.assertEqual(producer.committed[-1]["offsets"][0].offset, 42)

    def test_permanent_failure_routes_directly_to_dlq_without_retry(self):
        invalid = sample_event()
        invalid["readings"]["humidity_pct"] = 101
        processor, producer, _, _, _ = make_processor(
            config=self.retry_config()
        )

        outcome = processor.process_message(raw_message(invalid, offset=50))

        self.assertEqual(outcome, ProcessingOutcome.INVALID)
        self.assertEqual(len(producer.committed), 1)
        output = producer.committed[0]["outputs"][0]
        self.assertEqual(output[0], "sensor-events-dlq")
        envelope = json.loads(output[2])
        self.assertEqual(envelope["retry_metadata"]["attempt_number"], 0)
        self.assertEqual(envelope["retry_metadata"]["failure_class"], "permanent")
        self.assertEqual(
            producer.committed[0]["offsets"][0].offset,
            51,
        )

    def test_duplicate_raw_event_during_retry_does_not_duplicate_output(self):
        event = sample_event()
        processor, producer, _, _, _ = make_processor(
            config=self.retry_config()
        )
        calls = 0

        def fail_once(value):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TransientProcessingError("temporary failure")
            from atmo_sync.contracts import validate_sensor_event

            validate_sensor_event(value)

        with patch.object(processor, "_validate_for_processing", side_effect=fail_once):
            first = processor.process_message(raw_message(event, offset=60))
            duplicate = processor.process_message(raw_message(event, offset=61))
            retried = processor.process_message(
                self.retry_message(producer, offset=70, transaction_index=0)
            )

        self.assertEqual(first, ProcessingOutcome.RETRY_SCHEDULED)
        self.assertEqual(duplicate, ProcessingOutcome.VALIDATED)
        self.assertEqual(retried, ProcessingOutcome.DUPLICATE)
        validated = [
            output
            for transaction in producer.committed
            for output in transaction["outputs"]
            if output[0] == "sensor-events-validated"
        ]
        self.assertEqual(len(validated), 1)

    def test_restart_can_resume_a_transactionally_published_retry_record(self):
        event = sample_event()
        config = self.retry_config()
        first_processor, first_producer, _, _, _ = make_processor(config=config)
        with patch.object(
            first_processor,
            "_validate_for_processing",
            side_effect=TransientProcessingError("temporary failure"),
        ):
            outcome = first_processor.process_message(raw_message(event, offset=80))
        retry_record = self.retry_message(first_producer, offset=90)
        self.assertEqual(outcome, ProcessingOutcome.RETRY_SCHEDULED)
        self.assertEqual(first_producer.committed[0]["offsets"][0].offset, 81)
        first_processor.close()

        restarted, producer, _, _, _ = make_processor(config=config)
        outcome = restarted.process_message(retry_record)

        self.assertEqual(outcome, ProcessingOutcome.VALIDATED)
        self.assertEqual(
            producer.committed[0]["offsets"][0].topic,
            DEFAULT_RETRY_TOPIC,
        )

    def test_failed_retry_publication_aborts_without_committing_source_offset(self):
        processor, producer, _, _, _ = make_processor(
            config=self.retry_config()
        )
        producer.fail_produce = RuntimeError("retry broker unavailable")
        with patch.object(
            processor,
            "_validate_for_processing",
            side_effect=TransientProcessingError("temporary failure"),
        ):
            with self.assertRaisesRegex(RuntimeError, "retry broker unavailable"):
                processor.process_message(raw_message(sample_event(), offset=100))

        self.assertEqual(producer.committed, [])
        self.assertEqual(len(producer.aborted), 1)
        self.assertFalse(any(call[0] == "send_offsets" for call in producer.calls))

    def test_failed_dlq_publication_aborts_without_committing_source_offset(self):
        processor, producer, _, _, _ = make_processor()
        producer.fail_produce = RuntimeError("DLQ broker unavailable")
        invalid = sample_event()
        invalid["readings"]["humidity_pct"] = 101

        with self.assertRaisesRegex(RuntimeError, "DLQ broker unavailable"):
            processor.process_message(raw_message(invalid, offset=110))

        self.assertEqual(producer.committed, [])
        self.assertEqual(len(producer.aborted), 1)
        self.assertFalse(any(call[0] == "send_offsets" for call in producer.calls))

    def test_future_retry_is_paused_until_due_without_losing_its_offset(self):
        event = sample_event()
        future = time.time() + 60
        future_timestamp = utc_timestamp(
            datetime.fromtimestamp(future, timezone.utc)
        )
        retry = RetryEnvelope(
            event_id=event["event_id"],
            source_topic="sensor-events",
            source_partition=0,
            source_offset=120,
            source_key=event["event_id"].encode(),
            raw_payload=json.dumps(event, separators=(",", ":")).encode(),
            attempt_number=1,
            first_failure_at=future_timestamp,
            last_failure_at=future_timestamp,
            failure_reason="temporary failure",
            next_attempt_at=future_timestamp,
        )
        processor, producer, consumer, _, _ = make_processor(
            config=self.retry_config()
        )
        message = FakeMessage(
            DEFAULT_RETRY_TOPIC,
            0,
            130,
            retry.to_bytes(),
            event["event_id"].encode(),
        )
        consumer.messages.append(message)

        with patch("atmo_sync.processor.time.time", return_value=future - 1):
            outcome = processor.process_one(timeout=0.01)
        self.assertIsNone(outcome)
        self.assertEqual(len(consumer.paused), 1)
        self.assertEqual(producer.committed, [])

        with patch("atmo_sync.processor.time.time", return_value=future + 1):
            outcome = processor.process_one(timeout=0.01)

        self.assertEqual(outcome, ProcessingOutcome.VALIDATED)
        self.assertEqual(len(consumer.resumed), 1)
        self.assertEqual(
            producer.committed[0]["offsets"][0].offset,
            131,
        )


if __name__ == "__main__":
    unittest.main()
