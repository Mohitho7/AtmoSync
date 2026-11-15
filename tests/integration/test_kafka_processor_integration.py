"""Opt-in integration test for transactional processing against local Kafka."""

from __future__ import annotations

import base64
import copy
import json
import os
import time
import unittest
import uuid

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from atmo_sync.processor import (
    DEFAULT_DLQ_TOPIC,
    DEFAULT_DEDUP_TOPIC,
    DEFAULT_INPUT_TOPIC,
    DEFAULT_VALIDATED_TOPIC,
    MAX_DLQ_KAFKA_RECORD_BYTES,
    MAX_DLQ_MESSAGE_BYTES,
    MAX_RAW_MESSAGE_BYTES,
    ProcessorConfig,
    ProcessingOutcome,
    TransactionalValidationProcessor,
)
from atmo_sync.simulator import SensorSimulator


@unittest.skipUnless(
    os.environ.get("ATMOSYNC_RUN_KAFKA_INTEGRATION") == "1",
    "set ATMOSYNC_RUN_KAFKA_INTEGRATION=1 to run local Kafka integration tests",
)
class KafkaProcessorIntegrationTests(unittest.TestCase):
    def test_validation_dlq_and_dedup_survive_processor_restart(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        group_id = f"atmosync-processor-test-{uuid.uuid4()}"
        config = ProcessorConfig(
            bootstrap_servers=bootstrap,
            group_id=group_id,
            operation_timeout=10,
            transaction_timeout=30,
            ledger_restore_timeout=15,
            poll_timeout=0.25,
        )
        valid_event = SensorSimulator(seed=904).generate_events(steps=1)[0]
        valid_event["event_id"] = str(uuid.uuid4())
        conflict_event = copy.deepcopy(valid_event)
        conflict_event["readings"]["temperature_c"] += 0.5
        invalid_event = SensorSimulator(seed=905).generate_events(steps=1)[0]
        invalid_event["event_id"] = str(uuid.uuid4())
        invalid_event["shipment_id"] = "SHIP-" + "x" * 500_000
        invalid_event["readings"]["humidity_pct"] = 101
        malformed = b"\xff-atmosync-processor-integration-" + uuid.uuid4().bytes

        output_starts = {
            DEFAULT_VALIDATED_TOPIC: self._current_high_offsets(
                bootstrap, DEFAULT_VALIDATED_TOPIC, 6
            ),
            DEFAULT_DLQ_TOPIC: self._current_high_offsets(
                bootstrap, DEFAULT_DLQ_TOPIC, 3
            ),
            DEFAULT_DEDUP_TOPIC: self._current_high_offsets(
                bootstrap, DEFAULT_DEDUP_TOPIC, 1
            ),
        }
        starting_offset = self._seed_group_at_current_end(bootstrap, group_id)
        producer = Producer({"bootstrap.servers": bootstrap})
        try:
            for event in (valid_event, conflict_event, invalid_event):
                producer.produce(
                    DEFAULT_INPUT_TOPIC,
                    key=event["event_id"].encode("utf-8"),
                    value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                )
            producer.produce(
                DEFAULT_INPUT_TOPIC,
                key=b"malformed-integration-record",
                value=malformed,
            )
            self.assertEqual(producer.flush(15), 0)
        finally:
            producer.flush(15)

        target_offset = self._high_watermark(bootstrap, DEFAULT_INPUT_TOPIC)
        self.assertGreater(target_offset, starting_offset)
        processor = TransactionalValidationProcessor(config)
        try:
            processor.initialize()
            outcomes = self._process_through(processor, target_offset)
            self.assertEqual(
                outcomes,
                [
                    ProcessingOutcome.VALIDATED,
                    ProcessingOutcome.CONFLICT,
                    ProcessingOutcome.INVALID,
                    ProcessingOutcome.MALFORMED,
                ],
            )
        finally:
            processor.close()

        replay_start = self._high_watermark(bootstrap, DEFAULT_INPUT_TOPIC)
        replay_producer = Producer({"bootstrap.servers": bootstrap})
        try:
            replay_producer.produce(
                DEFAULT_INPUT_TOPIC,
                key=valid_event["event_id"].encode("utf-8"),
                value=json.dumps(
                    valid_event, separators=(",", ":")
                ).encode("utf-8"),
            )
            self.assertEqual(replay_producer.flush(15), 0)
        finally:
            replay_producer.flush(15)
        replay_end = self._high_watermark(bootstrap, DEFAULT_INPUT_TOPIC)
        self.assertGreater(replay_end, replay_start)

        restarted = TransactionalValidationProcessor(config)
        try:
            restarted.initialize()
            self.assertIn(valid_event["event_id"], restarted.ledger)
            self.assertEqual(
                self._process_through(restarted, replay_end),
                [ProcessingOutcome.DUPLICATE],
            )
        finally:
            restarted.close()

        validated = self._read_topic(
            bootstrap,
            DEFAULT_VALIDATED_TOPIC,
            6,
            output_starts[DEFAULT_VALIDATED_TOPIC],
        )
        dlq = self._read_topic(
            bootstrap,
            DEFAULT_DLQ_TOPIC,
            3,
            output_starts[DEFAULT_DLQ_TOPIC],
        )
        valid_records = [
            record
            for record in validated
            if record.get("event_id") == valid_event["event_id"]
        ]
        self.assertEqual(len(valid_records), 1)

        relevant_dlq = []
        for record in dlq:
            original = record.get("original_event")
            if (
                isinstance(original, dict)
                and original.get("event_id")
                in {valid_event["event_id"], invalid_event["event_id"]}
            ) or record.get("raw_payload_base64") == base64.b64encode(malformed).decode(
                "ascii"
            ):
                relevant_dlq.append(record)
        self.assertEqual(
            {record["error_code"] for record in relevant_dlq},
            {
                "EVENT_ID_PAYLOAD_CONFLICT",
                "INVALID_SENSOR_EVENT",
                "MALFORMED_JSON",
            },
        )
        self.assertEqual(
            sum(
                record["error_code"] == "EVENT_ID_PAYLOAD_CONFLICT"
                for record in relevant_dlq
            ),
            1,
        )
        self.assertEqual(
            sum(
                record["error_code"] == "INVALID_SENSOR_EVENT"
                for record in relevant_dlq
            ),
            1,
        )
        self.assertEqual(
            sum(record["error_code"] == "MALFORMED_JSON" for record in relevant_dlq),
            1,
        )

        ledger_records = self._read_topic(
            bootstrap,
            DEFAULT_DEDUP_TOPIC,
            1,
            output_starts[DEFAULT_DEDUP_TOPIC],
        )
        markers = [
            record
            for record in ledger_records
            if record.get("event_id") == valid_event["event_id"]
        ]
        self.assertEqual(len(markers), 1)

    def test_aborted_transaction_is_hidden_from_read_committed_consumers(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        topic = DEFAULT_VALIDATED_TOPIC
        start_offsets = self._current_high_offsets(bootstrap, topic, 6)
        event_id = str(uuid.uuid4())
        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "transactional.id": f"atmosync-abort-test-{uuid.uuid4()}",
                "message.max.bytes": MAX_DLQ_MESSAGE_BYTES,
            }
        )
        producer.init_transactions(10)
        producer.begin_transaction()
        try:
            producer.produce(
                topic,
                partition=0,
                key=event_id.encode("utf-8"),
                value=json.dumps({"event_id": event_id}).encode("utf-8"),
            )
            self.assertEqual(producer.flush(10), 0)
        finally:
            producer.abort_transaction(10)

        committed_view = self._read_topic(
            bootstrap, topic, 6, start_offsets, isolation_level="read_committed"
        )
        uncommitted_view = self._read_topic(
            bootstrap, topic, 6, start_offsets, isolation_level="read_uncommitted"
        )
        self.assertNotIn(event_id, {record.get("event_id") for record in committed_view})
        self.assertIn(event_id, {record.get("event_id") for record in uncommitted_view})

    def test_near_limit_raw_event_dlq_record_round_trips_from_kafka(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        group_id = f"atmosync-max-dlq-test-{uuid.uuid4()}"
        event = SensorSimulator(seed=906).generate_events(steps=1)[0]
        event["event_id"] = str(uuid.uuid4())
        event["shipment_id"] = "S"
        event["readings"]["humidity_pct"] = 101
        target_raw_size = MAX_RAW_MESSAGE_BYTES - 512
        initial = json.dumps(
            event, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        event["shipment_id"] += "x" * (target_raw_size - len(initial))
        raw = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        self.assertEqual(len(raw), target_raw_size)

        dlq_starts = self._current_high_offsets(
            bootstrap, DEFAULT_DLQ_TOPIC, 3
        )
        config = ProcessorConfig(
            bootstrap_servers=bootstrap,
            group_id=group_id,
            operation_timeout=10,
            transaction_timeout=30,
            ledger_restore_timeout=15,
            poll_timeout=0.25,
        )
        starting_offset = self._seed_group_at_current_end(bootstrap, group_id)
        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "message.max.bytes": 2 * MAX_RAW_MESSAGE_BYTES,
            }
        )
        try:
            producer.produce(
                DEFAULT_INPUT_TOPIC,
                key=event["event_id"].encode("utf-8"),
                value=raw,
            )
            self.assertEqual(producer.flush(15), 0)
        finally:
            producer.flush(15)

        target_offset = self._high_watermark(bootstrap, DEFAULT_INPUT_TOPIC)
        self.assertGreater(target_offset, starting_offset)
        processor = TransactionalValidationProcessor(config)
        try:
            processor.initialize()
            self.assertEqual(
                self._process_through(processor, target_offset),
                [ProcessingOutcome.INVALID],
            )
        finally:
            processor.close()

        records = self._read_topic(
            bootstrap,
            DEFAULT_DLQ_TOPIC,
            3,
            dlq_starts,
        )
        matching = [
            record
            for record in records
            if isinstance(record.get("original_event"), dict)
            and record["original_event"].get("event_id") == event["event_id"]
        ]
        self.assertEqual(len(matching), 1)
        envelope = matching[0]
        self.assertEqual(envelope["error_code"], "INVALID_SENSOR_EVENT")
        self.assertEqual(envelope["source_offset"], target_offset - 1)
        self.assertEqual(envelope["original_event"], event)
        self.assertEqual(
            base64.b64decode(envelope["raw_payload_base64"]),
            raw,
        )
        encoded_envelope = json.dumps(
            envelope,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertGreater(len(encoded_envelope), MAX_RAW_MESSAGE_BYTES)
        self.assertLessEqual(len(encoded_envelope), MAX_DLQ_MESSAGE_BYTES)
        self.assertLessEqual(
            len(encoded_envelope) + len(event["event_id"].encode("utf-8")),
            MAX_DLQ_KAFKA_RECORD_BYTES,
        )

    def test_maximum_dlq_envelope_round_trips_from_kafka(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        event_id = str(uuid.uuid4())
        envelope = {
            "error_code": "MAX_SIZE_INTEGRATION_TEST",
            "error_message": "maximum supported envelope",
            "source_topic": DEFAULT_INPUT_TOPIC,
            "source_partition": 0,
            "source_offset": 0,
            "raw_payload_base64": None,
            "original_event": {"event_id": event_id, "padding": ""},
        }
        target_padding = MAX_DLQ_MESSAGE_BYTES - len(
            json.dumps(
                envelope,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        envelope["original_event"]["padding"] = "x" * target_padding
        expected_value = json.dumps(
            envelope,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(len(expected_value), MAX_DLQ_MESSAGE_BYTES)

        starts = self._current_high_offsets(bootstrap, DEFAULT_DLQ_TOPIC, 3)
        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "message.max.bytes": MAX_DLQ_KAFKA_RECORD_BYTES,
            }
        )
        try:
            producer.produce(
                DEFAULT_DLQ_TOPIC,
                key=event_id.encode("utf-8"),
                value=expected_value,
            )
            self.assertEqual(producer.flush(20), 0)
        finally:
            producer.flush(20)

        records = self._read_topic(bootstrap, DEFAULT_DLQ_TOPIC, 3, starts)
        matching = [
            record
            for record in records
            if record.get("error_code") == "MAX_SIZE_INTEGRATION_TEST"
            and isinstance(record.get("original_event"), dict)
            and record["original_event"].get("event_id") == event_id
        ]
        self.assertEqual(len(matching), 1)
        actual_value = json.dumps(
            matching[0],
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(actual_value, expected_value)

    def _seed_group_at_current_end(self, bootstrap: str, group_id: str) -> int:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        try:
            _, end_offset = consumer.get_watermark_offsets(
                TopicPartition(DEFAULT_INPUT_TOPIC, 0), timeout=10
            )
            consumer.assign([TopicPartition(DEFAULT_INPUT_TOPIC, 0, end_offset)])
            consumer.commit(
                offsets=[TopicPartition(DEFAULT_INPUT_TOPIC, 0, end_offset)],
                asynchronous=False,
            )
            return end_offset
        finally:
            consumer.close()

    def _high_watermark(self, bootstrap: str, topic: str) -> int:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-watermark-{uuid.uuid4()}",
                "enable.auto.commit": False,
            }
        )
        try:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, 0), timeout=10
            )
            return high
        finally:
            consumer.close()

    def _process_through(
        self, processor: TransactionalValidationProcessor, end_offset: int
    ) -> list[ProcessingOutcome]:
        outcomes = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            outcome = processor.process_one(timeout=0.25)
            if outcome is not None:
                outcomes.append(outcome)
            committed = processor.consumer.committed(
                [TopicPartition(DEFAULT_INPUT_TOPIC, 0)],
                timeout=5,
            )[0]
            if committed.offset >= end_offset:
                return outcomes
        self.fail(f"processor did not transactionally commit through offset {end_offset}")

    def _current_high_offsets(
        self, bootstrap: str, topic: str, partition_count: int
    ) -> list[int]:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-watermark-{uuid.uuid4()}",
                "enable.auto.commit": False,
            }
        )
        try:
            return [
                consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )[1]
                for partition in range(partition_count)
            ]
        finally:
            consumer.close()

    def _read_topic(
        self,
        bootstrap: str,
        topic: str,
        partitions: int,
        start_offsets: list[int],
        *,
        isolation_level: str = "read_committed",
    ) -> list[dict]:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-output-test-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "isolation.level": isolation_level,
                "enable.partition.eof": True,
                "max.partition.fetch.bytes": MAX_DLQ_KAFKA_RECORD_BYTES,
                "fetch.max.bytes": 2 * MAX_DLQ_KAFKA_RECORD_BYTES,
            }
        )
        records = []
        try:
            metadata = consumer.list_topics(topic=topic, timeout=10).topics.get(topic)
            self.assertIsNotNone(metadata, f"Kafka topic does not exist: {topic}")
            self.assertEqual(len(metadata.partitions), partitions)
            self.assertEqual(len(start_offsets), partitions)
            targets = []
            assignments = []
            for partition in range(partitions):
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )
                targets.append(high)
                assignments.append(
                    TopicPartition(topic, partition, start_offsets[partition])
                )
            consumer.assign(assignments)
            reached_partitions = set()
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if len(reached_partitions) == partitions:
                    return records
                message = consumer.poll(0.5)
                if message is None:
                    continue
                if message.error() is not None:
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        if message.offset() >= targets[message.partition()]:
                            reached_partitions.add(message.partition())
                        continue
                    self.fail(f"Kafka output consumer error: {message.error()}")
                if message.value() is not None:
                    records.append(json.loads(message.value().decode("utf-8")))
                if message.offset() + 1 >= targets[message.partition()]:
                    reached_partitions.add(message.partition())
            self.fail(f"timed out reading {topic} through captured partition offsets")
        finally:
            consumer.close()


if __name__ == "__main__":
    unittest.main()
