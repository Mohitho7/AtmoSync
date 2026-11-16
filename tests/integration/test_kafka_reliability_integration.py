"""Opt-in live workload and broker-restart reliability measurements."""

from __future__ import annotations

import copy
import json
import os
import statistics
import subprocess
import time
import unittest
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from atmo_sync.processor import (
    DEFAULT_DLQ_TOPIC,
    DEFAULT_INPUT_TOPIC,
    DEFAULT_VALIDATED_TOPIC,
    ProcessorConfig,
    ProcessingOutcome,
    TransactionalValidationProcessor,
)
from atmo_sync.producer import KafkaSensorProducer
from atmo_sync.retry import DEFAULT_RETRY_TOPIC, RetryEnvelope, TransientProcessingError
from atmo_sync.simulator import SensorSimulator


_INTEGRATION_ENABLED = os.environ.get("ATMOSYNC_RUN_KAFKA_INTEGRATION") == "1"


@unittest.skipUnless(
    _INTEGRATION_ENABLED,
    "set ATMOSYNC_RUN_KAFKA_INTEGRATION=1 to run local Kafka integration tests",
)
class KafkaReliabilityIntegrationTests(unittest.TestCase):
    def test_sustained_batch_reports_throughput_latency_lag_and_failure_rates(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        group_id = f"atmosync-performance-{uuid.uuid4()}"
        config = ProcessorConfig(
            bootstrap_servers=bootstrap,
            group_id=group_id,
            max_retry_attempts=1,
            retry_base_delay_seconds=0,
            retry_max_delay_seconds=1,
            retry_jitter_fraction=0,
            operation_timeout=10,
            transaction_timeout=30,
            ledger_restore_timeout=15,
            poll_timeout=0.1,
        )
        events = SensorSimulator(seed=4401).generate_events(steps=10)
        transient_event = copy.deepcopy(events[-1])
        duplicate_events = [copy.deepcopy(event) for event in events[:3]]
        invalid_event = copy.deepcopy(events[16])
        invalid_event["readings"]["humidity_pct"] = 101
        workload: list[dict] = events + duplicate_events + [invalid_event]
        start_raw_offset, start_retry_offset = self._seed_processor_group(
            bootstrap, group_id
        )
        validated_starts = self._high_offsets(bootstrap, DEFAULT_VALIDATED_TOPIC, 6)
        dlq_starts = self._high_offsets(bootstrap, DEFAULT_DLQ_TOPIC, 3)
        retry_starts = self._high_offsets(bootstrap, DEFAULT_RETRY_TOPIC, 1)
        initial_retry_end = start_retry_offset

        producer = Producer({"bootstrap.servers": bootstrap})
        publish_times: dict[int, float] = {}
        delivery_errors: list[str] = []
        workload_start = time.perf_counter()
        try:
            for event in workload:
                raw = json.dumps(event, separators=(",", ":")).encode("utf-8")
                published_at = time.perf_counter()

                def on_delivery(error, message, timestamp=published_at):
                    if error is not None:
                        delivery_errors.append(str(error))
                    else:
                        publish_times[message.offset()] = timestamp

                producer.produce(
                    DEFAULT_INPUT_TOPIC,
                    key=event["event_id"].encode("utf-8"),
                    value=raw,
                    on_delivery=on_delivery,
                )
            self.assertEqual(producer.flush(20), 0)
        finally:
            producer.flush(20)
        self.assertEqual(delivery_errors, [])
        target_raw_offset = self._high_offset(bootstrap, DEFAULT_INPUT_TOPIC, 0)
        self.assertEqual(len(publish_times), len(workload))
        self.assertEqual(
            sorted(publish_times),
            list(range(start_raw_offset, target_raw_offset)),
        )

        processor = TransactionalValidationProcessor(config)
        process_outcomes: list[ProcessingOutcome] = []
        processing_latencies_ms: list[float] = []
        original_process_message = processor.process_message
        original_validation = processor._validate_for_processing
        transient_failure_seen = False

        def fail_target_once(event):
            nonlocal transient_failure_seen
            if (
                event["event_id"] == transient_event["event_id"]
                and not transient_failure_seen
            ):
                transient_failure_seen = True
                raise TransientProcessingError("injected performance retry")
            original_validation(event)

        def observe_message(message):
            if message.topic() == DEFAULT_RETRY_TOPIC:
                retry = RetryEnvelope.from_bytes(message.value())
                original_offset = retry.source_offset
            else:
                original_offset = message.offset()
            outcome = original_process_message(message)
            process_outcomes.append(outcome)
            if outcome != ProcessingOutcome.RETRY_SCHEDULED:
                published_at = publish_times.get(original_offset)
                if published_at is not None:
                    processing_latencies_ms.append(
                        (time.perf_counter() - published_at) * 1000
                    )
            return outcome

        try:
            processor.initialize()
            processor._validate_for_processing = fail_target_once
            processor.process_message = observe_message
            deadline = time.monotonic() + 90
            expected_processed_count = len(workload) + 1
            while (
                time.monotonic() < deadline
                and len(process_outcomes) < expected_processed_count
            ):
                processor.process_one(timeout=0.1)
            if len(process_outcomes) != expected_processed_count:
                self.fail("processor did not finish the sustained workload")
            committed = processor.consumer.committed(
                [
                    TopicPartition(DEFAULT_INPUT_TOPIC, 0),
                    TopicPartition(DEFAULT_RETRY_TOPIC, 0),
                ],
                timeout=10,
            )
            offsets = {partition.topic: partition.offset for partition in committed}
            final_raw_committed = offsets[DEFAULT_INPUT_TOPIC]
            final_retry_committed = offsets[DEFAULT_RETRY_TOPIC]
        finally:
            processor.close()

        elapsed = time.perf_counter() - workload_start
        counts = Counter(process_outcomes)
        self.assertEqual(counts[ProcessingOutcome.RETRY_SCHEDULED], 1)
        self.assertEqual(counts[ProcessingOutcome.DUPLICATE], len(duplicate_events))
        self.assertEqual(counts[ProcessingOutcome.INVALID], 1)
        self.assertEqual(counts[ProcessingOutcome.VALIDATED], len(events))
        self.assertEqual(len(processing_latencies_ms), len(workload))

        validated_records = self._read_committed_records(
            bootstrap, DEFAULT_VALIDATED_TOPIC, validated_starts
        )
        expected_validated_ids = {event["event_id"] for event in events}
        self.assertEqual(
            Counter(record["event_id"] for record in validated_records),
            Counter(expected_validated_ids),
        )
        retry_records = self._read_committed_records(
            bootstrap, DEFAULT_RETRY_TOPIC, retry_starts
        )
        self.assertEqual(
            [record["event_id"] for record in retry_records],
            [transient_event["event_id"]],
        )
        dlq_records = self._read_committed_records(
            bootstrap, DEFAULT_DLQ_TOPIC, dlq_starts
        )
        matching_dlq = [
            record
            for record in dlq_records
            if record.get("error_code") == "INVALID_SENSOR_EVENT"
            and isinstance(record.get("original_event"), dict)
            and record["original_event"].get("event_id")
            == invalid_event["event_id"]
        ]
        self.assertEqual(len(matching_dlq), 1)

        raw_lag = self._count_read_committed_records(
            bootstrap, DEFAULT_INPUT_TOPIC, 0, final_raw_committed
        )
        retry_lag = self._count_read_committed_records(
            bootstrap, DEFAULT_RETRY_TOPIC, 0, final_retry_committed
        )
        metrics = {
            "environment": "local Docker Kafka 3.9.1, single broker",
            "raw_records": len(workload),
            "retry_records": counts[ProcessingOutcome.RETRY_SCHEDULED],
            "total_processed_records": len(process_outcomes),
            "duration_seconds": round(elapsed, 6),
            "throughput_raw_records_per_second": round(len(workload) / elapsed, 3),
            "processing_latency_ms_p50": round(
                statistics.median(processing_latencies_ms), 3
            ),
            "processing_latency_ms_p95": round(
                self._percentile(processing_latencies_ms, 0.95), 3
            ),
            "consumer_lag_raw": raw_lag,
            "consumer_lag_retry": retry_lag,
            "duplicate_rate": round(
                counts[ProcessingOutcome.DUPLICATE] / len(workload), 6
            ),
            "retry_rate": round(
                counts[ProcessingOutcome.RETRY_SCHEDULED] / len(workload), 6
            ),
            "dlq_rate": round(counts[ProcessingOutcome.INVALID] / len(workload), 6),
        }
        print(f"DE04F_MEASUREMENTS={json.dumps(metrics, sort_keys=True)}")
        self.assertGreater(metrics["throughput_raw_records_per_second"], 0)
        self.assertGreaterEqual(metrics["processing_latency_ms_p50"], 0)
        self.assertEqual(raw_lag, 0)
        self.assertEqual(
            retry_lag,
            0,
            f"read_committed application records remain after committed offset "
            f"{final_retry_committed}; initial end={initial_retry_end}",
        )

    def _seed_processor_group(self, bootstrap: str, group_id: str) -> tuple[int, int]:
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
            raw_end = consumer.get_watermark_offsets(
                TopicPartition(DEFAULT_INPUT_TOPIC, 0), timeout=10
            )[1]
            retry_end = consumer.get_watermark_offsets(
                TopicPartition(DEFAULT_RETRY_TOPIC, 0), timeout=10
            )[1]
            offsets = [
                TopicPartition(DEFAULT_INPUT_TOPIC, 0, raw_end),
                TopicPartition(DEFAULT_RETRY_TOPIC, 0, retry_end),
            ]
            consumer.assign(offsets)
            consumer.commit(offsets=offsets, asynchronous=False)
            return raw_end, retry_end
        finally:
            consumer.close()

    @staticmethod
    def _high_offset(bootstrap: str, topic: str, partition: int) -> int:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-high-{uuid.uuid4()}",
                "enable.auto.commit": False,
            }
        )
        try:
            return consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )[1]
        finally:
            consumer.close()

    @staticmethod
    def _high_offsets(
        bootstrap: str, topic: str, partition_count: int
    ) -> dict[int, int]:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-high-{uuid.uuid4()}",
                "enable.auto.commit": False,
            }
        )
        try:
            return {
                partition: consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )[1]
                for partition in range(partition_count)
            }
        finally:
            consumer.close()

    @staticmethod
    def _read_committed_records(
        bootstrap: str, topic: str, starting_offsets: dict[int, int]
    ) -> list[dict]:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-read-committed-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "isolation.level": "read_committed",
                "enable.partition.eof": True,
            }
        )
        partitions = [
            TopicPartition(topic, partition, offset)
            for partition, offset in starting_offsets.items()
        ]
        records: list[dict] = []
        eof_partitions: set[int] = set()
        deadline = time.monotonic() + 15
        try:
            consumer.assign(partitions)
            while len(eof_partitions) < len(partitions):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out reading committed records from {topic}; "
                        f"EOF partitions={sorted(eof_partitions)}"
                    )
                message = consumer.poll(min(0.25, remaining))
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        eof_partitions.add(message.partition())
                        continue
                    raise AssertionError(
                        f"error reading committed records from {topic}: {error}"
                    )
                records.append(json.loads(message.value().decode("utf-8")))
            return records
        finally:
            consumer.close()

    @classmethod
    def _count_read_committed_records(
        cls, bootstrap: str, topic: str, partition: int, start_offset: int
    ) -> int:
        return len(
            cls._read_committed_records(
                bootstrap, topic, {partition: start_offset}
            )
        )

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * percentile)))
        return ordered[index]


@unittest.skipUnless(
    os.environ.get("ATMOSYNC_RUN_KAFKA_RESTART_TEST") == "1",
    "set ATMOSYNC_RUN_KAFKA_RESTART_TEST=1 to restart the local Kafka container",
)
class KafkaProducerRestartIntegrationTests(unittest.TestCase):
    def test_existing_producer_recovers_after_local_broker_restart(self):
        bootstrap = os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        start_offset = KafkaReliabilityIntegrationTests._high_offset(
            bootstrap, DEFAULT_INPUT_TOPIC, 0
        )
        first_event, second_event = SensorSimulator(seed=4402).generate_events(
            steps=2
        )[:2]
        first_event["event_id"] = str(uuid.uuid4())
        second_event["event_id"] = str(uuid.uuid4())
        producer = KafkaSensorProducer(bootstrap)
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"atmosync-producer-restart-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "isolation.level": "read_committed",
                "auto.offset.reset": "earliest",
            }
        )
        try:
            producer.publish(first_event)
            self.assertEqual(producer._producer.flush(20), 0)
            self.assertEqual(producer.delivered_count, 1)
            restarted = subprocess.run(
                ["docker", "restart", "atmosync-kafka"],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertIn("atmosync-kafka", restarted.stdout)
            self._wait_until_healthy()

            producer.publish(second_event)
            self.assertEqual(producer.close(30), 2)

            consumer.assign([TopicPartition(DEFAULT_INPUT_TOPIC, 0, start_offset)])
            expected_ids = {first_event["event_id"], second_event["event_id"]}
            observed_ids = set()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and observed_ids != expected_ids:
                message = consumer.poll(0.5)
                if message is None or message.error() is not None:
                    continue
                event = json.loads(message.value().decode("utf-8"))
                if event.get("event_id") in expected_ids:
                    observed_ids.add(event["event_id"])
            self.assertEqual(observed_ids, expected_ids)
        finally:
            if not producer._closed:
                producer.close(30)
            consumer.close()

    def _wait_until_healthy(self) -> None:
        deadline = time.monotonic() + 120
        last_status = "unknown"
        while time.monotonic() < deadline:
            result = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{.State.Health.Status}}",
                    "atmosync-kafka",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                last_status = result.stdout.strip()
                if last_status == "healthy":
                    return
            time.sleep(2)
        self.fail(f"Kafka did not become healthy after restart; status={last_status}")


if __name__ == "__main__":
    unittest.main()
