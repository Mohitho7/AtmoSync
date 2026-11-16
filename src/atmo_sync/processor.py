"""Transactional validation and event-ID deduplication for Kafka sensor events."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from .contracts import validate_sensor_event
from .retry import (
    DEFAULT_RETRY_TOPIC,
    MAX_RETRY_KAFKA_RECORD_BYTES,
    RetryEnvelope,
    RetryPolicy,
    TransientProcessingError,
    utc_timestamp,
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_INPUT_TOPIC = "sensor-events"
DEFAULT_VALIDATED_TOPIC = "sensor-events-validated"
DEFAULT_DLQ_TOPIC = "sensor-events-dlq"
DEFAULT_DEDUP_TOPIC = "sensor-event-dedup"
DEFAULT_GROUP_ID = "atmosync-validation-processor"
DEFAULT_TRANSACTIONAL_ID = "atmosync-validation-processor-1"
MAX_RAW_MESSAGE_BYTES = 1_048_588
MAX_DLQ_MESSAGE_BYTES = 4 * 1024 * 1024
MAX_DLQ_KAFKA_RECORD_BYTES = 6 * 1024 * 1024
DEFAULT_OPERATION_TIMEOUT = 15.0
DEFAULT_TRANSACTION_TIMEOUT = 30.0
DEFAULT_LEDGER_RESTORE_TIMEOUT = 30.0
DEFAULT_POLL_TIMEOUT = 1.0

LOGGER = logging.getLogger(__name__)
_UNPARSED = object()
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ProcessorError(RuntimeError):
    """Raised when a validation transaction cannot safely complete."""


class TransactionOutcomeUnknown(ProcessorError):
    """A transaction commit failed and its outcome must be recovered on restart."""


class ProcessingOutcome(str, Enum):
    VALIDATED = "validated"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    INVALID = "invalid"
    MALFORMED = "malformed"
    RETRY_SCHEDULED = "retry_scheduled"
    RETRY_EXHAUSTED = "retry_exhausted"


@dataclass(frozen=True)
class ProcessorConfig:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    input_topic: str = DEFAULT_INPUT_TOPIC
    validated_topic: str = DEFAULT_VALIDATED_TOPIC
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    dedup_topic: str = DEFAULT_DEDUP_TOPIC
    retry_topic: str = DEFAULT_RETRY_TOPIC
    group_id: str = DEFAULT_GROUP_ID
    max_retry_attempts: int = 3
    retry_base_delay_seconds: float = 1.0
    retry_max_delay_seconds: float = 30.0
    retry_jitter_fraction: float = 0.2
    operation_timeout: float = DEFAULT_OPERATION_TIMEOUT
    transaction_timeout: float = DEFAULT_TRANSACTION_TIMEOUT
    ledger_restore_timeout: float = DEFAULT_LEDGER_RESTORE_TIMEOUT
    poll_timeout: float = DEFAULT_POLL_TIMEOUT

    def __post_init__(self) -> None:
        for field_name in (
            "bootstrap_servers",
            "input_topic",
            "validated_topic",
            "dlq_topic",
            "dedup_topic",
            "retry_topic",
            "group_id",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        topic_names = (
            self.input_topic,
            self.validated_topic,
            self.dlq_topic,
            self.dedup_topic,
            self.retry_topic,
        )
        if len(set(topic_names)) != len(topic_names):
            raise ValueError(
                "input, validated, DLQ, dedup, and retry topics must be distinct"
            )
        RetryPolicy(
            max_attempts=self.max_retry_attempts,
            base_delay_seconds=self.retry_base_delay_seconds,
            max_delay_seconds=self.retry_max_delay_seconds,
            jitter_fraction=self.retry_jitter_fraction,
        )
        for field_name in (
            "operation_timeout",
            "transaction_timeout",
            "ledger_restore_timeout",
            "poll_timeout",
        ):
            _positive_timeout(getattr(self, field_name), field_name)


def _positive_timeout(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a positive finite number")
    try:
        timeout = float(value)
    except OverflowError as error:
        raise ValueError(f"{field_name} must be a positive finite number") from error
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError(f"{field_name} must be a positive finite number")
    return timeout


def payload_sha256(event: dict[str, Any]) -> str:
    """Return a deterministic SHA-256 hash of a complete valid sensor event."""
    validate_sensor_event(event)
    canonical_event = dict(event)
    canonical_event["event_id"] = _canonical_event_id(event["event_id"])
    canonical = json.dumps(
        canonical_event,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _canonical_event_id(event_id: str) -> str:
    return str(uuid.UUID(event_id))


def _parse_constant(value: str) -> None:
    raise ValueError(f"{value} is not a valid JSON number")


def _decode_ledger_record(
    ledger: dict[str, str], key: bytes | None, value: bytes | None
) -> None:
    if key is None:
        raise ProcessorError("deduplication ledger record has no key")
    try:
        event_id = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ProcessorError("deduplication ledger key is not UTF-8") from error
    if not event_id:
        raise ProcessorError("deduplication ledger key is empty")
    if value is None:
        ledger.pop(event_id, None)
        return

    try:
        marker = json.loads(value.decode("utf-8"), parse_constant=_parse_constant)
    except (UnicodeDecodeError, ValueError) as error:
        raise ProcessorError(
            f"deduplication ledger value for {event_id} is invalid JSON"
        ) from error
    if not isinstance(marker, dict) or marker.get("event_id") != event_id:
        raise ProcessorError(
            f"deduplication ledger value does not match key {event_id}"
        )
    if marker.get("schema_version") != "1.0":
        raise ProcessorError(
            f"deduplication ledger value for {event_id} has an unsupported schema_version"
        )
    payload_hash = marker.get("payload_sha256")
    if not isinstance(payload_hash, str) or not _SHA256_PATTERN.fullmatch(payload_hash):
        raise ProcessorError(
            f"deduplication ledger value for {event_id} has an invalid payload_sha256"
        )
    ledger[event_id] = payload_hash


def restore_deduplication_ledger(
    consumer_factory: Callable[[dict[str, Any]], Any],
    *,
    bootstrap_servers: str,
    group_id: str,
    topic: str = DEFAULT_DEDUP_TOPIC,
    timeout: float = DEFAULT_LEDGER_RESTORE_TIMEOUT,
) -> dict[str, str]:
    """Read committed ledger records through a captured exclusive end offset."""
    restore_timeout = _positive_timeout(timeout, "ledger_restore_timeout")
    consumer = consumer_factory(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"{group_id}-ledger-restore",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    ledger: dict[str, str] = {}
    partition = TopicPartition(topic, 0)
    try:
        watermarks = consumer.get_watermark_offsets(
            partition, timeout=restore_timeout, cached=False
        )
        if watermarks is None:
            raise ProcessorError("could not capture deduplication ledger end offset")
        low_offset, end_offset = watermarks
        if low_offset < 0 or end_offset < low_offset:
            raise ProcessorError("Kafka returned invalid deduplication ledger offsets")
        if end_offset == low_offset:
            return ledger

        consumer.assign([TopicPartition(topic, 0, low_offset)])
        deadline = time.monotonic() + restore_timeout
        last_record_offset = low_offset - 1
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProcessorError(
                    "timed out restoring deduplication ledger before captured "
                    f"end offset {end_offset}"
                )
            message = consumer.poll(min(0.25, remaining))
            if message is not None:
                error = message.error()
                if error is not None:
                    if error.code() != KafkaError._PARTITION_EOF:
                        raise ProcessorError(
                            f"error reading deduplication ledger: {error}"
                        )
                elif message.offset() < end_offset:
                    if message.offset() < last_record_offset:
                        raise ProcessorError(
                            "deduplication ledger records were not read in offset order"
                        )
                    last_record_offset = message.offset()
                    _decode_ledger_record(ledger, message.key(), message.value())

            position = consumer.position([partition])[0]
            if position.error is not None:
                raise ProcessorError(
                    f"could not read deduplication ledger position: {position.error}"
                )
            if position.offset >= end_offset:
                return ledger
    finally:
        consumer.close()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _validate_json_depth(value: Any, maximum: int = 1000) -> None:
    stack = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if isinstance(current, dict):
            children = current.values()
        elif isinstance(current, list):
            children = current
        else:
            continue
        child_depth = depth + 1
        if child_depth > maximum and len(children) > 0:
            raise ValueError(f"JSON nesting exceeds maximum depth of {maximum}")
        stack.extend((child, child_depth) for child in children)


def _event_id(event: Any) -> str | None:
    if isinstance(event, dict):
        value = event.get("event_id")
        if isinstance(value, str) and value:
            try:
                return _canonical_event_id(value)
            except ValueError:
                return value
    return None


class TransactionalValidationProcessor:
    """Consume raw events and atomically write results, ledger, and offsets."""

    def __init__(
        self,
        config: ProcessorConfig | None = None,
        *,
        producer_client: Any | None = None,
        consumer_client: Any | None = None,
        ledger_consumer_factory: Callable[[dict[str, Any]], Any] | None = None,
        admin_client: Any | None = None,
    ) -> None:
        self.config = config or ProcessorConfig()
        producer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "transactional.id": DEFAULT_TRANSACTIONAL_ID,
            "enable.idempotence": True,
            "acks": "all",
            "message.send.max.retries": 5,
            "retry.backoff.ms": 500,
            "message.timeout.ms": int(self.config.transaction_timeout * 1000),
            "transaction.timeout.ms": int(self.config.transaction_timeout * 1000),
            "queue.buffering.max.messages": 10000,
            "queue.buffering.max.kbytes": 10240,
            "message.max.bytes": MAX_DLQ_KAFKA_RECORD_BYTES,
        }
        consumer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "group.id": self.config.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "max.partition.fetch.bytes": 2 * MAX_RETRY_KAFKA_RECORD_BYTES,
            "fetch.max.bytes": 2 * MAX_RETRY_KAFKA_RECORD_BYTES,
        }
        self.producer = (
            Producer(producer_config)
            if producer_client is None
            else producer_client
        )
        self.consumer = (
            Consumer(consumer_config)
            if consumer_client is None
            else consumer_client
        )
        self._ledger_consumer_factory = (
            Consumer if ledger_consumer_factory is None else ledger_consumer_factory
        )
        self.admin_client = (
            AdminClient({"bootstrap.servers": self.config.bootstrap_servers})
            if admin_client is None
            else admin_client
        )
        self.ledger: dict[str, str] = {}
        self._initialized = False
        self._producer_initialized = False
        self._transaction_open = False
        self._commit_outcome_uncertain = False
        self._closed = False
        self._deferred_retry: tuple[Any, RetryEnvelope] | None = None
        self._retry_policy = RetryPolicy(
            max_attempts=self.config.max_retry_attempts,
            base_delay_seconds=self.config.retry_base_delay_seconds,
            max_delay_seconds=self.config.retry_max_delay_seconds,
            jitter_fraction=self.config.retry_jitter_fraction,
        )

    def _verify_topics(self) -> None:
        metadata = self.consumer.list_topics(timeout=self.config.operation_timeout)
        expected = {
            self.config.input_topic: 1,
            self.config.validated_topic: 6,
            self.config.dlq_topic: 3,
            self.config.dedup_topic: 1,
            self.config.retry_topic: 1,
        }
        for topic, partition_count in expected.items():
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None:
                raise ProcessorError(f"required Kafka topic does not exist: {topic}")
            if topic_metadata.error is not None:
                raise ProcessorError(
                    f"could not read metadata for topic {topic}: {topic_metadata.error}"
                )
            actual_count = len(topic_metadata.partitions)
            if actual_count != partition_count:
                raise ProcessorError(
                    f"topic {topic} has {actual_count} partitions; "
                    f"expected {partition_count}"
                )

        configured_topics = (
            self.config.input_topic,
            self.config.dlq_topic,
            self.config.dedup_topic,
            self.config.retry_topic,
        )
        resources = {
            topic: ConfigResource(ResourceType.TOPIC, topic)
            for topic in configured_topics
        }
        config_futures = self.admin_client.describe_configs(list(resources.values()))
        topic_configs = {}
        for topic, resource in resources.items():
            try:
                topic_configs[topic] = config_futures[resource].result(
                    timeout=self.config.operation_timeout
                )
            except (KeyError, KafkaException, TimeoutError) as error:
                raise ProcessorError(
                    f"could not read configuration for topic {topic}: {error}"
                ) from error

        def config_value(topic: str, key: str) -> str:
            entry = topic_configs[topic].get(key)
            if entry is None or entry.value is None:
                raise ProcessorError(f"topic {topic} has no {key} configuration")
            return str(entry.value)

        try:
            raw_max_bytes = int(
                config_value(self.config.input_topic, "max.message.bytes")
            )
            dlq_max_bytes = int(
                config_value(self.config.dlq_topic, "max.message.bytes")
            )
            retry_max_bytes = int(
                config_value(self.config.retry_topic, "max.message.bytes")
            )
        except (TypeError, ValueError) as error:
            raise ProcessorError(f"invalid Kafka message-size configuration: {error}") from error
        if raw_max_bytes > MAX_RAW_MESSAGE_BYTES:
            raise ProcessorError(
                f"topic {self.config.input_topic} max.message.bytes is "
                f"{raw_max_bytes}; expected at most {MAX_RAW_MESSAGE_BYTES}"
            )
        if dlq_max_bytes < MAX_DLQ_KAFKA_RECORD_BYTES:
            raise ProcessorError(
                f"topic {self.config.dlq_topic} max.message.bytes is "
                f"{dlq_max_bytes}; expected at least "
                f"{MAX_DLQ_KAFKA_RECORD_BYTES}"
            )
        if retry_max_bytes < MAX_RETRY_KAFKA_RECORD_BYTES:
            raise ProcessorError(
                f"topic {self.config.retry_topic} max.message.bytes is "
                f"{retry_max_bytes}; expected at least "
                f"{MAX_RETRY_KAFKA_RECORD_BYTES}"
            )

        cleanup_policy = config_value(self.config.dedup_topic, "cleanup.policy")
        if "compact" not in {
            policy.strip() for policy in cleanup_policy.split(",")
        }:
            raise ProcessorError(
                f"topic {self.config.dedup_topic} cleanup.policy must include compact"
            )
        retry_cleanup_policy = config_value(
            self.config.retry_topic, "cleanup.policy"
        )
        retry_policies = {
            policy.strip() for policy in retry_cleanup_policy.split(",")
        }
        if "delete" not in retry_policies or "compact" in retry_policies:
            raise ProcessorError(
                f"topic {self.config.retry_topic} cleanup.policy must be delete-only"
            )

    def initialize(self) -> None:
        """Initialize transactions, restore the ledger, and subscribe to work topics."""
        if self._initialized:
            return
        if self._closed:
            raise ProcessorError("processor is closed")
        self.producer.init_transactions(self.config.transaction_timeout)
        self._producer_initialized = True
        self._verify_topics()
        self.ledger = restore_deduplication_ledger(
            self._ledger_consumer_factory,
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            topic=self.config.dedup_topic,
            timeout=self.config.ledger_restore_timeout,
        )
        self.consumer.subscribe([self.config.input_topic, self.config.retry_topic])
        self._initialized = True
        LOGGER.info(
            "Initialized transactional processor; restored %d ledger entries",
            len(self.ledger),
        )

    def _abort_transaction(self) -> None:
        try:
            self.producer.abort_transaction(self.config.transaction_timeout)
        except KafkaException as error:
            self._commit_outcome_uncertain = True
            raise ProcessorError(
                "Kafka transaction abort failed; stop and restart the processor "
                f"to recover committed state: {error}"
            ) from error
        self._transaction_open = False

    @staticmethod
    def _kafka_error_flag(error: KafkaException, flag_name: str) -> bool:
        kafka_error = error.args[0] if error.args else None
        flag = getattr(kafka_error, flag_name, None)
        return bool(flag()) if callable(flag) else False

    def _commit_input(
        self,
        message: Any,
        outputs: list[tuple[str, bytes | None, bytes]],
        ledger_update: tuple[str, str] | None = None,
    ) -> None:
        try:
            self.producer.begin_transaction()
        except KafkaException as error:
            if self._kafka_error_flag(error, "fatal"):
                raise ProcessorError(
                    f"fatal or fenced Kafka transactional producer: {error}"
                ) from error
            raise ProcessorError(f"could not begin Kafka transaction: {error}") from error
        self._transaction_open = True

        try:
            for topic, key, value in outputs:
                self.producer.produce(topic, key=key, value=value)
            offsets = [
                TopicPartition(
                    message.topic(),
                    message.partition(),
                    message.offset() + 1,
                )
            ]
            self.producer.send_offsets_to_transaction(
                offsets,
                self.consumer.consumer_group_metadata(),
                self.config.transaction_timeout,
            )
        except BaseException as error:
            try:
                self._abort_transaction()
            except ProcessorError as abort_error:
                raise ProcessorError(
                    f"processing failed before commit ({error}); {abort_error}"
                ) from abort_error
            if isinstance(error, BufferError):
                raise ProcessorError(
                    "Kafka transactional producer queue is full; transaction was "
                    "aborted and the input offset was not committed"
                ) from error
            raise

        try:
            self.producer.commit_transaction(self.config.transaction_timeout)
        except KafkaException as error:
            if self._kafka_error_flag(error, "txn_requires_abort"):
                self._abort_transaction()
                raise ProcessorError(
                    "Kafka rejected the transaction; it was aborted and the input "
                    f"offset was not committed: {error}"
                ) from error
            self._commit_outcome_uncertain = True
            if self._kafka_error_flag(error, "fatal"):
                detail = "fatal or fenced transactional producer error"
            else:
                detail = "transaction commit outcome is uncertain"
            raise TransactionOutcomeUnknown(
                f"{detail}; stop and restart to recover committed offsets and "
                f"deduplication state: {error}"
            ) from error
        except BaseException as error:
            self._commit_outcome_uncertain = True
            raise TransactionOutcomeUnknown(
                "transaction commit outcome is uncertain; stop and restart to recover "
                "committed offsets and deduplication state"
            ) from error

        self._transaction_open = False
        if ledger_update is not None:
            event_id, payload_hash = ledger_update
            self.ledger[event_id] = payload_hash

    def _dlq_output(
        self,
        message: Any,
        error_code: str,
        error_message: str,
        raw_value: bytes | None,
        parsed_event: Any = _UNPARSED,
        *,
        source_context: RetryEnvelope | None = None,
        retry_metadata: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> list[tuple[str, bytes | None, bytes]]:
        source_topic = (
            message.topic() if source_context is None else source_context.source_topic
        )
        source_partition = (
            message.partition()
            if source_context is None
            else source_context.source_partition
        )
        source_offset = (
            message.offset() if source_context is None else source_context.source_offset
        )
        envelope: dict[str, Any] = {
            "error_code": error_code,
            "error_message": error_message,
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "raw_payload_base64": (
                None
                if raw_value is None
                else base64.b64encode(raw_value).decode("ascii")
            ),
        }
        if parsed_event is not _UNPARSED:
            envelope["original_event"] = parsed_event
        if retry_metadata is not None:
            envelope["retry_metadata"] = retry_metadata
        if extra:
            envelope.update(extra)
        event_id = _event_id(parsed_event)
        if event_id is not None:
            key = event_id.encode("utf-8")
        elif source_context is not None:
            key = source_context.source_key
        else:
            key = message.key()
        encoded_envelope = _json_bytes(envelope)
        if len(encoded_envelope) > MAX_DLQ_MESSAGE_BYTES:
            raise ProcessorError(
                f"serialized DLQ record is {len(encoded_envelope)} bytes; "
                f"maximum is {MAX_DLQ_MESSAGE_BYTES}"
            )
        return [(self.config.dlq_topic, key, encoded_envelope)]

    def _retry_metadata(
        self,
        error: Exception,
        failure_class: str,
        source_context: RetryEnvelope | None,
        *,
        attempt_number: int,
        first_failure_at: str | None = None,
        retry_delay_seconds: float | None = None,
        next_attempt_at: str | None = None,
    ) -> dict[str, Any]:
        now = utc_timestamp()
        return {
            "attempt_number": attempt_number,
            "max_retry_attempts": self.config.max_retry_attempts,
            "failure_class": failure_class,
            "first_failure_at": (
                source_context.first_failure_at
                if source_context is not None
                else (first_failure_at or now)
            ),
            "last_failure_at": now,
            "failure_reason": str(error),
            "attempt_history": self._attempt_history(
                error,
                source_context,
                attempt_number,
                failed_at=now,
                retry_delay_seconds=retry_delay_seconds,
                next_attempt_at=next_attempt_at,
            ),
        }

    @staticmethod
    def _attempt_history(
        error: Exception,
        source_context: RetryEnvelope | None,
        attempt_number: int,
        *,
        failed_at: str,
        retry_delay_seconds: float | None = None,
        next_attempt_at: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        history = (
            []
            if source_context is None
            else [dict(entry) for entry in source_context.retry_history]
        )
        history.append(
            {
                "attempt_number": attempt_number,
                "failed_at": failed_at,
                "failure_reason": str(error),
                "retry_delay_seconds": retry_delay_seconds,
                "next_attempt_at": next_attempt_at,
            }
        )
        return tuple(history)

    def _commit_dlq(
        self,
        message: Any,
        error_code: str,
        error_message: str,
        raw_value: bytes | None,
        parsed_event: Any = _UNPARSED,
        *,
        source_context: RetryEnvelope | None = None,
        retry_metadata: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        outputs = self._dlq_output(
            message,
            error_code,
            error_message,
            raw_value,
            parsed_event,
            source_context=source_context,
            retry_metadata=retry_metadata,
            extra=extra,
        )
        self._commit_input(message, outputs)
        LOGGER.warning(
            "dlq_routed",
            extra={
                "error_code": error_code,
                "source_topic": (
                    message.topic()
                    if source_context is None
                    else source_context.source_topic
                ),
                "source_partition": (
                    message.partition()
                    if source_context is None
                    else source_context.source_partition
                ),
                "source_offset": (
                    message.offset()
                    if source_context is None
                    else source_context.source_offset
                ),
                "event_id": _event_id(parsed_event),
                "attempt_number": (
                    0
                    if retry_metadata is None
                    else retry_metadata.get("attempt_number", 0)
                ),
            },
        )

    def _validate_for_processing(self, event: dict[str, Any]) -> None:
        validate_sensor_event(event)

    def _handle_transient_failure(
        self,
        message: Any,
        raw_value: bytes,
        event: dict[str, Any],
        source_context: RetryEnvelope | None,
        error: TransientProcessingError,
    ) -> ProcessingOutcome:
        current_attempt = (
            0 if source_context is None else source_context.attempt_number
        )
        first_failure_at = (
            utc_timestamp()
            if source_context is None
            else source_context.first_failure_at
        )
        if current_attempt >= self.config.max_retry_attempts:
            metadata = self._retry_metadata(
                error,
                "transient_exhausted",
                source_context,
                attempt_number=current_attempt,
                first_failure_at=first_failure_at,
            )
            self._commit_dlq(
                message,
                "RETRY_EXHAUSTED",
                str(error),
                raw_value,
                event,
                source_context=source_context,
                retry_metadata=metadata,
            )
            return ProcessingOutcome.RETRY_EXHAUSTED

        next_attempt = current_attempt + 1
        delay = self._retry_policy.delay_for_attempt(next_attempt)
        now = datetime.now(timezone.utc)
        next_attempt_at = utc_timestamp(now + timedelta(seconds=delay))
        failed_at = utc_timestamp(now)
        retry = RetryEnvelope(
            event_id=_event_id(event),
            source_topic=(
                message.topic()
                if source_context is None
                else source_context.source_topic
            ),
            source_partition=(
                message.partition()
                if source_context is None
                else source_context.source_partition
            ),
            source_offset=(
                message.offset()
                if source_context is None
                else source_context.source_offset
            ),
            source_key=message.key() if source_context is None else source_context.source_key,
            raw_payload=raw_value,
            attempt_number=next_attempt,
            first_failure_at=first_failure_at,
            last_failure_at=failed_at,
            failure_reason=str(error),
            next_attempt_at=next_attempt_at,
            retry_history=self._attempt_history(
                error,
                source_context,
                current_attempt,
                failed_at=failed_at,
                retry_delay_seconds=delay,
                next_attempt_at=next_attempt_at,
            ),
        )
        encoded_retry = retry.to_bytes()
        if len(encoded_retry) > MAX_RETRY_KAFKA_RECORD_BYTES:
            metadata = self._retry_metadata(
                error,
                "transient_retry_envelope_too_large",
                source_context,
                attempt_number=current_attempt,
                first_failure_at=first_failure_at,
            )
            self._commit_dlq(
                message,
                "RETRY_ENVELOPE_TOO_LARGE",
                "serialized retry record exceeds the configured Kafka record limit",
                raw_value,
                event,
                source_context=source_context,
                retry_metadata=metadata,
            )
            return ProcessingOutcome.RETRY_EXHAUSTED

        event_key = (
            retry.event_id.encode("utf-8")
            if retry.event_id is not None
            else retry.source_key
        )
        self._commit_input(
            message,
            [(self.config.retry_topic, event_key, encoded_retry)],
        )
        LOGGER.warning(
            "retry_scheduled",
            extra={
                "event_id": retry.event_id,
                "source_topic": retry.source_topic,
                "source_partition": retry.source_partition,
                "source_offset": retry.source_offset,
                "attempt_number": retry.attempt_number,
                "max_retry_attempts": self.config.max_retry_attempts,
                "delay_seconds": delay,
                "next_attempt_at": retry.next_attempt_at,
                "failure_reason": retry.failure_reason,
            },
        )
        return ProcessingOutcome.RETRY_SCHEDULED

    def process_message(self, message: Any) -> ProcessingOutcome:
        """Process one consumed record in a Kafka transaction."""
        if not self._initialized:
            raise ProcessorError("processor must be initialized before processing")

        source_context: RetryEnvelope | None = None
        raw_value = message.value()
        if message.topic() == self.config.retry_topic:
            try:
                if raw_value is None:
                    raise ValueError("retry record value is null")
                source_context = RetryEnvelope.from_bytes(raw_value)
                if source_context.attempt_number > self.config.max_retry_attempts:
                    raise ValueError(
                        "retry record attempt_number exceeds max_retry_attempts"
                    )
            except (ValueError, RecursionError) as error:
                self._commit_dlq(
                    message,
                    "INVALID_RETRY_ENVELOPE",
                    str(error),
                    raw_value,
                    retry_metadata={
                        "attempt_number": 0,
                        "max_retry_attempts": self.config.max_retry_attempts,
                        "failure_class": "permanent",
                        "first_failure_at": utc_timestamp(),
                        "last_failure_at": utc_timestamp(),
                        "failure_reason": str(error),
                    },
                )
                return ProcessingOutcome.MALFORMED
            raw_value = source_context.raw_payload

        return self._process_payload(message, raw_value, source_context)

    def _process_payload(
        self,
        message: Any,
        raw_value: bytes | None,
        source_context: RetryEnvelope | None,
    ) -> ProcessingOutcome:
        try:
            if raw_value is None:
                raise ValueError("record value is null")
            event = json.loads(
                raw_value.decode("utf-8"),
                parse_constant=_parse_constant,
            )
            _validate_json_depth(event)
            _json_bytes(event)
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValueError,
            RecursionError,
        ) as error:
            self._commit_dlq(
                message,
                "MALFORMED_JSON",
                str(error),
                raw_value,
                source_context=source_context,
                retry_metadata=self._retry_metadata(
                    error,
                    "permanent",
                    source_context,
                    attempt_number=(
                        0 if source_context is None else source_context.attempt_number
                    ),
                ),
            )
            return ProcessingOutcome.MALFORMED

        if not isinstance(event, dict):
            self._commit_dlq(
                message,
                "JSON_OBJECT_REQUIRED",
                "sensor event JSON must be a JSON object",
                raw_value,
                event,
                source_context=source_context,
                retry_metadata=self._retry_metadata(
                    ValueError("sensor event JSON must be a JSON object"),
                    "permanent",
                    source_context,
                    attempt_number=(
                        0 if source_context is None else source_context.attempt_number
                    ),
                ),
            )
            return ProcessingOutcome.INVALID

        try:
            self._validate_for_processing(event)
        except TransientProcessingError as error:
            return self._handle_transient_failure(
                message,
                raw_value if raw_value is not None else b"",
                event,
                source_context,
                error,
            )
        except ValueError as error:
            self._commit_dlq(
                message,
                "INVALID_SENSOR_EVENT",
                str(error),
                raw_value,
                event,
                source_context=source_context,
                retry_metadata=self._retry_metadata(
                    error,
                    "permanent",
                    source_context,
                    attempt_number=(
                        0 if source_context is None else source_context.attempt_number
                    ),
                ),
            )
            return ProcessingOutcome.INVALID

        event_id = _canonical_event_id(event["event_id"])
        event_hash = payload_sha256(event)
        known_hash = self.ledger.get(event_id)
        if known_hash == event_hash:
            self._commit_input(message, [])
            LOGGER.info("Suppressed duplicate sensor event %s", event_id)
            return ProcessingOutcome.DUPLICATE
        if known_hash is not None:
            conflict = ValueError(
                "event_id was previously committed with a different payload"
            )
            self._commit_dlq(
                message,
                "EVENT_ID_PAYLOAD_CONFLICT",
                str(conflict),
                raw_value,
                event,
                source_context=source_context,
                retry_metadata=self._retry_metadata(
                    conflict,
                    "permanent",
                    source_context,
                    attempt_number=(
                        0 if source_context is None else source_context.attempt_number
                    ),
                ),
                extra={
                    "existing_payload_sha256": known_hash,
                    "incoming_payload_sha256": event_hash,
                },
            )
            return ProcessingOutcome.CONFLICT

        validated_value = _json_bytes(event)
        ledger_marker = _json_bytes(
            {
                "schema_version": "1.0",
                "event_id": _canonical_event_id(event_id),
                "payload_sha256": event_hash,
            }
        )
        outputs = [
            (
                self.config.validated_topic,
                event["container_id"].encode("utf-8"),
                validated_value,
            ),
            (
                self.config.dedup_topic,
                event_id.encode("utf-8"),
                ledger_marker,
            ),
        ]
        self._commit_input(message, outputs, (event_id, event_hash))
        LOGGER.info("Validated and committed sensor event %s", event_id)
        return ProcessingOutcome.VALIDATED

    def process_one(self, timeout: float | None = None) -> ProcessingOutcome | None:
        if not self._initialized:
            self.initialize()
        poll_timeout = (
            self.config.poll_timeout
            if timeout is None
            else _positive_timeout(timeout, "poll_timeout")
        )
        if self._deferred_retry is not None:
            retry_message, retry_context = self._deferred_retry
            if time.time() >= retry_context.next_attempt_epoch:
                self.consumer.resume(
                    [
                        TopicPartition(
                            self.config.retry_topic, retry_message.partition()
                        )
                    ]
                )
                self._deferred_retry = None
                return self.process_message(retry_message)

        message = self.consumer.poll(poll_timeout)
        if message is None:
            return None
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            raise ProcessorError(f"Kafka input consumer error: {error}")
        if message.topic() == self.config.retry_topic:
            try:
                value = message.value()
                if value is None:
                    raise ValueError("retry record value is null")
                retry_context = RetryEnvelope.from_bytes(value)
                if retry_context.attempt_number > self.config.max_retry_attempts:
                    raise ValueError(
                        "retry record attempt_number exceeds max_retry_attempts"
                    )
            except (ValueError, RecursionError):
                return self.process_message(message)
            if time.time() < retry_context.next_attempt_epoch:
                self.consumer.pause(
                    [TopicPartition(self.config.retry_topic, message.partition())]
                )
                self._deferred_retry = (message, retry_context)
                return None
        return self.process_message(message)

    def run(self, max_messages: int | None = None) -> int:
        """Continuously process raw input; optionally stop after N records."""
        if max_messages is not None and (
            isinstance(max_messages, bool)
            or not isinstance(max_messages, int)
            or max_messages < 0
        ):
            raise ValueError("max_messages must be a non-negative integer")
        if not self._initialized:
            self.initialize()
        processed = 0
        while max_messages is None or processed < max_messages:
            outcome = self.process_one()
            if outcome is not None:
                processed += 1
        return processed

    def close(self) -> None:
        """Close without committing offsets outside their Kafka transactions."""
        if self._closed:
            return
        self._closed = True
        shutdown_error: Exception | None = None
        if self._transaction_open and not self._commit_outcome_uncertain:
            try:
                self._abort_transaction()
            except ProcessorError as error:
                shutdown_error = error
        if (
            self._producer_initialized
            and not self._transaction_open
            and not self._commit_outcome_uncertain
        ):
            try:
                remaining = self.producer.flush(self.config.operation_timeout)
                if remaining:
                    shutdown_error = ProcessorError(
                        f"{remaining} Kafka producer message(s) remained undelivered "
                        "during shutdown"
                    )
            except KafkaException as error:
                shutdown_error = ProcessorError(
                    f"Kafka producer flush failed during shutdown: {error}"
                )
        try:
            self.consumer.close()
        except (KafkaException, RuntimeError) as error:
            if shutdown_error is None:
                shutdown_error = ProcessorError(
                    f"Kafka consumer close failed: {error}"
                )
        if shutdown_error is not None:
            raise shutdown_error


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate, deduplicate, and transactionally route Kafka sensor events."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
        ),
    )
    parser.add_argument(
        "--input-topic",
        default=os.environ.get("ATMOSYNC_KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
    )
    parser.add_argument(
        "--validated-topic",
        default=os.environ.get(
            "ATMOSYNC_KAFKA_VALIDATED_TOPIC", DEFAULT_VALIDATED_TOPIC
        ),
    )
    parser.add_argument(
        "--dlq-topic",
        default=os.environ.get("ATMOSYNC_KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
    )
    parser.add_argument(
        "--dedup-topic",
        default=os.environ.get("ATMOSYNC_KAFKA_DEDUP_TOPIC", DEFAULT_DEDUP_TOPIC),
    )
    parser.add_argument(
        "--retry-topic",
        default=os.environ.get("ATMOSYNC_KAFKA_RETRY_TOPIC", DEFAULT_RETRY_TOPIC),
    )
    parser.add_argument(
        "--max-retry-attempts",
        type=int,
        default=os.environ.get("ATMOSYNC_MAX_RETRY_ATTEMPTS", "3"),
    )
    parser.add_argument(
        "--retry-base-delay",
        type=float,
        default=os.environ.get("ATMOSYNC_RETRY_BASE_DELAY_SECONDS", "1.0"),
    )
    parser.add_argument(
        "--retry-max-delay",
        type=float,
        default=os.environ.get("ATMOSYNC_RETRY_MAX_DELAY_SECONDS", "30.0"),
    )
    parser.add_argument(
        "--retry-jitter",
        type=float,
        default=os.environ.get("ATMOSYNC_RETRY_JITTER_FRACTION", "0.2"),
    )
    parser.add_argument(
        "--group-id",
        default=os.environ.get("ATMOSYNC_KAFKA_GROUP_ID", DEFAULT_GROUP_ID),
    )
    args = parser.parse_args(argv)
    try:
        ProcessorConfig(
            bootstrap_servers=args.bootstrap_servers,
            input_topic=args.input_topic,
            validated_topic=args.validated_topic,
            dlq_topic=args.dlq_topic,
            dedup_topic=args.dedup_topic,
            retry_topic=args.retry_topic,
            group_id=args.group_id,
            max_retry_attempts=args.max_retry_attempts,
            retry_base_delay_seconds=args.retry_base_delay,
            retry_max_delay_seconds=args.retry_max_delay,
            retry_jitter_fraction=args.retry_jitter,
        )
    except ValueError as error:
        parser.error(str(error))
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the transactional processor until interrupted or a fatal error."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    args = _parse_args(argv)
    config = ProcessorConfig(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        validated_topic=args.validated_topic,
        dlq_topic=args.dlq_topic,
        dedup_topic=args.dedup_topic,
        retry_topic=args.retry_topic,
        group_id=args.group_id,
        max_retry_attempts=args.max_retry_attempts,
        retry_base_delay_seconds=args.retry_base_delay,
        retry_max_delay_seconds=args.retry_max_delay,
        retry_jitter_fraction=args.retry_jitter,
    )
    processor: TransactionalValidationProcessor | None = None
    exit_code = 0
    try:
        processor = TransactionalValidationProcessor(config)
        processor.initialize()
        LOGGER.info(
            "Processing %s with transactional ID %s",
            config.input_topic,
            DEFAULT_TRANSACTIONAL_ID,
        )
        processor.run()
    except KeyboardInterrupt:
        LOGGER.info("Interrupted; stopping without an out-of-transaction offset commit.")
    except (KafkaException, ProcessorError, ValueError) as error:
        LOGGER.error("Validation processor stopped: %s", error)
        exit_code = 1
    finally:
        if processor is not None:
            try:
                processor.close()
            except ProcessorError as error:
                LOGGER.error("Processor shutdown error: %s", error)
                exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
