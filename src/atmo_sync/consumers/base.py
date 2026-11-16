"""Shared configuration and safe manual-offset handling for Kafka consumers."""

from __future__ import annotations

import json
import logging
import math
import os
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..contracts import validate_sensor_event

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_VALIDATED_TOPIC = "sensor-events-validated"
DEFAULT_VALIDATED_GROUP_ID = "atmosync-validated-consumer"
DEFAULT_ANALYTICS_GROUP_ID = "atmosync-analytics-consumer"
DEFAULT_MONITORING_GROUP_ID = "atmosync-monitoring-consumer"
MAX_RECENT_EVENT_IDS = 10_000
MAX_VALIDATED_KAFKA_RECORD_BYTES = 2 * 1024 * 1024

LOGGER = logging.getLogger(__name__)


class ConsumerError(RuntimeError):
    """Raised when a validated event cannot safely be handled or committed."""


class ConsumerOutcome(str, Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


def _positive_timeout(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("poll_timeout must be a positive finite number")
    try:
        timeout = float(value)
    except OverflowError as error:
        raise ValueError("poll_timeout must be a positive finite number") from error
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("poll_timeout must be a positive finite number")
    return timeout


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    group_id: str = DEFAULT_VALIDATED_GROUP_ID
    topic: str = DEFAULT_VALIDATED_TOPIC
    poll_timeout: float = 1.0
    auto_offset_reset: str = "earliest"

    def __post_init__(self) -> None:
        for field in ("bootstrap_servers", "group_id", "topic"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a non-empty string")
        if self.auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError(
                "auto_offset_reset must be earliest, latest, or error"
            )
        _positive_timeout(self.poll_timeout)

    def kafka_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": self.auto_offset_reset,
            "isolation.level": "read_committed",
            "max.partition.fetch.bytes": 2 * MAX_VALIDATED_KAFKA_RECORD_BYTES,
            "fetch.max.bytes": 4 * MAX_VALIDATED_KAFKA_RECORD_BYTES,
        }


class KafkaEventConsumer:
    """Process validated sensor events and commit each offset after handling."""

    def __init__(
        self,
        config: ConsumerConfig,
        *,
        event_handler: Callable[[dict[str, Any]], None],
        consumer_client: Any | None = None,
    ) -> None:
        self.config = config
        self._event_handler = event_handler
        self.consumer = (
            Consumer(config.kafka_config())
            if consumer_client is None
            else consumer_client
        )
        self.consumer.subscribe([config.topic])
        self._recent_event_ids: OrderedDict[str, None] = OrderedDict()
        self._closed = False

    @staticmethod
    def _reject_json_constant(value: str) -> None:
        raise ValueError(f"{value} is not a valid JSON number")

    def _decode_event(self, message: Any) -> dict[str, Any]:
        value = message.value()
        if value is None:
            raise ConsumerError("validated event record has a null value")
        try:
            event = json.loads(
                value.decode("utf-8"),
                parse_constant=self._reject_json_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ConsumerError(
                f"validated event record is not valid UTF-8 JSON: {error}"
            ) from error
        if not isinstance(event, dict):
            raise ConsumerError("validated event record must contain a JSON object")
        try:
            validate_sensor_event(event)
        except ValueError as error:
            raise ConsumerError(
                f"validated event failed DE-02 validation: {error}"
            ) from error
        return event

    def _remember_event_id(self, event_id: str) -> None:
        self._recent_event_ids[event_id] = None
        self._recent_event_ids.move_to_end(event_id)
        if len(self._recent_event_ids) > MAX_RECENT_EVENT_IDS:
            self._recent_event_ids.popitem(last=False)

    @staticmethod
    def _raise_commit_errors(result: Any) -> None:
        if result is None:
            return
        for partition in result:
            error = getattr(partition, "error", None)
            if error is not None:
                raise ConsumerError(f"Kafka offset commit failed: {error}")

    def process_one(self, timeout: float | None = None) -> ConsumerOutcome | None:
        if self._closed:
            raise ConsumerError("consumer is closed")
        poll_timeout = (
            self.config.poll_timeout if timeout is None else _positive_timeout(timeout)
        )
        message = self.consumer.poll(poll_timeout)
        if message is None:
            return None
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            raise ConsumerError(f"Kafka event consumer error: {error}")

        event = self._decode_event(message)
        event_id = event["event_id"]
        duplicate = event_id in self._recent_event_ids
        if duplicate:
            self._recent_event_ids.move_to_end(event_id)
            LOGGER.warning(
                "consumer_duplicate_suppressed",
                extra={
                    "consumer_group": self.config.group_id,
                    "event_id": event_id,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                },
            )
        else:
            self._event_handler(event)
            self._remember_event_id(event_id)

        try:
            result = self.consumer.commit(message=message, asynchronous=False)
        except KafkaException as error:
            raise ConsumerError(
                f"could not commit {message.topic()} partition "
                f"{message.partition()} offset {message.offset()}: {error}"
            ) from error
        self._raise_commit_errors(result)
        LOGGER.info(
            "consumer_offset_committed",
            extra={
                "consumer_group": self.config.group_id,
                "event_id": event_id,
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "duplicate": duplicate,
            },
        )
        return ConsumerOutcome.DUPLICATE if duplicate else ConsumerOutcome.PROCESSED

    def run(self, max_messages: int | None = None) -> int:
        if max_messages is not None and (
            isinstance(max_messages, bool)
            or not isinstance(max_messages, int)
            or max_messages < 0
        ):
            raise ValueError("max_messages must be a non-negative integer")
        processed = 0
        while max_messages is None or processed < max_messages:
            outcome = self.process_one()
            if outcome is not None:
                processed += 1
        return processed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.consumer.close()
        except (KafkaException, RuntimeError) as error:
            raise ConsumerError(f"Kafka consumer close failed: {error}") from error
