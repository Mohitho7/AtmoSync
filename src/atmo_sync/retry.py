"""Bounded retry policy and serialized retry records for Kafka processing."""

from __future__ import annotations

import base64
import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

DEFAULT_RETRY_TOPIC = "sensor-events-retry"
MAX_RETRY_KAFKA_RECORD_BYTES = 4 * 1024 * 1024


class TransientProcessingError(RuntimeError):
    """Raised when an event may succeed if processing is retried."""


def _utc_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be a UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"{field} must be a UTC timestamp")
    return parsed.astimezone(timezone.utc)


def utc_timestamp(value: datetime | None = None) -> str:
    timestamp = datetime.now(timezone.utc) if value is None else value
    return (
        timestamp.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    jitter_fraction: float = 0.2

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 0
        ):
            raise ValueError("max_retry_attempts must be a non-negative integer")
        for field, value, allow_zero in (
            ("retry_base_delay_seconds", self.base_delay_seconds, True),
            ("retry_max_delay_seconds", self.max_delay_seconds, False),
            ("retry_jitter_fraction", self.jitter_fraction, True),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{field} must be a finite number")
            try:
                finite_value = math.isfinite(float(value))
            except OverflowError:
                finite_value = False
            if not finite_value or value < 0 or (
                not allow_zero and value == 0
            ):
                raise ValueError(f"{field} must be a finite non-negative number")
        if self.base_delay_seconds > self.max_delay_seconds:
            raise ValueError(
                "retry_base_delay_seconds must not exceed retry_max_delay_seconds"
            )
        if self.jitter_fraction > 1:
            raise ValueError("retry_jitter_fraction must be between 0 and 1")

    def delay_for_attempt(
        self,
        attempt_number: int,
        *,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ) -> float:
        if (
            isinstance(attempt_number, bool)
            or not isinstance(attempt_number, int)
            or attempt_number < 1
        ):
            raise ValueError("attempt_number must be a positive integer")
        if self.base_delay_seconds == 0:
            nominal = 0.0
        else:
            max_exponent = math.log2(self.max_delay_seconds) - math.log2(
                self.base_delay_seconds
            )
            if attempt_number - 1 >= max_exponent:
                nominal = self.max_delay_seconds
            else:
                nominal = self.base_delay_seconds * (2 ** (attempt_number - 1))
        jitter = self.jitter_fraction
        return min(
            self.max_delay_seconds,
            nominal * random_uniform(1.0 - jitter, 1.0 + jitter),
        )


@dataclass(frozen=True)
class RetryEnvelope:
    event_id: str | None
    source_topic: str
    source_partition: int
    source_offset: int
    source_key: bytes | None
    raw_payload: bytes
    attempt_number: int
    first_failure_at: str
    last_failure_at: str
    failure_reason: str
    next_attempt_at: str
    retry_history: tuple[dict[str, Any], ...] = ()

    def to_bytes(self) -> bytes:
        payload = {
            "schema_version": "1.0",
            "event_id": self.event_id,
            "source_topic": self.source_topic,
            "source_partition": self.source_partition,
            "source_offset": self.source_offset,
            "source_key_base64": (
                None
                if self.source_key is None
                else base64.b64encode(self.source_key).decode("ascii")
            ),
            "raw_payload_base64": base64.b64encode(self.raw_payload).decode("ascii"),
            "attempt_number": self.attempt_number,
            "first_failure_at": self.first_failure_at,
            "last_failure_at": self.last_failure_at,
            "failure_reason": self.failure_reason,
            "next_attempt_at": self.next_attempt_at,
            "retry_history": list(self.retry_history),
        }
        return json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, payload: bytes) -> "RetryEnvelope":
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("retry record is not valid UTF-8 JSON") from error
        if not isinstance(value, dict) or value.get("schema_version") != "1.0":
            raise ValueError("retry record has an unsupported schema")

        def required_text(field: str) -> str:
            field_value = value.get(field)
            if not isinstance(field_value, str) or not field_value:
                raise ValueError(f"retry record {field} must be a non-empty string")
            return field_value

        def required_offset(field: str) -> int:
            field_value = value.get(field)
            if (
                isinstance(field_value, bool)
                or not isinstance(field_value, int)
                or field_value < 0
            ):
                raise ValueError(f"retry record {field} must be a non-negative integer")
            return field_value

        def decode_base64(field: str, *, optional: bool = False) -> bytes | None:
            field_value = value.get(field)
            if optional and field_value is None:
                return None
            if not isinstance(field_value, str):
                raise ValueError(f"retry record {field} must be Base64 text")
            try:
                return base64.b64decode(field_value, validate=True)
            except ValueError as error:
                raise ValueError(f"retry record {field} is invalid Base64") from error

        event_id = value.get("event_id")
        if event_id is not None and (not isinstance(event_id, str) or not event_id):
            raise ValueError("retry record event_id must be null or a non-empty string")
        attempt = required_offset("attempt_number")
        if attempt < 1:
            raise ValueError("retry record attempt_number must be positive")
        first_failure = required_text("first_failure_at")
        last_failure = required_text("last_failure_at")
        next_attempt = required_text("next_attempt_at")
        for field, timestamp in (
            ("first_failure_at", first_failure),
            ("last_failure_at", last_failure),
            ("next_attempt_at", next_attempt),
        ):
            _utc_datetime(timestamp, field)
        history_value = value.get("retry_history", [])
        if not isinstance(history_value, list):
            raise ValueError("retry record retry_history must be an array")
        if len(history_value) > 10_000:
            raise ValueError("retry record retry_history exceeds the supported limit")
        history: list[dict[str, Any]] = []
        for index, entry in enumerate(history_value):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"retry record retry_history[{index}] must be an object"
                )
            failed_attempt = entry.get("attempt_number")
            failure_reason = entry.get("failure_reason")
            failed_at = entry.get("failed_at")
            delay = entry.get("retry_delay_seconds")
            next_attempt_at = entry.get("next_attempt_at")
            if (
                isinstance(failed_attempt, bool)
                or not isinstance(failed_attempt, int)
                or failed_attempt < 0
            ):
                raise ValueError(
                    f"retry record retry_history[{index}].attempt_number "
                    "must be a non-negative integer"
                )
            if not isinstance(failure_reason, str) or not failure_reason:
                raise ValueError(
                    f"retry record retry_history[{index}].failure_reason "
                    "must be a non-empty string"
                )
            _utc_datetime(failed_at, f"retry_history[{index}].failed_at")
            if delay is not None:
                try:
                    finite_delay = (
                        not isinstance(delay, bool)
                        and isinstance(delay, (int, float))
                        and math.isfinite(float(delay))
                        and delay >= 0
                    )
                except OverflowError:
                    finite_delay = False
                if not finite_delay:
                    raise ValueError(
                        f"retry record retry_history[{index}].retry_delay_seconds "
                        "must be null or a finite non-negative number"
                    )
            if next_attempt_at is not None:
                _utc_datetime(
                    next_attempt_at, f"retry_history[{index}].next_attempt_at"
                )
            history.append(dict(entry))
        raw_payload = decode_base64("raw_payload_base64")
        source_key = decode_base64("source_key_base64", optional=True)
        if raw_payload is None:
            raise ValueError("retry record raw_payload_base64 is required")
        return cls(
            event_id=event_id,
            source_topic=required_text("source_topic"),
            source_partition=required_offset("source_partition"),
            source_offset=required_offset("source_offset"),
            source_key=source_key,
            raw_payload=raw_payload,
            attempt_number=attempt,
            first_failure_at=first_failure,
            last_failure_at=last_failure,
            failure_reason=required_text("failure_reason"),
            next_attempt_at=next_attempt,
            retry_history=tuple(history),
        )

    @property
    def next_attempt_epoch(self) -> float:
        return _utc_datetime(self.next_attempt_at, "next_attempt_at").timestamp()
