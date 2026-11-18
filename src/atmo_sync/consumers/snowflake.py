"""Persist validated Kafka sensor events in Snowflake before offset commits."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Callable

from ..snowflake import SnowflakeClient, SnowflakeClientError, SnowflakeConfig
from .base import (
    ConsumerConfig,
    ConsumerError,
    ConsumerOutcome,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_VALIDATED_TOPIC,
    KafkaEventConsumer,
)
from .consumer_cli import run_consumer_cli

LOGGER = logging.getLogger(__name__)

DEFAULT_SNOWFLAKE_GROUP_ID = "atmosync-snowflake-ingestion"
DEFAULT_SNOWFLAKE_RETRY_ATTEMPTS = 3
DEFAULT_SNOWFLAKE_RETRY_BACKOFF_SECONDS = 0.5


@dataclass(frozen=True)
class IngestionMetrics:
    messages_consumed: int = 0
    rows_inserted: int = 0
    duplicate_messages: int = 0
    failed_write_attempts: int = 0
    offset_commits: int = 0
    processing_duration_seconds: float = 0.0


class SnowflakeIngestionConsumer(KafkaEventConsumer):
    """Consume the validated topic and commit only after durable RAW writes."""

    def __init__(
        self,
        config: ConsumerConfig | None = None,
        *,
        snowflake_client: SnowflakeClient | None = None,
        snowflake_config: SnowflakeConfig | None = None,
        consumer_client: Any | None = None,
        retry_attempts: int = DEFAULT_SNOWFLAKE_RETRY_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_SNOWFLAKE_RETRY_BACKOFF_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if (
            isinstance(retry_attempts, bool)
            or not isinstance(retry_attempts, int)
            or retry_attempts < 1
        ):
            raise ValueError("retry_attempts must be a positive integer")
        if (
            isinstance(retry_backoff_seconds, bool)
            or not isinstance(retry_backoff_seconds, (int, float))
            or not math.isfinite(retry_backoff_seconds)
            or retry_backoff_seconds < 0
        ):
            raise ValueError("retry_backoff_seconds must be non-negative")
        if snowflake_client is not None and snowflake_config is not None:
            raise ValueError("provide snowflake_client or snowflake_config, not both")

        self.config = config or ConsumerConfig(
            bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
            group_id=DEFAULT_SNOWFLAKE_GROUP_ID,
            topic=DEFAULT_VALIDATED_TOPIC,
        )
        self.snowflake = (
            snowflake_client
            if snowflake_client is not None
            else SnowflakeClient(snowflake_config or SnowflakeConfig.from_env())
        )
        self._retry_attempts = retry_attempts
        self._retry_backoff_seconds = float(retry_backoff_seconds)
        self._sleep = sleep
        self._metrics = {
            "messages_consumed": 0,
            "rows_inserted": 0,
            "duplicate_messages": 0,
            "failed_write_attempts": 0,
            "offset_commits": 0,
            "processing_duration_seconds": 0.0,
        }

        self._check_snowflake_connection()

        try:
            super().__init__(
                self.config,
                message_handler=self._persist_event,
                consumer_client=consumer_client,
                on_revoke=self._on_partitions_revoked,
                on_lost=self._on_partitions_lost,
                suppress_recent_event_ids=False,
            )
        except Exception:
            try:
                self.snowflake.close()
            except SnowflakeClientError:
                LOGGER.error("snowflake_cleanup_after_startup_failure")
            raise

        LOGGER.info(
            "snowflake_ingestion_started",
            extra={
                "consumer_group": self.config.group_id,
                "topic": self.config.topic,
                "database": self.snowflake.config.database,
                "schema": self.snowflake.config.schema,
            },
        )

    @property
    def metrics(self) -> IngestionMetrics:
        return IngestionMetrics(**self._metrics)

    def _check_snowflake_connection(self) -> None:
        for attempt in range(1, self._retry_attempts + 1):
            try:
                self.snowflake.check_connection()
                return
            except SnowflakeClientError as error:
                if error.retryable and attempt < self._retry_attempts:
                    LOGGER.warning(
                        "snowflake_connectivity_check_retry",
                        extra={"attempt": attempt},
                    )
                    try:
                        self.snowflake.close()
                    except SnowflakeClientError as close_error:
                        raise ConsumerError(
                            "Snowflake connection reset failed before consumption"
                        ) from close_error
                    self._sleep(
                        self._retry_backoff_seconds * (2 ** (attempt - 1))
                    )
                    continue
                try:
                    self.snowflake.close()
                except SnowflakeClientError:
                    LOGGER.error("snowflake_cleanup_after_connectivity_failure")
                raise ConsumerError(
                    "Snowflake startup connectivity check failed"
                ) from error
            except ValueError as error:
                try:
                    self.snowflake.close()
                except SnowflakeClientError:
                    LOGGER.error("snowflake_cleanup_after_connectivity_failure")
                raise ConsumerError(
                    "Snowflake startup connectivity check failed"
                ) from error

    def _on_partitions_revoked(self, _consumer: Any, partitions: list[Any]) -> None:
        LOGGER.info(
            "snowflake_ingestion_partitions_revoked",
            extra={
                "consumer_group": self.config.group_id,
                "partitions": [
                    {"topic": item.topic, "partition": item.partition}
                    for item in partitions
                ],
            },
        )

    def _on_partitions_lost(self, _consumer: Any, partitions: list[Any]) -> None:
        LOGGER.warning(
            "snowflake_ingestion_partitions_lost",
            extra={
                "consumer_group": self.config.group_id,
                "partitions": [
                    {"topic": item.topic, "partition": item.partition}
                    for item in partitions
                ],
            },
        )

    def _on_message_received(self, _message: Any) -> None:
        self._metrics["messages_consumed"] += 1

    def _on_offset_committed(
        self, _event: dict[str, Any], _message: Any, *, duplicate: bool
    ) -> None:
        self._metrics["offset_commits"] += 1
        if duplicate:
            self._metrics["duplicate_messages"] += 1

    def _persist_event(self, event: dict[str, Any], message: Any) -> None:
        for attempt in range(1, self._retry_attempts + 1):
            try:
                inserted = self.snowflake.insert_raw_event(
                    event,
                    kafka_topic=message.topic(),
                    kafka_partition=message.partition(),
                    kafka_offset=message.offset(),
                )
            except SnowflakeClientError as error:
                self._metrics["failed_write_attempts"] += 1
                if not error.retryable or attempt == self._retry_attempts:
                    LOGGER.error(
                        "snowflake_ingestion_write_failed",
                        extra={
                            "consumer_group": self.config.group_id,
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "attempt": attempt,
                            "retryable": error.retryable,
                        },
                    )
                    raise ConsumerError(
                        "Snowflake persistence failed; Kafka offset was not committed"
                    ) from error

                LOGGER.warning(
                    "snowflake_ingestion_write_retry",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "attempt": attempt,
                    },
                )
                try:
                    self.snowflake.close()
                except SnowflakeClientError as close_error:
                    raise ConsumerError(
                        "Snowflake connection reset failed; Kafka offset was not committed"
                    ) from close_error
                self._sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))
                continue

            if inserted:
                self._metrics["rows_inserted"] += 1
            else:
                self._metrics["duplicate_messages"] += 1
            return

    def process_one(
        self, timeout: float | None = None
    ) -> ConsumerOutcome | None:
        consumed_before = self._metrics["messages_consumed"]
        started = time.perf_counter()
        try:
            return super().process_one(timeout)
        finally:
            if self._metrics["messages_consumed"] != consumed_before:
                self._metrics["processing_duration_seconds"] += (
                    time.perf_counter() - started
                )

    def close(self) -> None:
        kafka_error: ConsumerError | None = None
        try:
            super().close()
        except ConsumerError as error:
            kafka_error = error
        try:
            self.snowflake.close()
        except SnowflakeClientError as error:
            if kafka_error is None:
                raise ConsumerError("Snowflake shutdown failed") from error
            LOGGER.error("snowflake_connection_close_failed_during_shutdown")
        if kafka_error is not None:
            raise kafka_error
        LOGGER.info(
            "snowflake_ingestion_stopped",
            extra={
                "consumer_group": self.config.group_id,
                "metrics": self._metrics.copy(),
            },
        )


def main(argv: list[str] | None = None) -> int:
    return run_consumer_cli(
        SnowflakeIngestionConsumer,
        default_group_id=DEFAULT_SNOWFLAKE_GROUP_ID,
        group_env_var="ATMOSYNC_SNOWFLAKE_CONSUMER_GROUP_ID",
        description="Persist validated sensor events to Snowflake RAW.",
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
