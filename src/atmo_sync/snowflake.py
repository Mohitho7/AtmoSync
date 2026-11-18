"""Snowflake configuration and persistence for validated sensor events."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Mapping

from .config import load_environment
from .processor import payload_sha256

LOGGER = logging.getLogger(__name__)

_SUPPORTED_AUTHENTICATORS = {"snowflake", "externalbrowser"}
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")


class SnowflakeClientError(RuntimeError):
    """Raised when Snowflake configuration or an operation is unavailable."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str
    database: str
    schema: str
    password: str | None = field(default=None, repr=False)
    authenticator: str = "snowflake"
    role: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("account", "user", "warehouse", "database", "schema"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if self.authenticator not in _SUPPORTED_AUTHENTICATORS:
            raise ValueError(
                "authenticator must be 'snowflake' or 'externalbrowser'"
            )
        if self.authenticator == "snowflake" and not self.password:
            raise ValueError(
                "SNOWFLAKE_PASSWORD is required for password authentication"
            )
        if self.role is not None and not self.role.strip():
            raise ValueError("role must be a non-empty string when provided")
        for field_name in ("database", "schema"):
            if not _IDENTIFIER.fullmatch(getattr(self, field_name)):
                raise ValueError(
                    f"{field_name} must be an unquoted Snowflake identifier"
                )

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> SnowflakeConfig:
        """Create configuration from environment variables without logging values."""
        if environ is None:
            load_environment()
        env = environ if environ is not None else os.environ
        authenticator = env.get("SNOWFLAKE_AUTHENTICATOR", "snowflake").strip()
        if authenticator not in _SUPPORTED_AUTHENTICATORS:
            raise ValueError(
                "SNOWFLAKE_AUTHENTICATOR must be 'snowflake' or 'externalbrowser'"
            )
        required = {
            "SNOWFLAKE_ACCOUNT": env.get("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_USER": env.get("SNOWFLAKE_USER"),
            "SNOWFLAKE_WAREHOUSE": env.get("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_DATABASE": env.get("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_SCHEMA": env.get("SNOWFLAKE_SCHEMA"),
        }
        missing = [
            name
            for name, value in required.items()
            if not value or not value.strip()
        ]
        password = env.get("SNOWFLAKE_PASSWORD") or None
        if authenticator == "snowflake" and password is None:
            missing.append("SNOWFLAKE_PASSWORD")
        if missing:
            raise ValueError(
                "Missing required Snowflake configuration: " + ", ".join(missing)
            )

        return cls(
            account=env["SNOWFLAKE_ACCOUNT"].strip(),
            user=env["SNOWFLAKE_USER"].strip(),
            warehouse=env["SNOWFLAKE_WAREHOUSE"].strip(),
            database=env["SNOWFLAKE_DATABASE"].strip(),
            schema=env["SNOWFLAKE_SCHEMA"].strip(),
            password=password,
            authenticator=authenticator,
            role=(env.get("SNOWFLAKE_ROLE") or "").strip() or None,
        )

    def connection_parameters(self) -> dict[str, Any]:
        """Return connector arguments; callers must not log this mapping."""
        parameters: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "authenticator": self.authenticator,
            "autocommit": True,
        }
        if self.password is not None and self.authenticator == "snowflake":
            parameters["password"] = self.password
        if self.role is not None:
            parameters["role"] = self.role
        return parameters


@dataclass(frozen=True)
class RawSensorEvent:
    """Validated event plus the Kafka coordinates required for replay safety."""

    event_id: str
    shipment_id: str
    container_id: str
    sensor_id: str
    event_timestamp: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    payload_json: str
    payload_sha256: str

    @classmethod
    def from_kafka(
        cls,
        event: Mapping[str, Any],
        *,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> RawSensorEvent:
        if not isinstance(kafka_topic, str) or not kafka_topic.strip():
            raise ValueError("kafka_topic must be a non-empty string")
        for name, value in (
            ("kafka_partition", kafka_partition),
            ("kafka_offset", kafka_offset),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        event_dict = dict(event)
        digest = payload_sha256(event_dict)
        serialized = json.dumps(
            event_dict,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return cls(
            event_id=event_dict["event_id"],
            shipment_id=event_dict["shipment_id"],
            container_id=event_dict["container_id"],
            sensor_id=event_dict["sensor_id"],
            event_timestamp=event_dict["event_timestamp"],
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            payload_json=serialized,
            payload_sha256=digest,
        )

    @property
    def source_identity(self) -> tuple[str, int, int]:
        return (self.kafka_topic, self.kafka_partition, self.kafka_offset)

    @property
    def event_identity(self) -> tuple[str, str]:
        return (self.event_id, self.payload_sha256)

    def insert_parameters(self) -> tuple[Any, ...]:
        return (
            self.event_id,
            self.shipment_id,
            self.container_id,
            self.sensor_id,
            self.event_timestamp,
            self.kafka_topic,
            self.kafka_partition,
            self.kafka_offset,
            self.payload_json,
            self.payload_sha256,
        )


class SnowflakeClient:
    """Small synchronous client for the Snowflake RAW sensor-event table."""

    def __init__(
        self,
        config: SnowflakeConfig,
        *,
        connection_factory: Callable[..., Any] | None = None,
        connector_error_type: type[Exception] | None = None,
    ) -> None:
        if (connection_factory is None) != (connector_error_type is None):
            raise ValueError(
                "connection_factory and connector_error_type must be provided together"
            )
        self.config = config
        self._connection_factory = connection_factory
        self._connector_error_type = connector_error_type
        self._connection: Any | None = None

    def _connector_api(self) -> tuple[Callable[..., Any], type[Exception]]:
        if (
            self._connection_factory is not None
            and self._connector_error_type is not None
        ):
            return self._connection_factory, self._connector_error_type
        try:
            import snowflake.connector as connector
            from snowflake.connector.errors import Error as ConnectorError
        except ImportError as error:
            raise SnowflakeClientError(
                "Snowflake connector is not installed; install atmosync[snowflake]"
            ) from error
        return connector.connect, ConnectorError

    @staticmethod
    def _is_retryable_connector_error(error: Exception) -> bool:
        return isinstance(error, (TimeoutError, ConnectionError)) or type(
            error
        ).__name__ in {"InterfaceError", "NetworkError", "OperationalError"}

    def connect(self) -> Any:
        """Open one connection, reusing it for the life of this client."""
        if self._connection is not None:
            return self._connection
        connection_factory, error_type = self._connector_api()
        try:
            self._connection = connection_factory(
                **self.config.connection_parameters()
            )
        except error_type as error:
            LOGGER.error(
                "snowflake_connection_failed",
                extra={"database": self.config.database, "schema": self.config.schema},
            )
            raise SnowflakeClientError(
                "Snowflake connection failed",
                retryable=self._is_retryable_connector_error(error),
            ) from error
        LOGGER.info(
            "snowflake_connected",
            extra={"database": self.config.database, "schema": self.config.schema},
        )
        return self._connection

    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        connection = self.connect()
        _, error_type = self._connector_api()
        try:
            cursor = connection.cursor()
        except error_type as error:
            raise SnowflakeClientError(
                "Could not create Snowflake cursor",
                retryable=self._is_retryable_connector_error(error),
            ) from error
        try:
            yield cursor
        except error_type as error:
            raise SnowflakeClientError(
                "Snowflake operation failed",
                retryable=self._is_retryable_connector_error(error),
            ) from error
        finally:
            try:
                cursor.close()
            except error_type as error:
                if sys.exc_info()[0] is None:
                    raise SnowflakeClientError(
                        "Could not close Snowflake cursor",
                        retryable=self._is_retryable_connector_error(error),
                    ) from error
                LOGGER.error(
                    "snowflake_cursor_close_failed",
                    extra={
                        "database": self.config.database,
                        "schema": self.config.schema,
                    },
                )

    def check_connection(self) -> bool:
        """Run a minimal query and return true only after Snowflake responds."""
        with self._cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
        if result is None or result[0] != 1:
            raise SnowflakeClientError("Snowflake connectivity check returned no result")
        LOGGER.info(
            "snowflake_connectivity_check_succeeded",
            extra={"database": self.config.database, "schema": self.config.schema},
        )
        return True

    def insert_raw_event(
        self,
        event: Mapping[str, Any],
        *,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> bool:
        """Insert once per Kafka source coordinate; return false for a replay."""
        record = RawSensorEvent.from_kafka(
            event,
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
        )
        database = self.config.database.upper()
        schema = self.config.schema.upper()
        statement = f"""
            MERGE INTO "{database}"."{schema}"."SENSOR_EVENTS" AS target
            USING (
                SELECT
                    %s AS EVENT_ID,
                    %s AS SHIPMENT_ID,
                    %s AS CONTAINER_ID,
                    %s AS SENSOR_ID,
                    TO_TIMESTAMP_TZ(%s) AS EVENT_TIMESTAMP,
                    %s AS KAFKA_TOPIC,
                    %s::NUMBER(10, 0) AS KAFKA_PARTITION,
                    %s::NUMBER(38, 0) AS KAFKA_OFFSET,
                    PARSE_JSON(%s) AS PAYLOAD,
                    %s AS PAYLOAD_SHA256
            ) AS source
            ON target.KAFKA_TOPIC = source.KAFKA_TOPIC
                AND target.KAFKA_PARTITION = source.KAFKA_PARTITION
                AND target.KAFKA_OFFSET = source.KAFKA_OFFSET
            WHEN NOT MATCHED THEN INSERT (
                EVENT_ID, SHIPMENT_ID, CONTAINER_ID, SENSOR_ID, EVENT_TIMESTAMP,
                KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, INGESTED_AT, PAYLOAD,
                PAYLOAD_SHA256
            ) VALUES (
                source.EVENT_ID, source.SHIPMENT_ID, source.CONTAINER_ID,
                source.SENSOR_ID, source.EVENT_TIMESTAMP, source.KAFKA_TOPIC,
                source.KAFKA_PARTITION, source.KAFKA_OFFSET, CURRENT_TIMESTAMP(),
                source.PAYLOAD, source.PAYLOAD_SHA256
            )
        """
        with self._cursor() as cursor:
            cursor.execute(statement, record.insert_parameters())
            if cursor.rowcount == 1:
                return True
            if cursor.rowcount == 0:
                return False
            raise SnowflakeClientError(
                "Snowflake MERGE returned an unexpected affected-row count"
            )

    def close(self) -> None:
        """Close the current connection; repeated calls are safe."""
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        _, error_type = self._connector_api()
        try:
            connection.close()
        except error_type as error:
            LOGGER.error(
                "snowflake_connection_close_failed",
                extra={"database": self.config.database, "schema": self.config.schema},
            )
            raise SnowflakeClientError(
                "Could not close Snowflake connection",
                retryable=self._is_retryable_connector_error(error),
            ) from error

    def __enter__(self) -> SnowflakeClient:
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        if exc_type is None:
            self.close()
        else:
            try:
                self.close()
            except SnowflakeClientError:
                pass
        return False
