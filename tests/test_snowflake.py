"""Unit tests for the DE-05 Snowflake foundation; no live account required."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from atmo_sync.snowflake import (
    RawSensorEvent,
    SnowflakeClient,
    SnowflakeClientError,
    SnowflakeConfig,
)


ROOT = Path(__file__).resolve().parents[1]


class FakeSnowflakeError(Exception):
    pass


class OperationalError(FakeSnowflakeError):
    pass


class FakeCursor:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.executions = []
        self.closed = False

    def execute(self, statement, parameters=None):
        self.executions.append((statement, parameters))

    def fetchone(self):
        return (1,)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rowcounts=(1,)):
        self.cursors = []
        self.closed = False
        self.rowcounts = list(rowcounts)

    def cursor(self):
        rowcount = self.rowcounts.pop(0) if self.rowcounts else 1
        cursor = FakeCursor(rowcount)
        self.cursors.append(cursor)
        return cursor

    def close(self):
        self.closed = True


def valid_config(**overrides):
    values = {
        "account": "org-account",
        "user": "ingest-user",
        "warehouse": "INGEST_WH",
        "database": "ATMOSYNC",
        "schema": "RAW",
        "password": "do-not-log-this",
    }
    values.update(overrides)
    return SnowflakeConfig(**values)


def load_event():
    with (ROOT / "data" / "sample_sensor_event.json").open(encoding="utf-8") as file:
        return json.load(file)


class SnowflakeConfigTests(unittest.TestCase):
    def test_missing_configuration_names_missing_values_without_exposing_secrets(self):
        with self.assertRaisesRegex(ValueError, "SNOWFLAKE_ACCOUNT"):
            SnowflakeConfig.from_env({"SNOWFLAKE_PASSWORD": "private-value"})

    def test_valid_password_and_external_browser_configuration(self):
        password_config = SnowflakeConfig.from_env(
            {
                "SNOWFLAKE_ACCOUNT": "acct",
                "SNOWFLAKE_USER": "user",
                "SNOWFLAKE_PASSWORD": "secret",
                "SNOWFLAKE_WAREHOUSE": "WH",
                "SNOWFLAKE_DATABASE": "ATMOSYNC",
                "SNOWFLAKE_SCHEMA": "RAW",
                "SNOWFLAKE_ROLE": "INGEST_ROLE",
            }
        )
        self.assertEqual(password_config.connection_parameters()["password"], "secret")
        self.assertEqual(password_config.connection_parameters()["role"], "INGEST_ROLE")
        self.assertNotIn("secret", repr(password_config))

        browser_config = SnowflakeConfig.from_env(
            {
                "SNOWFLAKE_ACCOUNT": "acct",
                "SNOWFLAKE_USER": "user",
                "SNOWFLAKE_AUTHENTICATOR": "externalbrowser",
                "SNOWFLAKE_WAREHOUSE": "WH",
                "SNOWFLAKE_DATABASE": "ATMOSYNC",
                "SNOWFLAKE_SCHEMA": "RAW",
            }
        )
        self.assertNotIn("password", browser_config.connection_parameters())

    def test_database_and_schema_are_safe_unquoted_identifiers(self):
        with self.assertRaisesRegex(ValueError, "database"):
            valid_config(database='ATMOSYNC"; DROP TABLE USERS;--')


class SnowflakeClientTests(unittest.TestCase):
    def test_connection_failure_is_wrapped_without_echoing_connector_message(self):
        secret = "credential-fragment"

        def fail_connect(**_parameters):
            raise FakeSnowflakeError(f"login failed with {secret}")

        client = SnowflakeClient(
            valid_config(),
            connection_factory=fail_connect,
            connector_error_type=FakeSnowflakeError,
        )
        with self.assertRaises(SnowflakeClientError) as error:
            client.connect()
        self.assertEqual(str(error.exception), "Snowflake connection failed")
        self.assertNotIn(secret, str(error.exception))

    def test_operational_connection_failure_is_marked_retryable(self):
        def fail_connect(**_parameters):
            raise OperationalError("temporary network failure")

        client = SnowflakeClient(
            valid_config(),
            connection_factory=fail_connect,
            connector_error_type=FakeSnowflakeError,
        )
        with self.assertRaises(SnowflakeClientError) as error:
            client.connect()
        self.assertTrue(error.exception.retryable)

    def test_connectivity_and_context_manager_close_cursor_and_connection(self):
        connection = FakeConnection()
        client = SnowflakeClient(
            valid_config(),
            connection_factory=lambda **_parameters: connection,
            connector_error_type=FakeSnowflakeError,
        )
        with client as connected:
            self.assertTrue(connected.check_connection())
        self.assertTrue(connection.cursors[0].closed)
        self.assertTrue(connection.closed)

    def test_raw_event_serialization_and_parameter_construction(self):
        event = load_event()
        record = RawSensorEvent.from_kafka(
            event,
            kafka_topic="sensor-events-validated",
            kafka_partition=2,
            kafka_offset=41,
        )
        self.assertEqual(json.loads(record.payload_json), event)
        self.assertEqual(len(record.payload_sha256), 64)
        self.assertEqual(
            record.insert_parameters(),
            (
                event["event_id"],
                event["shipment_id"],
                event["container_id"],
                event["sensor_id"],
                event["event_timestamp"],
                "sensor-events-validated",
                2,
                41,
                record.payload_json,
                record.payload_sha256,
            ),
        )

    def test_merge_uses_parameterized_sql_and_source_coordinate_identity(self):
        event = load_event()
        connection = FakeConnection(rowcounts=(1, 0))
        client = SnowflakeClient(
            valid_config(),
            connection_factory=lambda **_parameters: connection,
            connector_error_type=FakeSnowflakeError,
        )
        kwargs = {
            "kafka_topic": "sensor-events-validated",
            "kafka_partition": 2,
            "kafka_offset": 41,
        }
        self.assertTrue(client.insert_raw_event(event, **kwargs))
        self.assertFalse(client.insert_raw_event(event, **kwargs))
        first_record = RawSensorEvent.from_kafka(event, **kwargs)
        second_record = RawSensorEvent.from_kafka(event, **kwargs)
        self.assertEqual(first_record.source_identity, second_record.source_identity)
        self.assertEqual(first_record.event_identity, second_record.event_identity)

        statement, parameters = connection.cursors[0].executions[0]
        self.assertIn("MERGE INTO", statement)
        self.assertIn("target.KAFKA_PARTITION = source.KAFKA_PARTITION", statement)
        self.assertEqual(parameters, first_record.insert_parameters())
        self.assertTrue(connection.cursors[0].closed)
        self.assertTrue(connection.cursors[1].closed)

    def test_merge_with_unknown_row_count_is_not_acknowledged_as_success(self):
        event = load_event()
        connection = FakeConnection(rowcounts=(-1,))
        client = SnowflakeClient(
            valid_config(),
            connection_factory=lambda **_parameters: connection,
            connector_error_type=FakeSnowflakeError,
        )
        with self.assertRaisesRegex(SnowflakeClientError, "affected-row count"):
            client.insert_raw_event(
                event,
                kafka_topic="sensor-events-validated",
                kafka_partition=0,
                kafka_offset=1,
            )
        self.assertTrue(connection.cursors[0].closed)

    def test_invalid_kafka_coordinates_are_rejected_before_connection(self):
        client = SnowflakeClient(
            valid_config(),
            connection_factory=lambda **_parameters: self.fail("unexpected connect"),
            connector_error_type=FakeSnowflakeError,
        )
        with self.assertRaisesRegex(ValueError, "kafka_offset"):
            client.insert_raw_event(
                load_event(),
                kafka_topic="sensor-events-validated",
                kafka_partition=0,
                kafka_offset=-1,
            )


if __name__ == "__main__":
    unittest.main()
