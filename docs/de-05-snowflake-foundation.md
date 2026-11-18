# DE-05 — Snowflake Foundation

## Architecture

Day 1 establishes the storage boundary for validated events:

```text
sensor-events-validated
    -> Day 2 Kafka consumer
    -> ATMOSYNC.RAW.SENSOR_EVENTS
    -> STAGING and ANALYTICS (schemas provisioned, models deferred)
```

DE-04 remains unchanged. This milestone does not add a Kafka consumer or
manage/commit Kafka offsets. The Python adapter accepts a validated event and
its Kafka topic, partition, and offset as input, stores the complete event
object in a Snowflake `VARIANT`, and uses a parameterized `MERGE` for the
RAW write.

## Prerequisites

- Python 3.11 or newer and the AtmoSync project environment.
- A Snowflake account, user, warehouse, and permission to create/use the
  `ATMOSYNC` database and its schemas/table (or equivalent pre-provisioned
  access).
- The optional Snowflake Python connector:

  ```powershell
  python -m pip install -e ".[snowflake]"
  ```

- Validated records are produced by the existing DE-04 flow. No live Kafka
  or Snowflake service is needed to run the unit tests.

## Environment configuration

Copy `.env.example` to a local, ignored `.env` file and fill in the values.
Consumer CLIs and `SnowflakeConfig.from_env()` load `.env` through the shared
`python-dotenv` helper without overriding variables already injected by the
runtime. Direct configuration construction continues to use the supplied
environment mapping.

Required for password authentication:

```text
SNOWFLAKE_ACCOUNT=<account identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<secret>
SNOWFLAKE_AUTHENTICATOR=snowflake
SNOWFLAKE_WAREHOUSE=<warehouse>
SNOWFLAKE_DATABASE=ATMOSYNC
SNOWFLAKE_SCHEMA=RAW
```

Browser SSO is also supported by setting
`SNOWFLAKE_AUTHENTICATOR=externalbrowser`; omit the password in that mode.
`SNOWFLAKE_ROLE` is optional. Do not put credentials in tracked files, logs,
or command output.

The configuration is parsed with
`SnowflakeConfig.from_env()`. The connection is lazy and reusable:

```python
from atmo_sync.snowflake import SnowflakeClient, SnowflakeConfig

with SnowflakeClient(SnowflakeConfig.from_env()) as client:
    client.check_connection()
```

`check_connection()` executes `SELECT 1`. It reports a failure by raising
`SnowflakeClientError`; it does not print connection parameters or secrets.

## Database and schema structure

Run [`001_create_raw_sensor_events.sql`](../sql/snowflake/001_create_raw_sensor_events.sql)
in Snowflake with an authorized role. The script idempotently creates:

- `ATMOSYNC.RAW` for the unmodified source event and ingestion metadata.
- `ATMOSYNC.STAGING` for future normalized dbt/source models.
- `ATMOSYNC.ANALYTICS` for future consumer-facing models.
- `ATMOSYNC.RAW.SENSOR_EVENTS` as the initial landing table.

## RAW table design

The event fields reflect the current DE-02 sensor event contract:
`event_id`, `shipment_id`, `container_id`, `sensor_id`, and
`event_timestamp`. The full validated event is retained as `PAYLOAD VARIANT`,
including schema version, commodity, location, and readings. The additional
columns preserve Kafka source coordinates, server-side ingestion time, and
the deterministic SHA-256 digest used by the DE-04 processor.

The table declares `(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)` as its
primary key and the adapter's `MERGE` matches on that source identity. The
event ID and payload digest are stored as a second logical identity for
auditing and event-level comparison. Snowflake standard-table primary key
constraints are informational rather than enforced; the `MERGE` makes
sequential retries of a source coordinate idempotent, but concurrent writers
can still race. Do not interpret this as exactly-once delivery.

## Testing

Run all unit tests (including Snowflake mocks):

```powershell
python -m pytest -q
```

The focused adapter tests use fake connector, connection, and cursor objects.
They cover missing/valid configuration, generic connector failure handling,
cleanup, serialization, bind parameter construction, and source-coordinate
duplicate identity. These tests do not contact Snowflake.

A live check is separate and only succeeds with valid account credentials
and network access:

```powershell
python -c "from atmo_sync.snowflake import SnowflakeClient, SnowflakeConfig; client = SnowflakeClient(SnowflakeConfig.from_env()); print(client.check_connection()); client.close()"
```

No live Snowflake connection or table smoke test is claimed by this
implementation; real account access was not provided for this task.

## Known limitations

- Day 1 does not consume Kafka or create/manage consumer groups.
- The adapter does not commit Kafka offsets. Day 2 must commit an offset only
  after the Snowflake write has returned successfully; write failure must
  leave the offset uncommitted for retry.
- The source-coordinate `MERGE` is sequentially replay-safe but does not
  provide a concurrency-enforced uniqueness guarantee or exactly-once
  semantics.
- Table provisioning is an explicit SQL step; the adapter does not silently
  create or alter database objects.
- `STAGING` and `ANALYTICS` are provisioned only; dbt models are deferred.

## Day 2 dependencies

Day 2 can build a separate consumer that:

1. Disables Kafka auto-commit and consumes
   `sensor-events-validated` with `read_committed`.
2. Passes each decoded event and its message topic/partition/offset to
   `insert_raw_event()`.
3. Commits the Kafka offset only after the method returns successfully.
4. Replays failed/uncommitted records safely through the RAW merge and
   surfaces serialization, Snowflake, and commit failures explicitly.

No such consumer or offset coordination is part of Day 1.
