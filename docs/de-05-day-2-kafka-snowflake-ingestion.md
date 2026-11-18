# DE-05 Day 2 — Kafka to Snowflake Ingestion

## Architecture

```text
Python Simulator → Kafka → Validated Topic → Snowflake Consumer → Snowflake RAW Table
                              sensor-events-validated
```

The `SnowflakeIngestionConsumer` reuses DE-04's validated-event decoder,
contract validation, `read_committed` consumer configuration, disabled auto
commit, synchronous per-message commit, and orderly Kafka shutdown. It uses
the Day 1 `SnowflakeClient` and `ATMOSYNC.RAW.SENSOR_EVENTS` `MERGE`; it does
not alter the transactional processor, input topic, retry topic, or DLQ.

The ingestion consumer uses its own group,
`atmosync-snowflake-ingestion`, independently of the validated, analytics,
and monitoring groups.

## Files and configuration

- [`consumers/snowflake.py`](../src/atmo_sync/consumers/snowflake.py):
  Snowflake ingestion, bounded transient-error retry, metrics, and CLI.
- [`consumers/base.py`](../src/atmo_sync/consumers/base.py): message-aware
  handling and post-commit hooks, while preserving the existing consumer
  defaults.
- [`config.py`](../src/atmo_sync/config.py): shared local `.env` loading.
- [`snowflake.py`](../src/atmo_sync/snowflake.py): Day 1 connection,
  configuration, and replay-safe RAW writes.
- [`test_snowflake_consumer.py`](../tests/test_snowflake_consumer.py): unit
  coverage with Kafka/Snowflake test doubles.
- [`test_snowflake_consumer_integration.py`](../tests/integration/test_snowflake_consumer_integration.py):
  opt-in live Kafka tests with a Snowflake test double.

The CLI loads a nearby `.env` via `python-dotenv` without overriding
environment variables injected by a deployment. Configure Kafka with
`KAFKA_BOOTSTRAP_SERVERS` or `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS`. Snowflake
settings are `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_AUTHENTICATOR`, `SNOWFLAKE_PASSWORD` (password auth only),
`SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, and
`SNOWFLAKE_SCHEMA`. Do not commit `.env` or put credentials in logs.

Install the base project requirements and optional connector, provision the
Day 1 SQL objects, and provide non-empty warehouse/database/schema values.
For browser SSO, use `SNOWFLAKE_AUTHENTICATOR=externalbrowser`; no password
is required.

## Starting ingestion

From the repository root with `.venv` active or using its interpreter:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[snowflake]"
.\.venv\Scripts\python.exe -m atmo_sync.consumers.snowflake
```

The installed entry point is `atmosync-snowflake-consumer`. Optional
`--bootstrap-servers`, `--topic`, `--group-id`, and `--max-messages` flags
follow the existing consumer CLI conventions. The process checks Snowflake
connectivity before it subscribes to Kafka. Startup and shutdown logs include
the group, target, and counters but never connection parameters.

## Offset management and rebalance safety

For each delivered record the sequence is:

1. Decode UTF-8 JSON and validate it with the existing DE-02 contract.
2. Obtain topic, partition, and offset from the Kafka message.
3. Persist with `SnowflakeClient.insert_raw_event()`.
4. Only after the write succeeds (or the idempotent merge reports an
   existing source coordinate), synchronously commit that message's offset.

Auto commit and automatic offset storage are disabled by the shared consumer
configuration. The Confluent client commits the message's next offset; commit
exceptions and partition-level commit errors are fatal and surfaced. No
consumer close path commits offsets.

Processing is serial: the consumer does not poll another message while a
Snowflake write or its offset commit is in progress. Revoke/lost callbacks
therefore have no queued or unpersisted application-side messages to flush.
The commit refers only to the message already persisted. If a rebalance makes
that commit invalid, the commit fails and Kafka may redeliver the message;
the Snowflake merge makes that replay safe. A revocation does not cause a
commit of unprocessed messages. Invalid records fail validation and remain
uncommitted for operator investigation; they are not silently skipped or
sent to a new DLQ.

This is **at-least-once** processing, not an atomic Kafka/Snowflake
transaction. A crash after the Snowflake write but before the Kafka commit
causes replay.

## Idempotency and recovery

The Snowflake `MERGE` matches the source identity
`(KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET)`. The same Kafka record replayed
after a write, commit failure, process restart, or rebalance therefore does
not add another RAW row in the sequential case. The ingestion consumer
disables the shared in-memory event-ID suppression so each record reaches
the source-coordinate merge; distinct offsets carrying the same event ID
remain distinct source records. Snowflake standard-table primary keys are not
enforced, and concurrent writers may race; run one active member per
partition and do not claim exactly-once semantics.

Transient Snowflake connector errors (`OperationalError`, `InterfaceError`,
`NetworkError`, and built-in connection/time-out errors) receive at most
three attempts with bounded exponential backoff. Before retrying, the
connection is closed so the next attempt reconnects. Other connector errors
are treated as permanent and stop the process. Exhausted retries, connection
reset failures, Kafka commit errors, and invalid messages stop with a
non-zero exit and without acknowledging the failed record. Restart the
consumer after restoring Snowflake/Kafka availability; the uncommitted record
is eligible for redelivery. Monitor repeated failures rather than allowing a
consumer to skip records.

## Metrics and limitations

Shutdown logs report consumed messages, inserted rows, duplicate/replayed
messages, failed write attempts, successful offset commits, and aggregate
processing duration. Consumer lag is not reported because the current
implementation does not measure it reliably.

- No full Kafka-to-live-Snowflake run was performed without configured
  warehouse/database/schema access and a live Snowflake account.
- The Kafka integration tests use a Snowflake test double and verify Kafka
  assignment, persistence-before-commit, restart, and member rebalance; they
  do not test Snowflake SQL or network behavior.
- Failed/invalid events are not automatically routed to the existing DE-04
  DLQ because that would require a separately designed, safely committed DLQ
  handoff.
- Snowflake table uniqueness is not enforced for concurrent writers.
- A long Snowflake request can hold a partition assignment; processing is
  deliberately serial to avoid acknowledging later records before earlier
  writes.

## Tests

Mock-based unit tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests\test_snowflake.py tests\test_snowflake_consumer.py
```

Live Kafka broker tests with Snowflake test doubles:

```powershell
$env:ATMOSYNC_RUN_KAFKA_INTEGRATION = "1"
.\.venv\Scripts\python.exe -m pytest -q tests\integration\test_snowflake_consumer_integration.py
```

The opt-in integration test writes to the existing validated topic and uses a
unique group ID. It does not create, delete, or reset Kafka topics or volumes.
It is not a full live Snowflake end-to-end test.
