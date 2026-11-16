# DE-04C — Transactional Validation and Deduplication

## Objective and scope

DE-04C consumes raw sensor events from Kafka, applies the existing DE-02
`validate_sensor_event()` contract, suppresses duplicate event IDs, and
atomically publishes validated events or dead-letter records with the input
offset. It uses the compacted `sensor-event-dedup` topic as the durable
event-ID ledger.

The processor handles only Kafka event validation, duplicate detection,
conflicting event IDs, and malformed/invalid event routing. It does not
implement retry topics, retry scheduling, independent downstream consumer
applications, shipment analytics, Snowflake, dbt, or dashboards. Simulator
events are synthetic and are not actual IoT measurements.

## Architecture and transaction behavior

```text
sensor-events
      |
      v
TransactionalValidationProcessor
      | valid, new event       | invalid/malformed/conflict
      v                        v
sensor-events-validated   sensor-events-dlq
      \                        /
       \ event ID marker      /
        v                    v
          sensor-event-dedup

Input consumer offset is committed in the same Kafka transaction.
```

For each raw record, the processor starts one Kafka transaction:

- A valid new event produces the unchanged event JSON to
  `sensor-events-validated`, keyed by UTF-8 `container_id`, and a marker to
  `sensor-event-dedup`, keyed by UTF-8 `event_id`.
- A repeated event ID with the same canonical SHA-256 payload hash produces
  no output; its input offset is committed transactionally.
- A repeated event ID with a different payload is written to the DLQ as an
  `EVENT_ID_PAYLOAD_CONFLICT`; the original ledger entry remains unchanged.
- Malformed UTF-8/JSON, valid JSON values that are not objects, and
  DE-02-invalid sensor events are written to the DLQ. Non-object JSON uses
  `JSON_OBJECT_REQUIRED`. The envelope includes source topic, partition,
  offset, and the original bytes as Base64. Parsed values are included as
  `original_event`.
- The input offset (`offset + 1`) is sent to the transaction with the Kafka
  consumer group's metadata. A crash before commit leaves outputs, ledger
  update, and offset uncommitted together.

The hash is SHA-256 over compact UTF-8 JSON with sorted object keys. The
UUID is normalized to lowercase canonical form for hashing and ledger identity;
the validated output preserves the original event fields. The processor
reuses `validate_sensor_event()` and does not change the DE-02 contract.

The parser rejects values that cannot be represented as finite UTF-8 JSON and
routes deep-parser failures to the DLQ as raw bytes. The raw topic record limit
is 1 MiB. Serialized DLQ JSON envelopes are limited to 4 MiB; the DLQ Kafka
record limit is 6 MiB to accommodate the envelope, source key, and Kafka
record framing. The processor producer and DLQ verification consumer use
matching 6 MiB record and fetch limits. The raw-input consumer fetch limit is
2 MiB so records near the raw topic limit are not constrained by the client's
default 1 MiB per-partition fetch size.

At startup, `init_transactions()` resolves any previous open transaction.
The processor then reads the compacted ledger using `read_committed` through
a captured end offset before subscribing to raw input. The ledger is held in
memory for lookups and updated only after a successful transaction commit.
If commit or abort outcome is uncertain, the process stops without assuming
success; restart with the same group and transactional IDs to recover
committed offsets and ledger state.

## Kafka requirements and limits

Use the existing DE-04A local Kafka service (Apache Kafka 3.9.1, KRaft,
single broker). The processor verifies the expected topics and partition
counts, raw/DLQ Kafka record limits, and the dedup topic's `compact` cleanup policy
before consuming:

| Topic | Partitions |
|---|---:|
| `sensor-events` | 1 |
| `sensor-events-validated` | 6 |
| `sensor-events-dlq` | 3 |
| `sensor-event-dedup` | 1 |

The processor does not create, delete, or reconfigure topics. Start the broker
and initialize the DE-04A topics as documented in
[DE-04A Kafka Infrastructure](de-04-kafka-infrastructure.md).

All application instances use one fixed transactional ID so Kafka fences a
competing AtmoSync processor instance. The consumer group can be configured,
but running independent processors with other transactional IDs or consumer
groups against these topics is unsupported and can duplicate output. This is a
local single-broker design, not high availability or a multi-active global
deduplication service. The raw topic has one partition, and the in-memory
ledger must be restored at each restart. Recovery scan time grows with the
ledger log records scanned; memory grows with retained event IDs.

Every downstream consumer of `sensor-events-validated`,
`sensor-events-dlq`, or `sensor-event-dedup` must use
`isolation.level=read_committed`. Consumers using Kafka's default
`read_uncommitted` can observe output records from aborted transactions.
Transactions provide atomic visibility only to read-committed Kafka consumers;
they do not provide end-to-end exactly-once processing.

## Installation and configuration

The project depends on `confluent-kafka` (already used by DE-04B). Install or
update the editable package:

```powershell
python -m pip install -e .
```

| Setting | Default | Environment override | CLI option |
|---|---|---|---|
| Bootstrap servers | `localhost:9092` | `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS` | `--bootstrap-servers` |
| Raw input topic | `sensor-events` | `ATMOSYNC_KAFKA_INPUT_TOPIC` | `--input-topic` |
| Validated topic | `sensor-events-validated` | `ATMOSYNC_KAFKA_VALIDATED_TOPIC` | `--validated-topic` |
| DLQ topic | `sensor-events-dlq` | `ATMOSYNC_KAFKA_DLQ_TOPIC` | `--dlq-topic` |
| Deduplication topic | `sensor-event-dedup` | `ATMOSYNC_KAFKA_DEDUP_TOPIC` | `--dedup-topic` |
| Consumer group | `atmosync-validation-processor` | `ATMOSYNC_KAFKA_GROUP_ID` | `--group-id` |
| Transactional ID | `atmosync-validation-processor-1` | Fixed by the application | Not configurable |

CLI arguments override environment variables. Transaction, restore, poll, and
operation timeouts use conservative built-in defaults; they are configurable
through `ProcessorConfig` when embedded as a Python API.

## Running the processor

Start Kafka and initialize its topics, then run the continuous processor:

```powershell
python -m atmo_sync.processor
```

The installed console command is equivalent:

```powershell
atmosync-processor
```

Override the broker or topic configuration as needed:

```powershell
python -m atmo_sync.processor --bootstrap-servers localhost:9092
python -m atmo_sync.processor --input-topic sensor-events
```

Stop with Ctrl+C. The processor does not commit offsets outside a Kafka
transaction during shutdown. Startup, processing outcomes, and failures are
logged to standard error. Broker, topic metadata, transaction, and shutdown
errors result in a non-zero CLI exit.

## Testing

Run unit and ordinary regression tests without requiring Docker:

```powershell
python -m pytest -q
python -m compileall -q src tests
python -m pip check
```

The live processor integration tests are opt-in. They publish unique synthetic
records to the existing `sensor-events` topic, verify validated and DLQ
outputs, close one processor instance, create another in the same Python test
process, and verify duplicate suppression from the restored ledger. They also
verify a near-limit raw event and its Base64-preserving DLQ record through the
processor, plus a full 4 MiB DLQ envelope published and read back using the
configured 6 MiB Kafka record and fetch limits. They also verify that an
aborted transaction is hidden from read-committed consumers. They leave test
records in Kafka; they do not delete topics, records, or volumes.

The instance-recreation test does not simulate an OS-level process crash or
forced termination, and does not test a broker restart while the processor is
unavailable. The separately gated broker-restart test verifies producer
recovery only; it is not evidence of processor crash recovery.

```powershell
$env:ATMOSYNC_RUN_KAFKA_INTEGRATION = "1"
python -m pytest -q tests/integration/test_kafka_processor_integration.py
Remove-Item Env:ATMOSYNC_RUN_KAFKA_INTEGRATION
```

Run this only when the local Kafka broker and all four DE-04A topics are
available. Without the environment variable, the integration test is skipped.

## Limitations

- The local broker has one replica, plaintext local listeners, and no
  high-availability guarantee.
- Run one processor instance with the stable group/transaction ID settings;
  this implementation is not designed for multiple independently configured
  processors to share one global event-ID ledger.
- The fixed transaction ID fences other instances of this application, but
  cannot prevent unrelated applications using different transaction IDs from
  consuming the raw topic and writing duplicate results.
- Ledger recovery scans committed records from the compacted topic to its
  captured end offset; startup cost depends on retained ledger size and Kafka
  compaction state.
- Actual process-crash recovery remains unverified. It requires a dedicated
  process-level integration test that exercises forced processor termination
  and recovery; the current in-process instance-recreation test is not a
  substitute.
- Kafka transactions atomically cover Kafka outputs, the deduplication
  marker, and the input offset for consumers configured with
  `isolation.level=read_committed`. They do not cover external systems.
- DLQ records are routed for inspection only; retry handling and DLQ
  consumption belong to later milestones.
- Events from the DE-03 simulator are synthetic, not actual IoT measurements.
