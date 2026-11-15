# DE-04B — Python Kafka Producer Integration

## Objective and scope

DE-04B publishes the existing DE-03 synthetic sensor events to the local
Kafka `sensor-events` topic. It adds a reusable producer and a separate CLI;
the simulator's generation logic and the DE-02 contract definitions remain
unchanged. This is not a producer for physical IoT readings.

This milestone does not implement transactional validation, event-ID
deduplication, consumer groups, retry topics, DLQ processing, Snowflake,
dbt, or dashboards. Kafka's idempotent producer setting handles safe Kafka
retries within a producer session; it is not an application-level deduplication
ledger.

## Architecture

1. `SensorSimulator` loads the existing simulator configuration, generates
   events, and validates each event using `validate_sensor_event()`.
2. The producer validates each event again before serialization, preserving
   the existing DE-02 rules at the publishing boundary.
3. Each event is serialized as compact UTF-8 JSON with its original fields.
   The canonical `event_id`, encoded as UTF-8, is used as the Kafka message
   key.
4. `confluent-kafka` queues messages asynchronously. Delivery callbacks
   confirm success or record failure; a successful `produce()` queue
   operation alone is not reported as delivery.
5. On normal exit or Ctrl+C, the CLI flushes pending messages and exits with
   an error if delivery callbacks report failures or messages remain queued.

The producer is available as `atmo_sync.producer.KafkaSensorProducer` and
through the `atmo_sync.producer` module CLI. It does not change or add a Kafka
dependency to the existing simulator module's runtime path.

## Dependency and configuration

The project declares `confluent-kafka>=2.6,<3.0` in `pyproject.toml` and
`requirements.txt`. Install the project and its producer command with:

```powershell
python -m pip install -e .
```

Defaults:

| Setting | Default | Environment override | CLI option |
|---|---|---|---|
| Kafka broker | `localhost:9092` | `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS` | `--bootstrap-server` |
| Topic | `sensor-events` | `ATMOSYNC_KAFKA_TOPIC` | `--topic` |
| Simulator config | Bundled simulator configuration | — | `--config` |
| Random seed | Configured simulator seed | — | `--seed` |
| Flush timeout | 20 seconds | — | `--flush-timeout` |
| Queue-full wait timeout | 15 seconds | — | `--queue-full-timeout` |

CLI arguments override environment variables. Simulator configuration,
random seed, and finite/continuous behavior match DE-03. A finite invocation
without `--steps` publishes one simulation interval.

Producer delivery configuration enables idempotence, `acks=all`, up to five
message retries, a 15-second message timeout, a 2 MiB maximum record size, and
bounded producer buffering. The client limit accommodates the raw topic's
1 MiB record cap. This local Kafka deployment has replication factor 1, so
`acks=all` does not provide broker redundancy.

## Finite execution

`--steps` counts simulation intervals; the simulator emits one event per
configured container per interval. Finite generation does not wait for the
configured real-time sensor interval.

```powershell
python -m atmo_sync.producer --steps 3
python -m atmo_sync.producer --steps 3 --seed 42 --topic sensor-events
```

The installed console entry point is also available:

```powershell
atmosync-producer --steps 3
```

## Continuous execution

Continuous mode uses the simulator's existing stream, including its
configured delay between intervals. Stop with Ctrl+C; the CLI then flushes
pending messages before exiting.

```powershell
python -m atmo_sync.producer --continuous
python -m atmo_sync.producer --continuous --bootstrap-server localhost:9092
```

## Delivery and error handling

- Delivery success is counted only in the Kafka delivery callback.
- Delivery errors are logged to standard error and cause a non-zero CLI exit.
- When the local producer queue is full, the publisher polls callbacks and
  retries the same event until queue capacity returns or the bounded
  queue-full timeout expires.
- Serialization and event validation failures stop publishing rather than
  silently dropping or altering the event.
- Broker connection failures are surfaced through delivery callbacks and
  flush results. A flush timeout reports the number of messages still
  undelivered.
- Diagnostics are written to standard error; the producer does not emit
  event JSON to standard output.

The client retries are finite and there are no retry topics or dead-letter
processing in this milestone.

## Testing

Run all unit tests, including the mocked producer tests:

```powershell
python -m pytest -q
```

The local Kafka integration test is in `tests/integration/` and is skipped
unless explicitly enabled. Ensure Docker Compose Kafka is healthy and
`sensor-events` exists, then run:

```powershell
$env:ATMOSYNC_RUN_KAFKA_INTEGRATION = "1"
python -m pytest -q tests/integration
Remove-Item Env:ATMOSYNC_RUN_KAFKA_INTEGRATION
```

The integration test publishes a generated event and reads it back from
`sensor-events`, checking its key and DE-02-valid JSON payload.

## Known limitations

- Events are synthetic and are not measurements from physical sensors.
- Delivery confirmation is Kafka producer acknowledgment, not downstream
  processing or persistence outside the local broker.
- The local broker has one replica, plaintext listeners, and no high
  availability.
- There is no transactional validation, deduplication ledger, retry topic,
  DLQ application logic, consumer, or warehouse integration.
