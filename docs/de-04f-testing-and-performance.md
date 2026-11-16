# DE-04F — Testing, Reliability, and Performance Evidence

## Test layout

Normal tests do not require Docker:

- `tests/test_contracts.py` and `tests/test_simulator.py`: DE-02 contract and
  DE-03 simulator behavior;
- `tests/test_producer.py`: mocked producer serialization, callbacks,
  queue-full handling, flush failures, CLI behavior, and simulator reuse;
- `tests/test_processor.py`: transactions, offsets, ledger restoration,
  DLQ size boundaries, retry behavior, and processor failure paths;
- `tests/test_retry.py`: retry policy and retry-envelope serialization; and
- `tests/test_consumers.py`: downstream consumer configuration, handlers,
  offsets, duplicate cache, aggregation, metrics, and shutdown.

Kafka integration tests under `tests/integration/` are opt-in through
`ATMOSYNC_RUN_KAFKA_INTEGRATION=1`. They use the existing local Kafka
deployment and leave records in its topics. They do not delete topics,
records, or volumes.

## Covered behaviors

The current tests exercise:

| Area | Evidence in the current tree |
|---|---|
| Producer | Successful delivery, delivery failure, queue saturation and timeout, serialization/validation failure, flush, and finite/continuous CLI behavior |
| Transactional processor | Validated output + dedup marker + input offset transaction, DLQ + offset, duplicate suppression, conflict routing, abort behavior, uncertain commit handling, ledger restore/tombstones, and topic-size checks |
| Retry | Retry scheduling, bounded exhaustion, permanent DLQ routing, publication failure without source offset commit, due-time pause/resume, duplicate input, and recovery after graceful processor-instance recreation |
| DLQ boundaries | Near-limit raw payload, Base64 byte preservation, envelope limit, and live maximum-envelope produce/read-back |
| Consumer groups | Independent groups each receive the complete committed stream; shared-group members receive non-empty, non-overlapping assignments; a controlled membership rebalance verifies exact aggregate delivery without loss or duplicate IDs; recreated consumer instances resume from committed offsets |
| Reliability | Small live workload checks retry and DLQ outcomes, exact validated output IDs, duplicate suppression, and read-committed application backlog; opt-in producer recovery after restarting the local broker |

The test named `test_existing_producer_recovers_after_local_broker_restart`
requires the additional environment variable
`ATMOSYNC_RUN_KAFKA_RESTART_TEST=1`. It is not enabled solely by
`ATMOSYNC_RUN_KAFKA_INTEGRATION=1`; the standard integration command skips
that broker-restart test unless the extra opt-in is set. A test that restarts
Kafka changes runtime availability but does not delete the data volume.

The processor restart tests close an instance and create its replacement
within the same Python test process. They do not simulate an OS-level process
crash, forced processor termination, or a broker restart while the processor
is unavailable. The separately gated broker-restart test covers producer
recovery only. Actual processor process-crash recovery remains unverified and
requires a dedicated process-level integration test.

## Running verification

From the repository root in PowerShell:

```powershell
python -m compileall -q src tests
python -m pip check
git diff --check
docker compose config --quiet
python -m pytest -q
$env:ATMOSYNC_RUN_KAFKA_INTEGRATION = "1"
python -m pytest -q tests/integration
```

For the separately gated broker-restart test:

```powershell
$env:ATMOSYNC_RUN_KAFKA_INTEGRATION = "1"
$env:ATMOSYNC_RUN_KAFKA_RESTART_TEST = "1"
python -m pytest -q tests/integration/test_kafka_reliability_integration.py::KafkaProducerRestartIntegrationTests
```

The reliability test's small workload includes 30 generated valid events,
three duplicate inputs, one invalid event, and one injected transient
failure. It verifies that the 30 expected event IDs appear exactly once in
validated output, that one retry envelope is published and processed, and
that one invalid event is represented in the DLQ. It is a functional
measurement probe, not a production load test.

## Actual local performance measurement

The live test reports its own measurements under
`ATMOSYNC_RUN_KAFKA_INTEGRATION=1`. The previously recorded observed results
were:

| Run | Environment | Raw records | Duration (s) | Raw-input throughput (records/s) | p50 latency (ms) | p95 latency (ms) | Raw application backlog | Retry application backlog |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Earlier recorded run | Docker Kafka 3.9.1, one broker | 34 | 15.806566 | 2.151 | 10,563.118 | 15,587.358 | 0 | 1 |
| Investigation rerun | Docker Kafka 3.9.1, one broker | 34 | 13.440237 | 2.530 | 9,860.499 | 13,327.001 | 0 | 1 |
| Current verification run | Docker Kafka 3.9.1, one broker | 34 | 6.305315 | 5.392 | 3,361.655 | 6,198.476 | 0 | 1 |
| Read-committed verification run | Docker Kafka 3.9.1, one broker | 34 | 6.537990 | 5.200 | 5,441.153 | 6,438.042 | 0 | 0 |

These are actual results from test runs, not forecasts or performance
targets. They must not be interpreted as production throughput or service
latency.

The final required verification sequence reported:

- `python -m compileall -q src tests`: exit 0.
- `python -m pip check`: exit 0, no broken requirements.
- `git diff --check`: exit 0.
- `docker compose config --quiet`: exit 0.
- `python -m pytest -q`: exit 0, 108 passed, 10 skipped, 49 subtests passed.
- `ATMOSYNC_RUN_KAFKA_INTEGRATION=1 python -m pytest -q tests/integration`:
  exit 0, 9 passed, 1 skipped.

The sole skipped live test is the producer broker-restart test, which also
requires `ATMOSYNC_RUN_KAFKA_RESTART_TEST=1`.
The focused consumer-group integration module reported 2 passed; the
controlled-rebalance test also passed in four additional consecutive live
runs.

### Measurement definitions and caveats

- The test starts its batch timer before publishing and stops it after
  processor close. Duration includes producer flush, processor initialization
  and ledger restore, processing, offset query, and shutdown.
- Throughput is `34 raw input records / duration`; the retried attempt is
  additional processor work but is not in the numerator.
- Each per-event timestamp is captured just before `produce()`. Latency ends
  when the processing call returns after its transaction completes.
- The retry scheduling outcome is omitted as a latency sample; the later
  retry completion is matched to the original raw offset, so any retry wait
  would be part of that event's final latency. In this workload the configured
  retry base delay and jitter are both zero.
- p50 is the median; p95 uses the test's finite-sample sorted index
  `int((n - 1) * 0.95)`.
- Samples include queueing and initialization, and there are only 34 raw
  inputs on one raw partition against a single local broker. This does not
  measure steady-state capacity.
- Application backlog is counted by assigning a `read_committed` consumer
  at the final group-committed offset and counting returned application
  records until partition EOF. Transaction control records are not returned
  and therefore are not counted as backlog. The count is bounded by an
  explicit timeout.

## Retry offset finding

The prior value `consumer_lag_retry = 1` was computed as physical Kafka
high-watermark `32` minus the group's committed offset `31`. The retry log
was inspected directly with Kafka's log-dump utility. It showed:

```text
baseOffset: 30 ... isTransactional: true isControl: false
offset: 30 ... source_offset: 678
baseOffset: 31 ... isTransactional: true isControl: true
offset: 31 ... endTxnMarker: COMMIT
```

Thus offset 31 is explicitly a Kafka transaction COMMIT control record, not
an application retry payload. A committed group position of 31 follows the
application record at offset 30; the physical high watermark of 32 includes
the control record at 31. The prior one-offset difference was not a pending
retry record in that run. The updated reliability test uses read-committed
application backlog instead of subtracting physical offsets; its retry
backlog result was 0.

This conclusion is specific to the inspected retained log and recorded
group. The reliability test should not generally interpret every
high-watermark gap on a transactional topic as an internal marker without
inspecting committed/read-committed state.

## Remaining verification gaps

- The standard integration suite does not enable the separate
  `ATMOSYNC_RUN_KAFKA_RESTART_TEST` test.
- The controlled rebalance test exercises two members and exact delivery
  across a membership change, but does not simulate broker failure or every
  possible Kafka rebalance protocol/race.
- The current reliability workload is too small and lifecycle-inclusive
  to qualify as a sustained performance benchmark.
- Consumer analytics and monitoring state is in-memory; external side
  effects would require their own idempotency/recovery design.
- Local Kafka uses one broker and replication factor 1, so broker failure
  stops service.
