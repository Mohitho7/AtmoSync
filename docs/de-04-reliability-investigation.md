# DE-04 Reliability Investigation

## Scope and conclusion

This investigation traces the retry-lag, duplicate-rate, and latency/
throughput figures emitted by the live reliability test. It inspected the
processor, reliability and processor integration tests, and the retained
Kafka retry partition log. The current reliability test additionally checks
read-committed application backlog and reads the validated, retry, and DLQ
outputs produced by its workload.

No DE-04 runtime defect was found in the investigated behavior, and no
production-code change was made for these findings. The earlier retry-lag
value was a physical-offset difference caused by a transactional COMMIT
marker. The current test instead counts read-committed application records.
The duplicate rate matches the deliberately constructed input, and the
latency/throughput figures describe a small end-to-end test batch, not
steady-state service performance.

## Finding 1: Retry lag of one

### Result

The reported `consumer_lag_retry = 1` did not represent an unprocessed retry
record in the observed run. The previously tentative explanation has now
been checked against the Kafka partition log.

The live broker evidence for that run was:

- The test group's raw-topic committed offset was `683`, equal to the raw
  topic's end offset.
- The test group's retry-topic committed offset was `31`.
- The retry topic's physical high watermark was `32`.
- The committed retry record at offset `30` had source raw offset `678`.
- A direct Kafka log dump identified offset `31` as an
  `isTransactional: true`, `isControl: true` record with
  `endTxnMarker: COMMIT`.
- The integration test observed the retry's final processing outcome and
  required all 35 outcomes before reading committed offsets.

Kafka committed offsets represent the next application record to consume.
Therefore committed offset `31` confirms the record at offset `30` was
processed. The high-watermark subtraction (`32 - 31`) counts the COMMIT
control record at offset `31`, not another retry payload. This was directly
verified in the retained Kafka log with `kafka-dump-log.sh`; it is not
inferred from the offset arithmetic alone.

The earlier test version called the high-watermark difference
`consumer_lag_retry`; that was physical-offset distance, not proof that a
committed retry event remained available to the consumer. The current test
measures backlog by assigning a `read_committed` consumer at the final group
committed offset and counting returned application records until partition
EOF. Transaction control records are not returned or counted. In the latest
measured run, raw and retry application backlog were both zero.

### Retry scheduling, eventual processing, and shutdown

In [`processor.py`](../src/atmo_sync/processor.py), `_handle_transient_failure`
publishes a bounded retry envelope and the input offset in the same Kafka
transaction. `process_one` defers a retry until `next_attempt_at`, and
`process_message` commits the retry record's offset with the resulting
validated output and dedup marker. The offset is sent as
`message.offset() + 1` with consumer group metadata.

The reliability test configures `retry_base_delay_seconds=0`, injects one
transient failure, expects one `RETRY_SCHEDULED` outcome, and expects
`len(workload) + 1` outcomes. Its retry processing must have returned from
`process_message` before the loop can finish. The committed offset query is
also performed before processor shutdown. Shutdown therefore does not
explain the observed one-position difference.

The separate
[`test_retry_survives_processor_restart_and_is_published_once`](../tests/integration/test_kafka_processor_integration.py)
uses a nonzero retry delay, gracefully closes the first processor after
transactional retry publication, creates a replacement instance in the same
Python test process, and verifies eventual validation and exactly one matching
retry record. This is integration evidence for delayed retry recovery after
graceful instance recreation, distinct from the performance test's zero-delay
case. It does not simulate an OS-level process crash, forced termination, or a
broker restart while the processor is unavailable.

### Fix required?

No retry-runtime fix is required based on this evidence. The test measurement
was updated to count consumer-visible application records rather than
subtracting physical offsets. Offset 31 remains identified from the retained
log as the COMMIT control record for the previously measured run; that
specific observation is not used as a general lag-accounting rule.

## Finding 2: Duplicate rate of 0.088235

### Exact numerator and denominator

The test uses `SensorSimulator(seed=4401).generate_events(steps=10)`. The
current simulator configuration has three containers, so this creates 30
original events. It then builds the raw workload as:

- 30 original events;
- 3 copies of the first three events; and
- 1 separately invalidated event.

Thus `raw_records = len(workload) = 34`. The duplicate outcome count is 3,
and the rate is exactly `3 / 34 = 0.088235294...`, rounded to `0.088235`.
The retried delivery is an additional processor outcome, not another raw
input record in this denominator.

### Output and restart behavior

In `process_message`, an event with the same canonical ID and payload hash
as the restored ledger is handled by the `known_hash == event_hash` branch.
That branch commits the input offset with an empty output list and returns
`DUPLICATE`; it does not write to the validated topic.

The current performance test asserts three `DUPLICATE` outcomes and 30
`VALIDATED` outcomes. It also reads committed output records beginning at
the captured topic offsets and verifies that the 30 expected event IDs each
appear exactly once in the validated topic. It checks for exactly one
matching retry envelope and one DLQ record for the invalid event. Separate
live integration evidence in
[`test_validation_dlq_and_dedup_survive_processor_restart`](../tests/integration/test_kafka_processor_integration.py)
checks that:

1. the original valid event produced one validated record and one ledger
   marker;
2. the first processor instance was closed and a replacement, created within
   the same Python test process, restored the event ID from the ledger;
3. replaying that event returned `DUPLICATE`; and
4. exactly one matching validated record remained.

Validated output, the dedup marker, and the input offset are committed in
one Kafka transaction. These test assertions and the restart regression test
provide evidence against duplicate validated output in the exercised
single-processor scenarios after graceful instance recreation. They do not
demonstrate recovery from actual OS-level process crash or forced termination.
The separately gated broker-restart test covers producer recovery only, not
processor crash recovery. A dedicated process-level integration test is
required to verify that scenario. These results are not a claim of end-to-end
exactly-once behavior for arbitrary downstream side effects.

### Fix required?

No deduplication fix is indicated. The rate's numerator and denominator
match the deliberately constructed workload. The current reliability test
checks the generated workload's validated output directly; the separate
output/recreation integration test covers deduplication after graceful
processor-instance recreation in the same test process.

## Finding 3: Latency and throughput

### How the test measures them

In
[`test_sustained_batch_reports_throughput_latency_lag_and_failure_rates`](../tests/integration/test_kafka_reliability_integration.py):

- Each `published_at` timestamp is captured immediately before calling
  `Producer.produce`, not in the successful delivery callback.
- Per-record latency is measured when the processor call returns, after the
  processing transaction has committed.
- A `RETRY_SCHEDULED` outcome is omitted from latency samples. The later
  retry outcome uses the envelope's original raw source offset to find the
  original publish timestamp, so retry waiting/processing time is included
  in that raw event's final latency.
- The configured retry delay for this test is zero. In a run with a
  nonzero configured delay, the scheduled delay would be part of the retry
  event's measured latency.
- `duration_seconds` starts before raw publication and ends after the
  processor closes. It includes producer flush, processor initialization
  (including ledger restoration), poll/queue time, processing transactions,
  the committed-offset query, and shutdown/flush.
- `throughput_raw_records_per_second` is `34 / duration_seconds`. The
  numerator is raw input records; it excludes the additional retry
  processing attempt. The denominator includes processor startup and
  shutdown. This is an end-to-end batch rate, not steady-state processor
  throughput.
- p50 uses `statistics.median`. p95 uses the test's sorted-list index
  `int((n - 1) * 0.95)`, i.e. a lower-index empirical percentile for this
  small sample.

Consequently, the reported p50/p95 are end-to-end per-input completion
latencies under a queued batch, not isolated Kafka processing times. They
include queueing behind earlier records and lifecycle overhead; the retrying
event's sample also follows its retry path.

### Actual measurements

These are live measurements from the test, not projections. The local
environment was Docker Kafka 3.9.1 with one broker.

| Run | Raw records | Duration (s) | Raw input throughput (records/s) | p50 latency (ms) | p95 latency (ms) | Lag measurement |
|---|---:|---:|---:|---:|---:|---|
| Earlier recorded run | 34 | 15.806566 | 2.151 | 10,563.118 | 15,587.358 | Physical offset difference: raw 0, retry 1 |
| Investigation rerun | 34 | 13.440237 | 2.530 | 9,860.499 | 13,327.001 | Physical offset difference: raw 0, retry 1 |
| Previous current-tree run | 34 | 6.305315 | 5.392 | 3,361.655 | 6,198.476 | Physical offset difference: raw 0, retry 1 |
| Latest read-committed verification | 34 | 6.537990 | 5.200 | 5,441.153 | 6,438.042 | Application backlog: raw 0, retry 0 |

The latest run completed with `1 passed` for the targeted live reliability
test. It verified 30 unique validated events, one retry envelope, one
matching DLQ record, and zero read-committed application backlog for both
raw and retry inputs. There was no performance or runtime code fix between
the measured runs; differences are run-to-run variation, not a before/after
improvement.

### Representativeness and fix required?

The workload is useful as a safe local functional/reliability probe: it
includes valid events, duplicates, one invalid event, and one injected
transient failure. It is not representative of a sustained production
throughput or latency benchmark. It is small, serial, single-partition raw
input against one local broker, creates a new processor group, and includes
startup and shutdown in its duration. The percentile sample is only 34 raw
events.

No processor-performance fix is justified by these numbers. If performance
claims are needed later, use a separate benchmark with warm-up, a larger
controlled workload, explicit steady-state timing boundaries, and
consumer-visible lag measurements. Do not present these current values as
production capacity or service-level latency.

## Remaining limitations

- The local broker has one node; these results do not characterize broker
  failover or production networking.
- The old physical-offset retry-lag value is historical only. The current
  test measures read-committed application backlog, but only for its small
  workload and at the captured group offsets.
- The current workload's latency and throughput are batch/lifecycle
  measurements, not steady-state service metrics.
- The test verifies output uniqueness for its generated workload and a
  separate processor integration test covers deduplication after graceful
  instance recreation in the same test process; neither establishes recovery
  after actual process crash, forced termination, or broker restart while the
  processor is unavailable, nor exactly-once behavior for external side
  effects.
- No runtime source code or Kafka record, topic, or volume was changed or
  removed for this investigation. The later reliability-test updates were
  test-only measurement and output-verification changes.
