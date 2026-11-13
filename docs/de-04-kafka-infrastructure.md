# DE-04A — Kafka Infrastructure

## Purpose and scope

DE-04A provides a local Apache Kafka broker for the AtmoSync event pipeline.
It configures Kafka, durable local broker storage, and idempotent topic
initialization only. It does not implement a Python producer, consumer,
transactional processor, validation processor, deduplication logic, retry
handling, or application-level DLQ behavior.

## Architecture

- Apache Kafka **3.9.1**, pinned in `docker-compose.yml`.
- One Kafka broker and controller in KRaft mode; no ZooKeeper.
- Docker Compose service: `kafka`.
- Persistent named volume: `kafka-data`, mounted at
  `/var/lib/kafka/data`.
- Host connection: `localhost:9092`.
- Compose-network connection: `kafka:29092`.
- Controller listener: `kafka:9093`, not published to the host.
- Plaintext listeners, suitable only for a trusted local development
  environment. There are no embedded credentials or secrets.
- Container health check calls Kafka's broker API versions command.

This is a local single-broker configuration, not a production or
high-availability deployment. Replication factor 1 means broker failure
cannot be tolerated without downtime. Kafka data durability also depends on
the host's storage and Docker volume lifecycle.

## Topics

Run `docker/kafka/init-topics.sh` inside the broker container. It uses
`--if-not-exists`, then verifies the existing or newly created topic's
partition count, replication factor, and explicit cleanup policy. A mismatch
causes a non-zero exit; the script intentionally does not alter incompatible
existing topics.

| Topic | Partitions | Replication factor | Cleanup policy |
|---|---:|---:|---|
| `sensor-events` | 1 | 1 | `delete` |
| `sensor-events-validated` | 6 | 1 | `delete` |
| `sensor-events-dlq` | 3 | 1 | `delete` |
| `sensor-event-dedup` | 1 | 1 | `compact` |

The compacted ledger topic is infrastructure only; no event-ID deduplication
behavior is implemented here.

## Requirements

- Docker Engine with Docker Compose v2 (`docker compose`).
- Network access to pull `apache/kafka:3.9.1` on first start.
- At least 2 GB of memory available to the Kafka container.
- Host port 9092 available.

No Python dependencies are added for this infrastructure task.

## Setup and verification

Run these commands from the repository root in PowerShell.

### Verification record

Verified on Windows with Docker Engine **29.9.3**, Docker Compose
**5.7.0**, and the pinned Apache Kafka **3.9.1** image:

- `docker compose config -q` completed successfully.
- Kafka started successfully, and its broker API health check reported
  `healthy`.
- The topic initializer completed successfully and verified all four topics:
  `sensor-events` (1 partition, `delete`), `sensor-events-validated`
  (6 partitions, `delete`), `sensor-events-dlq` (3 partitions, `delete`),
  and `sensor-event-dedup` (1 partition, `compact`). Each topic had
  replication factor 1.
- A valid test event was produced, Kafka was stopped and started, and the
  event was read back from `sensor-events` after restart.
- The topic initializer script was normalized to LF line endings after a
  Windows CRLF issue; its LF line endings were confirmed before execution.

These checks verify this local single-broker setup only. They do not establish
high availability or production readiness.

### Start and check broker health

```powershell
docker compose up -d kafka
docker compose ps
docker inspect --format="{{.State.Health.Status}}" atmosync-kafka
```

The health status should become `healthy`. Confirm Kafka is running in KRaft
mode by checking the broker log:

```powershell
docker compose logs kafka
```

The broker configuration sets the KRaft process roles to `broker,controller`
and defines a single voter in `KAFKA_CONTROLLER_QUORUM_VOTERS`.

### Initialize and inspect topics

```powershell
docker compose exec -T kafka /bin/bash /opt/kafka/scripts/init-topics.sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

The script reports each verified topic and exits non-zero on initialization
or verification failure. The detailed topic description should match the
partition counts and cleanup policies in the table above.

### Stop and restart

```powershell
docker compose stop kafka
docker compose start kafka
docker compose ps
docker inspect --format="{{.State.Health.Status}}" atmosync-kafka
docker compose exec -T kafka /opt/kafka/scripts/init-topics.sh
```

The `kafka-data` named volume is retained by `stop`, `start`, and
`docker compose down`. `docker compose down -v` explicitly deletes it.

### Verify persisted records

After topic initialization, record the current end offset, then send the
DE-02 sample event to the raw topic using Kafka's console utility. The
PowerShell example assigns a fresh event ID and serializes one compact JSON
line:

```powershell
$initialOffsetLine = docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sensor-events --time -1
$initialOffset = [int]($initialOffsetLine.Split(':')[-1])
$event = Get-Content -Raw data/sample_sensor_event.json | ConvertFrom-Json
$event.event_id = [guid]::NewGuid().ToString()
$eventId = $event.event_id
$event | ConvertTo-Json -Compress -Depth 10 | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sensor-events
Write-Host "Sent event_id=$eventId"
```

Restart the broker and wait for health. In the same PowerShell session, read
the records written since the initial offset and search for the test event ID:

```powershell
$finalOffsetLine = docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sensor-events --time -1
$finalOffset = [int]($finalOffsetLine.Split(':')[-1])
$newMessageCount = $finalOffset - $initialOffset
if ($newMessageCount -lt 1) { throw "No new Kafka messages found since the initial offset" }
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-events --partition 0 --offset $initialOffset --max-messages $newMessageCount | Select-String -SimpleMatch $eventId
```

The output should contain the generated event ID. This procedure verifies
Kafka storage only; it is not an implemented AtmoSync producer.

## Topic and persistence details

Topic creation is idempotent for correctly configured existing topics.
Kafka does not change a topic's partition count through `--if-not-exists`;
the initializer detects incompatible existing partition counts and cleanup
policies and reports an error for an operator to resolve.

The named volume protects broker log data across container recreation while
the volume is retained. It is not a backup. Removing the volume destroys the
local Kafka data.

## Troubleshooting

- **Health remains `starting` or becomes `unhealthy`:** inspect
  `docker compose logs kafka`, Docker resource availability, and port 9092.
- **Host client cannot connect:** use `localhost:9092`; the broker advertises
  this listener for host clients.
- **Another Compose service cannot connect:** use `kafka:29092`, not
  `localhost:9092`.
- **Topic initializer reports a mismatch:** inspect with `kafka-topics.sh
  --describe`. It does not modify existing topic settings.
- **KRaft storage errors:** retain the named volume and inspect broker logs.
  Do not remove the volume to troubleshoot unless its data may be discarded.

## Local limitations

This setup has one broker, one controller, no TLS or authentication, and a
single local persistent volume. It has no broker redundancy, production
security controls, backup, monitoring stack, or performance guarantees.
Do not expose it to untrusted networks or use it as a production Kafka
deployment.
