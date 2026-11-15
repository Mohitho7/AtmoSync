# Local Kafka infrastructure

The root `docker-compose.yml` runs one Apache Kafka 3.9.1 broker in KRaft
mode. It does not use ZooKeeper. Docker Compose stores Kafka data in the
persistent named volume `kafka-data`.

Host applications connect to `localhost:9092`. Other Compose services connect
to `kafka:29092`. The controller listener is internal to the Compose network.
This plaintext configuration is for local development only.

Start the broker from the repository root:

```powershell
docker compose up -d kafka
docker compose ps
docker inspect --format="{{.State.Health.Status}}" atmosync-kafka
```

Wait for the health status to become `healthy`, then initialize and verify
topics:

```powershell
docker compose exec -T kafka /bin/bash /opt/kafka/scripts/init-topics.sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

The initialization script uses `--if-not-exists`, then checks partition
counts, replication factor, cleanup policy, and maximum record size. It
idempotently sets topic message-size limits (1 MiB for raw, validated, and
deduplication topics; 6 MiB for DLQ Kafka records). The processor separately
caps serialized DLQ JSON envelopes at 4 MiB, leaving room for keys and Kafka
record framing. It exits non-zero for partition or cleanup-policy mismatches.

Stop and restart without removing persisted data:

```powershell
docker compose stop kafka
docker compose start kafka
docker compose ps
docker inspect --format="{{.State.Health.Status}}" atmosync-kafka
docker compose exec -T kafka /opt/kafka/scripts/init-topics.sh
```

Do not use `docker compose down -v` if you want to retain the named volume.
`docker compose down` stops and removes the container/network but keeps
`kafka-data`.

For the full topic matrix and a persistence check procedure, see
[`docs/de-04-kafka-infrastructure.md`](../../docs/de-04-kafka-infrastructure.md).
