#!/usr/bin/env bash
set -Eeuo pipefail

bootstrap_server="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"
kafka_bin="${KAFKA_BIN_DIR:-/opt/kafka/bin}"

topics=(
  "sensor-events:1:delete"
  "sensor-events-validated:6:delete"
  "sensor-events-dlq:3:delete"
  "sensor-event-dedup:1:compact"
)

fail() {
  printf 'topic initialization error: %s\n' "$*" >&2
  exit 1
}

for specification in "${topics[@]}"; do
  IFS=: read -r topic partitions cleanup_policy <<<"$specification"

  printf 'Creating topic if needed: %s (%s partitions, cleanup.policy=%s)\n' \
    "$topic" "$partitions" "$cleanup_policy"
  if ! "$kafka_bin/kafka-topics.sh" \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "cleanup.policy=$cleanup_policy"; then
    fail "could not create or locate topic '$topic'"
  fi

  description="$("$kafka_bin/kafka-topics.sh" \
    --bootstrap-server "$bootstrap_server" \
    --describe \
    --topic "$topic")" || fail "could not describe topic '$topic'"

  if ! grep -Eq "PartitionCount:[[:space:]]*${partitions}([[:space:]]|$)" <<<"$description"; then
    fail "topic '$topic' does not have the required $partitions partitions: $description"
  fi
  if ! grep -Eq 'ReplicationFactor:[[:space:]]*1([[:space:]]|$)' <<<"$description"; then
    fail "topic '$topic' does not have replication factor 1: $description"
  fi
  if ! grep -Fq "cleanup.policy=$cleanup_policy" <<<"$description"; then
    fail "topic '$topic' does not have cleanup.policy=$cleanup_policy: $description"
  fi

  printf 'Verified topic: %s\n%s\n' "$topic" "$description"
done

printf 'All AtmoSync Kafka topics are present and correctly configured.\n'
