"""Publish DE-02-validated sensor events from the simulator to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from typing import Any

from confluent_kafka import KafkaException, Producer

from .contracts import validate_sensor_event
from .simulator import SensorSimulator

DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "sensor-events"
DEFAULT_FLUSH_TIMEOUT = 20.0
DEFAULT_QUEUE_FULL_TIMEOUT = 15.0
MAX_SENSOR_KAFKA_RECORD_BYTES = 2 * 1024 * 1024

LOGGER = logging.getLogger(__name__)


class ProducerError(RuntimeError):
    """Raised when an event cannot be queued or delivery is not confirmed."""


def _positive_timeout(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a positive finite number")
    try:
        timeout = float(value)
    except OverflowError as error:
        raise ValueError(f"{field_name} must be a positive finite number") from error
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError(f"{field_name} must be a positive finite number")
    return timeout


class KafkaSensorProducer:
    """Serialize, queue, and confirm delivery of sensor events to Kafka."""

    def __init__(
        self,
        bootstrap_server: str = DEFAULT_BOOTSTRAP_SERVER,
        topic: str = DEFAULT_TOPIC,
        *,
        flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
        queue_full_timeout: float = DEFAULT_QUEUE_FULL_TIMEOUT,
        client: Any | None = None,
    ) -> None:
        if not isinstance(bootstrap_server, str) or not bootstrap_server.strip():
            raise ValueError("bootstrap_server must be a non-empty string")
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("topic must be a non-empty string")

        self.topic = topic
        self.flush_timeout = _positive_timeout(flush_timeout, "flush_timeout")
        self.queue_full_timeout = _positive_timeout(
            queue_full_timeout, "queue_full_timeout"
        )
        self._producer = (
            Producer(
                {
                    "bootstrap.servers": bootstrap_server,
                    "enable.idempotence": True,
                    "acks": "all",
                    "message.send.max.retries": 5,
                    "retry.backoff.ms": 500,
                    "message.timeout.ms": 15000,
                    "queue.buffering.max.messages": 10000,
                    "queue.buffering.max.kbytes": 10240,
                    "message.max.bytes": MAX_SENSOR_KAFKA_RECORD_BYTES,
                }
            )
            if client is None
            else client
        )
        self._delivered_count = 0
        self._delivery_failure_count = 0
        self._last_delivery_error: str | None = None
        self._closed = False

    @property
    def delivered_count(self) -> int:
        """Number of events confirmed by successful delivery callbacks."""
        return self._delivered_count

    def _on_delivery(self, event_id: str, error: Any, message: Any) -> None:
        if error is not None:
            self._delivery_failure_count += 1
            self._last_delivery_error = str(error)
            LOGGER.error("Kafka delivery failed for event %s: %s", event_id, error)
            return

        self._delivered_count += 1
        LOGGER.info(
            "Kafka delivered event_id=%s topic=%s partition=%s offset=%s",
            event_id,
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def publish(self, event: dict[str, Any]) -> None:
        """Queue one validated event; delivery is confirmed later by callback."""
        if self._closed:
            raise ProducerError("producer is closed")
        validate_sensor_event(event)
        event_id = event["event_id"]
        try:
            value = json.dumps(
                event,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise ProducerError(
                f"could not serialize event {event_id}: {error}"
            ) from error

        def on_delivery(error: Any, message: Any) -> None:
            self._on_delivery(event_id, error, message)

        deadline = time.monotonic() + self.queue_full_timeout
        warned_queue_full = False
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=event_id.encode("utf-8"),
                    value=value,
                    on_delivery=on_delivery,
                )
                self._producer.poll(0)
                return
            except BufferError as error:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProducerError(
                        f"Kafka producer queue remained full for event {event_id}"
                    ) from error
                if not warned_queue_full:
                    LOGGER.warning("Kafka producer queue is full; waiting for capacity")
                    warned_queue_full = True
                self._producer.poll(min(0.1, remaining))

    def close(self, timeout: float | None = None) -> int:
        """Flush pending messages and raise unless all deliveries are confirmed."""
        if self._closed:
            return self._delivered_count
        flush_timeout = (
            self.flush_timeout
            if timeout is None
            else _positive_timeout(timeout, "flush_timeout")
        )
        self._closed = True
        remaining = self._producer.flush(flush_timeout)
        errors: list[str] = []
        if remaining:
            errors.append(f"{remaining} message(s) remained undelivered after flush")
        if self._delivery_failure_count:
            errors.append(
                f"{self._delivery_failure_count} message(s) failed delivery; "
                f"last error: {self._last_delivery_error}"
            )
        if errors:
            raise ProducerError("; ".join(errors))
        return self._delivered_count


def publish_finite(
    simulator: SensorSimulator, producer: KafkaSensorProducer, steps: int
) -> None:
    """Publish a finite number of simulation intervals without real-time waits."""
    if isinstance(steps, bool) or not isinstance(steps, int) or steps < 0:
        raise ValueError("steps must be a non-negative integer")
    for _ in range(steps):
        for event in simulator.generate_events(steps=1):
            producer.publish(event)


def publish_continuously(
    simulator: SensorSimulator, producer: KafkaSensorProducer
) -> None:
    """Publish simulator events until the stream is interrupted."""
    for event in simulator.stream():
        producer.publish(event)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish synthetic AtmoSync sensor events to Kafka."
    )
    parser.add_argument("--config", help="Path to a simulator configuration JSON.")
    parser.add_argument(
        "--steps",
        type=int,
        help="Number of simulator intervals to publish (default: 1).",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Publish continuously until interrupted with Ctrl+C.",
    )
    parser.add_argument("--seed", type=int, help="Override the simulator random seed.")
    parser.add_argument(
        "--bootstrap-server",
        default=os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVER
        ),
        help="Kafka bootstrap server(s); defaults to ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS "
        "or localhost:9092.",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("ATMOSYNC_KAFKA_TOPIC", DEFAULT_TOPIC),
        help="Kafka destination topic; defaults to ATMOSYNC_KAFKA_TOPIC or sensor-events.",
    )
    parser.add_argument(
        "--flush-timeout",
        type=float,
        default=DEFAULT_FLUSH_TIMEOUT,
        help=f"Maximum flush wait in seconds (default: {DEFAULT_FLUSH_TIMEOUT:g}).",
    )
    parser.add_argument(
        "--queue-full-timeout",
        type=float,
        default=DEFAULT_QUEUE_FULL_TIMEOUT,
        help=(
            "Maximum wait for producer queue capacity in seconds "
            f"(default: {DEFAULT_QUEUE_FULL_TIMEOUT:g})."
        ),
    )
    args = parser.parse_args(argv)
    if args.continuous and args.steps is not None:
        parser.error("--steps cannot be used with --continuous")
    if not args.continuous and args.steps is not None and args.steps < 0:
        parser.error("--steps must be non-negative")
    if not args.bootstrap_server.strip():
        parser.error("--bootstrap-server must be non-empty")
    if not args.topic.strip():
        parser.error("--topic must be non-empty")
    for field in ("flush_timeout", "queue_full_timeout"):
        try:
            _positive_timeout(getattr(args, field), field.replace("_", "-"))
        except ValueError as error:
            parser.error(str(error))
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the producer CLI; logs and diagnostics are written to stderr."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    args = _parse_args(argv)
    try:
        simulator = SensorSimulator(args.config, seed=args.seed)
    except (OSError, ValueError) as error:
        LOGGER.error("Simulator configuration error: %s", error)
        return 2

    try:
        producer = KafkaSensorProducer(
            args.bootstrap_server,
            args.topic,
            flush_timeout=args.flush_timeout,
            queue_full_timeout=args.queue_full_timeout,
        )
    except (KafkaException, ValueError) as error:
        LOGGER.error("Could not initialize Kafka producer: %s", error)
        return 1

    exit_code = 0
    try:
        if args.continuous:
            publish_continuously(simulator, producer)
        else:
            publish_finite(
                simulator,
                producer,
                steps=1 if args.steps is None else args.steps,
            )
    except KeyboardInterrupt:
        LOGGER.info("Interrupted; flushing queued Kafka messages.")
    except (KafkaException, ProducerError, OSError, ValueError) as error:
        LOGGER.error("Kafka publishing failed: %s", error)
        exit_code = 1
    finally:
        try:
            delivered = producer.close(args.flush_timeout)
        except (KafkaException, ProducerError, ValueError) as error:
            LOGGER.error("Kafka shutdown did not confirm all deliveries: %s", error)
            exit_code = 1
        else:
            if exit_code == 0:
                LOGGER.info("Confirmed delivery of %d event(s).", delivered)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
