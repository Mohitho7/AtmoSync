"""Operational metrics consumer for the validated event stream."""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import (
    ConsumerConfig,
    DEFAULT_MONITORING_GROUP_ID,
    KafkaEventConsumer,
)
from .consumer_cli import run_consumer_cli

LOGGER = logging.getLogger(__name__)


class OperationalMetricsConsumer(KafkaEventConsumer):
    """Track process-local event counts and rates without external services."""

    def __init__(
        self,
        config: ConsumerConfig | None = None,
        *,
        consumer_client: Any | None = None,
    ) -> None:
        self._started_at = time.monotonic()
        self._processed_count = 0
        self._counts_by_container: dict[str, int] = {}
        super().__init__(
            config or ConsumerConfig(group_id=DEFAULT_MONITORING_GROUP_ID),
            event_handler=self._record_event,
            consumer_client=consumer_client,
        )

    @property
    def metrics(self) -> dict[str, Any]:
        elapsed = max(time.monotonic() - self._started_at, 1e-9)
        return {
            "processed_count": self._processed_count,
            "counts_by_container": self._counts_by_container.copy(),
            "elapsed_seconds": elapsed,
            "events_per_second": self._processed_count / elapsed,
        }

    def _record_event(self, event: dict[str, Any]) -> None:
        self._processed_count += 1
        container_id = event["container_id"]
        self._counts_by_container[container_id] = (
            self._counts_by_container.get(container_id, 0) + 1
        )
        LOGGER.info(
            "sensor_event_metric",
            extra={
                "consumer_group": self.config.group_id,
                "event_id": event["event_id"],
                "container_id": container_id,
                "processed_count": self._processed_count,
                "container_count": self._counts_by_container[container_id],
            },
        )


def main(argv: list[str] | None = None) -> int:
    return run_consumer_cli(
        OperationalMetricsConsumer,
        default_group_id=DEFAULT_MONITORING_GROUP_ID,
        group_env_var="ATMOSYNC_MONITORING_CONSUMER_GROUP_ID",
        description="Monitor validated sensor event processing metrics.",
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
