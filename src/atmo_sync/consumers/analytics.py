"""In-memory container aggregation consumer."""

from __future__ import annotations

import logging
from typing import Any

from .base import (
    ConsumerConfig,
    DEFAULT_ANALYTICS_GROUP_ID,
    KafkaEventConsumer,
)
from .consumer_cli import run_consumer_cli

LOGGER = logging.getLogger(__name__)
_READING_FIELDS = (
    "temperature_c",
    "humidity_pct",
    "vibration_g",
)


class AnalyticsAggregationConsumer(KafkaEventConsumer):
    """Maintain process-local aggregates from an independent event copy."""

    def __init__(
        self,
        config: ConsumerConfig | None = None,
        *,
        consumer_client: Any | None = None,
    ) -> None:
        self._aggregates: dict[str, dict[str, float | int]] = {}
        super().__init__(
            config or ConsumerConfig(group_id=DEFAULT_ANALYTICS_GROUP_ID),
            event_handler=self._aggregate_event,
            consumer_client=consumer_client,
        )

    @property
    def aggregates(self) -> dict[str, dict[str, float | int]]:
        return {key: value.copy() for key, value in self._aggregates.items()}

    def _aggregate_event(self, event: dict[str, Any]) -> None:
        container_id = event["container_id"]
        readings = event["readings"]
        aggregate = self._aggregates.setdefault(
            container_id,
            {
                "event_count": 0,
                "temperature_c_sum": 0.0,
                "humidity_pct_sum": 0.0,
                "vibration_g_sum": 0.0,
            },
        )
        aggregate["event_count"] += 1
        for field in _READING_FIELDS:
            aggregate[f"{field}_sum"] += readings[field]
        LOGGER.info(
            "analytics_aggregate_updated",
            extra={
                "consumer_group": self.config.group_id,
                "container_id": container_id,
                "event_count": aggregate["event_count"],
                "event_id": event["event_id"],
            },
        )


def main(argv: list[str] | None = None) -> int:
    return run_consumer_cli(
        AnalyticsAggregationConsumer,
        default_group_id=DEFAULT_ANALYTICS_GROUP_ID,
        group_env_var="ATMOSYNC_ANALYTICS_CONSUMER_GROUP_ID",
        description="Aggregate validated sensor events by container.",
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
