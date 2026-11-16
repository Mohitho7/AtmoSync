"""Validated-event logging consumer."""

from __future__ import annotations

import logging
from typing import Any, Callable

from .base import (
    ConsumerConfig,
    DEFAULT_VALIDATED_GROUP_ID,
    KafkaEventConsumer,
)
from .consumer_cli import run_consumer_cli

LOGGER = logging.getLogger(__name__)


class ValidatedSensorConsumer(KafkaEventConsumer):
    """Consume and validate the event stream in its own consumer group."""

    def __init__(
        self,
        config: ConsumerConfig | None = None,
        *,
        consumer_client: Any | None = None,
        event_handler: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        super().__init__(
            config or ConsumerConfig(group_id=DEFAULT_VALIDATED_GROUP_ID),
            event_handler=self._log_event if event_handler is None else event_handler,
            consumer_client=consumer_client,
        )

    @staticmethod
    def _log_event(event: dict[str, Any]) -> None:
        LOGGER.info(
            "validated_event_consumed",
            extra={
                "event_id": event["event_id"],
                "container_id": event["container_id"],
                "shipment_id": event["shipment_id"],
            },
        )


def main(argv: list[str] | None = None) -> int:
    return run_consumer_cli(
        ValidatedSensorConsumer,
        default_group_id=DEFAULT_VALIDATED_GROUP_ID,
        group_env_var="ATMOSYNC_VALIDATED_CONSUMER_GROUP_ID",
        description="Consume validated sensor events.",
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
