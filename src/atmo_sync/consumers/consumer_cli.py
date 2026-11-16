"""Shared command-line lifecycle for the downstream Kafka consumers."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Type

from confluent_kafka import KafkaException

from .base import (
    ConsumerConfig,
    ConsumerError,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_VALIDATED_TOPIC,
    KafkaEventConsumer,
)

LOGGER = logging.getLogger(__name__)


def run_consumer_cli(
    consumer_type: Type[KafkaEventConsumer],
    *,
    default_group_id: str,
    group_env_var: str,
    description: str,
    argv: list[str] | None = None,
) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
        ),
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get(
            "ATMOSYNC_KAFKA_VALIDATED_TOPIC", DEFAULT_VALIDATED_TOPIC
        ),
    )
    parser.add_argument(
        "--group-id",
        default=os.environ.get(
            group_env_var,
            os.environ.get("ATMOSYNC_KAFKA_CONSUMER_GROUP_ID", default_group_id),
        ),
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        help="Stop after processing this many records; omit for continuous mode.",
    )
    args = parser.parse_args(argv)
    if args.max_messages is not None and args.max_messages < 0:
        parser.error("--max-messages must be non-negative")
    try:
        config = ConsumerConfig(
            bootstrap_servers=args.bootstrap_servers,
            group_id=args.group_id,
            topic=args.topic,
        )
    except ValueError as error:
        parser.error(str(error))

    consumer: KafkaEventConsumer | None = None
    result = 0
    try:
        consumer = consumer_type(config)
        processed = consumer.run(max_messages=args.max_messages)
        LOGGER.info(
            "consumer_stopped",
            extra={"consumer_group": config.group_id, "processed_count": processed},
        )
    except KeyboardInterrupt:
        LOGGER.info("Consumer interrupted; closing without an unprocessed offset commit.")
    except (KafkaException, ConsumerError, ValueError) as error:
        LOGGER.error("Consumer stopped: %s", error)
        result = 1
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except ConsumerError as error:
                LOGGER.error("Consumer shutdown error: %s", error)
                result = 1
    return result
