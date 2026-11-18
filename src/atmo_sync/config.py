"""Shared environment loading for local commands and injected deployments."""

from __future__ import annotations

from dotenv import load_dotenv


def load_environment() -> None:
    """Load a nearby .env without overriding variables provided by the runtime."""
    load_dotenv(override=False)
