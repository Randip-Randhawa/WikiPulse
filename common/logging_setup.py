"""Structured logging configuration shared by all WikiPulse services."""

from __future__ import annotations

import logging
import sys

from common.config import get_settings

_CONFIGURED = False


def configure_logging(service_name: str) -> logging.Logger:
    """Configure root logging once per process and return a logger scoped
    to `service_name`. Safe to call multiple times."""
    global _CONFIGURED
    settings = get_settings()

    if not _CONFIGURED:
        level = getattr(logging, settings.logging.level.upper(), logging.INFO)
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)

        root = logging.getLogger()
        root.setLevel(level)
        # Avoid duplicate handlers if reconfigured in the same process.
        root.handlers = [handler]
        _CONFIGURED = True

    return logging.getLogger(service_name)
