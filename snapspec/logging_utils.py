from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

_LOG_START_MONOTONIC = time.monotonic()


class _HybridFormatter(logging.Formatter):
    """Render structured event logs cleanly while preserving legacy logs."""

    def format(self, record: logging.LogRecord) -> str:
        if getattr(record, "structured", False):
            return record.getMessage()
        return super().format(record)


def _format_elapsed(elapsed_s: float) -> str:
    minutes = int(elapsed_s // 60)
    seconds = elapsed_s - (minutes * 60)
    return f"{minutes:02d}:{seconds:04.1f}"


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (list, tuple, set)):
        return "[" + ",".join(str(item) for item in value) + "]"
    return str(value)


def format_structured_event(component: str, event: str, **fields: Any) -> str:
    elapsed = time.monotonic() - _LOG_START_MONOTONIC
    prefix = f"[{_format_elapsed(elapsed)}] {component:<12} | {event:<18}"
    if not fields:
        return prefix
    field_text = " ".join(
        f"{key}={_format_value(value)}"
        for key, value in fields.items()
        if value is not None
    )
    return f"{prefix} | {field_text}"


def log_event(
    logger: logging.Logger,
    *,
    component: str,
    event: str,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    logger.log(
        level,
        format_structured_event(component, event, **fields),
        extra={"structured": True},
    )


def configure_logging(
    *,
    default_basename: str,
    log_dir_env: str = "SNAPSPEC_LOG_DIR",
    log_path_env: str = "SNAPSPEC_LOG_PATH",
    log_basename_env: str = "SNAPSPEC_LOG_BASENAME",
    level: int = logging.INFO,
) -> str | None:
    """Configure root logging to stdout and, optionally, a file."""

    log_path = os.environ.get(log_path_env)
    if not log_path:
        log_dir = os.environ.get(log_dir_env)
        if log_dir:
            basename = os.environ.get(log_basename_env, default_basename)
            log_path = os.path.join(log_dir, f"{basename}.log")

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))

    formatter = _HybridFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(level=level, handlers=handlers, force=True)
    return log_path
