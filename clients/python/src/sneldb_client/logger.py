"""Lightweight logging wrapper mirroring the JavaScript client's interface."""

from __future__ import annotations

import logging
from typing import Any, Callable, Literal, Protocol

LogLevel = Literal["trace", "debug", "info", "warn", "error"]

TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


class LoggerProtocol(Protocol):
    def trace(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    def warn(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


LOG_LEVEL_PRIORITY: dict[LogLevel, int] = {
    "trace": 0,
    "debug": 1,
    "info": 2,
    "warn": 3,
    "error": 4,
}


class BoundLogger:
    """Wraps a logging.Logger (or duck-typed object) with SnelDB's log levels."""

    def __init__(
        self,
        logger: Any | None = None,
        *,
        level: LogLevel = "info",
    ) -> None:
        self._logger = logger or _default_logger()
        self._level = level

    def trace(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self._enabled("trace"):
            self._log(TRACE_LEVEL, msg, *args, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self._enabled("debug"):
            self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self._enabled("info"):
            self._log(logging.INFO, msg, *args, **kwargs)

    def warn(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self._enabled("warn"):
            self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self._enabled("error"):
            self._log(logging.ERROR, msg, *args, **kwargs)

    def child(self, name: str) -> "BoundLogger":
        """Create a child logger anchored to the same Python logger."""
        if isinstance(self._logger, logging.Logger):
            base = self._logger.getChild(name)
        else:
            base = self._logger
        return BoundLogger(base, level=self._level)

    def _enabled(self, level: LogLevel) -> bool:
        return LOG_LEVEL_PRIORITY[level] >= LOG_LEVEL_PRIORITY[self._level]

    def _log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        try:
            if hasattr(self._logger, "log"):
                self._logger.log(level, msg, *args, **kwargs)
                return

            # Fall back to direct method invocation (duck typing)
            method_map: dict[int, Callable[..., Any]] = {
                TRACE_LEVEL: getattr(self._logger, "trace", None),
                logging.DEBUG: getattr(self._logger, "debug", None),
                logging.INFO: getattr(self._logger, "info", None),
                logging.WARNING: getattr(self._logger, "warn", None),
                logging.ERROR: getattr(self._logger, "error", None),
            }
            handler = method_map.get(level)
            if handler:
                handler(msg, *args, **kwargs)
        except Exception:
            # Never let logging failures bubble up into client code
            pass


def _default_logger() -> logging.Logger:
    logger = logging.getLogger("sneldb")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def create_logger(*, logger: Any | None = None, level: LogLevel = "info") -> BoundLogger:
    if isinstance(logger, BoundLogger):
        return logger
    return BoundLogger(logger, level=level)


__all__ = ["BoundLogger", "LogLevel", "LoggerProtocol", "create_logger"]
