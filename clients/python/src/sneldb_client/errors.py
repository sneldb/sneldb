"""Custom exceptions raised by the SnelDB Python client."""

from __future__ import annotations

from typing import Any


class SnelDBError(Exception):
    """Base error for all client failures."""

    def __init__(self, message: str, *, context: Any | None = None) -> None:
        super().__init__(message)
        self.context = context


class ConnectionError(SnelDBError):
    """Raised when the client cannot reach the server."""


class AuthenticationError(SnelDBError):
    """Raised when authentication fails or credentials are missing."""


class AuthorizationError(SnelDBError):
    """Raised when the server denies access to a resource."""


class CommandError(SnelDBError):
    """Raised when the submitted command is invalid."""


class NotFoundError(SnelDBError):
    """Raised when the target resource does not exist."""


class ServerError(SnelDBError):
    """Raised for 5xx style failures."""


class ParseError(SnelDBError):
    """Raised when a response cannot be parsed."""


__all__ = [
    "AuthenticationError",
    "AuthorizationError",
    "CommandError",
    "ConnectionError",
    "NotFoundError",
    "ParseError",
    "SnelDBError",
    "ServerError",
]
