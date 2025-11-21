"""Public surface for the SnelDB Python client."""

from .client import SnelDBClient
from .errors import (
    AuthenticationError,
    AuthorizationError,
    CommandError,
    ConnectionError,
    NotFoundError,
    ParseError,
    SnelDBError,
    ServerError,
)
from .transport import HttpTransport, TcpTransport
from .types import ExecuteResult, NormalizedRecord
from .version import __version__

__all__ = [
    "__version__",
    "AuthenticationError",
    "AuthorizationError",
    "CommandError",
    "ConnectionError",
    "ExecuteResult",
    "HttpTransport",
    "NormalizedRecord",
    "NotFoundError",
    "ParseError",
    "SnelDBClient",
    "SnelDBError",
    "ServerError",
    "TcpTransport",
]
