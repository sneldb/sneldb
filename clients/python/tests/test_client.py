import pytest

from sneldb_client import (
    AuthenticationError,
    AuthorizationError,
    CommandError,
    NotFoundError,
    SnelDBClient,
    ServerError,
)
from sneldb_client.transport.base import Transport, TransportResponse
from sneldb_client.transport.http import HttpTransport
from sneldb_client.transport.tcp import TcpTransport


class DummyTransport:
    def __init__(self, response: TransportResponse, *, kind: Transport.Kind = "http") -> None:
        self.response = response
        self.kind: Transport.Kind = kind
        self.commands: list[str] = []
        self.headers: list[dict[str, str]] = []

    def execute(self, command: str, headers=None) -> TransportResponse:
        self.commands.append(command)
        self.headers.append(dict(headers or {}))
        return self.response

    def close(self) -> None:  # pragma: no cover - not needed
        pass


def test_execute_parses_json_payload() -> None:
    transport = DummyTransport(TransportResponse(status=200, body=b'[{"ok": true}]', headers={}))
    client = SnelDBClient(base_url="http://localhost:8085", transport=transport)
    rows = client.execute("PING")
    assert rows == [{"ok": True}]
    assert transport.commands == ["PING"]


def test_execute_injects_http_headers() -> None:
    transport = DummyTransport(TransportResponse(status=200, body=b"OK\n", headers={}))
    client = SnelDBClient(
        base_url="http://localhost:8085",
        user_id="user",
        secret_key="secret",
        transport=transport,
    )
    client.execute("PING")
    assert transport.headers[0]["X-Auth-User"] == "user"


def test_execute_raises_for_auth_errors() -> None:
    transport = DummyTransport(TransportResponse(status=401, body=b"Unauthorized", headers={}))
    client = SnelDBClient(base_url="http://localhost:8085", transport=transport)
    try:
        client.execute("PING")
        raise AssertionError("Expected AuthenticationError")
    except AuthenticationError:
        pass


@pytest.mark.parametrize(
    ("status", "exc"),
    [
        (400, CommandError),
        (401, AuthenticationError),
        (403, AuthorizationError),
        (404, NotFoundError),
        (500, ServerError),
        (503, ServerError),
    ],
)
def test_execute_maps_status_codes_to_exceptions(status: int, exc: type[Exception]) -> None:
    transport = DummyTransport(TransportResponse(status=status, body=b"boom", headers={}))
    client = SnelDBClient(base_url="http://localhost:8085", transport=transport)
    with pytest.raises(exc):
        client.execute("PING")


def test_execute_safe_wraps_exceptions() -> None:
    transport = DummyTransport(TransportResponse(status=500, body=b"boom", headers={}))
    client = SnelDBClient(base_url="http://localhost:8085", transport=transport)
    result = client.execute_safe("PING")
    assert result.ok is False
    assert isinstance(result.error, ServerError)


def test_default_headers_are_preserved() -> None:
    transport = DummyTransport(TransportResponse(status=200, body=b"[{}]", headers={}))
    client = SnelDBClient(
        base_url="http://localhost:8085",
        transport=transport,
        default_headers={"X-Test": "1"},
    )
    client.execute("PING")
    assert transport.headers[0]["X-Test"] == "1"


def test_transport_selection_based_on_scheme() -> None:
    http_client = SnelDBClient(base_url="http://example.com:8000")
    assert isinstance(http_client._transport, HttpTransport)
    https_client = SnelDBClient(base_url="https://example.com:8443")
    assert isinstance(https_client._transport, HttpTransport)
    tcp_client = SnelDBClient(base_url="tcp://127.0.0.1:9000")
    assert isinstance(tcp_client._transport, TcpTransport)
    assert http_client.base_url == "http://example.com:8000"
    assert https_client.base_url == "https://example.com:8443"
    assert tcp_client.base_url == "tcp://127.0.0.1:9000"


def test_tls_transport_uses_ssl() -> None:
    tcp_client = SnelDBClient(base_url="tcp://localhost:9000")
    tls_client = SnelDBClient(base_url="tls://localhost:9000")
    assert isinstance(tls_client._transport, TcpTransport)
    assert hasattr(tls_client._transport, "_use_ssl")
    assert getattr(tls_client._transport, "_use_ssl") is True
    assert getattr(tcp_client._transport, "_use_ssl") is False


def test_missing_scheme_defaults_to_http() -> None:
    client = SnelDBClient(base_url="localhost")
    assert isinstance(client._transport, HttpTransport)
    assert getattr(client._transport, "_endpoint").startswith("http://localhost:80")
