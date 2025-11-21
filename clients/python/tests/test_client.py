from sneldb_client import AuthenticationError, SnelDBClient
from sneldb_client.transport.base import Transport, TransportResponse


class DummyTransport:
    def __init__(self, response: TransportResponse) -> None:
        self.response = response
        self.kind: Transport.Kind = "http"
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
