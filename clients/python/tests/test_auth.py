from sneldb_client.auth import AuthManager
from sneldb_client.logger import create_logger
from sneldb_client.transport.base import Transport, TransportResponse


class DummyTransport:
    kind: Transport.Kind = "tcp"

    def __init__(self, response: TransportResponse) -> None:
        self._response = response
        self.executed: list[str] = []

    def execute(self, command: str, headers=None) -> TransportResponse:
        self.executed.append(command)
        return self._response

    def close(self) -> None:  # pragma: no cover - not used
        pass


def test_format_command_uses_inline_credentials() -> None:
    manager = AuthManager("user", "secret", create_logger())
    formatted = manager.format_command("PING", "tcp")
    assert formatted.startswith("user:")


def test_add_http_headers_sets_hmac_signature() -> None:
    manager = AuthManager("user", "secret", create_logger())
    headers = manager.add_http_headers("PING")
    assert headers["X-Auth-User"] == "user"
    assert "X-Auth-Signature" in headers


def test_authenticate_extracts_token() -> None:
    transport = DummyTransport(TransportResponse(status=200, body=b"OK TOKEN abc123", headers={}))
    manager = AuthManager("user", "secret", create_logger())
    token = manager.authenticate(transport)
    assert token == "abc123"
    assert transport.executed[0].startswith("AUTH user")
