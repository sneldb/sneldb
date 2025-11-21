"""HTTP transport built on top of httpx."""

from __future__ import annotations

import httpx

from ..errors import ConnectionError
from ..logger import BoundLogger, create_logger
from .base import Transport, TransportResponse


class HttpTransport:
    kind: Transport.Kind = "http"

    def __init__(
        self,
        endpoint: str,
        *,
        read_timeout: float = 60.0,
        client: httpx.Client | None = None,
        logger: BoundLogger | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/") + "/command"
        self._read_timeout = read_timeout
        self._client = client or httpx.Client(timeout=httpx.Timeout(read_timeout))
        self._owns_client = client is None
        self._logger = (logger or create_logger()).child("http")

    def execute(self, command: str, headers: dict[str, str] | None = None) -> TransportResponse:
        payload = command
        request_headers = {"Content-Type": "text/plain"}
        if headers:
            request_headers.update(headers)

        try:
            self._logger.debug("HTTP POST %s bytes=%d", self._endpoint, len(payload))
            response = self._client.post(self._endpoint, content=payload, headers=request_headers)
            body = response.content
            self._logger.debug(
                "HTTP <- %s status=%s bytes=%d",
                self._endpoint,
                response.status_code,
                len(body),
            )
            return TransportResponse(
                status=response.status_code,
                body=body,
                headers={k.lower(): v for k, v in response.headers.items()},
            )
        except httpx.TimeoutException as exc:
            raise ConnectionError(f"HTTP request timeout after {self._read_timeout}s") from exc
        except httpx.RequestError as exc:
            raise ConnectionError(f"Cannot connect to {self._endpoint}: {exc}") from exc

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


__all__ = ["HttpTransport"]
