"""High-level client mirroring the JavaScript and Ruby behavior."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlparse, urlunparse

from .auth import AuthManager
from .errors import (
    AuthenticationError,
    AuthorizationError,
    CommandError,
    ConnectionError,
    NotFoundError,
    ServerError,
)
from .logger import BoundLogger, LogLevel, create_logger
from .parser import extract_error_message, is_arrow_response, parse_and_normalize_response
from .transport import HttpTransport, TcpTransport, Transport, TransportResponse
from .types import ExecuteResult, NormalizedRecord


@dataclass
class ClientOptions:
    base_url: str
    user_id: str | None = None
    secret_key: str | None = None
    output_format: str = "text"
    read_timeout: float = 60.0
    transport: Transport | None = None
    default_headers: Mapping[str, str] | None = None
    logger: object | None = None
    log_level: LogLevel = "info"


class SnelDBClient:
    """Primary entry point for interacting with a SnelDB server."""

    def __init__(
        self,
        *,
        base_url: str,
        user_id: str | None = None,
        secret_key: str | None = None,
        output_format: str = "text",
        read_timeout: float = 60.0,
        transport: Transport | None = None,
        default_headers: Mapping[str, str] | None = None,
        logger: object | None = None,
        log_level: LogLevel = "info",
    ) -> None:
        options = ClientOptions(
            base_url=base_url,
            user_id=user_id,
            secret_key=secret_key,
            output_format=output_format,
            read_timeout=read_timeout,
            transport=transport,
            default_headers=default_headers,
            logger=logger,
            log_level=log_level,
        )
        self.base_url = options.base_url.rstrip("/")
        self.output_format = options.output_format
        self._logger = create_logger(logger=options.logger, level=options.log_level)
        self._logger.info("Initializing SnelDBClient for %s", self.base_url)
        self._transport = options.transport or self._create_transport(self.base_url, options.read_timeout)
        self._auth_manager = AuthManager(options.user_id, options.secret_key, self._logger)
        self._default_headers = dict(options.default_headers or {})

    def execute(self, command: str) -> list[NormalizedRecord]:
        result = self.execute_safe(command)
        if not result.ok:
            raise result.error or CommandError("Command execution failed")
        return result.data or []

    def execute_safe(self, command: str) -> ExecuteResult[list[NormalizedRecord]]:
        try:
            data = self._execute_internal(command)
            return ExecuteResult(ok=True, data=data)
        except Exception as exc:  # pragma: no cover - thin wrapper
            return ExecuteResult(ok=False, error=exc)

    def authenticate(self) -> str:
        return self._auth_manager.authenticate(self._transport)

    def close(self) -> None:
        self._transport.close()

    def _execute_internal(self, command: str) -> list[NormalizedRecord]:
        formatted = self._auth_manager.format_command(command, self._transport.kind)
        headers = dict(self._default_headers)
        if self._transport.kind == "http":
            headers = self._auth_manager.add_http_headers(formatted, headers)

        response = self._transport.execute(formatted, headers=headers)
        return self._handle_response(response)

    def _handle_response(self, response: TransportResponse) -> list[NormalizedRecord]:
        status = response.status
        if status == 200:
            is_arrow = is_arrow_response(response)
            data = parse_and_normalize_response(response)
            if is_arrow:
                sample = data[:3]
                self._logger.debug(
                    "Arrow response decoded rows=%d bytes=%d sample=%s",
                    len(data),
                    len(response.body or b""),
                    sample,
                )
            return data
        body_text = response.body.decode("utf-8", errors="replace")
        message = extract_error_message(body_text)
        if status in {400, 405}:
            raise CommandError(message)
        if status == 401:
            raise AuthenticationError(message)
        if status == 403:
            raise AuthorizationError(message)
        if status == 404:
            raise NotFoundError(message)
        if status in {500, 503}:
            raise ServerError(message)
        raise ConnectionError(f"Unexpected response {status}: {body}")

    def _create_transport(self, base_url: str, read_timeout: float) -> Transport:
        normalized = self._normalize_url(base_url)
        parsed = urlparse(normalized)
        scheme = parsed.scheme or "http"
        host = parsed.hostname or "localhost"
        port = parsed.port

        if scheme in {"http", "https"}:
            port = port or (443 if scheme == "https" else 80)
            endpoint = f"{scheme}://{host}:{port}{parsed.path or ''}".rstrip("/")
            return HttpTransport(endpoint, read_timeout=read_timeout, logger=self._logger)

        if scheme in {"tcp", "tls"}:
            port = port or 8086
            use_ssl = scheme == "tls"
            return TcpTransport(host, port, use_ssl=use_ssl, read_timeout=read_timeout, logger=self._logger)

        raise ValueError(f"Unsupported scheme: {scheme}")

    def _normalize_url(self, base_url: str) -> str:
        parsed = urlparse(base_url)
        if not parsed.scheme:
            return urlunparse(("http",) + parsed[1:])
        return base_url


__all__ = ["SnelDBClient"]
