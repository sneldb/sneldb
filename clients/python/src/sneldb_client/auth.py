"""Authentication utilities for the Python client."""

from __future__ import annotations

import hmac
import hashlib
from typing import Mapping

from .errors import AuthenticationError, ConnectionError
from .logger import BoundLogger
from .transport.base import Transport


class AuthManager:
    """Formats commands and headers according to the configured credentials."""

    def __init__(
        self,
        user_id: str | None,
        secret_key: str | None,
        logger: BoundLogger,
    ) -> None:
        self.user_id = user_id
        self.secret_key = secret_key
        self._logger = logger.child("auth")
        self._session_token: str | None = None
        self._authenticated_user: str | None = None

    @property
    def has_credentials(self) -> bool:
        return bool(self.user_id and self.secret_key)

    def compute_signature(self, message: str) -> str:
        if not self.secret_key:
            raise AuthenticationError("Secret key is not configured")

        trimmed = message.strip().encode("utf-8")
        secret = self.secret_key.encode("utf-8")
        digest = hmac.new(secret, trimmed, hashlib.sha256).hexdigest()
        return digest

    def format_command(self, command: str, transport_kind: Transport.Kind) -> str:
        if transport_kind != "tcp":
            return command

        trimmed = command.strip()
        if self._session_token:
            self._logger.trace("Using cached session token for TCP command")
            return f"{trimmed} TOKEN {self._session_token}"
        if self._authenticated_user:
            signature = self.compute_signature(trimmed)
            return f"{signature}:{trimmed}"
        if self.user_id and self.secret_key:
            signature = self.compute_signature(trimmed)
            return f"{self.user_id}:{signature}:{trimmed}"
        return trimmed

    def add_http_headers(
        self,
        command: str,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        merged = dict(headers or {})
        if not self.has_credentials:
            return merged

        signature = self.compute_signature(command)
        merged["X-Auth-User"] = self.user_id or ""
        merged["X-Auth-Signature"] = signature
        return merged

    def authenticate(self, transport: Transport) -> str:
        if transport.kind != "tcp":
            raise AuthenticationError("AUTH is only supported for TCP/TLS transports")

        if not self.has_credentials:
            raise AuthenticationError("User ID and secret key are required for AUTH")

        assert self.user_id is not None
        signature = self.compute_signature(self.user_id)
        command = f"AUTH {self.user_id}:{signature}"
        self._logger.info("Issuing AUTH command over TCP transport")
        response = transport.execute(command)
        body_text = response.body.decode("utf-8", errors="replace")
        if response.status != 200:
            raise AuthenticationError(f"Authentication failed: {body_text}")

        token = self._extract_token(body_text)
        if not token:
            raise ConnectionError(f"Unexpected AUTH response: {body_text}")

        self._session_token = token
        self._authenticated_user = self.user_id
        return token

    def clear(self) -> None:
        self._session_token = None
        self._authenticated_user = None

    def _extract_token(self, body: str) -> str | None:
        marker = "OK TOKEN "
        if marker in body:
            idx = body.find(marker) + len(marker)
            token = body[idx:].split()[0]
            return token.strip() or None
        return None


__all__ = ["AuthManager"]
