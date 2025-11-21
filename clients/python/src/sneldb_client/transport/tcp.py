"""TCP/TLS transport using the standard library socket module."""

from __future__ import annotations

import re
import select
import socket
import ssl
from typing import IO

from ..errors import ConnectionError
from ..logger import BoundLogger, create_logger
from .base import Transport, TransportResponse


class TcpTransport:
    kind: Transport.Kind = "tcp"

    def __init__(
        self,
        host: str,
        port: int,
        *,
        use_ssl: bool = False,
        read_timeout: float = 60.0,
        logger: BoundLogger | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._use_ssl = use_ssl
        self._read_timeout = read_timeout
        self._logger = (logger or create_logger()).child("tcp")
        self._socket: socket.socket | ssl.SSLSocket | None = None
        self._reader: IO[str] | None = None

    def execute(self, command: str, headers: dict[str, str] | None = None) -> TransportResponse:
        del headers  # TCP transport does not use headers
        self._ensure_connection()

        payload = command if command.endswith("\n") else f"{command}\n"
        self._logger.debug("TCP sending bytes=%d", len(payload))
        try:
            assert self._socket is not None
            self._socket.sendall(payload.encode("utf-8"))
        except OSError as exc:
            self._reset()
            raise ConnectionError(f"TCP write failed: {exc}") from exc

        lines = self._read_response_lines()
        body = "\n".join(lines).encode("utf-8")
        self._logger.debug(
            "TCP received %d lines (%d bytes)", len(lines), len(body)
        )
        status = self._derive_status(lines[0] if lines else "")
        return TransportResponse(status=status, body=body, headers={})

    def close(self) -> None:
        if self._reader:
            try:
                self._reader.close()
            except Exception:
                pass
            self._reader = None
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def _ensure_connection(self) -> None:
        if self._socket and self._reader:
            return
        self._logger.info("Connecting to %s:%s (%s)", self._host, self._port, "tls" if self._use_ssl else "tcp")
        try:
            raw_socket = socket.create_connection((self._host, self._port), timeout=self._read_timeout)
            raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            raw_socket.settimeout(self._read_timeout)
            if self._use_ssl:
                context = ssl.create_default_context()
                wrapped = context.wrap_socket(raw_socket, server_hostname=self._host)
                self._socket = wrapped
            else:
                self._socket = raw_socket
            self._reader = self._socket.makefile("r", encoding="utf-8", newline="\n")
        except (OSError, ssl.SSLError) as exc:
            self._reset()
            raise ConnectionError(f"Cannot connect to {self._host}:{self._port}: {exc}") from exc

    def _read_response_lines(self) -> list[str]:
        lines: list[str] = []
        iterations = 0
        while iterations < 1000:
            iterations += 1
            line = self._readline()
            if line is None:
                break
            stripped = line.rstrip("\r\n")
            if stripped:
                lines.append(stripped)
                self._logger.trace("TCP line: %s", stripped[:200])

            if self._is_stream_end(stripped) or self._is_error_line(stripped):
                break

            if self._looks_like_status_line(stripped):
                self._drain_content(lines)
                break
        return lines

    def _readline(self) -> str | None:
        if not self._socket or not self._reader:
            return None
        try:
            line = self._reader.readline()
        except (socket.timeout, TimeoutError) as exc:  # pragma: no cover - network timing
            raise ConnectionError(f"TCP read timeout after {self._read_timeout}s") from exc
        if line == "":
            return None
        return line

    def _drain_content(self, dest: list[str]) -> None:
        if not self._socket or not self._reader:
            return

        for _ in range(1000):
            ready, _, _ = select.select([self._socket], [], [], 0.1)
            if not ready:
                break
            line = self._reader.readline()
            if line == "":
                break
            stripped = line.rstrip("\r\n")
            if not stripped:
                break
            dest.append(stripped)
            if self._is_stream_end(stripped):
                break

    def _derive_status(self, first_line: str) -> int:
        if first_line.startswith("ERROR:"):
            return 400
        match = re.match(r"^(\d{3})", first_line)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        if first_line.startswith("OK"):
            return 200
        return 200

    def _is_error_line(self, line: str) -> bool:
        if not line:
            return False
        if line.startswith("ERROR:"):
            return True
        return bool(re.match(r"^(?:400|401|403|404|500|503)\s+", line))

    def _looks_like_status_line(self, line: str) -> bool:
        if not line:
            return False
        return line.startswith("OK") or bool(re.match(r"^\d{3}\s+OK", line))

    def _is_stream_end(self, line: str) -> bool:
        if not line:
            return False
        return '"type"' in line and '"end"' in line

    def _reset(self) -> None:
        try:
            if self._reader:
                self._reader.close()
        except Exception:
            pass
        self._reader = None
        try:
            if self._socket:
                self._socket.close()
        except Exception:
            pass
        self._socket = None


__all__ = ["TcpTransport"]
