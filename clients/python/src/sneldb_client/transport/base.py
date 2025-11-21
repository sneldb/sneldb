"""Common transport abstractions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping, Protocol, runtime_checkable


TransportKind = Literal["http", "tcp"]


@dataclass
class TransportResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


@runtime_checkable
class Transport(Protocol):
    Kind = TransportKind

    @property
    def kind(self) -> TransportKind: ...

    def execute(self, command: str, headers: Mapping[str, str] | None = None) -> TransportResponse: ...

    def close(self) -> None: ...


__all__ = ["Transport", "TransportKind", "TransportResponse"]
