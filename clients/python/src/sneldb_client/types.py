"""Shared typing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from .parser import NormalizedRecord

T = TypeVar("T")


@dataclass
class ExecuteResult(Generic[T]):
    ok: bool
    data: T | None = None
    error: Exception | None = None


__all__ = ["ExecuteResult", "NormalizedRecord"]
