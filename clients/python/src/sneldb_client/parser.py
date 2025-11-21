"""Response parsing helpers shared across transports."""

from __future__ import annotations

import json
from typing import Any

from .errors import ParseError
from .transport.base import TransportResponse

try:  # Optional dependency for Arrow parsing
    import pyarrow as pa

    _HAS_ARROW = True
except Exception:  # pragma: no cover - optional import
    pa = None
    _HAS_ARROW = False

NormalizedRecord = dict[str, Any]


def parse_and_normalize_response(response: TransportResponse) -> list[NormalizedRecord]:
    body = response.body or b""
    content_type = (response.headers.get("content-type", "") or "").lower()

    if _looks_like_arrow(content_type, body):
        return parse_arrow_to_records(body)

    text = _decode_body(body)
    return parse_text_to_records(text)


def extract_error_message(body: str | None) -> str:
    if not body:
        return "Error occurred"
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body.strip() or "Error occurred"

    if isinstance(parsed, dict) and "message" in parsed:
        message = parsed["message"]
        if isinstance(message, str):
            return message
        return str(message)
    return body.strip() or "Error occurred"


def parse_text_to_records(text_data: str) -> list[NormalizedRecord]:
    text = (text_data or "").strip()
    if not text:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    data_lines = drop_status_prefix(lines)

    if data_lines and data_lines[0].startswith("{") and '"type"' in data_lines[0]:
        return parse_streaming_json_response(data_lines)

    if looks_like_json_object(text):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ParseError(f"Invalid JSON response: {exc}") from exc
        if isinstance(parsed, list):
            return list(parsed)
        if isinstance(parsed, dict):
            return [parsed]

    if looks_like_json_array(text):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ParseError(f"Invalid JSON response: {exc}") from exc
        if isinstance(parsed, list):
            return list(parsed)
        if isinstance(parsed, dict):
            return [parsed]

    if not data_lines:
        return []

    if "|" in data_lines[0]:
        return parse_pipe_delimited(data_lines)

    return [{"raw": line} for line in data_lines]


def drop_status_prefix(lines: list[str]) -> list[str]:
    if not lines:
        return lines
    first, *rest = lines
    if first.upper().startswith("OK") or first.startswith("200 ") or first[:3].isdigit():
        return rest
    return lines


def looks_like_json_object(text: str) -> bool:
    stripped = text.strip()
    return stripped.startswith("{") and stripped.endswith("}")


def looks_like_json_array(text: str) -> bool:
    stripped = text.strip()
    return stripped.startswith("[") and stripped.endswith("]")


def parse_pipe_delimited(lines: list[str]) -> list[NormalizedRecord]:
    headers = [header.strip() for header in lines[0].split("|")]
    data_start = 0

    if headers and all(header.replace("_", "").isalnum() for header in headers):
        data_start = 1

    records: list[NormalizedRecord] = []
    for line in lines[data_start:]:
        values = [value.strip() for value in line.split("|")]
        if data_start == 1 and len(values) == len(headers):
            record: NormalizedRecord = {}
            for header, value in zip(headers, values):
                record[header.lower()] = value
            records.append(record)
        else:
            records.append({"raw": line, "parts": values})
    return records


def parse_streaming_json_response(lines: list[str]) -> list[NormalizedRecord]:
    records: list[NormalizedRecord] = []
    schema: list[dict[str, Any]] | None = None

    for line in lines:
        if not line:
            continue
        frame = json.loads(line)
        frame_type = frame.get("type")
        if frame_type == "schema":
            columns = frame.get("columns")
            if isinstance(columns, list):
                schema = columns  # type: ignore[assignment]
        elif frame_type == "batch":
            rows = frame.get("rows")
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, list) and schema:
                        record = _row_from_schema(row, schema)
                        records.append(record)
                    elif isinstance(row, list):
                        records.append({"values": row})
        elif frame_type == "row":
            values = frame.get("values")
            if isinstance(values, dict):
                records.append(values)
            elif isinstance(values, list) and schema:
                record = _row_from_schema(values, schema)
                records.append(record)
            elif isinstance(values, list):
                records.append({"values": values})
        elif frame_type == "end":
            break
    return records


def _row_from_schema(values: list[Any], schema: list[dict[str, Any]]) -> NormalizedRecord:
    record: NormalizedRecord = {}
    for idx, value in enumerate(values):
        column_name = schema[idx].get("name") if idx < len(schema) else f"col_{idx}"
        record[str(column_name)] = value
    return record


def _looks_like_arrow(content_type: str, body: bytes) -> bool:
    if "arrow" in content_type:
        return True
    # Arrow IPC streams start with 0x4C 0x50 ("LP")
    return len(body) > 4 and body[:2] == b"LP"


def parse_arrow_to_records(data: bytes) -> list[NormalizedRecord]:
    if not _HAS_ARROW:
        raise ParseError(
            "Arrow response received but pyarrow is not installed. "
            "Install with `pip install sneldb-client[arrow]` or set server.output_format=text."
        )

    try:
        reader = pa.ipc.open_stream(pa.py_buffer(data))
    except Exception as exc:  # pragma: no cover - pyarrow handles parsing
        raise ParseError(f"Failed to open Arrow stream: {exc}") from exc

    records: list[NormalizedRecord] = []
    try:
        for batch in reader:
            table = batch.to_pydict()
            row_count = len(next(iter(table.values()), []))
            for idx in range(row_count):
                row: NormalizedRecord = {}
                for column, values in table.items():
                    row[column] = values[idx]
                records.append(row)
    except Exception as exc:  # pragma: no cover - pyarrow iteration
        raise ParseError(f"Failed to read Arrow batch: {exc}") from exc

    return records


def _decode_body(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")


def is_arrow_response(response: TransportResponse) -> bool:
    """Helper for callers that want to know when an Arrow payload arrived."""
    body = response.body or b""
    content_type = (response.headers.get("content-type", "") or "").lower()
    return _looks_like_arrow(content_type, body)


__all__ = ["NormalizedRecord", "extract_error_message", "parse_and_normalize_response", "is_arrow_response"]
