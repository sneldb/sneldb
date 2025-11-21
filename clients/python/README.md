# SnelDB Python Client

The official Python binding for [SnelDB](https://sneldb.com). It mirrors the JavaScript and Ruby clients: automatic transport selection, HMAC authentication, safe/raising execution helpers, streaming parser, and batteries-included examples. The client uses HTTP by default and upgrades to a persistent TCP connection when you pass a `tcp://` or `tls://` URL.

## Installation

```bash
pip install sneldb-client
# or install locally from this repo
pip install -e clients/python
```

If your server is configured to return Apache Arrow responses (the default in `prod.toml`), install the optional Arrow extra so the client can decode binary result sets:

```bash
pip install "sneldb-client[arrow]"
```

## Quick Start

```python
from sneldb_client import SnelDBClient

# TCP session (stateful AUTH + token reuse)
tcp_client = SnelDBClient(
    base_url="tcp://localhost:8086",
    user_id="admin",
    secret_key="admin-secret",
    log_level="debug",
)
tcp_client.authenticate()  # establish session token
rows = tcp_client.execute("QUERY order_created WHERE amount > 100 LIMIT 10")
print(rows)
tcp_client.close()

# HTTP session (stateless request/response)
http_client = SnelDBClient(
    base_url="https://localhost:8085",
    user_id="admin",
    secret_key="admin-secret",
    log_level="info",
)
rows = http_client.execute(
    "QUERY order_created RETURN [\"order_id\",\"amount\"] WHERE amount > 100 LIMIT 10"
)
print(rows)
http_client.close()
```

## Features

- Automatic transport selection (HTTP/HTTPS vs TCP/TLS) based on `base_url`
- Secure HMAC-SHA256 authentication headers/tokens
- `execute_safe()` helper that returns `(ok, data, error)` without raising
- Streaming parser compatible with schema/row/end frames, JSON, or pipe output
- Clean logging hooks and pluggable transports for custom environments
- Real-world scenario example that mirrors the JavaScript and Ruby demos

## Examples

```bash
cd clients/python
python -m examples.realworld_scenario
```

Set `SNELDB_DEMO_URL`, `SNELDB_ADMIN_USER`, or `SNELDB_ADMIN_KEY` env vars to point at a remote instance. The script provisions schemas, stores sample data, and runs analyst-style queries so you can see the full workflow end-to-end.

## Development

```bash
cd clients/python
pip install -e .[dev,arrow]
ruff check src
pytest
```

The codebase is fully typed (PEP 484) and intentionally mirrors the layout of the other official clients (auth manager, transports, parser, client facade). Contributions are welcome—follow the top-level `CONTRIBUTING.md` guidelines.
