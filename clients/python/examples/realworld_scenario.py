"""End-to-end scenario demonstrating the Python client API."""

from __future__ import annotations

import os
import random
import string
from typing import Any
from urllib.parse import urlparse

from sneldb_client import CommandError, ConnectionError, ServerError, SnelDBClient

BASE_URL = os.getenv("SNELDB_DEMO_URL", "tcp://localhost:8086")
ADMIN_USER = os.getenv("SNELDB_ADMIN_USER", "admin")
ADMIN_KEY = os.getenv("SNELDB_ADMIN_KEY", "admin-key-123")


def log_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def ensure_server(client: SnelDBClient) -> None:
    result = client.execute_safe("PING")
    if not result.ok:
        raise ConnectionError(f"Cannot reach {BASE_URL}: {result.error}")


def quote(value: str) -> str:
    return value if value.replace("-", "").isalnum() else f'"{value}"'


def build_create_user_command(user_id: str, secret_key: str | None = None, roles: list[str] | None = None) -> str:
    parts = [f"CREATE USER {user_id}"]
    if secret_key:
        key = secret_key if secret_key.replace("-", "").isalnum() else f'"{secret_key}"'
        parts.append(f"WITH KEY {key}")
    if roles:
        serialized = ", ".join(f'"{role}"' for role in roles)
        parts.append(f"WITH ROLES [{serialized}]")
    return " ".join(parts)


def build_define_command(event_type: str, fields: dict[str, str]) -> str:
    return f"DEFINE {event_type} FIELDS {fields}".replace("'", '"')


def build_grant_command(user_id: str, permissions: list[str], event_types: list[str]) -> str:
    perms = ", ".join(permission.upper() for permission in permissions)
    events = ", ".join(quote(event) for event in event_types)
    return f"GRANT {perms} ON {events} TO {user_id}"


def build_store_command(event_type: str, context_id: str, payload: dict[str, Any]) -> str:
    return f"STORE {event_type} FOR {quote(context_id)} PAYLOAD {payload}".replace("'", '"')


def pretty_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("  (no data)")
        return
    for row in rows:
        print("  " + repr(row))


def derive_analyst_url(base_url: str) -> str:
    override = os.getenv("SNELDB_ANALYST_URL")
    if override:
        return override

    parsed = urlparse(base_url)
    if parsed.scheme in {"tcp", "tls"}:
        host = parsed.hostname or "localhost"
        http_port = os.getenv("SNELDB_ANALYST_HTTP_PORT")
        if http_port is not None:
            port = http_port
        else:
            port = "8443" if parsed.scheme == "tls" else "8085"
        scheme = "https" if parsed.scheme == "tls" else "http"
        return f"{scheme}://{host}:{port}"

    return base_url


def main() -> None:
    log_section("SnelDB Python Client: Real-World Scenario")
    print(f"Connecting to {BASE_URL} as {ADMIN_USER}")
    log_level = os.getenv("SNELDB_CLIENT_LOG", "info")
    admin_client = SnelDBClient(
        base_url=BASE_URL,
        user_id=ADMIN_USER,
        secret_key=ADMIN_KEY,
        log_level=log_level,
    )

    ensure_server(admin_client)
    if BASE_URL.startswith("tcp"):
        token = admin_client.authenticate()
        print(f"→ Authenticated via AUTH (token: {token[:12]}...)")

    analyst_user_id = f"analyst_{random.randint(1000, 9999)}"
    analyst_secret = "secret-" + "".join(random.choices(string.ascii_lowercase, k=6))

    log_section("Step 1: Create Analyst User")
    admin_client.execute(build_create_user_command(analyst_user_id, analyst_secret, ["read-only"]))
    print(f"→ Created user {analyst_user_id}")

    log_section("Step 2: Define Schemas")
    event_types = {
        "order_created": {
            "order_id": "int",
            "customer_id": "int",
            "amount": "float",
            "currency": "string",
            "status": "string",
            "created_at": "datetime",
        },
        "payment_processed": {
            "payment_id": "int",
            "order_id": "int",
            "amount": "float",
            "payment_method": "string",
            "processed_at": "datetime",
        },
        "order_shipped": {
            "order_id": "int",
            "tracking_number": "string",
            "carrier": "string",
            "shipped_at": "datetime",
        },
    }

    for event_type, fields in event_types.items():
        try:
            admin_client.execute(build_define_command(event_type, fields))
            print(f"→ Defined {event_type}")
        except (CommandError, ServerError) as exc:
            if "already defined" in str(exc):
                print(f"→ {event_type} already defined, skipping")
            else:
                raise

    log_section("Step 3: Grant Permissions")
    for event_type in event_types:
        admin_client.execute(build_grant_command(analyst_user_id, ["read"], [event_type]))
        print(f"→ Granted READ on {event_type}")

    log_section("Step 4: Store Sample Events")
    orders = [
        {"order_id": 1001, "customer_id": 501, "amount": 149.99, "currency": "USD", "status": "confirmed", "created_at": "2025-01-15T10:30:00Z"},
        {"order_id": 1002, "customer_id": 502, "amount": 79.50, "currency": "EUR", "status": "confirmed", "created_at": "2025-01-15T11:15:00Z"},
        {"order_id": 1003, "customer_id": 501, "amount": 299.99, "currency": "USD", "status": "pending", "created_at": "2025-01-15T12:00:00Z"},
    ]
    for order in orders:
        context = f"customer-{order['customer_id']}"
        admin_client.execute(build_store_command("order_created", context, order))
        print(f"→ Stored order {order['order_id']}")

    log_section("Step 5: Query as Analyst (HTTP)")
    analyst_base = derive_analyst_url(BASE_URL)
    print(f"→ Analyst client using {analyst_base}")
    analyst_client = SnelDBClient(
        base_url=analyst_base,
        user_id=analyst_user_id,
        secret_key=analyst_secret,
        log_level=log_level,
    )

    if analyst_base.startswith("tcp"):
        analyst_token = analyst_client.authenticate()
        print(f"→ Analyst authenticated via TCP (token: {analyst_token[:12]}...)")
    else:
        print("→ Analyst using HTTP transport (no AUTH required)")

    print("→ Running analyst high-value query")
    result = analyst_client.execute_safe(
        'QUERY order_created RETURN ["order_id","amount","currency"] WHERE amount >= 100.0 LIMIT 5'
    )
    if not result.ok:
        raise result.error  # noqa: B904
    pretty_rows(result.data or [])
    analyst_client.close()

    log_section("Step 6: Query as Analyst (TCP)")
    tcp_analyst = SnelDBClient(
        base_url=BASE_URL,
        user_id=analyst_user_id,
        secret_key=analyst_secret,
        log_level=log_level,
    )
    tcp_token = tcp_analyst.authenticate()
    print(f"→ Analyst authenticated via TCP token {tcp_token[:12]}...")
    print("→ Running analyst TCP query")
    tcp_result = tcp_analyst.execute_safe(
        'QUERY order_created RETURN ["order_id","amount","currency"] WHERE amount >= 100.0 LIMIT 5'
    )
    if not tcp_result.ok:
        print(f"→ TCP query failed: {tcp_result.error}")
        raise tcp_result.error  # noqa: B904
    pretty_rows(tcp_result.data or [])
    tcp_analyst.close()

    print("→ Analyst scenarios complete, closing admin client")
    admin_client.close()


if __name__ == "__main__":
    main()
