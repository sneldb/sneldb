# Syntax & Operators

## Command forms

```sneldb
DEFINE <event_type> FIELDS { "key": "type", … }

STORE  <event_type> FOR <context_id> PAYLOAD <json_object>

QUERY  <event_type> [FOR <context_id>] [SINCE ] [WHERE ] [LIMIT ]

REPLAY [<event_type>] FOR <context_id> [SINCE ]

FLUSH
```

- **Keywords**: case-insensitive.
- **Literals**:
  - Strings: double-quoted (`"NL"`, `"a string"`).
  - Numbers: unquoted (`42`, `3`, `900`).
  - Booleans: `true`, `false` (unquoted).
- **WHERE operators**: `=`, `!=`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `NOT`.
- **Precedence**: `NOT` > `AND` > `OR`. Use parentheses sparingly by structuring conditions; (parentheses not required in current grammar).
- **LIMIT**: positive integer; caps returned rows.
- **SINCE**: ISO-8601 timestamp string (e.g., `2025-08-01T00:00:00Z`). When present, only events at/after this instant are considered.

### Mini-grammar (informal)

```bash
expr      := cmp | NOT expr | expr AND expr | expr OR expr
cmp       :=
op        := = | != | > | >= | < | <=
value     := string | number | boolean
```

## Examples

```sneldb
DEFINE order_created AS 1 FIELDS {
  id: "uuid",
  amount: "float",
  currency: "string"
}

STORE order_created FOR ctx_123 PAYLOAD {
  "id": "a1-b2",
  "amount": 42.5,
  "currency": "EUR",
  "tags": ["new","vip"],
  "flag": true
}

QUERY order_created FOR "ctx_123" SINCE "2025-08-01T00:00:00Z"
WHERE amount >= 40 AND currency = "EUR"
LIMIT 100

REPLAY order_created FOR ctx_123 SINCE "2025-08-01T00:00:00Z"
```
