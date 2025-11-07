# Syntax & Operators

## Command forms

```sneldb
DEFINE <event_type> FIELDS { "key": "type", … }

STORE  <event_type> FOR <context_id> PAYLOAD <json_object>

QUERY  <event_type> [FOR <context_id>] [SINCE <ts>] [USING <time_field>] [WHERE <expr>] [LIMIT <n>]
QUERY  <event_type_a> [FOLLOWED BY|PRECEDED BY] <event_type_b> LINKED BY <link_field> [WHERE <expr>] [LIMIT <n>]

REPLAY [<event_type>] FOR <context_id> [SINCE <ts>] [USING <time_field>]

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
- **SINCE**: ISO-8601 timestamp string (e.g., `2025-08-01T00:00:00Z`) or numeric epoch (s/ms/µs/ns). Parsed and normalized to epoch seconds (fractional parts truncated to whole seconds).
- **USING**: Selects the time field used by SINCE and bucketing; defaults to core `timestamp`. Common choices: a schema field like `created_at` declared as `"datetime"`.

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
  currency: "string",
  created_at: "datetime"
}

STORE order_created FOR ctx_123 PAYLOAD {
  "id": "a1-b2",
  "amount": 42.5,
  "currency": "EUR",
  "created_at": "2025-09-07T12:00:00Z"
}

QUERY order_created FOR "ctx_123" SINCE "2025-08-01T00:00:00Z" USING created_at
WHERE amount >= 40 AND currency = "EUR"
LIMIT 100

QUERY page_view FOLLOWED BY order_created LINKED BY user_id WHERE page_view.page="/checkout"

REPLAY order_created FOR ctx_123 SINCE "2025-08-01T00:00:00Z" USING created_at
```
