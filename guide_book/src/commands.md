# Commands

SnelDB has a compact, human-friendly command language. **Keywords are case-insensitive** (`store`, `STORE`, `StOrE` all work). **Event type names and context IDs are case-preserving**.

Core verbs:

- `DEFINE` — declare a schema for an event type
- `STORE` — append a new event with a JSON payload
- `QUERY` — filter events
- `REPLAY` — stream events in original order (per context, optionally per type)
- `FLUSH` — force a memtable → segment flush
- `PING` — health check

If a command returns no rows, you’ll see: `No matching events found`.

See pages below for full syntax and examples.
