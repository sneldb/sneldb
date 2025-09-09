# Infrastructure

SnelDB isn’t just a storage engine — it needs the scaffolding around it to feel safe, predictable, and easy to integrate. That’s what the infrastructure layer provides.

## Configuration

Every system needs a single source of truth for its settings.
SnelDB loads a configuration once at startup and makes it available everywhere. This means:

- Consistency — all components (server, engine, WAL, logging) read from the same snapshot.
- Flexibility — settings can be changed through a config file or environment variable without recompiling.
- Safety — startup fails fast if something critical is missing or invalid.

Think of it as the contract between how you run SnelDB and how the engine behaves.

## Logging

Logs are the “black box recorder” of SnelDB. They serve two purposes:

- For operators: real-time feedback in the console (levels like info/debug/warn).
- For long-term visibility: structured logs rotated daily on disk.

The philosophy is simple: logs should be human-readable, lightweight, and always available when you need to explain “what just happened.”

## Responses

Every command produces a response. SnelDB keeps them minimal and predictable:

- A clear status code (OK, BadRequest, NotFound, InternalError).
- A message for humans.
- A body that can be either lines (for CLI-like tools) or structured JSON arrays (for programmatic use).

Two renderers handle the output: one friendly for terminals, one clean for machines. This way, SnelDB speaks both languages without complicating the core.

## Why it matters

These pieces aren’t “extra code” — they’re the glue that makes SnelDB usable in the real world:

- Configuration means you can run the same binary in development, staging, and production with confidence.
- Logging means you can trust the system to tell you what it’s doing, even when things go wrong.
- Responses mean every client, from shell scripts to dashboards, gets consistent feedback.

Together, they provide the operational safety net: when you store events, you know how to configure it, you see what’s happening, and you get a clear answer back.
