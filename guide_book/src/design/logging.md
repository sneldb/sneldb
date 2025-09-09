# Logging

## What it is

- SnelDB uses the `tracing` ecosystem for structured, leveled logs.
- Logs are emitted to stdout and to a daily‑rotated file, with independent levels.
- Levels and output directory are configured via the config file.

## Core pieces

- Initializer — sets up `tracing_subscriber` layers for stdout and file.
- Config — `[logging]` section controls `log_dir`, `stdout_level`, and `file_level`.
- Levels — `error`, `warn`, `info`, `debug`, `trace`.

## How it works

- Startup

  - `logging::init()` is called from `main.rs` before starting frontends.
  - Reads `CONFIG.logging` to build filters and writers.
  - Installs two layers: ANSI stdout and file appender (`sneldb.log`, daily rotation).

- Emitting logs

  - Use `tracing::{error!, warn!, info!, debug!, trace!}` in code.
  - Prefer spans (e.g., `#[instrument]`) to capture context around operations.

## Configuration

Example snippet from `config.toml`:

```toml
[logging]
log_dir = "../data/logs"
stdout_level = "debug"
file_level = "error"
```

- `stdout_level`: global level for console logs.
- `file_level`: global level for file logs.
- `log_dir`: directory where `sneldb.log` is created (daily rotation).

## Why this design

- Structured logs with levels and spans ease debugging and operations.
- Separate stdout/file control supports local development and production hygiene.

## Operational notes

- Tune levels per environment (e.g., `stdout_level=warn` in prod).
- Ensure `log_dir` exists and is writable; it is created on first write by the appender.
- Use targets when necessary to scope logs for noisy modules.

## Further Reading

- `tracing` crate docs
- `tracing_subscriber` filters and formatters
