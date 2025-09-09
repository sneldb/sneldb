# Contributing to SnelDB

Thank you for your interest in contributing! This guide explains how to build, test, document, and propose changes to this repository.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Project Overview](#project-overview)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Build](#build)
- [Running the Database](#running-the-database)
- [Testing](#testing)
- [Documentation](#documentation)
- [Style and Conventions](#style-and-conventions)
- [Configuration](#configuration)
- [Submitting Changes](#submitting-changes)
- [Issue Reporting](#issue-reporting)
- [License](#license)

## Code of Conduct

Please be respectful and constructive. By participating, you agree to foster a welcoming, harassment‑free community.

## Project Overview

SnelDB is an event database with:

- A compact command language (`DEFINE`, `STORE`, `QUERY`, `REPLAY`, `FLUSH`, `PING`).
- Pluggable frontends (Unix socket, HTTP; TCP scaffold available).
- Storage and indexing layers designed for fast append/query and efficient compaction.
- A comprehensive mdBook user guide under `guide_book/`.

## Prerequisites

- Rust toolchain (stable) with `cargo`.
- macOS/Linux environment (CI runs on Ubuntu).
- For E2E tests: `socat` must be installed (CI installs via `apt`). On macOS: `brew install socat`.

## Setup

```bash
# Clone
git clone <your-fork-or-this-repo>
cd sneldb

# Optional: set a default log level
export RUST_LOG=info
```

## Build

```bash
cargo build
```

## Running the Database

The server starts Unix and HTTP frontends.

```bash
# Use default config
SNELDB_CONFIG='config/dev.toml' cargo run
```

- Unix socket: `/tmp/sneldb.sock`
- HTTP: `127.0.0.1:8080`

Note: `main.rs` clears data directories on startup (based on `SNELDB_CONFIG`), intended for a clean dev loop.

## Testing

- Unit tests:

```bash
SNELDB_CONFIG='config/test.toml' cargo test
```

- End‑to‑end (E2E) tests:

```bash
# Runs all randomized scenarios
SNELDB_CONFIG='config/test.toml' cargo run --bin test_runner

# Run a specific scenario by name (example)
SNELDB_CONFIG='config/test.toml' cargo run --bin test_runner -- ScenarioName
```

CI replicates this flow: build, unit tests, then E2E tests.

## Documentation

The guide is in `guide_book/` (mdBook format).

- Live preview (requires `mdbook`):

```bash
cargo install mdbook
cd guide_book
mdbook serve -n 127.0.0.1 -p 4000
```

- Build static docs:

```bash
cd guide_book
mdbook build
```

Edit content under `guide_book/src/`. The built book outputs to `guide_book/book/`.

## Style and Conventions

- Rust 2024 edition. Prefer clear, explicit code over cleverness.
- Encapsulate related data and behavior in `struct`s; implement methods in `impl` blocks; favor trait‑based polymorphism.
- Keep functions small with early returns and robust error handling (use `anyhow`/`thiserror` as appropriate).
- Run format and lints locally:

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
```

- Tests should be deterministic and isolated. Use helpers under `tests/helpers/` where possible.

## Configuration

Runtime configuration is loaded via the `SNELDB_CONFIG` environment variable pointing to a TOML file. Examples:

- `config/dev.toml` for development
- `config/test.toml` for unit tests
- `config/prod.toml` for production

Key sections include `[wal]`, `[engine]`, `[schema]`, `[server]`, and `[logging]`. The Unix socket path defaults to `/tmp/sneldb.sock`.

## Submitting Changes

1. Fork and create a feature branch: `git checkout -b feature/short-description`.
2. Make focused commits with clear messages.
3. Ensure:
   - `cargo fmt` passes.
   - `cargo clippy -- -D warnings` is clean.
   - `SNELDB_CONFIG='config/test.toml' cargo test` passes.
   - `SNELDB_CONFIG='config/test.toml' cargo run --bin test_runner` passes locally if you touched integration surfaces.
4. Update docs in `guide_book/` if user‑visible behavior changes.
5. Open a Pull Request:
   - Describe the problem, approach, and trade‑offs.
   - Link related issues.
   - Include benchmarks or perf notes if relevant.

## Issue Reporting

- Use a clear title and a minimal reproduction if possible.
- Include OS, Rust version, config file (or deltas), and logs.
- Tag whether it’s a bug, enhancement, or documentation.

## License

This project is under the Business Source License 1.1. See `LICENSE.md` for details, including the change date and restrictions. By contributing, you agree your contributions are licensed under the same terms.
