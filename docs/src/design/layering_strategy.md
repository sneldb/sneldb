# Layering Strategy in SnelDB

This page gives a high-level view of how SnelDB is structured. It focuses on what each layer does and how requests flow through the system.

## Layer 1: `frontend` — Transport and Connections

- Listens for client connections (e.g., Unix/TCP/HTTP/WebSocket).
- Reads requests and writes responses.
- Hands off parsing and execution to the `command` and `engine` layers.

## Layer 2: `command` — Parse and Dispatch

- Parses user input (e.g., `DEFINE`, `STORE`, `QUERY`).
- Validates and turns text into typed commands.
- Dispatches to the appropriate operation in the engine.

## Layer 3: `engine` — Core Logic

- Implements the main behaviors: define schemas, store events, run queries, replay, and flush.
- Chooses the right shard and updates on-disk data as needed.
- Stays independent from how clients connect or send requests.

## Layer 4: `shared` — Common Utilities

- Configuration and response types used across layers.
- Logging setup and other small shared helpers.

## Flow Summary (STORE example)

1. Frontend receives a request.
2. `command` parses and validates it.
3. The dispatcher routes to the correct engine operation.
4. `engine` executes and updates storage.
5. A response is returned to the client.

## Why this layering?

- Clean separation: parsing, logic, and transport are independent.
- Easy to test: engine logic can be tested without real sockets.
- Scales well: clear boundaries support growth and optimization.
