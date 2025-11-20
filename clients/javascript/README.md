# SnelDB JavaScript Client

An isomorphic HTTP/WebSocket client for [SnelDB](https://sneldb.com) that runs in browsers, React/Vue apps, and Node.js without code changes. It mirrors the Ruby client concepts while embracing JavaScript ergonomics and best practices.

## Features

- Automatic transport selection (HTTP/HTTPS vs WS/WSS) based on the connection string
- Secure authentication with HMAC-SHA256 headers (HTTP) or inline tokens (WebSocket)
- Promise-based API with both safe (`executeSafe`) and raising (`execute`) flavors
- Streaming response parser compatible with SnelDB's schema/row/end frames
- Extendable transport interface for custom environments
- Built-in structured logging with configurable levels (trace → error)

## Installation

```bash
npm install @sneldb/javascript-client
# or
yarn add @sneldb/javascript-client
```

## Quick Start

```ts
import { SnelDBClient } from "@sneldb/javascript-client";

const client = new SnelDBClient({
  baseUrl: "wss://demo.sneldb.com:8086",
  userId: "frontend-app",
  secretKey: process.env.SNELDB_SECRET!,
  logLevel: "debug" // trace|debug|info|warn|error (default: info)
});

await client.authenticate();
const rows = await client.execute("QUERY order_created WHERE amount > 100 LIMIT 10");
console.log(rows);
```

HTTP usage works the same way—pass `http://` or `https://` in `baseUrl` and the client automatically switches to fetch-based requests.

## Examples

- `examples/realworld_scenario.ts` mirrors the Ruby real-world scenario. It provisions schemas, stores demo events, and runs analyst queries. Execute it with any TypeScript runner, e.g.:

```bash
cd clients/javascript
npx tsx examples/realworld_scenario.ts
```

## Logging

Pass a custom logger (anything with `trace/debug/info/warn/error`) or rely on the built-in console logger. Set `logLevel` to control volume. Example:

```ts
const client = new SnelDBClient({
  baseUrl: "ws://localhost:8086",
  userId: "admin",
  secretKey: "admin-key",
  logLevel: "trace",
  logger: {
    info: (...args) => myTelemetry.info(args),
    error: (...args) => myTelemetry.error(args)
  }
});
```

## Testing & Building

```bash
npm test      # run Vitest suite
npm run lint  # type-check with tsc
npm run build # generate ESM + CJS bundles with declarations
```

## License

MIT
