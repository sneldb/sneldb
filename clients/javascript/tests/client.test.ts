import { describe, expect, it } from "vitest";

import { SnelDBClient } from "../src/client";
import { CommandError } from "../src/errors";
import type { Transport, TransportResponse, TransportExecuteOptions } from "../src/transport/base";

class StubTransport implements Transport {
  public readonly kind: "http" | "websocket";
  public lastCommand?: string;
  public lastHeaders?: Record<string, string>;
  private responses: TransportResponse[];

  constructor(kind: "http" | "websocket", responses: TransportResponse[]) {
    this.kind = kind;
    this.responses = responses;
  }

  async execute(command: string, options?: TransportExecuteOptions): Promise<TransportResponse> {
    this.lastCommand = command;
    this.lastHeaders = options?.headers;
    if (this.responses.length === 0) {
      throw new Error("No response queued");
    }
    return this.responses.shift()!;
  }

  async close(): Promise<void> {}
}

describe("SnelDBClient", () => {
  it("executes commands over HTTP and parses JSON", async () => {
    const responses: TransportResponse[] = [
      { status: 200, body: '[{"foo": "bar"}]', headers: { "content-type": "application/json" } }
    ];
    const transport = new StubTransport("http", responses);
    const client = new SnelDBClient({
      baseUrl: "http://localhost:8085",
      userId: "user",
      secretKey: "secret-key",
      transport,
      logLevel: "error"
    });

    const rows = await client.execute("QUERY test");
    expect(rows).toEqual([{ foo: "bar" }]);
    expect(transport.lastHeaders?.["X-Auth-User"]).toBe("user");
  });

  it("returns safe errors", async () => {
    const responses: TransportResponse[] = [
      { status: 400, body: '{"message":"bad"}', headers: {} },
      { status: 400, body: '{"message":"bad"}', headers: {} }
    ];
    const client = new SnelDBClient({
      baseUrl: "http://localhost:8085",
      transport: new StubTransport("http", responses),
      logLevel: "error"
    });

    await expect(client.execute("BAD"))
      .rejects.toBeInstanceOf(CommandError);

    const safe = await client.executeSafe("BAD");
    expect(safe.ok).toBe(false);
    expect(safe.error).toBeInstanceOf(CommandError);
  });

  it("formats WebSocket commands inline when no token is available", async () => {
    const responses: TransportResponse[] = [
      { status: 200, body: "{\"data\":[]}", headers: {} }
    ];
    const transport = new StubTransport("websocket", responses);
    const client = new SnelDBClient({
      baseUrl: "ws://localhost:8086",
      userId: "user",
      secretKey: "secret-key",
      transport,
      logLevel: "error"
    });

    await client.execute("PING");
    expect(transport.lastCommand).toMatch(/^user:/);
  });
});
