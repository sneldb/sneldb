import { describe, expect, it } from "vitest";

import { AuthManager } from "../src/auth/authManager";
import { AuthenticationError } from "../src/errors";
import type { Transport } from "../src/transport/base";
import { createLogger } from "../src/utils/logger";

const logger = createLogger({ level: "error" });

describe("AuthManager", () => {
  it("computes deterministic signatures", () => {
    const manager = new AuthManager("user", "secret-key", logger);
    expect(manager.computeSignature("example")).toBe("2324d3fbf5f36d5d59f01b2ce68178667d4c4cb7d06e82e48248d070ecfcc865");
  });

  it("adds HTTP headers when credentials are provided", () => {
    const manager = new AuthManager("user", "secret-key", logger);
    const headers = manager.addHttpHeaders("PING");
    expect(headers["X-Auth-User"]).toBe("user");
    expect(headers["X-Auth-Signature"]).toBe("5e30fe82c73083d961cb1772bde18879a3a9a3570f48fb19ebff403db4fb9ff7");
  });

  it("formats WebSocket commands with inline credentials", () => {
    const manager = new AuthManager("user", "secret-key", logger);
    const formatted = manager.formatCommand("PING", "websocket");
    expect(formatted).toBe("user:5e30fe82c73083d961cb1772bde18879a3a9a3570f48fb19ebff403db4fb9ff7:PING");
  });

  it("adds session tokens after AUTH", async () => {
    const manager = new AuthManager("user", "secret-key", logger);
    const transport: Transport & { lastCommand?: string } = {
      kind: "websocket",
      async execute(command: string) {
        transport.lastCommand = command;
        return { status: 200, body: "OK TOKEN abcd", headers: {} };
      },
      async close() {}
    };

    const token = await manager.authenticate(transport);
    expect(token).toBe("abcd");
    const formatted = manager.formatCommand("PING", "websocket");
    expect(formatted.endsWith("TOKEN abcd")).toBe(true);
    expect(transport.lastCommand).toBe("AUTH user:df37b567e8b2a3219bae6fcb3e68f24e890403f78002120106c7f3b8a93a8b6d");
  });

  it("rejects AUTH for HTTP transports", async () => {
    const manager = new AuthManager("user", "secret-key", logger);
    const transport: Transport = {
      kind: "http",
      async execute() {
        throw new Error("should not execute");
      },
      async close() {}
    };

    await expect(manager.authenticate(transport)).rejects.toBeInstanceOf(AuthenticationError);
  });
});
