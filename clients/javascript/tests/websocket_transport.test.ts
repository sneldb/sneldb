import { describe, expect, it } from "vitest";

import { WebSocketTransport } from "../src/transport/websocket";
import { createLogger } from "../src/utils/logger";

class FakeWebSocket {
  readonly listeners: Record<string, Array<(...args: any[]) => void>> = {
    open: [],
    message: [],
    error: [],
    close: []
  };
  readyState = 0;
  sent: string[] = [];
  constructor(public readonly url: string) {
    setTimeout(() => {
      this.readyState = 1;
      this.emit("open");
    }, 0);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = 3;
    this.emit("close", { reason: "closed" });
  }

  addEventListener(event: string, handler: (...args: any[]) => void): void {
    this.listeners[event]?.push(handler);
  }

  removeEventListener(event: string, handler: (...args: any[]) => void): void {
    this.listeners[event] = (this.listeners[event] || []).filter((fn) => fn !== handler);
  }

  on(event: string, handler: (...args: any[]) => void): void {
    this.addEventListener(event, handler);
  }

  off(event: string, handler: (...args: any[]) => void): void {
    this.removeEventListener(event, handler);
  }

  emit(event: string, payload?: any): void {
    for (const handler of this.listeners[event] || []) {
      handler(payload);
    }
  }

  emitMessage(data: string): void {
    this.emit("message", { data });
  }
}

describe("WebSocketTransport", () => {
  const createTransport = () => {
    let socket!: FakeWebSocket;
    const transport = new WebSocketTransport({
      url: "ws://localhost:8086",
      webSocketFactory: (url) => {
        socket = new FakeWebSocket(url);
        return socket as unknown as WebSocket;
      },
      readTimeoutMs: 5000,
      logger: createLogger({ level: "error" })
    });

    return { transport, getSocket: () => socket! };
  };

  it("resolves a response after receiving data", async () => {
    const { transport, getSocket } = createTransport();
    const promise = transport.execute("PING");

    await new Promise((resolve) => setTimeout(resolve, 0));
    getSocket().emitMessage('{"type":"end","row_count":0}');

    const response = await promise;
    expect(response.status).toBe(200);
    expect(response.body.trim()).toContain('"row_count":0');
  });

  it("queues commands sequentially", async () => {
    const { transport, getSocket } = createTransport();
    const firstPromise = transport.execute("CMD1");
    const secondPromise = transport.execute("CMD2");

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(getSocket().sent).toHaveLength(1);
    expect(getSocket().sent[0].trim()).toBe("CMD1");

    getSocket().emitMessage('{"type":"end"}');
    await firstPromise;

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(getSocket().sent).toHaveLength(2);
    expect(getSocket().sent[1].trim()).toBe("CMD2");

    getSocket().emitMessage('{"type":"end"}');
    await secondPromise;
  });
});
