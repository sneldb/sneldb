import { ConnectionError } from "../errors";
import type { Transport, TransportExecuteOptions, TransportResponse } from "./base";
import type { Logger } from "../utils/logger";

const OPEN_STATE = 1;

export interface WebSocketTransportOptions {
  url: string;
  webSocketFactory?: (url: string) => WebSocket;
  connectTimeoutMs?: number;
  readTimeoutMs?: number;
  logger: Logger;
}

interface PendingRequest {
  command: string;
  resolve: (response: TransportResponse) => void;
  reject: (error: Error) => void;
  buffer: string;
  timer?: ReturnType<typeof setTimeout>;
}

export class WebSocketTransport implements Transport {
  readonly kind = "websocket" as const;

  private socket?: WebSocket;
  private connectPromise?: Promise<void>;
  private readonly url: string;
  private readonly connectTimeoutMs: number;
  private readonly readTimeoutMs: number;
  private readonly webSocketFactory: (url: string) => WebSocket;
  private readonly pendingQueue: PendingRequest[] = [];
  private activeRequest?: PendingRequest;
  private removeListeners?: () => void;
  private readonly textDecoder = new TextDecoder();
  private readonly logger: Logger;

  constructor(options: WebSocketTransportOptions) {
    this.url = options.url;
    this.connectTimeoutMs = options.connectTimeoutMs ?? 5000;
    this.readTimeoutMs = options.readTimeoutMs ?? 60_000;
    this.webSocketFactory = options.webSocketFactory ?? this.defaultFactory;
    this.logger = options.logger;
  }

  async execute(command: string, _options?: TransportExecuteOptions): Promise<TransportResponse> {
    await this.ensureConnected();

    return new Promise<TransportResponse>((resolve, reject) => {
      const request: PendingRequest = {
        command: command.endsWith("\n") ? command : `${command}\n`,
        resolve,
        reject,
        buffer: ""
      };
      this.pendingQueue.push(request);
      this.logger.debug(
        `WS enqueue command (pending=${this.pendingQueue.length}, active=${Boolean(this.activeRequest)})`
      );
      this.processQueue();
    });
  }

  async close(): Promise<void> {
    this.pendingQueue.splice(0).forEach(({ reject }) => {
      reject(new ConnectionError("WebSocket connection closed"));
    });
    if (this.activeRequest) {
      this.activeRequest.reject(new ConnectionError("WebSocket connection closed"));
      this.activeRequest = undefined;
    }
    if (this.socket && this.socket.readyState === OPEN_STATE) {
      this.socket.close();
    }
    this.detachSocket();
    this.socket = undefined;
    this.connectPromise = undefined;
  }

  private async ensureConnected(): Promise<void> {
    if (this.socket && this.socket.readyState === OPEN_STATE) {
      return;
    }

    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.logger.debug("WS connecting to %s", this.url);
    this.connectPromise = new Promise<void>((resolve, reject) => {
      let settled = false;
      const timeoutId = setTimeout(() => {
        if (!settled) {
          settled = true;
          this.logger.error(
            `WS connection timeout after ${this.connectTimeoutMs}ms (${this.url})`
          );
          reject(new ConnectionError(`WebSocket connection timeout after ${this.connectTimeoutMs}ms`));
        }
      }, this.connectTimeoutMs);

      try {
        const socket = this.webSocketFactory(this.url);
        this.socket = socket;

        const handleOpen = () => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timeoutId);
          this.attachSocket(socket);
           this.logger.info(`WS connected to ${this.url}`);
          resolve();
        };

        const handleError = (event: Event | Error) => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timeoutId);
          const message = event instanceof Error ? event.message : "Failed to connect";
          this.logger.error(`WS connect error: ${message}`);
          reject(new ConnectionError(message));
        };

        this.addListener(socket, "open", handleOpen);
        this.addListener(socket, "error", handleError);
      } catch (error) {
        settled = true;
        clearTimeout(timeoutId);
        const message = error instanceof Error ? error.message : String(error);
        this.logger.error(`WS factory error: ${message}`);
        reject(new ConnectionError(`Failed to create WebSocket: ${message}`));
      }
    });

    return this.connectPromise;
  }

  private processQueue(): void {
    if (this.activeRequest || this.pendingQueue.length === 0) {
      return;
    }

    if (!this.socket || this.socket.readyState !== OPEN_STATE) {
      return;
    }

    this.activeRequest = this.pendingQueue.shift();
    if (!this.activeRequest) {
      return;
    }

    this.activeRequest.timer = setTimeout(() => {
      this.logger.warn(`WS read timeout after ${this.readTimeoutMs}ms`);
      this.failActive(new ConnectionError(`WebSocket read timeout after ${this.readTimeoutMs}ms`));
    }, this.readTimeoutMs);

    this.logger.debug("WS sending command chunk bytes=%d", this.activeRequest.command.length);
    this.socket.send(this.activeRequest.command);
  }

  private detachSocket(): void {
    if (this.removeListeners) {
      this.removeListeners();
      this.removeListeners = undefined;
    }
  }

  private attachSocket(socket: WebSocket): void {
    this.detachSocket();

    const messageHandler = (...args: any[]) => {
      const data = this.extractPayload(args[0]);
      void this.processIncomingPayload(data);
    };

    const errorHandler = (event: any) => {
      const message = event?.message || event?.error?.message || "WebSocket error";
      this.logger.error(`WS error: ${message}`);
      this.failActive(new ConnectionError(message));
      this.flushPendingQueue(new ConnectionError(message));
    };

    const closeHandler = (event: any) => {
      const reason = event?.reason || "WebSocket closed";
      const error = new ConnectionError(reason);
      this.logger.warn(`WS closed: ${reason}`);
      this.failActive(error);
      this.flushPendingQueue(error);
      this.socket = undefined;
      this.connectPromise = undefined;
    };

    const disposers = [
      this.addListener(socket, "message", messageHandler),
      this.addListener(socket, "error", errorHandler),
      this.addListener(socket, "close", closeHandler)
    ];

    this.removeListeners = () => {
      disposers.forEach((dispose) => dispose());
    };
  }

  private addListener(socket: any, event: string, handler: (...args: any[]) => void): () => void {
    if (typeof socket.addEventListener === "function") {
      socket.addEventListener(event, handler as EventListener);
      return () => socket.removeEventListener?.(event, handler as EventListener);
    }

    if (typeof socket.on === "function") {
      socket.on(event, handler);
      return () => socket.off?.(event, handler);
    }

    const key = `on${event}`;
    const previous = socket[key];
    socket[key] = handler;
    return () => {
      if (socket[key] === handler) {
        socket[key] = previous;
      }
    };
  }

  private async processIncomingPayload(data: unknown): Promise<void> {
    if (!this.activeRequest) {
      return;
    }

    const chunk = await this.decodeData(data);
    this.activeRequest.buffer += chunk;

    // Each SnelDB command response is delivered as a single WebSocket message.
    // Once we receive a frame, treat it as the complete response.
    const response = this.buildResponse(this.activeRequest.buffer);
    this.logger.debug(
      `WS response received (bytes=${this.activeRequest.buffer.length}, status=${response.status})`
    );
    this.resolveActive(response);
  }

  private resolveActive(response: TransportResponse): void {
    if (!this.activeRequest) {
      return;
    }

    if (this.activeRequest.timer) {
      clearTimeout(this.activeRequest.timer);
    }

    const resolver = this.activeRequest.resolve;
    this.logger.trace("WS resolving active request");
    this.activeRequest = undefined;
    resolver(response);
    this.processQueue();
  }

  private failActive(error: Error): void {
    if (!this.activeRequest) {
      return;
    }

    if (this.activeRequest.timer) {
      clearTimeout(this.activeRequest.timer);
    }

    const reject = this.activeRequest.reject;
    this.logger.trace("WS failing active request");
    this.activeRequest = undefined;
    reject(error);
    this.processQueue();
  }

  private flushPendingQueue(error: Error): void {
    while (this.pendingQueue.length > 0) {
      const pending = this.pendingQueue.shift();
      pending?.reject(error);
    }
  }

  private async decodeData(data: unknown): Promise<string> {
    if (typeof data === "string") {
      return data;
    }

    if (data instanceof ArrayBuffer) {
      return this.textDecoder.decode(data);
    }

    if (ArrayBuffer.isView(data)) {
      return this.textDecoder.decode(data);
    }

    if (typeof Blob !== "undefined" && data instanceof Blob) {
      return await data.text();
    }

    if (typeof Buffer !== "undefined" && data instanceof Buffer) {
      return data.toString("utf8");
    }

    return String(data ?? "");
  }

  private buildResponse(buffer: string): TransportResponse {
    const lines = buffer.split(/\r?\n/).filter((line) => line.length > 0);
    const firstLine = lines[0] ?? "";
    let status = 200;

    if (/^ERROR:/i.test(firstLine)) {
      status = 400;
    } else {
      const match = firstLine.match(/^(\d{3})/);
      if (match) {
        status = parseInt(match[1], 10);
      }
    }

    return {
      status,
      body: buffer.trimEnd(),
      headers: {}
    };
  }

  private extractPayload(payload: any): unknown {
    if (payload && typeof payload === "object" && "data" in payload) {
      return (payload as { data: unknown }).data;
    }
    return payload;
  }

  private defaultFactory(url: string): WebSocket {
    if (typeof WebSocket === "undefined") {
      throw new ConnectionError("WebSocket is not available in this environment. Provide a webSocketFactory option.");
    }
    return new WebSocket(url);
  }
}
