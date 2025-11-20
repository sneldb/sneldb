import { ConnectionError } from "../errors";
import type { Transport, TransportExecuteOptions, TransportResponse } from "./base";
import type { Logger } from "../utils/logger";

export interface HttpTransportOptions {
  endpoint: string;
  readTimeoutMs?: number;
  fetchImpl?: typeof fetch;
  logger: Logger;
}

export class HttpTransport implements Transport {
  readonly kind = "http" as const;

  private readonly endpoint: string;
  private readonly readTimeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly logger: Logger;

  constructor(options: HttpTransportOptions) {
    this.endpoint = options.endpoint.replace(/\/$/, "") + "/command";
    this.readTimeoutMs = options.readTimeoutMs ?? 60_000;
    this.fetchImpl = options.fetchImpl ?? globalThis.fetch;
    this.logger = options.logger;

    if (!this.fetchImpl) {
      throw new ConnectionError("Global fetch is not available. Provide a fetch implementation in the client options.");
    }
  }

  async execute(command: string, options?: TransportExecuteOptions): Promise<TransportResponse> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.readTimeoutMs);
    const headers = {
      "Content-Type": "text/plain",
      ...(options?.headers ?? {})
    };

    try {
      this.logger.debug(
        `HTTP execute -> ${this.endpoint} (bytes=${command.length})`
      );
      const response = await this.fetchImpl(this.endpoint, {
        method: "POST",
        body: command,
        headers,
        signal: controller.signal
      });

      const body = await response.text();
      this.logger.debug(
        `HTTP response <- ${this.endpoint} status=${response.status} bytes=${body.length}`
      );
      return {
        status: response.status,
        body,
        headers: this.normalizeHeaders(response.headers)
      };
    } catch (error) {
      if ((error as Error).name === "AbortError") {
        this.logger.warn(
          `HTTP request to ${this.endpoint} aborted after ${this.readTimeoutMs}ms`
        );
        throw new ConnectionError(`HTTP request timeout after ${this.readTimeoutMs}ms`);
      }
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`HTTP request failed: ${message}`);
      throw new ConnectionError(`Cannot connect to server at ${this.endpoint}: ${message}`);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  // Stateless transport, nothing to close
  async close(): Promise<void> {
    return Promise.resolve();
  }

  private normalizeHeaders(headers: Headers): Record<string, string> {
    const normalized: Record<string, string> = {};
    headers.forEach((value, key) => {
      normalized[key.toLowerCase()] = value;
    });
    return normalized;
  }
}
