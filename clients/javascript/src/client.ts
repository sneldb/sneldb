import { AuthManager } from "./auth/authManager";
import {
  AuthenticationError,
  AuthorizationError,
  CommandError,
  ConnectionError,
  NotFoundError,
  ServerError
} from "./errors";
import { extractErrorMessage, parseAndNormalizeResponse } from "./parser";
import type { OutputFormat, ExecuteSafeResult, NormalizedRecord, ClientOptions } from "./types";
import type { Transport, TransportResponse } from "./transport/base";
import { HttpTransport } from "./transport/http";
import { WebSocketTransport } from "./transport/websocket";
import { createLogger } from "./utils/logger";
import type { Logger } from "./utils/logger";

export class SnelDBClient {
  private readonly transport: Transport;
  private readonly authManager: AuthManager;
  private readonly outputFormat: OutputFormat;
  private readonly defaultHeaders?: Record<string, string>;
  private readonly baseUrl: string;
  private readonly logger: Logger;

  constructor(options: ClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.logger = createLogger({ logger: options.logger, level: options.logLevel });
    this.logger.info(`SnelDBClient init for ${this.baseUrl}`);
    this.authManager = new AuthManager(options.userId, options.secretKey, this.logger);
    this.outputFormat = options.outputFormat ?? "text";
    this.defaultHeaders = options.defaultHeaders;

    if (options.transport) {
      this.transport = options.transport;
      this.logger.debug("Using custom transport");
    } else {
      this.transport = this.createTransport(this.baseUrl, options);
    }
  }

  async execute(command: string): Promise<NormalizedRecord[]> {
    this.logger.debug(`execute() -> ${command}`);
    const result = await this.executeSafe(command);
    if (!result.ok) {
      this.logger.error(`execute() failed: ${result.error?.message}`);
      throw result.error ?? new CommandError("Command execution failed");
    }
    this.logger.debug(`execute() <- ${result.data?.length ?? 0} rows`);
    return result.data ?? [];
  }

  async executeSafe(command: string): Promise<ExecuteSafeResult> {
    try {
      const data = await this.executeInternal(command);
      return { ok: true, data };
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      return { ok: false, error: err };
    }
  }

  async authenticate(): Promise<string> {
    this.logger.info("Authenticating via transport");
    return this.authManager.authenticate(this.transport);
  }

  async close(): Promise<void> {
    this.logger.info("Closing transport");
    await this.transport.close();
  }

  private async executeInternal(command: string): Promise<NormalizedRecord[]> {
    const formattedCommand = this.authManager.formatCommand(command, this.transport.kind);
    this.logger.trace(`Formatted command -> ${formattedCommand}`);
    const headers = this.transport.kind === "http"
      ? this.authManager.addHttpHeaders(formattedCommand, this.defaultHeaders)
      : { ...(this.defaultHeaders ?? {}) };
    if (Object.keys(headers).length > 0) {
      this.logger.trace(`Headers -> ${JSON.stringify(headers)}`);
    }

    const response = await this.transport.execute(formattedCommand, { headers });
    this.logger.debug(`Response status <- ${response.status}`);
    return this.handleResponse(response);
  }

  private handleResponse(response: TransportResponse): NormalizedRecord[] {
    switch (response.status) {
      case 200:
        return parseAndNormalizeResponse(response, this.outputFormat);
      case 400:
      case 405:
        throw new CommandError(extractErrorMessage(response.body));
      case 401:
        throw new AuthenticationError(extractErrorMessage(response.body));
      case 403:
        throw new AuthorizationError(extractErrorMessage(response.body));
      case 404:
        throw new NotFoundError(extractErrorMessage(response.body));
      case 500:
      case 503:
        throw new ServerError(extractErrorMessage(response.body));
      default:
        throw new ConnectionError(`Unexpected status code ${response.status}: ${response.body}`);
    }
  }

  private createTransport(baseUrl: string, options: ClientOptions): Transport {
    const normalizedUrl = this.normalizeUrl(baseUrl);
    const parsed = new URL(normalizedUrl);
    const protocol = parsed.protocol.replace(":", "");

    if (protocol === "http" || protocol === "https") {
      this.logger.info(`Creating HTTP transport for ${normalizedUrl}`);
      return new HttpTransport({
        endpoint: `${parsed.protocol}//${parsed.host}${this.buildPath(parsed)}`,
        readTimeoutMs: options.readTimeoutMs,
        fetchImpl: options.fetch,
        logger: this.logger
      });
    }

    const wsProtocol = protocol === "wss" || protocol === "tls" ? "wss" : "ws";
    const wsUrl = `${wsProtocol}://${parsed.host}${this.buildPath(parsed)}`;
    this.logger.info(`Creating WebSocket transport for ${wsUrl}`);
    return new WebSocketTransport({
      url: wsUrl,
      readTimeoutMs: options.readTimeoutMs,
      webSocketFactory: options.webSocketFactory,
      logger: this.logger
    });
  }

  private normalizeUrl(baseUrl: string): string {
    try {
      return new URL(baseUrl).toString();
    } catch {
      this.logger.warn(`Base URL missing scheme, defaulting to http://${baseUrl}`);
      return `http://${baseUrl}`;
    }
  }

  private buildPath(parsed: URL): string {
    return parsed.pathname === "/" ? "" : parsed.pathname.replace(/\/$/, "");
  }
}
