import type { Transport } from "./transport/base";
import type { Logger, LogLevel } from "./utils/logger";

export type OutputFormat = "text" | "json";

export interface ClientOptions {
  baseUrl: string;
  userId?: string;
  secretKey?: string;
  outputFormat?: OutputFormat;
  readTimeoutMs?: number;
  fetch?: typeof fetch;
  webSocketFactory?: (url: string) => WebSocket;
  transport?: Transport;
  defaultHeaders?: Record<string, string>;
  logger?: Partial<Logger>;
  logLevel?: LogLevel;
}

export interface ExecuteSafeResult<T = NormalizedRecord[]> {
  ok: boolean;
  data?: T;
  error?: Error;
}

export type NormalizedRecord = Record<string, unknown>;
