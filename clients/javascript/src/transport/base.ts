export interface TransportResponse {
  status: number;
  body: string;
  headers: Record<string, string>;
}

export interface TransportExecuteOptions {
  headers?: Record<string, string>;
}

export interface Transport {
  readonly kind: "http" | "websocket";
  execute(command: string, options?: TransportExecuteOptions): Promise<TransportResponse>;
  close(): Promise<void> | void;
}
