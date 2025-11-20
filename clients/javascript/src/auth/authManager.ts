import { hmac } from "@noble/hashes/hmac";
import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils";
import {
  AuthenticationError,
  ConnectionError
} from "../errors";
import type { Transport } from "../transport/base";
import type { Logger } from "../utils/logger";

export class AuthManager {
  private sessionToken?: string;
  private authenticatedUserId?: string;
  private readonly logger: Logger;

  constructor(
    private readonly userId: string | undefined,
    private readonly secretKey: string | undefined,
    logger: Logger
  ) {
    this.logger = logger;
  }

  get hasCredentials(): boolean {
    return Boolean(this.userId && this.secretKey);
  }

  getSessionToken(): string | undefined {
    return this.sessionToken;
  }

  getAuthenticatedUser(): string | undefined {
    return this.authenticatedUserId;
  }

  computeSignature(message: string): string {
    if (!this.secretKey) {
      throw new AuthenticationError("Secret key not set");
    }

    const trimmed = message.trim();
    const mac = hmac(sha256, utf8ToBytes(this.secretKey), utf8ToBytes(trimmed));
    return bytesToHex(mac);
  }

  formatCommand(command: string, transportKind: Transport["kind"]): string {
    if (transportKind !== "websocket") {
      return command;
    }
    return this.formatStatefulCommand(command);
  }

  private formatStatefulCommand(command: string): string {
    const trimmed = command.trim();

    if (this.sessionToken) {
      this.logger.trace("AuthManager using cached session token");
      return `${trimmed} TOKEN ${this.sessionToken}`;
    }

    if (this.authenticatedUserId) {
      this.logger.trace("AuthManager using connection-scoped auth signature");
      const signature = this.computeSignature(trimmed);
      return `${signature}:${trimmed}`;
    }

    if (this.userId && this.secretKey) {
      this.logger.trace("AuthManager injecting inline auth credentials");
      const signature = this.computeSignature(trimmed);
      return `${this.userId}:${signature}:${trimmed}`;
    }

    this.logger.trace("AuthManager leaving command unauthenticated");
    return trimmed;
  }

  addHttpHeaders(command: string, headers: Record<string, string> = {}): Record<string, string> {
    if (!this.userId || !this.secretKey) {
      return { ...headers };
    }

    const signature = this.computeSignature(command);
    return {
      ...headers,
      "X-Auth-User": this.userId,
      "X-Auth-Signature": signature
    };
  }

  async authenticate(transport: Transport): Promise<string> {
    if (transport.kind !== "websocket") {
      throw new AuthenticationError("AUTH command is only supported for WebSocket connections");
    }

    if (!this.userId || !this.secretKey) {
      throw new AuthenticationError("User ID and secret key are required for authentication");
    }

    const signature = this.computeSignature(this.userId);
    this.logger.info("Sending AUTH command over WebSocket transport");
    const response = await transport.execute(`AUTH ${this.userId}:${signature}`);

    if (response.status !== 200) {
      throw new AuthenticationError(`Authentication failed: ${response.body}`);
    }

    const tokenMatch = response.body.match(/OK TOKEN\s+(\S+)/);
    if (!tokenMatch) {
      throw new ConnectionError(`Unexpected AUTH response: ${response.body}`);
    }

    this.sessionToken = tokenMatch[1];
    this.authenticatedUserId = this.userId;
    this.logger.info("Authentication successful, session token cached");
    return this.sessionToken;
  }

  clear(): void {
    this.sessionToken = undefined;
    this.authenticatedUserId = undefined;
  }
}
