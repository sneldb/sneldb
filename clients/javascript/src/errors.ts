export class SnelDBError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class ConnectionError extends SnelDBError {}
export class AuthenticationError extends SnelDBError {}
export class AuthorizationError extends SnelDBError {}
export class CommandError extends SnelDBError {}
export class NotFoundError extends SnelDBError {}
export class ServerError extends SnelDBError {}
export class ParseError extends SnelDBError {}
