export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

export interface Logger {
  trace: (...args: unknown[]) => void;
  debug: (...args: unknown[]) => void;
  info: (...args: unknown[]) => void;
  warn: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
}

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  trace: 0,
  debug: 1,
  info: 2,
  warn: 3,
  error: 4
};

const noop = () => {
  // intentionally empty
};

const defaultConsole: Logger = typeof console !== "undefined"
  ? {
      trace: (...args) => console.trace(...args),
      debug: (...args) => console.debug(...args),
      info: (...args) => console.info(...args),
      warn: (...args) => console.warn(...args),
      error: (...args) => console.error(...args)
    }
  : {
      trace: noop,
      debug: noop,
      info: noop,
      warn: noop,
      error: noop
    };

export interface LoggerOptions {
  logger?: Partial<Logger>;
  level?: LogLevel;
}

export function createLogger(options: LoggerOptions = {}): Logger {
  const target = { ...defaultConsole, ...(options.logger ?? {}) };
  const configuredLevel = options.level ?? "info";

  const wrap = (method: LogLevel): ((...args: unknown[]) => void) => {
    if (LEVEL_PRIORITY[method] < LEVEL_PRIORITY[configuredLevel]) {
      return noop;
    }

    const fn = target[method] || target.info || noop;
    return (...args: unknown[]) => {
      try {
        fn.apply(target, args);
      } catch {
        // ignore logging failures
      }
    };
  };

  return {
    trace: wrap("trace"),
    debug: wrap("debug"),
    info: wrap("info"),
    warn: wrap("warn"),
    error: wrap("error")
  };
}

