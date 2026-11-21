/**
 * Explicit unhandled-rejection policy for the indexer process.
 *
 * Before this, the outcome of a stray rejected promise depended on SENTRY_DSN:
 * with the DSN set, Sentry's onUnhandledRejection integration (mode "warn")
 * registers a listener, so the process logged and kept running; without it,
 * Node's default `--unhandled-rejections=throw` exited the process with code 1.
 * (Report: J-20261121-IDX-REPORT-FLOATINGPROMISES.)
 *
 * Policy, now independent of SENTRY_DSN: log the rejection and STAY UP. Every
 * background loop (discovery, stats, polling, LP vault, event stream) has its
 * own catch-all, so a stray rejection is a bug to fix, not a reason to drop
 * in-flight webhook writes; and startup failures already keep the process alive
 * on purpose (see `start().catch` in index.ts). When SENTRY_DSN is set, Sentry's
 * own listener still captures the event, so this handler only logs (calling
 * captureException here too would report every rejection twice).
 *
 * The handler must never throw: a throw inside an 'unhandledRejection'
 * listener becomes an uncaughtException and kills the process. So the reason is
 * reduced to plain strings before it reaches the logger (the shared logger has
 * no cycle guard).
 */

export interface RejectionLogger {
  error: (msg: string, meta?: Record<string, unknown>) => void;
}

interface ProcessLike {
  on(event: "unhandledRejection", listener: (reason: unknown) => void): unknown;
}

export function describeRejection(reason: unknown): { message: string; stack?: string } {
  try {
    if (reason instanceof Error) {
      return { message: String(reason.message), stack: typeof reason.stack === "string" ? reason.stack : undefined };
    }
    return { message: String(reason) };
  } catch {
    return { message: "unprintable rejection reason" };
  }
}

export function createUnhandledRejectionHandler(logger: RejectionLogger): (reason: unknown) => void {
  return (reason: unknown) => {
    try {
      logger.error("Unhandled promise rejection (process stays up)", describeRejection(reason));
    } catch {
      // Never throw from this listener (see file doc).
    }
  };
}

export function installUnhandledRejectionPolicy(proc: ProcessLike, logger: RejectionLogger): (reason: unknown) => void {
  const handler = createUnhandledRejectionHandler(logger);
  proc.on("unhandledRejection", handler);
  return handler;
}
