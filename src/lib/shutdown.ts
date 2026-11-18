/**
 * Shutdown sequencing and env parsing helpers for the indexer entrypoint
 * (extracted from index.ts, which runs on import, so they can be unit tested).
 */

/**
 * Parse a positive decimal-integer env value. Number() accepts "1e3", "0x10", "", " " and
 * NaN, and setInterval(fn, NaN|0) is a tight loop, so anything that is not plain digits
 * (or is below `min`, default 1 / unsafe) falls back.
 */
export function parseDecimalIntEnv(raw: string | undefined, fallback: number, min = 1): number {
  if (raw === undefined || !/^[0-9]+$/.test(raw.trim())) return fallback;
  const n = Number(raw.trim());
  return Number.isSafeInteger(n) && n >= min ? n : fallback;
}

/** Run an async step, never throwing and never waiting longer than timeoutMs. */
export async function bestEffort(
  step: () => Promise<unknown>,
  timeoutMs: number,
  onError: (err: unknown) => void,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      Promise.resolve().then(step),
      new Promise<void>((_, rej) => {
        timer = setTimeout(() => rej(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } catch (err) {
    onError(err);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export interface ShutdownDeps {
  /** Ordered synchronous stop steps: intake first (http, streams), then services. */
  stopSteps: Array<() => void>;
  alert: () => Promise<unknown>;
  alertTimeoutMs: number;
  drainMs: number;
  onAlertError: (err: unknown) => void;
  onStepError: (err: unknown) => void;
  exit: (code: number) => void;
}

/** Stop intake and services first, alert best-effort with a timeout, drain, exit. */
export async function runShutdown(d: ShutdownDeps): Promise<void> {
  for (const step of d.stopSteps) {
    try {
      step();
    } catch (err) {
      d.onStepError(err);
    }
  }
  await bestEffort(d.alert, d.alertTimeoutMs, d.onAlertError);
  await new Promise((r) => setTimeout(r, d.drainMs));
  d.exit(0);
}
