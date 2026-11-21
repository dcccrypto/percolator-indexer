/**
 * DB connection monitor (D-1, J-20261121-IDX-FIX-DBMONITORERROR).
 *
 * supabase-js does not throw when the database or PostgREST is unreachable: it
 * resolves `{ data: null, error }`. The old monitor in index.ts only had a
 * `catch`, so a lost DB never raised the "connection lost" alert and never
 * logged "DB connection check failed". A probe now counts as failed when it
 * throws, when it resolves with a non-null `error`, or when it does not settle
 * within `timeoutMs` (a hung fetch would otherwise stall the monitor forever).
 */

export interface DbMonitorOptions {
  /** One cheap round-trip to the DB. Resolves `{ error }` the way supabase-js does. */
  probe: () => PromiseLike<{ error?: unknown }>;
  /** A probe that has not settled after this long counts as a failure. */
  timeoutMs: number;
  sendLostAlert: (message: string) => Promise<unknown>;
  sendRestoredAlert: () => Promise<unknown>;
  logError: (msg: string, meta: Record<string, unknown>) => void;
}

export interface DbMonitor {
  /** Run one check. Never rejects. */
  tick(): Promise<void>;
  isLost(): boolean;
}

/** Best-effort, never-throwing one-line description of a probe failure. */
export function describeDbError(err: unknown): string {
  try {
    if (err instanceof Error) return err.message;
    if (err && typeof err === "object") {
      const e = err as { message?: unknown; code?: unknown };
      const msg = typeof e.message === "string" && e.message ? e.message : "unknown error";
      return typeof e.code === "string" && e.code ? `${e.code}: ${msg}` : msg;
    }
    return String(err);
  } catch {
    return "unprintable error";
  }
}

export function createDbMonitor(opts: DbMonitorOptions): DbMonitor {
  let lost = false;

  async function probeOnce(): Promise<unknown | null> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`DB check timed out after ${opts.timeoutMs}ms`)), opts.timeoutMs);
      });
      const res = await Promise.race([Promise.resolve(opts.probe()), timeout]);
      return res?.error ?? null;
    } catch (err) {
      return err ?? new Error("DB check rejected");
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  async function tick(): Promise<void> {
    const failure = await probeOnce();
    if (failure === null) {
      if (lost) {
        lost = false;
        try {
          await opts.sendRestoredAlert();
        } catch (alertErr) {
          opts.logError("Failed to send DB restored alert", { error: describeDbError(alertErr) });
        }
      }
      return;
    }
    const message = describeDbError(failure).slice(0, 200);
    if (!lost) {
      lost = true;
      try {
        await opts.sendLostAlert(message);
      } catch (alertErr) {
        opts.logError("Failed to send DB connection lost alert", { error: describeDbError(alertErr) });
      }
    }
    opts.logError("DB connection check failed", { error: message });
  }

  return {
    tick: async () => {
      try {
        await tick();
      } catch {
        // Defensive: tick() already catches every await; never let a timer callback reject.
      }
    },
    isLost: () => lost,
  };
}
