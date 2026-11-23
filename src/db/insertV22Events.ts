import { createLogger, getSupabase, captureException } from "@percolatorct/shared";
import { CURRENT_NETWORK } from "../network.js";
import type { V22EventRow } from "../parsers/v22Events.js";

const logger = createLogger("indexer:v22-events");

/** Re-log a missing-table condition at most this often (the migration is applied by hand). */
const TABLE_MISSING_RELOG_MS = 10 * 60_000;
let lastMissingLog = 0;
let lastKindLog = 0;
let missingTotal = 0;
let kindTotal = 0;
let writtenTotal = 0;
let failedTotal = 0;

/**
 * A v22_events write failed for a reason that may be transient (DB down, timeout, 5xx). Thrown so the caller can answer
 * 500 (the webhook provider redelivers) / retry (the event stream). The permanent "migration not applied" conditions
 * below are NOT this error: they are counted and logged loudly, and redelivery could never fix them.
 */
export class V22EventWriteError extends Error {
  constructor(message: string, readonly count: number, readonly code?: string) {
    super(message);
    this.name = "V22EventWriteError";
  }
}

/** Test hooks / metrics. */
export function getV22EventStats(): { written: number; droppedTableMissing: number; droppedKindConstraint: number; writeFailed: number } {
  return { written: writtenTotal, droppedTableMissing: missingTotal, droppedKindConstraint: kindTotal, writeFailed: failedTotal };
}
export function resetV22EventStats(): void {
  lastMissingLog = 0;
  lastKindLog = 0;
  missingTotal = 0;
  kindTotal = 0;
  writtenTotal = 0;
  failedTotal = 0;
}

/**
 * Postgres check_violation on the kind constraint: the log-event migration (20261009120000_v22_log_events.sql) is not applied.
 * Matched by the constraint NAME, not by code 23514 alone: any other CHECK on v22_events (a future amount/slot CHECK, or a
 * temporary one during a migration) is a real write failure and must be thrown, so the caller holds its cursor and retries.
 */
const KIND_CONSTRAINT = /\bv22_events_kind_check\b/;
function isKindConstraint(error: { code?: string; message?: string; details?: string | null }): boolean {
  if (error.code !== undefined && error.code !== "23514") return false;
  return KIND_CONSTRAINT.test(error.message ?? "") || KIND_CONSTRAINT.test(error.details ?? "");
}

/** Postgres undefined_table (42P01) or PostgREST "table not in schema cache" (PGRST205 / 404). */
function isTableMissing(error: { code?: string; message?: string }): boolean {
  return error.code === "42P01" || error.code === "PGRST205" || /relation .*v22_events.* does not exist|Could not find the table/i.test(error.message ?? "");
}

/**
 * Write v2.2 events. Idempotent (upsert on (signature, ix_index, inner_index, network), duplicates ignored).
 *
 * If the `v22_events` migration has not been applied the rows are dropped with one loud error per 10 minutes (and
 * counted), not retried forever. Any OTHER write failure (DB down, timeout) is counted, alerted and THROWN as a
 * V22EventWriteError: swallowing it silently lost the events, because the caller answered 200 / acked and the provider
 * never redelivered. Successful writes behave exactly as before.
 *
 * @returns the number of rows submitted successfully (0 for an empty batch or a permanent migration drop).
 * @throws V22EventWriteError on a possibly-transient write failure.
 */
export async function insertV22Events(rows: readonly V22EventRow[]): Promise<number> {
  if (rows.length === 0) return 0;
  try {
    const network = CURRENT_NETWORK;
    const { error } = await getSupabase()
      .from("v22_events")
      .upsert(
        rows.map((r) => ({ ...r, network })),
        { onConflict: "signature,ix_index,inner_index,network", ignoreDuplicates: true },
      );
    if (error) {
      if (isTableMissing(error)) {
        missingTotal += rows.length;
        const now = Date.now();
        if (now - lastMissingLog >= TABLE_MISSING_RELOG_MS) {
          lastMissingLog = now;
          logger.error("v22_events table is missing: v2.2 events are NOT being recorded (apply supabase/migrations/20261007120000_v22_events.sql)", {
            metric: "indexer_v22_events_dropped_total",
            dropped: missingTotal,
            code: error.code,
          });
        }
        return 0;
      }
      if (isKindConstraint(error)) {
        kindTotal += rows.length;
        const now = Date.now();
        if (now - lastKindLog >= TABLE_MISSING_RELOG_MS) {
          lastKindLog = now;
          logger.error("v22_events rejected a log-event kind: v2.2 fill / reduce / move events are NOT being recorded (apply supabase/migrations/20261009120000_v22_log_events.sql)", {
            metric: "indexer_v22_events_dropped_total",
            dropped: kindTotal,
            code: error.code,
          });
        }
        return 0;
      }
      throw writeFailed(rows.length, error.message, error.code, error);
    }
    writtenTotal += rows.length;
    return rows.length;
  } catch (err) {
    if (err instanceof V22EventWriteError) throw err;
    throw writeFailed(rows.length, err instanceof Error ? err.message : String(err), undefined, err);
  }
}

/** Count + alert a failed write and build the error to throw. */
function writeFailed(count: number, message: string, code: string | undefined, cause: unknown): V22EventWriteError {
  failedTotal += count;
  logger.error("v22_events write FAILED: v2.2 events not recorded for this batch (caller will 500 / retry)", {
    metric: "indexer_v22_events_write_failed_total",
    count,
    total: failedTotal,
    code,
    error: message,
  });
  try {
    captureException(cause instanceof Error ? cause : new Error(message), { tags: { context: "v22-events-write-failed" } });
  } catch {
    /* alerting must not break ingestion */
  }
  return new V22EventWriteError(`v22_events write failed: ${message}`, count, code);
}

/** insertV22Events with a bounded retry on V22EventWriteError (for paths with no provider redelivery, i.e. the stream). Rethrows the last error. */
export async function insertV22EventsWithRetry(rows: readonly V22EventRow[], attempts = 3, delayMs = 250): Promise<number> {
  let last: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await insertV22Events(rows);
    } catch (err) {
      if (!(err instanceof V22EventWriteError)) throw err;
      last = err;
      if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs * (i + 1)));
    }
  }
  throw last;
}
