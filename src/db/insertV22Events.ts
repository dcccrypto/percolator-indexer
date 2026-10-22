import { createLogger, getSupabase, getNetwork, captureException } from "@percolatorct/shared";
import type { V22EventRow } from "../parsers/v22Events.js";

const logger = createLogger("indexer:v22-events");

/** Re-log a missing-table condition at most this often (the migration is applied by hand). */
const TABLE_MISSING_RELOG_MS = 10 * 60_000;
let lastMissingLog = 0;
let lastKindLog = 0;
let missingTotal = 0;
let kindTotal = 0;
let writtenTotal = 0;

/** Test hooks / metrics. */
export function getV22EventStats(): { written: number; droppedTableMissing: number; droppedKindConstraint: number } {
  return { written: writtenTotal, droppedTableMissing: missingTotal, droppedKindConstraint: kindTotal };
}
export function resetV22EventStats(): void {
  lastMissingLog = 0;
  lastKindLog = 0;
  missingTotal = 0;
  kindTotal = 0;
  writtenTotal = 0;
}

/** Postgres check_violation on the kind constraint: the log-event migration (20261009120000_v22_log_events.sql) is not applied. */
function isKindConstraint(error: { code?: string; message?: string }): boolean {
  return error.code === "23514" || /v22_events_kind_check/i.test(error.message ?? "");
}

/** Postgres undefined_table (42P01) or PostgREST "table not in schema cache" (PGRST205 / 404). */
function isTableMissing(error: { code?: string; message?: string }): boolean {
  return error.code === "42P01" || error.code === "PGRST205" || /relation .*v22_events.* does not exist|Could not find the table/i.test(error.message ?? "");
}

/**
 * Write v2.2 events. Idempotent (upsert on (signature, ix_index, inner_index, network), duplicates ignored) and
 * BEST EFFORT: it never throws, so an events problem can never fail a trade batch or a webhook delivery. If the
 * `v22_events` migration has not been applied the rows are dropped with one loud error per 10 minutes (and counted),
 * not retried forever.
 *
 * @returns the number of rows submitted successfully (0 on any failure).
 */
export async function insertV22Events(rows: readonly V22EventRow[]): Promise<number> {
  if (rows.length === 0) return 0;
  try {
    const network = getNetwork();
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
      logger.warn("insertV22Events failed", { count: rows.length, code: error.code, error: error.message });
      return 0;
    }
    writtenTotal += rows.length;
    return rows.length;
  } catch (err) {
    logger.warn("insertV22Events threw", { count: rows.length, error: err instanceof Error ? err.message : String(err) });
    try {
      captureException(err instanceof Error ? err : new Error(String(err)), { tags: { context: "v22-events" } });
    } catch {
      /* alerting must not break ingestion */
    }
    return 0;
  }
}
