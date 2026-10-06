import { accessSync, appendFileSync, constants, existsSync } from "node:fs";
import { dirname } from "node:path";
import { createLogger, getSupabase, getNetwork, captureException } from "@percolatorct/shared";

const logger = createLogger("indexer:skipped-signatures");

export interface SkippedSignature {
  /** FULL signature (never truncated: this is the re-index handle). */
  signature: string;
  source: "trade-indexer" | "lp-vault" | "creator-lookup";
  /** Slab (or LP-vault registry) whose signature window contained it. */
  slab: string;
  error: string;
}

let skippedTotal = 0;
/** Counter metric `indexer_skipped_signatures_total` (process lifetime). Alert on any increase. */
export function getSkippedSignatureCount(): number {
  return skippedTotal;
}
/** Test hook. */
export function resetSkippedSignatureCount(): void {
  skippedTotal = 0;
}

/**
 * Make skipped signatures durable and loud. Never throws (it runs inside an ingestion loop):
 * 1. counter metric incremented and the FULL signature + slab logged at error level (+ Sentry),
 * 2. upserted into `skipped_signatures` (migration 20261005120000, applied by hand),
 * 3. ONLY if SKIPPED_SIGNATURES_FILE is set explicitly (checked writable at startup by
 *    {@link assertSkippedSignatureSinkReady}), a failed table write is appended there as JSONL. There is NO default
 *    file: the durable record is the error log + the Sentry event + the table (apply the migration before deploy).
 * Re-records of the same (source, signature) in one process are ignored (no repeated log/Sentry/table spam).
 */
const seen = new Set<string>();
/** Test hook. */
export function resetSkippedSignatureDedupe(): void {
  seen.clear();
}

/**
 * Startup check: if SKIPPED_SIGNATURES_FILE is set it must be appendable, otherwise throw (fail loudly at boot, not at the
 * first skip). Unset is fine (no file fallback).
 */
export function assertSkippedSignatureSinkReady(env: Record<string, string | undefined> = process.env): void {
  const file = env.SKIPPED_SIGNATURES_FILE?.trim();
  if (!file) return;
  const target = existsSync(file) ? file : dirname(file);
  try {
    accessSync(target, constants.W_OK);
  } catch {
    throw new Error(`SKIPPED_SIGNATURES_FILE=${file} is not writable (checked ${target}); fix the path or unset it`);
  }
}

export async function recordSkippedSignatures(allRows: readonly SkippedSignature[]): Promise<void> {
  const rows = allRows.filter((r) => {
    const key = `${r.source}:${r.signature}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  if (rows.length === 0) return;
  skippedTotal += rows.length;
  for (const r of rows) {
    logger.error("SKIPPED unreadable transaction: trades in it are NOT indexed until re-indexed", {
      metric: "indexer_skipped_signatures_total",
      signature: r.signature,
      slab: r.slab,
      source: r.source,
      error: r.error.slice(0, 200),
    });
  }
  try {
    captureException(new Error(`indexer skipped ${rows.length} unreadable transaction(s)`), {
      tags: { context: "indexer-skipped-signature" },
      extra: { signatures: rows.map((r) => r.signature), slabs: [...new Set(rows.map((r) => r.slab))] },
    });
  } catch {
    /* alerting must not break ingestion */
  }
  let network: string | null = null;
  try {
    network = getNetwork();
  } catch {
    /* unset in some test/dev setups */
  }
  try {
    const { error } = await getSupabase()
      .from("skipped_signatures")
      .upsert(rows.map((r) => ({ signature: r.signature, source: r.source, slab: r.slab, network, error: r.error.slice(0, 500) })), {
        onConflict: "signature,source",
        ignoreDuplicates: true,
      });
    if (!error) return;
    throw new Error(error.message);
  } catch (err) {
    const file = process.env.SKIPPED_SIGNATURES_FILE?.trim();
    if (!file) {
      logger.error("skipped_signatures table write failed and no SKIPPED_SIGNATURES_FILE is set: the error log + Sentry event above are the record", {
        error: err instanceof Error ? err.message : String(err),
        signatures: rows.map((r) => r.signature),
      });
      return;
    }
    try {
      appendFileSync(file, rows.map((r) => JSON.stringify({ ...r, network, at: new Date().toISOString() })).join("\n") + "\n");
      logger.warn("skipped_signatures table write failed; appended to file instead", { file, error: err instanceof Error ? err.message : String(err) });
    } catch (fileErr) {
      logger.error("could not persist skipped signatures anywhere (they are in this log line stream only)", {
        error: fileErr instanceof Error ? fileErr.message : String(fileErr),
        signatures: rows.map((r) => r.signature),
      });
    }
  }
}
