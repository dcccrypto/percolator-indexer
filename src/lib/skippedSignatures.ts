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
 * 2. upserted into `skipped_signatures` (supabase/migrations/20261005120000_skipped_signatures.sql, applied by hand; if the table is absent this logs + reports to Sentry instead of failing),
 * 3. ONLY if SKIPPED_SIGNATURES_FILE is set explicitly (checked writable at startup by
 *    {@link assertSkippedSignatureSinkReady}), a failed table write is appended there as JSONL. There is NO default
 *    file: the durable record is the error log + the Sentry event + the table (apply the migration before deploy).
 * Re-records of the same (source, signature) in one process are ignored once the record is DURABLE (table row written, or
 * JSONL line appended). A record whose table write failed (and no file took it) is not marked seen, so the next call for it
 * writes again; the log/Sentry/counter alert still fires only once per (source, signature).
 * Returns true when every row is durable (now or by an earlier call), false when any row is not (the caller should keep
 * whatever it holds, e.g. the cursor, and call again later).
 */
const seen = new Set<string>(); // durable records
const alerted = new Set<string>(); // log/Sentry/counter already raised (the write may not have landed)
/** Test hook. */
export function resetSkippedSignatureDedupe(): void {
  seen.clear();
  alerted.clear();
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

export async function recordSkippedSignatures(allRows: readonly SkippedSignature[]): Promise<boolean> {
  const keyOf = (r: SkippedSignature) => `${r.source}:${r.signature}`;
  const pending = new Map<string, SkippedSignature>();
  for (const r of allRows) if (!seen.has(keyOf(r)) && !pending.has(keyOf(r))) pending.set(keyOf(r), r);
  if (pending.size === 0) return true;
  const rows = [...pending.values()];
  const markDurable = () => { for (const k of pending.keys()) seen.add(k); };
  const fresh = rows.filter((r) => !alerted.has(keyOf(r))); // alert once per key, even across failed writes
  for (const r of fresh) alerted.add(keyOf(r));
  skippedTotal += fresh.length;
  for (const r of fresh) {
    logger.error("SKIPPED unreadable transaction: trades in it are NOT indexed until re-indexed", {
      metric: "indexer_skipped_signatures_total",
      signature: r.signature,
      slab: r.slab,
      source: r.source,
      error: r.error.slice(0, 200),
    });
  }
  if (fresh.length > 0) {
    try {
      captureException(new Error(`indexer skipped ${fresh.length} unreadable transaction(s)`), {
        tags: { context: "indexer-skipped-signature" },
        extra: { signatures: fresh.map((r) => r.signature), slabs: [...new Set(fresh.map((r) => r.slab))] },
      });
    } catch {
      /* alerting must not break ingestion */
    }
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
    if (!error) {
      markDurable();
      return true;
    }
    throw new Error(error.message);
  } catch (err) {
    const file = process.env.SKIPPED_SIGNATURES_FILE?.trim();
    if (!file) {
      logger.error("skipped_signatures table write failed and no SKIPPED_SIGNATURES_FILE is set: NOT recorded yet, the next call for these signatures writes again", {
        error: err instanceof Error ? err.message : String(err),
        signatures: rows.map((r) => r.signature),
      });
      return false;
    }
    try {
      appendFileSync(file, rows.map((r) => JSON.stringify({ ...r, network, at: new Date().toISOString() })).join("\n") + "\n");
      logger.warn("skipped_signatures table write failed; appended to file instead", { file, error: err instanceof Error ? err.message : String(err) });
      markDurable();
      return true;
    } catch (fileErr) {
      logger.error("could not persist skipped signatures anywhere: NOT recorded yet, the next call for these signatures writes again", {
        error: fileErr instanceof Error ? fileErr.message : String(fileErr),
        signatures: rows.map((r) => r.signature),
      });
      return false;
    }
  }
}
