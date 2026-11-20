/**
 * Finalized reconcile pass: remove the rows of transactions that were ingested at `confirmed` and then dropped by a fork.
 *
 * Every ingest path writes at `confirmed`: the Helius webhook (it has no commitment option), the TradeIndexer poll, the
 * Atlas stream and the LP-vault poll. A transaction that a fork later drops is never listed again, so nothing re-reads
 * it, and its `trades`, `v22_events` and `lp_vault_events` rows stayed for good. `market_stats` then carried the dropped
 * fill, and when another transaction took the slot (the same order re-signed) the fill was counted twice.
 *
 * This pass adds no ingest latency. Every `intervalMs` it:
 *  1. reads the signatures written since the last pass that are at least `minAgeMs` old (`trades.created_at`,
 *     `v22_events.indexed_at`, `lp_vault_events.created_at`; `trades` has no slot column, so a time margin is used for
 *     all three). `minAgeMs` covers finality (32 slots) plus the blockhash lifetime (151 slots), about 75 s at 400 ms a
 *     slot, so by then a live transaction is finalized and a dropped one can no longer land;
 *  2. asks `getSignatureStatuses(…, { searchTransactionHistory: true })` about them, 256 per call;
 *  3. forgets the finalized ones, keeps the not-yet-finalized ones pending, and counts a strike for each `null`;
 *  4. a signature that is `null` on `nullPassesToDrop` separate passes (default 2) is dropped: its rows are deleted from all three tables, the touched LP positions are recomputed, the removal is
 *     recorded in `skipped_signatures` (source `fork-dropped`) and `onTradesRemoved` re-runs the `market_stats` rollup for
 *     the touched markets.
 * A `null` can also come from a node that lags or has no transaction history, so a strike only counts when
 *  - the call also carried a CANARY, a signature known to be finalized (the newest one this pass has seen finalize, or
 *    else an old `trades` / `lp_vault_events` row), and the canary came back with a status: the node can see history;
 *  - the answering node's `context.slot` is at least {@link SLOT_MARGIN} slots past the slot at which the signature was
 *    first checked (finality + blockhash lifetime): the node has moved on far enough that the tx can no longer land.
 * With no canary available (an empty database) nothing is removed.
 */
import { createLogger, captureException, getSupabase, getNetwork } from "@percolatorct/shared";
import type { SupabaseClient } from "@supabase/supabase-js";
import { recomputePosition, type LpVaultStore } from "../lpVault/store.js";

const logger = createLogger("indexer:dropped-tx-reconcile");

/** The one RPC method used. A web3.js Connection satisfies it. */
export interface SignatureStatusReader {
  getSignatureStatuses(
    signatures: string[],
    config?: { searchTransactionHistory?: boolean },
  ): Promise<{ context?: { slot?: number }; value: ReadonlyArray<{ confirmationStatus?: string | null } | null> }>;
}

export interface DroppedTxReconcilerOptions {
  connection: SignatureStatusReader;
  /** Network of `trades` / `v22_events` rows. Default getNetwork(), as their writers use. */
  network?: string;
  /** Network of `lp_vault_events` rows. Default `network`. */
  lpNetwork?: string;
  db?: () => SupabaseClient;
  /** Needed to recompute the LP positions whose events were removed. */
  lpStore?: LpVaultStore;
  /** Called with the markets whose trades were removed (re-run the market_stats rollup for them). */
  onTradesRemoved?: (slabs: string[]) => Promise<void> | void;
  intervalMs?: number;
  minAgeMs?: number;
  /** How far back the first pass after a start reads. */
  lookbackMs?: number;
  nullPassesToDrop?: number;
  now?: () => number;
}

export interface ReconcileResult {
  candidates: number;
  checked: number;
  rpcCalls: number;
  finalized: number;
  pending: number;
  dropped: string[];
  removed: { trades: number; v22_events: number; lp_vault_events: number };
  suspect: boolean;
}

export const STATUS_PAGE = 256;
/** 32 slots to finality + 151 slots of blockhash lifetime. */
export const SLOT_MARGIN = 183;
const DB_PAGE = 1000;
const DELETE_CHUNK = 50;
/** Rows read per table per pass; more than this is read on the next passes (the watermark only moves past what was read). */
const MAX_ROWS_PER_PASS = 20_000;
/** Re-read this much before the watermark: a row's created_at is its transaction start, a little before it is visible. */
const OVERLAP_MS = 60_000;
/** A signature still unresolved after this long is given up on (logged), so the pending set stays bounded. */
const PENDING_MAX_AGE_MS = 24 * 60 * 60_000;

interface Source {
  table: "trades" | "v22_events" | "lp_vault_events";
  sig: string;
  at: string;
  lp: boolean;
}
const SOURCES: readonly Source[] = [
  { table: "trades", sig: "tx_signature", at: "created_at", lp: false },
  { table: "v22_events", sig: "signature", at: "indexed_at", lp: false },
  { table: "lp_vault_events", sig: "signature", at: "created_at", lp: true },
];

let droppedTotal = 0;
/** Counter metric `indexer_fork_dropped_signatures_total` (process lifetime). */
export function getForkDroppedCount(): number {
  return droppedTotal;
}

export class DroppedTxReconciler {
  private readonly pending = new Map<string, { nulls: number; since: number; firstSlot: number | null }>();
  /** A signature known to be finalized, sent with every status call (see the class doc). */
  private canary: string | null = null;
  /** Sources whose table is missing (hand-applied migrations): skipped, logged once. */
  private readonly missing = new Set<string>();
  /** Finalized signatures, kept while they can still be re-read through the overlap. */
  private readonly finalized = new Map<string, number>();
  private readonly lpRepair = new Map<string, { registry: string; user: string }>();
  private scannedThrough: number | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private busy = false;

  constructor(private readonly opts: DroppedTxReconcilerOptions) {}

  private get now(): number {
    return (this.opts.now ?? Date.now)();
  }
  private get db(): SupabaseClient {
    return (this.opts.db ?? (getSupabase as unknown as () => SupabaseClient))();
  }
  private get network(): string {
    return this.opts.network ?? getNetwork();
  }
  private get lpNetwork(): string {
    return this.opts.lpNetwork ?? this.network;
  }

  start(): void {
    if (this.timer) return;
    const interval = this.opts.intervalMs ?? 60_000;
    this.timer = setInterval(() => void this.tick(), interval);
    logger.info("DroppedTxReconciler started", { intervalMs: interval, minAgeMs: this.opts.minAgeMs ?? 120_000 });
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** One pass that never throws (for the timer). */
  async tick(): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      const r = await this.runOnce();
      if (r.dropped.length > 0 || r.suspect) logger.info("dropped-tx reconcile pass", { ...r, dropped: r.dropped.length });
    } catch (err) {
      logger.error("dropped-tx reconcile pass failed (retried next pass)", { error: err instanceof Error ? err.message : String(err) });
    } finally {
      this.busy = false;
    }
  }

  /** One pass. Throws on a DB or RPC failure; whatever it had not finished is retried by the next pass. */
  async runOnce(): Promise<ReconcileResult> {
    const now = this.now;
    const minAge = this.opts.minAgeMs ?? 120_000;
    const upper = now - minAge;
    const lookbackFloor = now - (this.opts.lookbackMs ?? 30 * 60_000);
    const lower = this.scannedThrough === null ? lookbackFloor : Math.max(lookbackFloor, this.scannedThrough - OVERLAP_MS);
    const result: ReconcileResult = {
      candidates: 0, checked: 0, rpcCalls: 0, finalized: 0, pending: 0, dropped: [],
      removed: { trades: 0, v22_events: 0, lp_vault_events: 0 }, suspect: false,
    };

    await this.retryLpRepairs();

    // 1. New candidates since the last pass.
    let newUpper = upper;
    if (upper > lower) {
      for (const src of SOURCES) {
        const { sigs, readThrough } = await this.readSignatures(src, lower, upper);
        newUpper = Math.min(newUpper, readThrough);
        for (const s of sigs) {
          if (this.finalized.has(s) || this.pending.has(s)) continue;
          this.pending.set(s, { nulls: 0, since: now, firstSlot: null });
        }
      }
      this.scannedThrough = newUpper;
    }
    for (const [s, at] of this.finalized) if (at < now - (this.opts.lookbackMs ?? 30 * 60_000) - OVERLAP_MS) this.finalized.delete(s);

    const sigs = [...this.pending.keys()];
    result.candidates = sigs.length;
    if (sigs.length === 0) return result;

    // 2. Statuses, 256 per call (255 + the canary). Any RPC failure aborts the pass with no strikes counted.
    const canary = this.canary ?? (await this.findCanary());
    const page = canary ? STATUS_PAGE - 1 : STATUS_PAGE;
    const statuses = new Map<string, { st: { confirmationStatus?: string | null } | null; slot: number | null; trusted: boolean }>();
    for (let i = 0; i < sigs.length; i += page) {
      const chunk = sigs.slice(i, i + page);
      const ask = canary && !chunk.includes(canary) ? [...chunk, canary] : chunk;
      result.rpcCalls++;
      const resp = await this.opts.connection.getSignatureStatuses(ask, { searchTransactionHistory: true });
      const value = resp?.value;
      if (!Array.isArray(value) || value.length !== ask.length) {
        throw new Error(`getSignatureStatuses returned ${Array.isArray(value) ? value.length : "no"} statuses for ${ask.length} signatures`);
      }
      const slot = typeof resp.context?.slot === "number" ? resp.context.slot : null;
      const trusted = canary !== null && value[ask.indexOf(canary)] != null;
      if (!trusted) {
        result.suspect = true;
        if (canary !== null && this.canary === canary) this.canary = null; // pick a fresh one next pass
      }
      chunk.forEach((s, k) => statuses.set(s, { st: value[k] ?? null, slot, trusted }));
    }
    result.checked = statuses.size;
    if (result.suspect) {
      logger.warn(canary
        ? "getSignatureStatuses returned null for a finalized canary: treated as an RPC fault, no strikes counted"
        : "no finalized canary signature available yet: no strikes counted", { canary, signatures: sigs.length });
    }

    // 3. Resolve.
    const toDrop: string[] = [];
    for (const s of sigs) {
      const { st, slot, trusted } = statuses.get(s)!;
      const p = this.pending.get(s)!;
      if (p.firstSlot === null && slot !== null) p.firstSlot = slot;
      if (st === null) {
        // A strike needs a node that can see history (canary) and has moved SLOT_MARGIN slots past the first check.
        if (trusted && slot !== null && p.firstSlot !== null && slot >= p.firstSlot + SLOT_MARGIN) p.nulls++;
        if (p.nulls >= (this.opts.nullPassesToDrop ?? 2)) toDrop.push(s);
        else if (now - p.since > PENDING_MAX_AGE_MS) this.giveUp(s, "still null");
        continue;
      }
      p.nulls = 0;
      if (st.confirmationStatus === "finalized") {
        this.pending.delete(s);
        this.finalized.set(s, now);
        this.canary = s;
        result.finalized++;
      } else if (now - p.since > PENDING_MAX_AGE_MS) {
        this.giveUp(s, `still ${st.confirmationStatus ?? "unknown"}`);
      }
    }

    // 4. Remove the dropped ones.
    for (let i = 0; i < toDrop.length; i += DELETE_CHUNK) {
      const chunk = toDrop.slice(i, i + DELETE_CHUNK);
      const removed = await this.removeRows(chunk);
      result.removed.trades += removed.trades;
      result.removed.v22_events += removed.v22_events;
      result.removed.lp_vault_events += removed.lp_vault_events;
      for (const s of chunk) this.pending.delete(s);
      result.dropped.push(...chunk);
    }
    result.pending = this.pending.size;
    return result;
  }

  /** An old row's signature: long finalized unless it was itself dropped, which this pass would then remove. */
  private async findCanary(): Promise<string | null> {
    const before = new Date(this.now - (this.opts.lookbackMs ?? 30 * 60_000)).toISOString();
    for (const src of SOURCES) {
      if (src.table === "v22_events" || this.missing.has(src.table)) continue;
      const { data, error } = await this.db
        .from(src.table)
        .select(src.sig)
        .eq("network", src.lp ? this.lpNetwork : this.network)
        .lt(src.at, before)
        .not(src.sig, "is", null)
        .order(src.at, { ascending: false })
        .limit(1);
      if (error) {
        if (this.noteMissing(src.table, error)) continue;
        throw new Error(`${src.table} canary read: ${error.message}`);
      }
      const sig = ((data ?? []) as unknown as Array<Record<string, string | null>>)[0]?.[src.sig];
      if (sig) return (this.canary = sig);
    }
    return null;
  }

  /** True (and logged once) when `error` says the table does not exist: its migration is applied by hand. */
  private noteMissing(table: string, error: { code?: string; message?: string }): boolean {
    const gone = error.code === "42P01" || error.code === "PGRST205" || /does not exist|Could not find the table/i.test(error.message ?? "");
    if (gone && !this.missing.has(table)) {
      this.missing.add(table);
      logger.error(`dropped-tx reconcile: table ${table} is missing; it is skipped until the indexer restarts`, { error: error.message });
    }
    return gone;
  }

  private giveUp(sig: string, why: string): void {
    this.pending.delete(sig);
    logger.warn("dropped-tx reconcile gave up on a signature that never resolved", { signature: sig, why });
  }

  /** Distinct signatures of `src` written in (lower, upper]. `readThrough` < upper when the per-pass row cap was hit. */
  private async readSignatures(src: Source, lower: number, upper: number): Promise<{ sigs: Set<string>; readThrough: number }> {
    const network = src.lp ? this.lpNetwork : this.network;
    const sigs = new Set<string>();
    if (this.missing.has(src.table)) return { sigs, readThrough: upper };
    let rows = 0;
    let lastAt: string | null = null;
    for (;;) {
      let q = this.db
        .from(src.table)
        .select(`${src.sig},${src.at}`)
        .eq("network", network)
        .gt(src.at, new Date(lower).toISOString())
        .lte(src.at, new Date(upper).toISOString());
      if (src.table === "trades") q = q.not("tx_signature", "is", null);
      const { data, error } = await q
        .order(src.at, { ascending: true })
        .order(src.sig, { ascending: true })
        .range(rows, rows + DB_PAGE - 1);
      if (error) {
        if (this.noteMissing(src.table, error)) return { sigs, readThrough: upper };
        throw new Error(`${src.table} signature read: ${error.message}`);
      }
      const page = (data ?? []) as unknown as Array<Record<string, string | null>>;
      if (page.length === 0) break;
      for (const r of page) {
        const s = r[src.sig];
        if (s) sigs.add(s);
        lastAt = r[src.at] ?? lastAt;
      }
      rows += page.length;
      if (rows >= MAX_ROWS_PER_PASS) {
        // Resume from the last timestamp read; rows sharing it are read again (the sets dedupe them).
        const t = lastAt ? Date.parse(lastAt) : NaN;
        logger.warn("dropped-tx reconcile: row cap reached, the rest is read next pass", { table: src.table, rows });
        return { sigs, readThrough: Number.isFinite(t) ? Math.max(lower + 1, t - 1) : lower };
      }
    }
    return { sigs, readThrough: upper };
  }

  private async removeRows(sigs: string[]): Promise<{ trades: number; v22_events: number; lp_vault_events: number }> {
    const db = this.db;
    // trades go LAST: if an earlier delete fails, the retry still finds the trades and so still resyncs market_stats.
    const del = async <T>(table: Source["table"], col: string, network: string, cols: string): Promise<T[]> => {
      if (this.missing.has(table)) return [];
      const { data, error } = await db.from(table).delete().in(col, sigs).eq("network", network).select(cols);
      if (error) {
        if (this.noteMissing(table, error)) return [];
        throw new Error(`${table} delete: ${error.message}`);
      }
      return (data ?? []) as unknown as T[];
    };
    const v22 = await del<{ signature: string; slab_address: string | null }>("v22_events", "signature", this.network, "signature,slab_address");
    const lp = await del<{ signature: string; registry: string; user_wallet: string }>("lp_vault_events", "signature", this.lpNetwork, "signature,registry,user_wallet");
    for (const r of lp) this.lpRepair.set(`${r.registry}|${r.user_wallet}`, { registry: r.registry, user: r.user_wallet });
    const trades = await del<{ tx_signature: string; slab_address: string }>("trades", "tx_signature", this.network, "tx_signature,slab_address");
    await this.retryLpRepairs();

    droppedTotal += sigs.length;
    const audit = sigs.map((s) => {
      const nt = trades.filter((r) => r.tx_signature === s);
      const nv = v22.filter((r) => r.signature === s);
      const nl = lp.filter((r) => r.signature === s);
      const slab = nt[0]?.slab_address ?? nv.find((r) => r.slab_address)?.slab_address ?? nl[0]?.registry ?? null;
      logger.warn("DROPPED BY FORK: ingested at confirmed, never finalized; its rows were removed", {
        metric: "indexer_fork_dropped_signatures_total",
        signature: s,
        slab,
        trades: nt.length,
        v22_events: nv.length,
        lp_vault_events: nl.length,
      });
      return {
        signature: s,
        source: "fork-dropped",
        slab,
        network: this.network,
        // Not re-indexable (the tx no longer exists): keep it out of the skipped_signatures_pending queue.
        reindexed_at: new Date(this.now).toISOString(),
        error: `dropped by a fork: getSignatureStatuses null on ${this.opts.nullPassesToDrop ?? 2} passes; removed trades=${nt.length} v22_events=${nv.length} lp_vault_events=${nl.length}`,
      };
    });
    try {
      const { error } = await db.from("skipped_signatures").upsert(audit, { onConflict: "signature,source", ignoreDuplicates: true });
      if (error) throw new Error(error.message);
    } catch (err) {
      logger.error("skipped_signatures audit write failed: the DROPPED BY FORK log lines are the record", {
        error: err instanceof Error ? err.message : String(err),
        signatures: sigs,
      });
    }

    const slabs = [...new Set(trades.map((r) => r.slab_address).filter(Boolean))];
    if (slabs.length > 0 && this.opts.onTradesRemoved) {
      try {
        await this.opts.onTradesRemoved(slabs);
      } catch (err) {
        logger.warn("market_stats resync after a fork drop failed (the periodic rollup heals it)", { error: err instanceof Error ? err.message : String(err) });
      }
    }
    return { trades: trades.length, v22_events: v22.length, lp_vault_events: lp.length };
  }

  /** Recompute the LP positions whose events were removed; a failure stays queued for the next pass. */
  private async retryLpRepairs(): Promise<void> {
    if (this.lpRepair.size === 0) return;
    if (!this.opts.lpStore) {
      logger.error("LP events were removed but no LP store is configured: positions are stale until the next LP fold", { positions: this.lpRepair.size });
      this.lpRepair.clear();
      return;
    }
    for (const [k, { registry, user }] of [...this.lpRepair]) {
      try {
        await recomputePosition(this.opts.lpStore, this.lpNetwork, registry, user, { forkRepair: true });
        this.lpRepair.delete(k);
      } catch (err) {
        logger.warn("LP position recompute after a fork drop failed (retried next pass)", { registry, user, error: err instanceof Error ? err.message : String(err) });
        try {
          captureException(err instanceof Error ? err : new Error(String(err)), { tags: { context: "fork-drop-lp-recompute" } });
        } catch {
          /* alerting must not break the pass */
        }
      }
    }
  }
}
