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
 *  4. a signature that is `null` on `nullPassesToDrop` separate passes (default 2: a lagging node also answers `null`)
 *     is dropped: its rows are deleted from all three tables, the touched LP positions are recomputed, the removal is
 *     recorded in `skipped_signatures` (source `fork-dropped`) and `onTradesRemoved` re-runs the `market_stats` rollup for
 *     the touched markets.
 * A pass in which every status is `null` (at least {@link SUSPECT_ALL_NULL_MIN} of them) is treated as an RPC fault:
 * no strikes are counted, so a node without transaction history cannot wipe the tables.
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
  ): Promise<{ value: ReadonlyArray<{ confirmationStatus?: string | null } | null> }>;
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
export const SUSPECT_ALL_NULL_MIN = 4;
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
  private readonly pending = new Map<string, { nulls: number; since: number }>();
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
          this.pending.set(s, { nulls: 0, since: now });
        }
      }
      this.scannedThrough = newUpper;
    }
    for (const [s, at] of this.finalized) if (at < now - (this.opts.lookbackMs ?? 30 * 60_000) - OVERLAP_MS) this.finalized.delete(s);

    const sigs = [...this.pending.keys()];
    result.candidates = sigs.length;
    if (sigs.length === 0) return result;

    // 2. Statuses, 256 per call. Any RPC failure aborts the pass with no strikes counted.
    const statuses = new Map<string, { confirmationStatus?: string | null } | null>();
    for (let i = 0; i < sigs.length; i += STATUS_PAGE) {
      const chunk = sigs.slice(i, i + STATUS_PAGE);
      result.rpcCalls++;
      const resp = await this.opts.connection.getSignatureStatuses(chunk, { searchTransactionHistory: true });
      const value = resp?.value;
      if (!Array.isArray(value) || value.length !== chunk.length) {
        throw new Error(`getSignatureStatuses returned ${Array.isArray(value) ? value.length : "no"} statuses for ${chunk.length} signatures`);
      }
      chunk.forEach((s, k) => statuses.set(s, value[k] ?? null));
    }
    result.checked = statuses.size;

    // 3. Resolve. A pass where EVERY status is null looks like a node without history, not a fork.
    const nulls = sigs.filter((s) => statuses.get(s) === null);
    result.suspect = nulls.length === sigs.length && sigs.length >= SUSPECT_ALL_NULL_MIN;
    if (result.suspect) {
      logger.warn("getSignatureStatuses returned null for every signature in the pass: treated as an RPC fault, no rows removed", { signatures: sigs.length });
    }
    const toDrop: string[] = [];
    for (const s of sigs) {
      const st = statuses.get(s);
      const p = this.pending.get(s)!;
      if (st === null) {
        if (!result.suspect) p.nulls++;
        if (p.nulls >= (this.opts.nullPassesToDrop ?? 2)) toDrop.push(s);
        else if (now - p.since > PENDING_MAX_AGE_MS) this.giveUp(s, "still null");
        continue;
      }
      p.nulls = 0;
      if (st?.confirmationStatus === "finalized") {
        this.pending.delete(s);
        this.finalized.set(s, now);
        result.finalized++;
      } else if (now - p.since > PENDING_MAX_AGE_MS) {
        this.giveUp(s, `still ${st?.confirmationStatus ?? "unknown"}`);
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

  private giveUp(sig: string, why: string): void {
    this.pending.delete(sig);
    logger.warn("dropped-tx reconcile gave up on a signature that never resolved", { signature: sig, why });
  }

  /** Distinct signatures of `src` written in (lower, upper]. `readThrough` < upper when the per-pass row cap was hit. */
  private async readSignatures(src: Source, lower: number, upper: number): Promise<{ sigs: Set<string>; readThrough: number }> {
    const network = src.lp ? this.lpNetwork : this.network;
    const sigs = new Set<string>();
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
      if (error) throw new Error(`${src.table} signature read: ${error.message}`);
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
    const t = await db.from("trades").delete().in("tx_signature", sigs).eq("network", this.network).select("tx_signature,slab_address");
    if (t.error) throw new Error(`trades delete: ${t.error.message}`);
    const v = await db.from("v22_events").delete().in("signature", sigs).eq("network", this.network).select("signature,slab_address");
    if (v.error) throw new Error(`v22_events delete: ${v.error.message}`);
    const l = await db.from("lp_vault_events").delete().in("signature", sigs).eq("network", this.lpNetwork).select("signature,registry,user_wallet");
    if (l.error) throw new Error(`lp_vault_events delete: ${l.error.message}`);

    const trades = (t.data ?? []) as Array<{ tx_signature: string; slab_address: string }>;
    const v22 = (v.data ?? []) as Array<{ signature: string; slab_address: string | null }>;
    const lp = (l.data ?? []) as Array<{ signature: string; registry: string; user_wallet: string }>;

    for (const r of lp) this.lpRepair.set(`${r.registry}|${r.user_wallet}`, { registry: r.registry, user: r.user_wallet });
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
