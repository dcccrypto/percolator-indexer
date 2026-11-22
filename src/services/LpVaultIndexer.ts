/**
 * GH#207 — ingest Earn LP-vault deposits/redemptions and maintain per-user
 * average-cost basis.
 *
 * Ingest is a per-VAULT poll: every LP-vault instruction that moves value
 * (75 deposit, 76 request, 77 execute, 81 cancel) takes the vault's registry PDA
 * as an account, so `getSignaturesForAddress(registry)` is a complete, ordered
 * feed for one vault — no dependence on webhook delivery, and a restart simply
 * re-reads history idempotently (events are keyed (signature, ix_index) and the
 * positions are recomputed from them).
 *
 * Reconciliation: LP shares are plain SPL tokens, so a user can receive or send
 * them without touching the wrapper. After each sync, and periodically for every
 * open position, the on-chain claim (all LP token accounts + a live redemption's
 * escrowed shares) is compared with the indexed shares. A mismatch marks the
 * basis unknown. To avoid a false positive from a vault transaction that landed
 * after the sync, the registry's newest signature is re-read AFTER the balance
 * read and the comparison is dropped if it moved — every value-moving LP-vault
 * instruction touches the registry, while a plain SPL transfer does not, so a
 * transfer is exactly what survives the guard.
 */
import { createLogger, captureException } from "@percolatorct/shared";
import { breakerAlertPolls, createBreakerTracker, isPoisonTxError, skipsAcceptable } from "../lib/tolerantTxFetch.js";
import { recordSkippedSignatures, type SkippedSignature } from "../lib/skippedSignatures.js";
import { decodeLpVaultEvents } from "../lpVault/decoder.js";
import type { LpVaultChain, RegistryInfo } from "../lpVault/chain.js";
import { rpcCallTimeoutMs, withCallDeadlines } from "../lib/rpcDeadline.js";
import { loopLiveness, staleBudgetMs } from "../lib/loopLiveness.js";
import { computePositionRow, positionKey, recomputePosition, type LpVaultStore, type PositionRow, type StoredEvent } from "../lpVault/store.js";

const logger = createLogger("indexer:lp-vault");
const lpBreaker = createBreakerTracker(breakerAlertPolls(), (registry, polls) => {
  logger.error("ALERT: LP-vault cursor held by the mass-skip circuit breaker for consecutive polls", { registry, consecutivePolls: polls });
  captureException(new Error(`LP-vault cursor held by mass-skip breaker for ${polls} consecutive polls (${registry})`), { tags: { context: "lp-vault-breaker-held" }, extra: { registry, polls } });
});

export interface LpVaultIndexerOptions {
  chain: LpVaultChain;
  store: LpVaultStore;
  network: string;
  programIds: readonly string[];
  pollIntervalMs?: number;
  discoveryIntervalMs?: number;
  reconcileIntervalMs?: number;
  /**
   * GH#231 — how many transactions are FOLDED in one sync. Enumerating signatures costs one
   * RPC per 1000; it is the per-signature `getTransaction` that is expensive, so this is the
   * bound that matters. A larger backlog drains over several polls rather than being refused.
   */
  maxSignaturesPerSync?: number;
  /**
   * GH#231 — hard stop on *enumeration*, so a vault whose cursor is lost and whose history is
   * enormous cannot page forever in one tick. Reaching it still throws, but with the durable
   * cursor it is not reachable by ordinary traffic or by cheap spam.
   */
  maxBacklogSignatures?: number;
  /**
   * #231 follow-up: keep the cursor in memory only -- never seed it from the durable cursor and never write it. For the
   * backfill tool, whose whole purpose is to re-read a vault's FULL history (a repair after lost event rows, a skipped
   * signature or a corrected decoder). With a durable cursor it would otherwise resume from the live service's position and
   * do nothing, and a full re-read must not move the live service's cursor either. The service leaves this unset.
   */
  ephemeralCursor?: boolean;
  /**
   * Hung-RPC liveness: deadline for each chain call (default RPC_CALL_TIMEOUT_MS, 60 s; 0 = none). Without it one call
   * that never settles keeps `busy` set and every later tick returns at the guard.
   */
  rpcCallTimeoutMs?: number;
}

/** Liveness name of the LP-vault poll loop (lib/loopLiveness.ts). */
export const LP_VAULT_LOOP = "lpVault";

export interface SyncResult {
  registry: string;
  /** Transactions folded in THIS sync (<= maxSignaturesPerSync). */
  signatures: number;
  /** Transactions outstanding when this sync started; > `signatures` means it is still draining. */
  backlog: number;
  events: number;
  users: string[];
  reconciled: number;
  reconcileSkipped: number;
}

const PAGE = 1000;
/** Position rows per upsert statement in the batched full reconcile. */
const RECONCILE_WRITE_CHUNK = 500;

export class LpVaultIndexer {
  private readonly registries = new Map<string, RegistryInfo>();
  private readonly cursors = new Map<string, string>();
  /** Registries whose durable cursor has already been looked up this process (#231). */
  private readonly cursorLoaded = new Set<string>();
  private readonly wrapperIds: ReadonlySet<string>;
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;
  private busy = false;
  private lastDiscovery = 0;
  private lastFullReconcile = 0;

  private readonly opts: LpVaultIndexerOptions;

  constructor(opts: LpVaultIndexerOptions) {
    this.opts = { ...opts, chain: withCallDeadlines(opts.chain, opts.rpcCallTimeoutMs ?? rpcCallTimeoutMs(), "lpVaultChain") };
    this.wrapperIds = new Set(opts.programIds);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    const interval = this.opts.pollIntervalMs ?? 30_000;
    this.timer = setInterval(() => void this.tick(), interval);
    setTimeout(() => void this.tick(), 5_000);
    loopLiveness.register(LP_VAULT_LOOP, staleBudgetMs(interval));
    logger.info("LpVaultIndexer started", { intervalMs: interval, programIds: this.opts.programIds });
  }

  stop(): void {
    this.running = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    loopLiveness.unregister(LP_VAULT_LOOP);
  }

  getRegistries(): RegistryInfo[] {
    return [...this.registries.values()];
  }

  async refreshRegistries(): Promise<RegistryInfo[]> {
    const found = await this.opts.chain.listRegistries(this.opts.programIds);
    for (const r of found) this.registries.set(r.registry, r);
    this.lastDiscovery = Date.now();
    return found;
  }

  /** One polling round. Never throws; per-vault failures are logged and retried next round. */
  async tick(): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      if (Date.now() - this.lastDiscovery > (this.opts.discoveryIntervalMs ?? 300_000)) {
        await this.refreshRegistries();
      }
      const fullReconcile = Date.now() - this.lastFullReconcile > (this.opts.reconcileIntervalMs ?? 600_000);
      const reconcileDue: RegistryInfo[] = [];
      for (const reg of this.registries.values()) {
        try {
          const r = await this.syncRegistry(reg);
          if (r.backlog > r.signatures) {
            // #231: a backlog this pass could not finish. It drains over the next polls; log it
            // so "this vault's Earn data is behind" is visible without reading the database.
            logger.warn("LP-vault sync is draining a backlog", {
              registry: reg.registry, folded: r.signatures, backlog: r.backlog,
            });
          }
          // Only a vault whose sync succeeded is reconciled (as before), but all of them together after the loop:
          // a handful of DB round-trips in total instead of 2 per vault + 4 per open position.
          if (fullReconcile) reconcileDue.push(reg);
          loopLiveness.beat(LP_VAULT_LOOP);
        } catch (err) {
          this.reportVaultFailure(reg.registry, err);
        }
      }
      if (reconcileDue.length > 0) await this.reconcileOpenPositionsMany(reconcileDue);
      if (fullReconcile) this.lastFullReconcile = Date.now();
    } catch (err) {
      logger.error("LP-vault tick failed", { error: err instanceof Error ? err.message : String(err) });
    } finally {
      this.busy = false;
      loopLiveness.beat(LP_VAULT_LOOP);
    }
  }

  /**
   * Ingest every new transaction for one vault (all of history on the first call
   * for it), recompute the touched positions, and reconcile them.
   *
   * The cursor advances only after the whole batch is written, so a failure
   * part-way re-reads the batch next round (idempotent) instead of skipping it.
   */
  async syncRegistry(reg: RegistryInfo): Promise<SyncResult> {
    const { chain, store, network } = this.opts;
    const perSync = this.opts.maxSignaturesPerSync ?? 2_000;
    const hardCap = this.opts.maxBacklogSignatures ?? 500_000;
    const until = await this.cursorFor(reg.registry);

    const sigs: { signature: string; slot: number; failed: boolean }[] = [];
    const seen = new Set<string>();
    let before: string | undefined;
    for (;;) {
      const page = await chain.getSignatures(reg.registry, { until, before, limit: PAGE });
      // A SHORT page does not mean the cursor was reached: a node can cut a page short while older signatures remain,
      // and stopping there committed the cursor past them for good (RPC fault replay, "shortpage": a deposit lost and
      // the position folded wrong). Page on until a page brings nothing new: one extra `before` call per short page.
      const fresh = page.filter((s) => !seen.has(s.signature));
      if (fresh.length === 0) break;
      for (const s of fresh) seen.add(s.signature);
      sigs.push(...fresh);
      if (sigs.length >= hardCap) {
        throw new Error(`LP-vault ${reg.registry}: backlog exceeds ${hardCap} signatures; raise maxBacklogSignatures`);
      }
      before = page[page.length - 1]!.signature;
    }

    const result: SyncResult = { registry: reg.registry, signatures: 0, backlog: sigs.length, events: 0, users: [], reconciled: 0, reconcileSkipped: 0 };
    if (sigs.length === 0) return result;

    // #231: oldest first, and at most `perSync` of them, so a large backlog DRAINS over
    // several polls instead of wedging the vault forever.
    //
    // A prefix of history folds to a correct basis -- just one that is temporarily behind --
    // because `recomputePosition` always re-folds a user's whole stored history, and the
    // cursor commits after each pass, so the next pass picks up exactly where this one
    // stopped. Folding the NEWEST slice instead would produce a wrong basis AND never reach
    // the rest, which is why this used to refuse outright rather than truncate.
    const window = [...sigs].reverse().slice(0, perSync);
    // While draining, the chain head is ahead of what has been folded, so on-chain shares
    // include events this pass has not seen. Reconciling against them would read as a
    // transfer and wrongly clear `basis_known`; defer it to the pass that catches up.
    const draining = window.length < sigs.length;
    const newest = window[window.length - 1]!.signature;
    const newestSlot = window[window.length - 1]!.slot;
    result.signatures = window.length;
    const touched = new Set<string>();
    const skippedTxs: SkippedSignature[] = [];
    let readOk = 0;
    // Oldest first, so a partially failed batch never leaves a gap behind the cursor.
    for (const s of window) {
      if (s.failed) continue;
      let tx;
      try {
        tx = await chain.getTransaction(s.signature);
      } catch (err) {
        // X-1: a transaction this client cannot return (a v1 tx on an old reader, -32015) must not wedge the vault's
        // cursor forever. Skip and log it; any other error still aborts the sync so the batch is re-read.
        if (!isPoisonTxError(err)) throw err;
        skippedTxs.push({ signature: s.signature, source: "lp-vault", slab: reg.registry, error: String(err instanceof Error ? err.message : err) });
        continue;
      }
      if (!tx) throw new Error(`getTransaction returned null for ${s.signature}`);
      readOk++;
      const events = decodeLpVaultEvents(tx, this.wrapperIds)
        .filter((e) => e.registry === reg.registry)
        .map((e) => ({ ...e, market: e.market ?? reg.market, lpMint: e.lpMint ?? reg.lpMint }));
      if (events.length === 0) continue;
      await store.upsertEvents(network, events);
      result.events += events.length;
      for (const e of events) touched.add(e.user);
    }

    // Circuit breaker, "successful sibling" rule (same as the trade indexer): up to 5 skips and only if at least one other
    // transaction of the window was read successfully; otherwise the reader is broken, hold the cursor.
    if (!skipsAcceptable(skippedTxs.length, readOk)) {
      const polls = lpBreaker.held(reg.registry);
      throw new Error(`LP-vault ${reg.registry}: mass skip refused (${skippedTxs.length} unreadable, ${readOk} read ok in one window; ${polls} consecutive polls); cursor held`);
    }
    lpBreaker.clear(reg.registry);

    for (const user of touched) {
      await recomputePosition(store, network, reg.registry, user, { marketSlab: reg.market });
    }
    result.users = [...touched];

    if (skippedTxs.length > 0) {
      // The folded positions are missing the skipped tx's events: never advance past it silently. The signatures are
      // recorded durably (re-indexable) and every open position is reconciled against on-chain shares now.
      await recordSkippedSignatures(skippedTxs);
      logger.error("LP-vault cursor advanced past skipped transaction(s): reconciling every open position against on-chain shares", {
        registry: reg.registry,
        signatures: skippedTxs.map((t) => t.signature),
      });
      const open = await store.listOpenPositions(network, reg.registry);
      const rec = await this.reconcileUsers(reg, open.map((p) => p.user_wallet), newest, draining);
      result.reconciled += rec.reconciled;
      result.reconcileSkipped += rec.skipped;
    }

    const rec = await this.reconcileUsers(reg, result.users, newest, draining);
    result.reconciled += rec.reconciled;
    result.reconcileSkipped += rec.skipped;
    // Only after the skipped signatures are recorded durably AND the reconcile ran does the cursor move past them.
    await this.commitCursor(reg.registry, newest, newestSlot);
    return result;
  }

  /**
   * #231: the in-memory cursor, seeded once per process from the durable one so a restart
   * does not re-page the vault's whole history.
   *
   * If the stored signature is one the RPC no longer returns (pruned history), `until` is
   * simply not found and the enumeration behaves as though it were absent -- the same
   * position as before this change, and now drained in chunks rather than refused.
   */
  private async cursorFor(registry: string): Promise<string | undefined> {
    const mem = this.cursors.get(registry);
    if (mem) return mem;
    // Ask the store once per registry per process: a vault with no stored cursor is the
    // normal case for a new vault and must not cost a query every poll.
    if (this.cursorLoaded.has(registry)) return undefined;
    this.cursorLoaded.add(registry);
    if (this.opts.ephemeralCursor) return undefined;
    const stored = await this.opts.store.loadCursor(this.opts.network, registry);
    if (stored) this.cursors.set(registry, stored);
    return stored ?? undefined;
  }

  /** Advance both cursors. The durable write is best-effort (see LpVaultStore.saveCursor). */
  private async commitCursor(registry: string, signature: string, slot: number): Promise<void> {
    this.cursors.set(registry, signature);
    this.cursorLoaded.add(registry);
    if (this.opts.ephemeralCursor) return;
    await this.opts.store.saveCursor(this.opts.network, registry, signature, slot);
  }

  /** Log + capture one vault's failure; the tick carries on with the other vaults. */
  private reportVaultFailure(registry: string, err: unknown, stage?: "reconcile"): void {
    logger.error("LP-vault sync failed", {
      registry, error: err instanceof Error ? err.message : String(err), ...(stage ? { stage } : {}),
    });
    captureException(err instanceof Error ? err : new Error(String(err)), {
      tags: { context: "lp-vault-sync" },
      extra: { registry, ...(stage ? { stage } : {}) },
    });
  }

  /** The per-vault full reconcile, one vault at a time; a failing vault is reported and the others still run. */
  private async reconcileEach(regs: readonly RegistryInfo[]): Promise<{ reconciled: number; skipped: number }> {
    let reconciled = 0;
    let skipped = 0;
    for (const reg of regs) {
      try {
        const r = await this.reconcileOpenPositions(reg);
        reconciled += r.reconciled;
        skipped += r.skipped;
      } catch (err) {
        this.reportVaultFailure(reg.registry, err, "reconcile");
      }
      loopLiveness.beat(LP_VAULT_LOOP);
    }
    return { reconciled, skipped };
  }

  /**
   * The full reconcile of many vaults with batched DB access: one paged read of their open positions, one paged read
   * of those positions' events, and the rows written RECONCILE_WRITE_CHUNK per statement. The per-user chain reads
   * (claim + newest signature) and the "a vault tx landed after our sync" skip are the same as reconcileOpenPositions.
   *
   * Never throws. Failure handling is at least as safe as the per-vault path:
   * - a failed batched read: nothing was written yet, so every vault falls back to reconcileOpenPositions;
   * - a failed chain read: that vault is reported and its remaining users are skipped (its earlier users still land);
   * - a failed batched write: one statement is all or nothing, so none of that chunk landed; its rows are written one
   *   by one, every vault with a row that still fails is reported, and one bad row no longer blocks its siblings.
   */
  async reconcileOpenPositionsMany(regs: readonly RegistryInfo[]): Promise<{ reconciled: number; skipped: number }> {
    const { store, network, chain } = this.opts;
    if (!store.listOpenPositionsMany || !store.loadEventsMany || !store.writePositions) return this.reconcileEach(regs);

    const due: { reg: RegistryInfo; expected: string }[] = [];
    for (const reg of regs) {
      try {
        const expected = await this.cursorFor(reg.registry);
        if (expected) due.push({ reg, expected });
      } catch (err) {
        this.reportVaultFailure(reg.registry, err, "reconcile");
      }
    }
    if (due.length === 0) return { reconciled: 0, skipped: 0 };

    let open: PositionRow[];
    let events: Map<string, StoredEvent[]>;
    try {
      open = await store.listOpenPositionsMany(network, due.map((d) => d.reg.registry));
      events = open.length > 0 ? await store.loadEventsMany(network, open) : new Map();
    } catch (err) {
      logger.warn("batched LP-vault reconcile read failed; reconciling vault by vault", {
        vaults: due.length, error: err instanceof Error ? err.message : String(err),
      });
      return this.reconcileEach(due.map((d) => d.reg));
    }

    const openByVault = new Map<string, PositionRow[]>();
    for (const p of open) {
      const list = openByVault.get(p.registry);
      if (list) list.push(p);
      else openByVault.set(p.registry, [p]);
    }

    const rows: PositionRow[] = [];
    let skipped = 0;
    for (const { reg, expected } of due) {
      try {
        for (const prev of openByVault.get(reg.registry) ?? []) {
          const user = prev.user_wallet;
          const claim = await chain.getClaim(user, reg.lpMint, reg.registry, reg.programId);
          const [latest] = await chain.getSignatures(reg.registry, { limit: 1 });
          if (latest && latest.signature !== expected) {
            // A vault transaction landed after our sync; the balance may include it.
            skipped++;
            continue;
          }
          const history = events.get(positionKey(reg.registry, user));
          // Never fold a history the read did not return: an empty fold would zero the row.
          if (!history) throw new Error(`lp_vault_events: no history returned for ${reg.registry}/${user}`);
          rows.push(computePositionRow(network, reg.registry, user, history, prev, {
            marketSlab: reg.market,
            reconcile: { onchainShares: claim.heldShares + claim.pendingShares, contextSlot: claim.contextSlot },
          }));
        }
      } catch (err) {
        this.reportVaultFailure(reg.registry, err, "reconcile");
      }
      loopLiveness.beat(LP_VAULT_LOOP);
    }

    let reconciled = 0;
    for (let i = 0; i < rows.length; i += RECONCILE_WRITE_CHUNK) {
      const chunk = rows.slice(i, i + RECONCILE_WRITE_CHUNK);
      try {
        await store.writePositions(chunk);
        reconciled += chunk.length;
        loopLiveness.beat(LP_VAULT_LOOP);
        continue;
      } catch (err) {
        logger.warn("batched LP-vault position write failed; writing its rows one by one", {
          rows: chunk.length, error: err instanceof Error ? err.message : String(err),
        });
      }
      // Nothing of the chunk landed (one statement). Row by row costs at most one request per row, never more than
      // the per-vault path spends on the same rows, and a row that fails costs only itself.
      const failed = new Map<string, unknown>();
      for (const row of chunk) {
        try {
          await store.writePosition(row);
          reconciled++;
        } catch (err) {
          if (!failed.has(row.registry)) failed.set(row.registry, err);
        }
      }
      for (const [registry, err] of failed) this.reportVaultFailure(registry, err, "reconcile");
      loopLiveness.beat(LP_VAULT_LOOP);
    }
    return { reconciled, skipped };
  }

  /** Reconcile every position in the vault that holds shares (indexed or on-chain). */
  async reconcileOpenPositions(reg: RegistryInfo): Promise<{ reconciled: number; skipped: number }> {
    const expected = await this.cursorFor(reg.registry);
    if (!expected) return { reconciled: 0, skipped: 0 };
    const open = await this.opts.store.listOpenPositions(this.opts.network, reg.registry);
    return this.reconcileUsers(reg, open.map((p) => p.user_wallet), expected);
  }

  private async reconcileUsers(
    reg: RegistryInfo,
    users: readonly string[],
    expectedNewest: string,
    draining = false,
  ): Promise<{ reconciled: number; skipped: number }> {
    let reconciled = 0;
    let skipped = 0;
    // #231: mid-drain the chain is knowingly ahead of the fold, so every user would be
    // skipped by the check below anyway. Skip them without spending two RPCs each.
    if (draining) return { reconciled: 0, skipped: users.length };
    for (const user of users) {
      const claim = await this.opts.chain.getClaim(user, reg.lpMint, reg.registry, reg.programId);
      const [latest] = await this.opts.chain.getSignatures(reg.registry, { limit: 1 });
      if (latest && latest.signature !== expectedNewest) {
        // A vault transaction landed after our sync; the balance may include it.
        skipped++;
        continue;
      }
      await recomputePosition(this.opts.store, this.opts.network, reg.registry, user, {
        marketSlab: reg.market,
        reconcile: { onchainShares: claim.heldShares + claim.pendingShares, contextSlot: claim.contextSlot },
      });
      reconciled++;
    }
    return { reconciled, skipped };
  }
}
