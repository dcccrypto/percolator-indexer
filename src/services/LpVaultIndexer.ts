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
import { recomputePosition, type LpVaultStore } from "../lpVault/store.js";

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
}

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

  constructor(private readonly opts: LpVaultIndexerOptions) {
    this.wrapperIds = new Set(opts.programIds);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    const interval = this.opts.pollIntervalMs ?? 30_000;
    this.timer = setInterval(() => void this.tick(), interval);
    setTimeout(() => void this.tick(), 5_000);
    logger.info("LpVaultIndexer started", { intervalMs: interval, programIds: this.opts.programIds });
  }

  stop(): void {
    this.running = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
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
          if (fullReconcile) await this.reconcileOpenPositions(reg);
        } catch (err) {
          logger.error("LP-vault sync failed", { registry: reg.registry, error: err instanceof Error ? err.message : String(err) });
          captureException(err instanceof Error ? err : new Error(String(err)), {
            tags: { context: "lp-vault-sync" },
            extra: { registry: reg.registry },
          });
        }
      }
      if (fullReconcile) this.lastFullReconcile = Date.now();
    } catch (err) {
      logger.error("LP-vault tick failed", { error: err instanceof Error ? err.message : String(err) });
    } finally {
      this.busy = false;
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
    let before: string | undefined;
    for (;;) {
      const page = await chain.getSignatures(reg.registry, { until, before, limit: PAGE });
      sigs.push(...page);
      if (page.length < PAGE) break;
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
    const stored = await this.opts.store.loadCursor(this.opts.network, registry);
    if (stored) this.cursors.set(registry, stored);
    return stored ?? undefined;
  }

  /** Advance both cursors. The durable write is best-effort (see LpVaultStore.saveCursor). */
  private async commitCursor(registry: string, signature: string, slot: number): Promise<void> {
    this.cursors.set(registry, signature);
    this.cursorLoaded.add(registry);
    await this.opts.store.saveCursor(this.opts.network, registry, signature, slot);
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
