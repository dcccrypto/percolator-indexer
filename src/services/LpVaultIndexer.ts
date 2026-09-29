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
import { decodeLpVaultEvents } from "../lpVault/decoder.js";
import type { LpVaultChain, RegistryInfo } from "../lpVault/chain.js";
import { recomputePosition, type LpVaultStore } from "../lpVault/store.js";

const logger = createLogger("indexer:lp-vault");

export interface LpVaultIndexerOptions {
  chain: LpVaultChain;
  store: LpVaultStore;
  network: string;
  programIds: readonly string[];
  pollIntervalMs?: number;
  discoveryIntervalMs?: number;
  reconcileIntervalMs?: number;
  /** Upper bound on history read for one vault in one sync (backfill safety valve). */
  maxSignaturesPerSync?: number;
}

export interface SyncResult {
  registry: string;
  signatures: number;
  events: number;
  users: string[];
  reconciled: number;
  reconcileSkipped: number;
}

const PAGE = 1000;

export class LpVaultIndexer {
  private readonly registries = new Map<string, RegistryInfo>();
  private readonly cursors = new Map<string, string>();
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
          await this.syncRegistry(reg);
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
    const cap = this.opts.maxSignaturesPerSync ?? 20_000;
    const until = this.cursors.get(reg.registry);

    const sigs: { signature: string; slot: number; failed: boolean }[] = [];
    let before: string | undefined;
    for (;;) {
      const page = await chain.getSignatures(reg.registry, { until, before, limit: PAGE });
      sigs.push(...page);
      if (page.length < PAGE || sigs.length >= cap) break;
      before = page[page.length - 1]!.signature;
    }
    if (sigs.length >= cap) {
      // Folding a partial history would produce a wrong basis, and processing
      // only the newest `cap` would never reach the rest. Fail loudly instead.
      throw new Error(`LP-vault ${reg.registry}: > ${cap} unread signatures; raise maxSignaturesPerSync`);
    }

    const result: SyncResult = { registry: reg.registry, signatures: sigs.length, events: 0, users: [], reconciled: 0, reconcileSkipped: 0 };
    if (sigs.length === 0) return result;

    const newest = sigs[0]!.signature;
    const touched = new Set<string>();
    // Oldest first, so a partially failed batch never leaves a gap behind the cursor.
    for (const s of [...sigs].reverse()) {
      if (s.failed) continue;
      const tx = await chain.getTransaction(s.signature);
      if (!tx) throw new Error(`getTransaction returned null for ${s.signature}`);
      const events = decodeLpVaultEvents(tx, this.wrapperIds)
        .filter((e) => e.registry === reg.registry)
        .map((e) => ({ ...e, market: e.market ?? reg.market, lpMint: e.lpMint ?? reg.lpMint }));
      if (events.length === 0) continue;
      await store.upsertEvents(network, events);
      result.events += events.length;
      for (const e of events) touched.add(e.user);
    }

    for (const user of touched) {
      await recomputePosition(store, network, reg.registry, user, { marketSlab: reg.market });
    }
    this.cursors.set(reg.registry, newest);
    result.users = [...touched];

    const rec = await this.reconcileUsers(reg, result.users, newest);
    result.reconciled = rec.reconciled;
    result.reconcileSkipped = rec.skipped;
    return result;
  }

  /** Reconcile every position in the vault that holds shares (indexed or on-chain). */
  async reconcileOpenPositions(reg: RegistryInfo): Promise<{ reconciled: number; skipped: number }> {
    const expected = this.cursors.get(reg.registry);
    if (!expected) return { reconciled: 0, skipped: 0 };
    const open = await this.opts.store.listOpenPositions(this.opts.network, reg.registry);
    return this.reconcileUsers(reg, open.map((p) => p.user_wallet), expected);
  }

  private async reconcileUsers(
    reg: RegistryInfo,
    users: readonly string[],
    expectedNewest: string,
  ): Promise<{ reconciled: number; skipped: number }> {
    let reconciled = 0;
    let skipped = 0;
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
