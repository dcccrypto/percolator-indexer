/**
 * GH#207 — persistence for LP-vault events and cost-basis positions.
 *
 * Two implementations behind one interface: Supabase (production) and in-memory
 * (tests, and the backfill script's default dry run, which must never write).
 */
import type { SupabaseClient } from "@supabase/supabase-js";
import type { LpVaultEvent, LpVaultEventKind } from "./decoder.js";
import { basisKnown, foldPosition, type FoldEvent, type FoldedPosition } from "./position.js";

export interface PositionRow {
  network: string;
  registry: string;
  user_wallet: string;
  market_slab: string | null;
  lp_mint: string | null;
  lp_shares: string;
  pending_redeem_shares: string;
  cost_basis_atoms: string;
  realized_pnl_atoms: string;
  total_deposited_atoms: string;
  total_redeemed_atoms: string;
  basis_known: boolean;
  inconsistency: string | null;
  transfer_detected_slot: number | null;
  onchain_lp_shares: string | null;
  reconciled_slot: number | null;
  event_count: number;
  updated_slot: number;
}

export interface StoredEvent extends FoldEvent {
  marketSlab: string | null;
  lpMint: string | null;
}

export interface LpVaultStore {
  /** Idempotent on (signature, ix_index). */
  upsertEvents(network: string, events: readonly LpVaultEvent[]): Promise<void>;
  loadEvents(network: string, registry: string, user: string): Promise<StoredEvent[]>;
  loadPosition(network: string, registry: string, user: string): Promise<PositionRow | null>;
  writePosition(row: PositionRow): Promise<void>;
  /** Positions in a registry that may hold shares (indexed or on-chain). */
  listOpenPositions(network: string, registry: string): Promise<PositionRow[]>;
}

export interface Reconciliation {
  /** Held LP (all of the user's token accounts for the mint) + pending redemption shares. */
  onchainShares: bigint;
  contextSlot: number;
}

/**
 * Recompute a position from its full event history and persist it.
 *
 * With `reconcile`, the on-chain claim is compared with the indexed shares:
 * a mismatch records `transfer_detected_slot` (sticky — see basisKnown), and a
 * match at zero on both sides clears it (full exit).
 */
export async function recomputePosition(
  store: LpVaultStore,
  network: string,
  registry: string,
  user: string,
  opts: { marketSlab?: string | null; reconcile?: Reconciliation } = {},
): Promise<PositionRow> {
  const [events, prev] = await Promise.all([
    store.loadEvents(network, registry, user),
    store.loadPosition(network, registry, user),
  ]);
  const folded: FoldedPosition = foldPosition(events);

  let transferDetectedSlot = prev?.transfer_detected_slot ?? null;
  let onchain = prev?.onchain_lp_shares ?? null;
  let reconciledSlot = prev?.reconciled_slot ?? null;
  if (opts.reconcile) {
    const { onchainShares, contextSlot } = opts.reconcile;
    onchain = onchainShares.toString();
    reconciledSlot = contextSlot;
    if (onchainShares !== folded.lpShares) transferDetectedSlot = contextSlot;
    else if (onchainShares === 0n) transferDetectedSlot = null;
  }

  const marketSlab =
    opts.marketSlab ?? prev?.market_slab ?? events.find((e) => e.marketSlab)?.marketSlab ?? null;
  const lpMint = prev?.lp_mint ?? events.find((e) => e.lpMint)?.lpMint ?? null;

  const row: PositionRow = {
    network,
    registry,
    user_wallet: user,
    market_slab: marketSlab,
    lp_mint: lpMint,
    lp_shares: folded.lpShares.toString(),
    pending_redeem_shares: folded.pendingRedeemShares.toString(),
    cost_basis_atoms: folded.costBasisAtoms.toString(),
    realized_pnl_atoms: folded.realizedPnlAtoms.toString(),
    total_deposited_atoms: folded.totalDepositedAtoms.toString(),
    total_redeemed_atoms: folded.totalRedeemedAtoms.toString(),
    basis_known: basisKnown(folded, transferDetectedSlot),
    inconsistency:
      folded.inconsistency ??
      (transferDetectedSlot !== null && !basisKnown(folded, transferDetectedSlot)
        ? `on-chain LP claim differed from indexed shares at slot ${transferDetectedSlot} (LP transferred outside the vault)`
        : null),
    transfer_detected_slot: transferDetectedSlot,
    onchain_lp_shares: onchain,
    reconciled_slot: reconciledSlot,
    event_count: folded.eventCount,
    updated_slot: folded.lastSlot,
  };
  await store.writePosition(row);
  return row;
}

// ── in-memory ───────────────────────────────────────────────────────────────

export class MemoryLpVaultStore implements LpVaultStore {
  readonly events = new Map<string, { network: string; ev: LpVaultEvent }>();
  readonly positions = new Map<string, PositionRow>();

  private static pk(network: string, registry: string, user: string): string {
    return `${network}|${registry}|${user}`;
  }

  async upsertEvents(network: string, events: readonly LpVaultEvent[]): Promise<void> {
    for (const ev of events) {
      const k = `${ev.signature}|${ev.ixIndex}`;
      if (!this.events.has(k)) this.events.set(k, { network, ev });
    }
  }

  async loadEvents(network: string, registry: string, user: string): Promise<StoredEvent[]> {
    return [...this.events.values()]
      .filter((r) => r.network === network && r.ev.registry === registry && r.ev.user === user)
      .map(({ ev }) => ({
        kind: ev.kind, slot: ev.slot, signature: ev.signature, ixIndex: ev.ixIndex,
        collateralAtoms: ev.collateralAtoms, lpAmount: ev.lpAmount,
        marketSlab: ev.market, lpMint: ev.lpMint,
      }));
  }

  async loadPosition(network: string, registry: string, user: string): Promise<PositionRow | null> {
    return this.positions.get(MemoryLpVaultStore.pk(network, registry, user)) ?? null;
  }

  async writePosition(row: PositionRow): Promise<void> {
    this.positions.set(MemoryLpVaultStore.pk(row.network, row.registry, row.user_wallet), { ...row });
  }

  async listOpenPositions(network: string, registry: string): Promise<PositionRow[]> {
    return [...this.positions.values()].filter(
      (p) => p.network === network && p.registry === registry &&
        (p.lp_shares !== "0" || (p.onchain_lp_shares !== null && p.onchain_lp_shares !== "0")),
    );
  }
}

// ── Supabase ────────────────────────────────────────────────────────────────

interface EventRow {
  signature: string;
  ix_index: number;
  kind: LpVaultEventKind;
  slot: number;
  market_slab: string | null;
  lp_mint: string | null;
  collateral_atoms: string;
  lp_amount: string | null;
}

/** numeric columns are selected ::text so u128 values never pass through a JS number. */
const POSITION_COLUMNS = [
  "network", "registry", "user_wallet", "market_slab", "lp_mint",
  "lp_shares::text", "pending_redeem_shares::text", "cost_basis_atoms::text",
  "realized_pnl_atoms::text", "total_deposited_atoms::text", "total_redeemed_atoms::text",
  "basis_known", "inconsistency", "transfer_detected_slot", "onchain_lp_shares::text",
  "reconciled_slot", "event_count", "updated_slot",
].join(",");

export class SupabaseLpVaultStore implements LpVaultStore {
  constructor(private readonly db: SupabaseClient) {}

  async upsertEvents(network: string, events: readonly LpVaultEvent[]): Promise<void> {
    if (events.length === 0) return;
    const rows = events.map((ev) => ({
      signature: ev.signature,
      ix_index: ev.ixIndex,
      network,
      kind: ev.kind,
      program_id: ev.programId,
      market_slab: ev.market,
      registry: ev.registry,
      user_wallet: ev.user,
      lp_mint: ev.lpMint,
      collateral_atoms: ev.collateralAtoms.toString(),
      lp_amount: ev.lpAmount === null ? null : ev.lpAmount.toString(),
      domain: ev.domain,
      slot: ev.slot,
      block_time: ev.blockTime === null ? null : new Date(ev.blockTime * 1000).toISOString(),
    }));
    const { error } = await this.db
      .from("lp_vault_events")
      .upsert(rows, { onConflict: "signature,ix_index", ignoreDuplicates: true });
    if (error) throw new Error(`lp_vault_events upsert: ${error.message}`);
  }

  async loadEvents(network: string, registry: string, user: string): Promise<StoredEvent[]> {
    const { data, error } = await this.db
      .from("lp_vault_events")
      .select("signature,ix_index,kind,slot,market_slab,lp_mint,collateral_atoms::text,lp_amount::text")
      .eq("network", network)
      .eq("registry", registry)
      .eq("user_wallet", user)
      .order("slot", { ascending: true })
      .limit(10_000);
    if (error) throw new Error(`lp_vault_events select: ${error.message}`);
    const rows = (data ?? []) as unknown as EventRow[];
    if (rows.length >= 10_000) {
      // A fold over a truncated history would be silently wrong. Refuse instead.
      throw new Error(`lp_vault_events: ${registry}/${user} has >= 10000 events; fold refused`);
    }
    return rows.map((r) => ({
      kind: r.kind,
      slot: Number(r.slot),
      signature: r.signature,
      ixIndex: r.ix_index,
      collateralAtoms: BigInt(r.collateral_atoms),
      lpAmount: r.lp_amount === null ? null : BigInt(r.lp_amount),
      marketSlab: r.market_slab,
      lpMint: r.lp_mint,
    }));
  }

  async loadPosition(network: string, registry: string, user: string): Promise<PositionRow | null> {
    const { data, error } = await this.db
      .from("lp_vault_positions")
      .select(POSITION_COLUMNS)
      .eq("network", network)
      .eq("registry", registry)
      .eq("user_wallet", user)
      .maybeSingle();
    if (error) throw new Error(`lp_vault_positions select: ${error.message}`);
    return (data as unknown as PositionRow | null) ?? null;
  }

  async writePosition(row: PositionRow): Promise<void> {
    const { error } = await this.db
      .from("lp_vault_positions")
      .upsert({ ...row, updated_at: new Date().toISOString() }, { onConflict: "network,registry,user_wallet" });
    if (error) throw new Error(`lp_vault_positions upsert: ${error.message}`);
  }

  async listOpenPositions(network: string, registry: string): Promise<PositionRow[]> {
    const { data, error } = await this.db
      .from("lp_vault_positions")
      .select(POSITION_COLUMNS)
      .eq("network", network)
      .eq("registry", registry)
      .or("lp_shares.gt.0,onchain_lp_shares.gt.0")
      .limit(5_000);
    if (error) throw new Error(`lp_vault_positions list: ${error.message}`);
    return (data ?? []) as unknown as PositionRow[];
  }
}
