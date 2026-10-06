/**
 * v2.2 activity events: the instructions the v2.2 wrapper adds that are worth a row each and cheap to decode.
 *
 *   bond_deposit (108), bond_withdraw_request (109), bond_withdraw_execute (110)
 *   rescue_deposit (112), insurance_units_init (116)
 *   backstop_propose / backstop_draw / backstop_restore (111, mode 2 / 0 / 1)
 *   holding_rent_settled (106), band_dust_swept (118), eviction (119, EvictAndTradeCpi)
 *
 * Wire layouts mirror the SDK's encoders (`encode*V22` in @percolatorct/sdk) and account positions are taken from
 * the SDK's `ACCOUNTS_*_V22` specs BY NAME, so no account index is a literal in the indexer.
 *
 * What an event carries is what the INSTRUCTION asked for (the args), not what the program settled: a failed tx is
 * never indexed, but a successful BondDeposit with `min_shares` carries the exact `amount`; a backstop draw carries
 * the `max_amount` cap, not the amount drawn. Exact settled amounts are on-chain account state (decode with the SDK's
 * `decodeBondPositionV20` / `decodeInsuranceUnitsV20`). `detail` says which.
 *
 * Input is normalised to {@link RawInstruction} by two adapters (jsonParsed RPC / Atlas WS, and Helius enhanced), so
 * both ingestion paths produce identical rows.
 */
import {
  ACCOUNTS_BOND_DEPOSIT_V22,
  ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22,
  ACCOUNTS_BOND_REQUEST_WITHDRAW_V22,
  ACCOUNTS_INIT_INSURANCE_UNITS_V22,
  ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22,
  ACCOUNTS_RESCUE_DEPOSIT_V22,
  ACCOUNTS_SETTLE_HOLDING_RENT_V22,
  ACCOUNTS_SWEEP_BAND_DUST_LEG_V22,
  ACCOUNTS_EVICT_PREFIX_V22,
  BackstopMode,
  IX_TAG_V22,
  IX_TAG,
  type AccountSpec,
} from "@percolatorct/sdk";
import { decodeBase58 } from "@percolatorct/shared";
import { unwrapEvictAndTradeIx } from "./evictTrade.js";
import { decodeV18SingleFill } from "./percolatorTxParser.js";

export type V22EventKind =
  | "bond_deposit"
  | "bond_withdraw_request"
  | "bond_withdraw_execute"
  | "rescue_deposit"
  | "insurance_units_init"
  | "backstop_propose"
  | "backstop_draw"
  | "backstop_restore"
  | "holding_rent_settled"
  | "band_dust_swept"
  | "eviction";

/** Every kind, in the order of the migration's CHECK constraint (a test pins the two together). */
export const V22_EVENT_KINDS: readonly V22EventKind[] = [
  "bond_deposit", "bond_withdraw_request", "bond_withdraw_execute", "rescue_deposit", "insurance_units_init",
  "backstop_propose", "backstop_draw", "backstop_restore", "holding_rent_settled", "band_dust_swept", "eviction",
];

export interface V22EventRow {
  signature: string;
  /** Top-level instruction index in the tx. */
  ix_index: number;
  /** -1 for a top-level instruction, else the index inside its inner-instruction group. */
  inner_index: number;
  kind: V22EventKind;
  slab_address: string | null;
  asset_index: number | null;
  /** The signer / beneficiary account the instruction names (depositor, holder, rescuer, cranker, caller, trader). */
  actor: string | null;
  /** A portfolio, bond position or units account the event concerns (the victim for an eviction). */
  subject: string | null;
  /** Exact amount in atoms when the instruction carries one (deposits); otherwise null (see `detail`). */
  amount: string | null;
  detail: Record<string, string | number | boolean | null>;
  slot: number | null;
  block_time: string | null;
}

export interface RawInstruction {
  programId: string;
  accounts: string[];
  data: Uint8Array;
  ixIndex: number;
  innerIndex: number;
}

/** Every tag this module decodes (the indexer's webhook/stream filters can use it). */
export const V22_EVENT_TAGS: ReadonlySet<number> = new Set<number>([
  IX_TAG_V22.SettleHoldingRent,
  IX_TAG_V22.BondDeposit,
  IX_TAG_V22.BondRequestWithdraw,
  IX_TAG_V22.BondExecuteWithdraw,
  IX_TAG_V22.InsuranceBackstopDraw,
  IX_TAG_V22.RescueDeposit,
  IX_TAG_V22.InitInsuranceUnits,
  IX_TAG_V22.SweepBandDustLeg,
  IX_TAG_V22.EvictAndTradeCpi,
]);

const idx = (spec: readonly AccountSpec[], name: string): number => {
  const i = spec.findIndex((a) => a.name === name);
  if (i < 0) throw new Error(`v22Events: account '${name}' missing from the SDK account spec`);
  return i;
};

const A = {
  bondDeposit: { actor: idx(ACCOUNTS_BOND_DEPOSIT_V22, "depositor"), market: idx(ACCOUNTS_BOND_DEPOSIT_V22, "market"), subject: idx(ACCOUNTS_BOND_DEPOSIT_V22, "bondPosition") },
  bondRequest: { actor: idx(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22, "holder"), market: idx(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22, "market"), subject: idx(ACCOUNTS_BOND_REQUEST_WITHDRAW_V22, "bondPosition") },
  bondExecute: { actor: idx(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22, "holder"), market: idx(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22, "market"), subject: idx(ACCOUNTS_BOND_EXECUTE_WITHDRAW_V22, "bondPosition") },
  rescue: { actor: idx(ACCOUNTS_RESCUE_DEPOSIT_V22, "rescuer"), market: idx(ACCOUNTS_RESCUE_DEPOSIT_V22, "market") },
  units: { actor: idx(ACCOUNTS_INIT_INSURANCE_UNITS_V22, "payer"), market: idx(ACCOUNTS_INIT_INSURANCE_UNITS_V22, "market"), subject: idx(ACCOUNTS_INIT_INSURANCE_UNITS_V22, "insuranceUnits") },
  backstop: { actor: idx(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22, "cranker"), market: idx(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22, "market"), subject: idx(ACCOUNTS_INSURANCE_BACKSTOP_DRAW_V22, "lpPortfolio") },
  rent: { actor: idx(ACCOUNTS_SETTLE_HOLDING_RENT_V22, "caller"), market: idx(ACCOUNTS_SETTLE_HOLDING_RENT_V22, "market"), subject: idx(ACCOUNTS_SETTLE_HOLDING_RENT_V22, "portfolio") },
  dust: { actor: idx(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22, "caller"), market: idx(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22, "market"), subject: idx(ACCOUNTS_SWEEP_BAND_DUST_LEG_V22, "portfolio") },
  evictVictim: idx(ACCOUNTS_EVICT_PREFIX_V22, "victimPortfolio"),
} as const;

const dv = (d: Uint8Array): DataView => new DataView(d.buffer, d.byteOffset, d.byteLength);
const u16 = (d: Uint8Array, o: number): number => dv(d).getUint16(o, true);
const u64 = (d: Uint8Array, o: number): bigint => dv(d).getBigUint64(o, true);
const u128 = (d: Uint8Array, o: number): bigint => dv(d).getBigUint64(o, true) | (dv(d).getBigUint64(o + 8, true) << 64n);

export interface EventContext {
  signature: string;
  slot?: number | null;
  blockTimeSec?: number | null;
}

/**
 * Decode one instruction into an event row, or null when it is not a v2.2 event, is too short, or lacks the accounts
 * it needs (a malformed instruction is dropped, never half-decoded).
 */
export function decodeV22Event(ix: RawInstruction, ctx: EventContext): V22EventRow | null {
  const d = ix.data;
  if (d.length < 1) return null;
  const tag = d[0];
  if (!V22_EVENT_TAGS.has(tag)) return null;
  const acc = ix.accounts;
  const at = (i: number): string | null => (i < acc.length && acc[i] ? acc[i] : null);

  const base = {
    signature: ctx.signature,
    ix_index: ix.ixIndex,
    inner_index: ix.innerIndex,
    slot: ctx.slot ?? null,
    block_time: ctx.blockTimeSec != null ? new Date(ctx.blockTimeSec * 1000).toISOString() : null,
  };
  const row = (
    kind: V22EventKind,
    a: { actor: number; market: number; subject?: number },
    extra: { asset?: number; amount?: bigint; detail?: V22EventRow["detail"] },
  ): V22EventRow | null => {
    const market = at(a.market);
    if (!market) return null;
    return {
      ...base,
      kind,
      slab_address: market,
      asset_index: extra.asset ?? null,
      actor: at(a.actor),
      subject: a.subject !== undefined ? at(a.subject) : null,
      amount: extra.amount !== undefined ? extra.amount.toString() : null,
      detail: extra.detail ?? {},
    };
  };

  switch (tag) {
    case IX_TAG_V22.BondDeposit: // [108, amount u64, min_shares u128]
      if (d.length < 1 + 8 + 16) return null;
      return row("bond_deposit", A.bondDeposit, { amount: u64(d, 1), detail: { min_shares: u128(d, 9).toString() } });
    case IX_TAG_V22.BondRequestWithdraw: { // [109, shares u128]; 0 cancels
      if (d.length < 1 + 16) return null;
      const shares = u128(d, 1);
      return row("bond_withdraw_request", A.bondRequest, { detail: { shares: shares.toString(), cancel: shares === 0n } });
    }
    case IX_TAG_V22.BondExecuteWithdraw: // [110, min_out u64, source_domain u16]
      if (d.length < 1 + 8 + 2) return null;
      return row("bond_withdraw_execute", A.bondExecute, { detail: { min_out: u64(d, 1).toString(), source_domain: u16(d, 9) } });
    case IX_TAG_V22.RescueDeposit: // [112, tranche u8, amount u64, min_shares u128]
      if (d.length < 1 + 1 + 8 + 16) return null;
      return row("rescue_deposit", A.rescue, { amount: u64(d, 2), detail: { tranche: d[1], min_shares: u128(d, 10).toString() } });
    case IX_TAG_V22.InitInsuranceUnits: // [116]
      return row("insurance_units_init", A.units, {});
    case IX_TAG_V22.InsuranceBackstopDraw: { // [111, mode u8, max_amount u128]
      if (d.length < 1 + 1 + 16) return null;
      const mode = d[1];
      const kind: V22EventKind | null =
        mode === BackstopMode.Propose ? "backstop_propose" : mode === BackstopMode.Draw ? "backstop_draw" : mode === BackstopMode.Restore ? "backstop_restore" : null;
      if (kind === null) return null; // the program refuses any other mode
      return row(kind, A.backstop, { detail: { mode, max_amount: u128(d, 2).toString() } });
    }
    case IX_TAG_V22.SettleHoldingRent: // [106, asset u16, now_slot u64]
      if (d.length < 1 + 2 + 8) return null;
      return row("holding_rent_settled", A.rent, { asset: u16(d, 1), detail: { now_slot: u64(d, 3).toString() } });
    case IX_TAG_V22.SweepBandDustLeg: // [118, asset u16]
      if (d.length < 1 + 2) return null;
      return row("band_dust_swept", A.dust, { asset: u16(d, 1) });
    case IX_TAG_V22.EvictAndTradeCpi: { // [119] + TradeCpi body; accounts = [victim] + TradeCpi accounts (trader at +1, market at +2)
      const victim = at(A.evictVictim);
      const inner = d[0] === IX_TAG_V22.EvictAndTradeCpi && acc.length >= 2 ? unwrapEvictAndTradeIx(d, acc) : null;
      if (!victim || !inner) return null;
      const fill = decodeV18SingleFill(IX_TAG.TradeCpi, inner.data);
      const market = inner.accounts[1] ?? null; // TradeCpi: [0]=trader, [1]=market
      if (!fill || !market) return null;
      return {
        ...base,
        kind: "eviction",
        slab_address: market,
        asset_index: fill.assetIndex,
        actor: inner.accounts[0] ?? null,
        subject: victim,
        amount: null,
        detail: { entrant_size: fill.sizeValue.toString(), entrant_side: fill.side },
      };
    }
    default:
      return null;
  }
}

/** Decode every event in a list of normalised instructions. Never throws. */
export function decodeV22Events(ixs: readonly RawInstruction[], ctx: EventContext): V22EventRow[] {
  const out: V22EventRow[] = [];
  for (const ix of ixs) {
    try {
      const e = decodeV22Event(ix, ctx);
      if (e) out.push(e);
    } catch {
      /* one bad instruction must not lose the others */
    }
  }
  return out;
}

function keyOf(k: unknown): string | null {
  if (!k) return null;
  if (typeof k === "string") return k;
  const o = k as { toBase58?: () => string; pubkey?: unknown };
  if (typeof o.toBase58 === "function") return o.toBase58();
  if (o.pubkey) return keyOf(o.pubkey);
  return null;
}

/**
 * Adapter 1: a jsonParsed transaction (getTransaction / Atlas `transactionSubscribe`). Failed transactions yield
 * nothing. Only instructions of `programIds` are returned; parsed (system/token) instructions are skipped.
 */
export function rawInstructionsFromParsedTx(
  tx: {
    transaction?: { message?: { instructions?: any[] } };
    meta?: { err?: unknown; innerInstructions?: Array<{ index: number; instructions: any[] }> } | null;
  },
  programIds: readonly string[],
): RawInstruction[] {
  if (!tx.meta || tx.meta.err) return [];
  const ids = new Set(programIds);
  const out: RawInstruction[] = [];
  const take = (ix: any, ixIndex: number, innerIndex: number): void => {
    if (!ix || typeof ix !== "object" || "parsed" in ix) return;
    const programId = keyOf(ix.programId);
    if (!programId || !ids.has(programId)) return;
    const data = typeof ix.data === "string" ? decodeBase58(ix.data) : null;
    if (!data) return;
    out.push({ programId, accounts: (ix.accounts ?? []).map((a: unknown) => keyOf(a) ?? ""), data, ixIndex, innerIndex });
  };
  (tx.transaction?.message?.instructions ?? []).forEach((ix, i) => take(ix, i, -1));
  for (const g of tx.meta.innerInstructions ?? []) (g.instructions ?? []).forEach((ix, j) => take(ix, g.index, j));
  return out;
}

/** Adapter 2: a Helius enhanced transaction (`instructions[]` with string accounts, `innerInstructions` groups). */
export function rawInstructionsFromEnhancedTx(
  tx: {
    instructions?: Array<{ programId?: string; accounts?: string[]; data?: string; innerInstructions?: any[] }>;
    innerInstructions?: Array<{ instructions?: any[] }>;
  },
  programIds: ReadonlySet<string>,
): RawInstruction[] {
  const out: RawInstruction[] = [];
  const take = (ix: any, ixIndex: number, innerIndex: number): void => {
    const programId = ix?.programId ?? "";
    if (!programIds.has(programId)) return;
    const data = ix.data ? decodeBase58(ix.data) : null;
    if (!data) return;
    out.push({ programId, accounts: Array.isArray(ix.accounts) ? ix.accounts : [], data, ixIndex, innerIndex });
  };
  let nestedSeen = false;
  (tx.instructions ?? []).forEach((ix, i) => {
    take(ix, i, -1);
    for (const [j, inner] of (ix.innerInstructions ?? []).entries()) {
      nestedSeen = true;
      take(inner, i, j);
    }
  });
  // The flat `innerInstructions` groups are only read when the per-instruction nesting is absent, so one CPI is never
  // counted twice (the (signature, ix_index, inner_index) key would not catch a double count across the two shapes).
  if (!nestedSeen) {
    (tx.innerInstructions ?? []).forEach((g, gi) => (g.instructions ?? []).forEach((inner: unknown, j: number) => take(inner, 1000 + gi, j)));
  }
  return out;
}
