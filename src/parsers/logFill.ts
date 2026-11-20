/**
 * The wrapper's own `fill_event` record for one trade leg, looked up from the tx's decoded log events
 * (parsers/v22FillEvents.ts). `executed_q`, the ZERO flag, `price_e6` and `fee_atoms` are the wrapper's
 * statements (trusted); `requested_q` / `quoted_price_e6` are not used here.
 *
 * Shared by the poll path and the event stream so both store the same size and fee for a fill.
 */
import type { V22EventRow } from "./v22Events.js";

export interface LogFill {
  zero: boolean;
  /** Signed executed size (POS_SCALE units). */
  executedQ: bigint;
  priceE6: bigint;
  /** Total engine trade fee of this leg across both portfolios, in collateral atoms. */
  feeAtoms: bigint;
}

export interface LogFillKey {
  /** Top-level instruction index of the trade instruction (with the market it pins the instruction; the event's actor is
   *  the taker portfolio, not the signer `trades.trader` stores, so it is not part of the key). */
  ixIndex: number;
  slab: string;
  assetIndex: number;
  /** Position of the leg inside its instruction (0 for single fills). */
  legPos: number;
}

const big = (v: unknown): bigint | null => {
  if (typeof v !== "string" || !/^-?\d+$/.test(v)) return null;
  return BigInt(v);
};

/**
 * The single `fill_event` record for this leg, or null when the tx has none (no logs, truncated logs, a wrapper that
 * does not emit them) or the match is ambiguous. Null means "fall back to what the caller did before".
 */
export function logFillFor(rows: readonly V22EventRow[] | null | undefined, k: LogFillKey): LogFill | null {
  if (!rows || rows.length === 0) return null;
  let hits = rows.filter((r) =>
    r.kind === "fill_event" && r.detail?.source === "log" &&
    r.ix_index === k.ixIndex && r.slab_address === k.slab && r.asset_index === k.assetIndex);
  if (hits.length > 1) hits = hits.filter((r) => r.detail.rec_index === k.legPos);
  if (hits.length !== 1) return null;
  const d = hits[0].detail;
  const executedQ = big(d.executed_q);
  const priceE6 = big(d.price_e6);
  const feeAtoms = big(d.fee_atoms_total);
  if (executedQ === null || priceE6 === null || feeAtoms === null || typeof d.zero !== "boolean") return null;
  return { zero: d.zero, executedQ, priceE6, feeAtoms };
}

/**
 * `trades.fee` from the fill_event: fee atoms in the same quote units as `trades.price x size / POS_SCALE`
 * (the engine charges the fee on `|executed_q| x price_e6 / POS_SCALE` atoms, so the scale is 1e6, as for price).
 */
export function feeFromAtoms(feeAtoms: bigint): number {
  return Number(feeAtoms) / 1_000_000;
}
