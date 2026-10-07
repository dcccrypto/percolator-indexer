import { getSupabase, getNetwork } from "@percolatorct/shared";
import { rebalanceReduceFill } from "../parsers/percolatorTxParser.js";

const PAGE = 1000;

function toBigInt(v: unknown): bigint | null {
  if (typeof v === "bigint") return v;
  if (typeof v === "string" && /^\d+(\.\d+)?$/.test(v)) return BigInt(v.split(".")[0]);
  if (typeof v === "number" && Number.isFinite(v) && v >= 0) return BigInt(Math.trunc(v));
  return null;
}

export interface TraderPositionEvidence {
  /** Signed net of the trader's indexed fills on (slab, asset): long = +size, short = -size. */
  netQ: bigint;
  /**
   * Why `netQ` cannot be trusted as the position immediately before the transaction being
   * indexed, or null when it can. A reason means: do NOT write a row from it.
   */
  uncertain: string | null;
}

/**
 * The trader's indexed position on (slab, asset), plus whether it can be trusted as the
 * position immediately BEFORE the transaction `excludeSignature` (block time `txTimeSec`).
 *
 * The `trades` table has no slot/block time, only `created_at` (index time), and no
 * liquidation size. So the net is only trusted when:
 *  - the transaction's block time is known;
 *  - no liquidation marker exists for this (trader, slab, asset) (a forced close changes the
 *    position without any recorded size);
 *  - every OTHER fill row was indexed strictly before the transaction's block time (a row cannot
 *    be indexed before it happens, so such a row is provably earlier). A row indexed at/after
 *    that time may be a LATER fill (re-index / backfill of an old transaction), which would
 *    corrupt the inference.
 *
 * Throws on a DB error (callers treat that like any other failed insert for the transaction).
 */
export async function fetchTraderPositionEvidence(
  trader: string,
  slabAddress: string,
  assetIndex: number,
  excludeSignature: string,
  txTimeSec: number | null,
): Promise<TraderPositionEvidence> {
  let net = 0n;
  let sawLiquidation = false;
  let sawNotProvablyEarlier = false;
  const cutoffMs = txTimeSec == null ? null : txTimeSec * 1000;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await getSupabase()
      .from("trades")
      .select("side, size, is_liquidation, created_at")
      .eq("trader", trader)
      .eq("slab_address", slabAddress)
      .eq("asset_index", assetIndex)
      .eq("network", getNetwork())
      .neq("tx_signature", excludeSignature)
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`fetchTraderPositionEvidence failed: ${error.message}`);
    const rows = (data ?? []) as Array<{ side: string | null; size: unknown; is_liquidation?: boolean | null; created_at?: string | null }>;
    for (const r of rows) {
      if (r.is_liquidation) { sawLiquidation = true; continue; }
      const size = toBigInt(r.size);
      if (size === null) continue;
      if (r.side === "long") net += size;
      else if (r.side === "short") net -= size;
      const created = r.created_at ? Date.parse(r.created_at) : NaN;
      if (cutoffMs !== null && !(Number.isFinite(created) && created < cutoffMs)) sawNotProvablyEarlier = true;
    }
    if (rows.length < PAGE) break;
  }
  let uncertain: string | null = null;
  if (cutoffMs === null) uncertain = "transaction block time unknown: cannot order this close against indexed fills";
  else if (sawLiquidation) uncertain = "a liquidation marker exists for this position: the indexed net may be wrong";
  else if (sawNotProvablyEarlier) uncertain = "indexed fills for this position are not provably earlier than this close (re-index/backfill after later fills)";
  return { netQ: net, uncertain };
}

export type RebalanceReduceResolution =
  | { ok: true; side: "long" | "short"; sizeValue: bigint }
  | { ok: false; reason: string };

/**
 * Turn a decoded RebalanceReduce into a fill, or say why that cannot be done with certainty.
 * `repeatInTx` = an earlier tag 44 on the same (trader, slab, asset) in the same transaction:
 * its executed size is not known, so the position this one started from is not either.
 */
export async function resolveRebalanceReduce(args: {
  trader: string;
  slabAddress: string;
  assetIndex: number;
  reduceQ: bigint;
  signature: string;
  txTimeSec: number | null;
  repeatInTx: boolean;
}): Promise<RebalanceReduceResolution> {
  if (args.repeatInTx) {
    return { ok: false, reason: "RebalanceReduce (tag 44): a second reduce on the same position in one transaction, executed size of the first is unknown" };
  }
  let ev: TraderPositionEvidence;
  try {
    ev = await fetchTraderPositionEvidence(args.trader, args.slabAddress, args.assetIndex, args.signature, args.txTimeSec);
  } catch (err) {
    // A lookup failure must not abort the rest of the transaction or vanish in a log line.
    return { ok: false, reason: `RebalanceReduce (tag 44): position lookup failed (${err instanceof Error ? err.message : String(err)})` };
  }
  if (ev.uncertain) return { ok: false, reason: `RebalanceReduce (tag 44): ${ev.uncertain}` };
  const fill = rebalanceReduceFill(args.reduceQ, ev.netQ);
  if (!fill) return { ok: false, reason: "RebalanceReduce (tag 44): no indexed open position, side unknown" };
  return { ok: true, ...fill };
}
