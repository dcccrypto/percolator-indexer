import { getSupabase, getNetwork } from "@percolatorct/shared";
import { rebalanceReduceFill } from "../parsers/percolatorTxParser.js";

/**
 * KNOWN LIMITS of resolving a RebalanceReduce (tag 44) from indexed history. The transaction carries
 * no executed size (the wrapper emits no log or return data, and a transaction has no post-state
 * account data), so the fill is inferred. Read these before relying on the rows:
 *
 *  1. SIZE IS AN UPPER BOUND. The written size is min(reduce_q, indexed net position). The engine
 *     executes min(reduce_q, unilateral close capacity, |position|), where capacity depends on matched
 *     OI and the ADL scale at that slot. When it clipped the close by OI or ADL (likely on the
 *     close-only markets where tag 44 is used) the true size is smaller than the one written.
 *  2. THE NET IS PER OWNER WALLET, not per portfolio: fills are keyed by (trader, slab, asset), and
 *     one wallet can own several portfolios.
 *  3. THE LIQUIDATION-MARKER GUARD CANNOT FIRE ON v18: no liquidation markers are emitted there, so a
 *     forced close is invisible to this inference. The guard only protects older data.
 *  4. A SKIPPED SIGNATURE IS RECORDED, NOT RETRIED. It lands in `skipped_signatures` (loud, durable),
 *     but nothing re-indexes it automatically.
 */
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
  /**
   * Signatures this pass/delivery has ALREADY indexed, in chain order, before this transaction.
   * Rows of those are provably earlier even though they were indexed (created_at) after this
   * transaction's block time, which is exactly the real-time case: two transactions seconds apart
   * are both indexed after the second one happened.
   */
  knownEarlier: ReadonlySet<string> = new Set(),
): Promise<TraderPositionEvidence> {
  let net = 0n;
  let sawLiquidation = false;
  let sawNotProvablyEarlier = false;
  const cutoffMs = txTimeSec == null ? null : txTimeSec * 1000;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await getSupabase()
      .from("trades")
      .select("side, size, is_liquidation, created_at, tx_signature")
      .eq("trader", trader)
      .eq("slab_address", slabAddress)
      .eq("asset_index", assetIndex)
      .eq("network", getNetwork())
      .neq("tx_signature", excludeSignature)
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`fetchTraderPositionEvidence failed: ${error.message}`);
    const rows = (data ?? []) as Array<{ side: string | null; size: unknown; is_liquidation?: boolean | null; created_at?: string | null; tx_signature?: string | null }>;
    for (const r of rows) {
      if (r.is_liquidation) { sawLiquidation = true; continue; }
      const size = toBigInt(r.size);
      if (size === null) continue;
      if (r.side === "long") net += size;
      else if (r.side === "short") net -= size;
      const created = r.created_at ? Date.parse(r.created_at) : NaN;
      if (cutoffMs !== null && !(Number.isFinite(created) && created < cutoffMs) && !(r.tx_signature && knownEarlier.has(r.tx_signature))) sawNotProvablyEarlier = true;
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
 * `repeatInTx` = an earlier fill or tag 44 on the same (trader, slab, asset) in the same transaction:
 * it is not in the table yet (or, for a clipped reduce, its executed size is unknown), so the
 * position this close started from is not known either.
 */
export async function resolveRebalanceReduce(args: {
  trader: string;
  slabAddress: string;
  assetIndex: number;
  reduceQ: bigint;
  signature: string;
  txTimeSec: number | null;
  repeatInTx: boolean;
  /** Signatures already indexed earlier in this pass/delivery (chain order); see fetchTraderPositionEvidence. */
  earlierSignatures?: ReadonlySet<string>;
  /** An earlier transaction of this pass/delivery failed, was unreadable or was skipped: the position may have changed unseen. */
  earlierIncomplete?: boolean;
}): Promise<RebalanceReduceResolution> {
  if (args.repeatInTx) {
    return { ok: false, reason: "RebalanceReduce (tag 44): an earlier fill on the same position in this transaction, so the position before this close is not known" };
  }
  if (args.earlierIncomplete) {
    return { ok: false, reason: "RebalanceReduce (tag 44): an earlier transaction in this window could not be indexed, so the position before this close is not known" };
  }
  let ev: TraderPositionEvidence;
  try {
    ev = await fetchTraderPositionEvidence(args.trader, args.slabAddress, args.assetIndex, args.signature, args.txTimeSec, args.earlierSignatures);
  } catch (err) {
    // A lookup failure must not abort the rest of the transaction or vanish in a log line.
    return { ok: false, reason: `RebalanceReduce (tag 44): position lookup failed (${err instanceof Error ? err.message : String(err)})` };
  }
  if (ev.uncertain) return { ok: false, reason: `RebalanceReduce (tag 44): ${ev.uncertain}` };
  const fill = rebalanceReduceFill(args.reduceQ, ev.netQ);
  if (!fill) return { ok: false, reason: "RebalanceReduce (tag 44): no indexed open position, side unknown" };
  return { ok: true, ...fill };
}
