import { getSupabase, getNetwork, createLogger } from "@percolatorct/shared";

const logger = createLogger("indexer:stored-legs");

/** A fill row already stored for a transaction (the columns needed to recognise the same fill). */
export interface StoredLeg {
  slab_address: string;
  asset_index: number;
  leg_index: number;
  trader: string;
  side: string | null;
  size: unknown;
  is_liquidation?: boolean | null;
}

/**
 * Every row stored for `signature` (one indexed query per transaction; the unique index leads
 * with tx_signature). Returns null when the read fails, so callers fall back to the per-leg
 * dedup of the insert itself (23505 / ignoreDuplicates): a failed pre-check must never block
 * indexing.
 */
export async function fetchStoredLegs(signature: string): Promise<StoredLeg[] | null> {
  try {
    const { data, error } = await getSupabase()
      .from("trades")
      .select("slab_address, asset_index, leg_index, trader, side, size, is_liquidation")
      .eq("tx_signature", signature)
      .eq("network", getNetwork());
    if (error) throw new Error(error.message);
    return (data ?? []) as StoredLeg[];
  } catch (err) {
    logger.warn("stored-leg lookup failed; relying on insert dedup", {
      signature: signature.slice(0, 12),
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

const IN_CHUNK = 100;

/**
 * Stored rows for MANY transactions in as few queries as possible (`in(tx_signature, [...])`, chunked
 * at 100). Every requested signature gets an entry (empty = nothing stored), so a caller can tell
 * "looked, none" from "not looked". Returns null if any chunk failed (callers then fall back to
 * the per-transaction read or to the insert's own dedup).
 */
export async function fetchStoredLegsMany(signatures: string[]): Promise<Map<string, StoredLeg[]> | null> {
  const out = new Map<string, StoredLeg[]>();
  for (const sig of signatures) out.set(sig, []);
  const unique = [...out.keys()];
  try {
    for (let i = 0; i < unique.length; i += IN_CHUNK) {
      const chunk = unique.slice(i, i + IN_CHUNK);
      const { data, error } = await getSupabase()
        .from("trades")
        .select("tx_signature, slab_address, asset_index, leg_index, trader, side, size, is_liquidation")
        .in("tx_signature", chunk)
        .eq("network", getNetwork());
      if (error) throw new Error(error.message);
      for (const r of (data ?? []) as Array<StoredLeg & { tx_signature: string }>) out.get(r.tx_signature)?.push(r);
    }
    return out;
  } catch (err) {
    logger.warn("batched stored-leg lookup failed; falling back", { error: err instanceof Error ? err.message : String(err) });
    return null;
  }
}

const sizeStr = (v: unknown): string => {
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "number" && Number.isFinite(v)) return Math.trunc(v).toString();
  if (typeof v === "string") return v.split(".")[0];
  return "";
};

export interface FillIdentity {
  slab: string;
  assetIndex: number;
  legIndex: number;
  trader: string;
  side: "long" | "short";
  size: string;
  /** 1-based rank of this fill among identical fills (same slab, asset, trader, side, size) in the tx, in leg order. */
  ordinal: number;
}

/**
 * Is this fill already stored, under ANY leg number? True when:
 *  - a row exists at exactly (slab, asset, leg_index), or
 *  - the stored rows hold at least `ordinal` identical fills (same slab, asset, trader, side,
 *    size) at other leg numbers: the same fill indexed under the old per-instruction numbering.
 *
 * The ordinal matters: a split order is several IDENTICAL legs (each the matcher's per-fill
 * cap), so "an identical row exists elsewhere" alone would drop real legs 1..N. A leg is only
 * treated as a duplicate when the stored rows already account for it.
 */
export function fillAlreadyStored(
  stored: StoredLeg[] | null,
  f: FillIdentity,
  /**
   * Leg numbers of this transaction's RebalanceReduce (tag 44) instructions. A tag 44's own row
   * (an inferred fill) is not a TradeCpi fill: it must not count towards "an identical fill is
   * already stored", or a real TradeCpi identical to the inferred close would be dropped.
   */
  reduceLegs: ReadonlySet<number> = new Set(),
): boolean {
  if (!stored) return false;
  const fills = stored.filter((r) => !r.is_liquidation && r.slab_address === f.slab && r.asset_index === f.assetIndex);
  if (fills.some((r) => r.leg_index === f.legIndex)) return true;
  const identical = fills.filter((r) => !reduceLegs.has(r.leg_index) && r.trader === f.trader && r.side === f.side && sizeStr(r.size) === f.size).length;
  return identical >= f.ordinal;
}

/**
 * Is the RebalanceReduce (tag 44) at this leg already stored? Looks for a row at (slab, asset,
 * leg_index) for the same trader whose size could be this close (<= reduce_q: the executed size
 * never exceeds the request). Its side/size are not on the wire, so that is all that can be matched.
 */
export function reduceAlreadyStored(
  stored: StoredLeg[] | null,
  r: { slab: string; assetIndex: number; legIndex: number; trader: string; reduceQ: bigint },
): boolean {
  if (!stored) return false;
  return stored.some((s) => {
    if (s.is_liquidation || s.slab_address !== r.slab || s.asset_index !== r.assetIndex || s.leg_index !== r.legIndex || s.trader !== r.trader) return false;
    const sz = sizeStr(s.size);
    return /^\d+$/.test(sz) && BigInt(sz) <= r.reduceQ;
  });
}
