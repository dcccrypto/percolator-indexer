import { getSupabase, getNetwork } from "@percolatorct/shared";

const PAGE = 1000;

function toBigInt(v: unknown): bigint | null {
  if (typeof v === "bigint") return v;
  if (typeof v === "string" && /^\d+(\.\d+)?$/.test(v)) return BigInt(v.split(".")[0]);
  if (typeof v === "number" && Number.isFinite(v) && v >= 0) return BigInt(Math.trunc(v));
  return null;
}

/**
 * The trader's signed net position on (slab, asset) from their indexed fills
 * (long = +size, short = -size), excluding `excludeSignature` so re-indexing a
 * transaction gives the same answer. Liquidation markers (no side/size) are skipped.
 *
 * Used to give a RebalanceReduce (tag 44) close its side: the instruction carries
 * only the reduce magnitude, and after a full close the portfolio holds nothing that
 * says which side it was on. Throws on a DB error (callers treat that like any other
 * failed insert for the transaction).
 */
export async function fetchTraderNetPositionQ(
  trader: string,
  slabAddress: string,
  assetIndex: number,
  excludeSignature: string,
): Promise<bigint> {
  let net = 0n;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await getSupabase()
      .from("trades")
      .select("side, size")
      .eq("trader", trader)
      .eq("slab_address", slabAddress)
      .eq("asset_index", assetIndex)
      .eq("network", getNetwork())
      .eq("is_liquidation", false)
      .neq("tx_signature", excludeSignature)
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`fetchTraderNetPositionQ failed: ${error.message}`);
    const rows = (data ?? []) as Array<{ side: string | null; size: unknown }>;
    for (const r of rows) {
      const size = toBigInt(r.size);
      if (size === null) continue;
      if (r.side === "long") net += size;
      else if (r.side === "short") net -= size;
    }
    if (rows.length < PAGE) return net;
  }
}
