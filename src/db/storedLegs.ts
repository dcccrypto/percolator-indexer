import { getSupabase, createLogger } from "@percolatorct/shared";
import { CURRENT_NETWORK } from "../network.js";

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
 * with tx_signature), paged past PostgREST max_rows like the batched read. Returns null when the
 * read fails or cannot be shown complete, so callers fall back to the per-leg dedup of the insert
 * itself (23505 / ignoreDuplicates): a failed pre-check must never block indexing.
 */
export async function fetchStoredLegs(signature: string): Promise<StoredLeg[] | null> {
  try {
    return await readAllPages((q) => q.eq("tx_signature", signature));
  } catch (err) {
    logger.warn("stored-leg lookup failed; relying on insert dedup", {
      signature: signature.slice(0, 12),
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

export const IN_CHUNK = 100;
/** Rows requested per page. PostgREST may cap a response lower (max_rows); paging runs until an empty page. */
export const STORED_LEGS_PAGE = 1000;

type PagedRow = StoredLeg & { tx_signature: string; id?: string };
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- postgrest's filter builder generics are not worth spelling out here
type Filter = (q: any) => any;

/**
 * Read every `trades` row matching `filter` on this network, keyset-paged on the unique `id`
 * (not offset: a concurrent insert must not shift rows across a page boundary and count one
 * twice, which would make a leg look stored). Throws unless the read is provably complete: a
 * caller treats a returned list as "these are ALL the stored rows", and a short list makes stored
 * legs look new (inserted twice).
 *  - Only an empty page ends the read: if PostgREST max_rows is below STORED_LEGS_PAGE every
 *    response is capped short, and stopping on a short page would drop the remaining legs.
 *  - The first page asks for the exact row count, so an empty page that comes before all of
 *    them were read (a lagging replica, a dropped body) is caught instead of taken as the end.
 *  - `data: null` with no error (postgrest-js gives that for an empty 2xx body) and a row with
 *    no `id` (keyset cannot continue past it) are failures, not ends.
 */
async function readAllPages(filter: Filter): Promise<PagedRow[]> {
  const rows: PagedRow[] = [];
  let expected: number | null = null;
  let lastId: string | null = null;
  for (;;) {
    const first = lastId === null;
    let q = filter(
      getSupabase()
        .from("trades")
        .select(
          "id, tx_signature, slab_address, asset_index, leg_index, trader, side, size, is_liquidation",
          first ? { count: "exact" } : undefined,
        ),
    ).eq("network", CURRENT_NETWORK);
    if (!first) q = q.gt("id", lastId);
    const { data, error, count } = await q.order("id", { ascending: true }).limit(STORED_LEGS_PAGE);
    if (error) throw new Error(error.message);
    if (!Array.isArray(data)) throw new Error("stored-leg page returned no data and no error");
    if (first) {
      if (typeof count !== "number") throw new Error("stored-leg read returned no row count");
      expected = count;
    }
    const page = data as PagedRow[];
    if (page.length === 0) {
      if (rows.length < (expected ?? 0)) throw new Error(`stored-leg read ended at ${rows.length} of ${expected} rows`);
      return rows;
    }
    if (page.some((r) => r.id == null)) throw new Error("stored-leg row without id");
    const last = String(page[page.length - 1].id);
    // gt(id) never returns the previous last id again; if it does the filter was ignored and
    // paging would loop (or count rows twice)
    if (last === lastId) throw new Error("stored-leg page did not advance past the previous id");
    rows.push(...page);
    lastId = last;
  }
}

/**
 * Stored rows for MANY transactions in as few queries as possible (`in(tx_signature, [...])`, chunked
 * at 100). Every requested signature gets an entry (empty = nothing stored), so a caller can tell
 * "looked, none" from "not looked". Returns null if any chunk failed or could not be read
 * completely (callers then fall back to the per-transaction read or to the insert's own dedup);
 * never a partial map.
 */
export async function fetchStoredLegsMany(signatures: string[]): Promise<Map<string, StoredLeg[]> | null> {
  const out = new Map<string, StoredLeg[]>();
  for (const sig of signatures) out.set(sig, []);
  const unique = [...out.keys()];
  try {
    for (let i = 0; i < unique.length; i += IN_CHUNK) {
      const chunk = unique.slice(i, i + IN_CHUNK);
      // Paged: a chunk of split-order transactions can hold more rows than max_rows, and a
      // silently truncated read would leave the cut-off signatures as "looked, none".
      for (const r of await readAllPages((q) => q.in("tx_signature", chunk))) out.get(r.tx_signature)?.push(r);
    }
    return out;
  } catch (err) {
    logger.warn("batched stored-leg lookup failed; falling back", { error: err instanceof Error ? err.message : String(err) });
    return null;
  }
}

const sizeStr = (v: unknown): string => {
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "number" && Number.isFinite(v)) return BigInt(Math.trunc(v)).toString(); // not Number#toString: it gives '1e+21' from 1e21 up
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

/**
 * TradeCpi / BatchTradeCpi legs: is this fill already stored, whatever SIZE it was stored with?
 * A TradeCpi row's size is the executed (or post-clip) size, not the wire size, and a transaction
 * indexed before the executed size was read holds the wire size, so identity by size no longer
 * works. True when a row exists at exactly (slab, asset, leg_index), or the stored non-liquidation,
 * non-tag-44 rows hold at least `ordinal` fills of the same (slab, asset, trader, side) at other
 * leg numbers (the same fill under the old per-instruction numbering; `ordinal` is the 1-based rank
 * of this leg among the transaction's TradeCpi legs with that key, so a split order's legs are not
 * all dropped because one is stored).
 */
export function cpiFillAlreadyStored(
  stored: StoredLeg[] | null,
  f: { slab: string; assetIndex: number; legIndex: number; trader: string; side: "long" | "short"; ordinal: number },
  reduceLegs?: ReadonlySet<number>,
): boolean {
  if (!stored) return false;
  const fills = stored.filter((r) => !r.is_liquidation && r.slab_address === f.slab && r.asset_index === f.assetIndex && !reduceLegs?.has(r.leg_index));
  if (fills.some((r) => r.leg_index === f.legIndex)) return true;
  return fills.filter((r) => r.trader === f.trader && r.side === f.side).length >= f.ordinal;
}
