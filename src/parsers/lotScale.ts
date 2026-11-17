import { ACCOUNT_KIND, LOT_EXP_MAX_V22, lotExpOfMarketV22, resolveLayout, type LayoutTable } from "@percolatorct/sdk";
import { BoundedTtlMap } from "../lib/boundedTtlMap.js";

/**
 * Per-market lot scale (percolator-prog docs/fixes/tiny-price-format.md, option 3).
 *
 * The engine price is quote-E6 per `10^lot_exp` base units, where `lot_exp` is the immutable profile byte (+19)
 * set at InitMarket. So the PER-TOKEN price is `price_e6 / 1e6 / 10^lot_exp`. Every price the engine, the wrapper
 * and the matcher CPI hand us (`effective_price`, `oracle_price_e6`, `price_e6` in the fill events) is per lot.
 * A default market has `lot_exp = 0`, which makes this the identity: `price_e6 / 1e6`, bit for bit what the
 * indexer computed before.
 */

/**
 * `lot_exp` of asset `assetIndex` read from the market account's bytes; 0 when it cannot be read (not a wrapper
 * market, a VERSION the SDK does not know, slot not present, byte out of range). 0 is today's behaviour, so an
 * unreadable market is never priced worse than it was before.
 */
export function lotExpOfSlabData(data: Uint8Array, assetIndex: number): number {
  try {
    return lotExpOfMarketV22(data, assetIndex);
  } catch {
    return 0;
  }
}

/** Per-token price in quote units (dollars) from an engine/matcher price in E6 per lot. lotExp 0 == `priceE6 / 1_000_000`. */
export function perTokenPrice(priceE6: number | bigint, lotExp: number): number {
  const lot = Number.isInteger(lotExp) && lotExp > 0 ? lotExp : 0;
  return Number(priceE6) / 10 ** (6 + lot);
}

/**
 * Per-lot price from a per-token price. Position sizes are in lots (engine Q), so USD notional and fees are
 * `size / POS_SCALE x per-lot price`; `trades.price` is per token, so fee math must scale it back. lot 0: identity.
 */
export function perLotPrice(perToken: number, lotExp: number): number {
  const lot = Number.isInteger(lotExp) && lotExp > 0 ? lotExp : 0;
  return lot === 0 ? perToken : perToken * 10 ** lot;
}

// lot_exp is immutable per (market, asset), so a value read once is good for the life of the process (no TTL;
// the size cap evicts the oldest entries, which are simply re-read on demand by resolveLotExp).
const newCache = () => new BoundedTtlMap<string, number>(200_000, Number.POSITIVE_INFINITY);
let lotExpCache = newCache();
const MAX_SLOTS = 64;

/** Test hook: forget every cached lot_exp. */
export function resetLotExpCache(): void {
  lotExpCache = newCache();
}

/**
 * Remember `lot_exp` of every asset slot present in a market account's bytes. Called wherever the indexer already
 * holds a market's data (the StatsCollector sweep, the slab price reads), so no extra RPC read is ever made for it.
 * Never throws; an unreadable account records nothing.
 */
export function noteLotExps(slabAddress: string, data: Uint8Array): void {
  // Resolve the market geometry ONCE (the SDK's lotExpOfMarketV22 re-resolves it per call and the old loop ended on a
  // thrown RangeError), then read each slot's lot_exp byte directly. Same cache contents as the per-slot SDK loop:
  // not a market / unknown VERSION / too short records nothing; a byte above the cap is one skipped slot.
  let layout: LayoutTable;
  try {
    layout = resolveLayout(data, { parser: "noteLotExps", kind: ACCOUNT_KIND.Market });
  } catch {
    return;
  }
  const slotsBase = layout.marketGroupOff + layout.marketGroupLen;
  if (data.length < slotsBase) return;
  const stride = layout.assetSlotStride;
  const slotCount = Math.min(MAX_SLOTS, Math.floor((data.length - slotsBase) / stride));
  const lotOff = layout.wrapperSlot.profileLotExp;
  for (let a = 0; a < slotCount; a++) {
    const v = data[slotsBase + a * stride + lotOff];
    if (v > LOT_EXP_MAX_V22) continue;
    lotExpCache.set(`${slabAddress}:${a}`, v);
  }
}

/**
 * Cached `lot_exp` of a market asset, or 0 when the market has not been seen yet. TradeCpi fills are priced from the
 * matcher CPI and, by design, never read the slab (RPC budget), so they can only use what the sweep or an earlier
 * slab read has recorded. 0 is today's behaviour.
 */
export function cachedLotExp(slabAddress: string, assetIndex: number): number {
  return lotExpCache.get(`${slabAddress}:${assetIndex}`) ?? 0;
}

/** Cached `lot_exp`, or `undefined` when this (market, asset) has never been read (unlike cachedLotExp, which says 0). */
export function knownLotExp(slabAddress: string, assetIndex: number): number | undefined {
  return lotExpCache.get(`${slabAddress}:${assetIndex}`);
}

/**
 * `lot_exp` to price a TradeCpi fill with, NEVER guessed: the cache, else one read of the market account
 * (`readSlab`, which also fills the cache for every asset slot). Returns `null` when the market could not be read
 * at all (RPC failure / account missing) so the caller defers or skips the row and retries later instead of writing
 * an unscaled price that the insert's ignoreDuplicates would keep forever. A market that was read but has no
 * readable v2.2 slot (legacy layout) is lot 0, which is what it always was.
 */
export async function resolveLotExp(
  slabAddress: string,
  assetIndex: number,
  readSlab: () => Promise<Uint8Array | null | undefined>,
): Promise<number | null> {
  const known = knownLotExp(slabAddress, assetIndex);
  if (known !== undefined) return known;
  let data: Uint8Array | null | undefined;
  try {
    data = await readSlab();
  } catch {
    return null;
  }
  if (!data) return null;
  noteLotExps(slabAddress, data);
  return knownLotExp(slabAddress, assetIndex) ?? 0;
}
