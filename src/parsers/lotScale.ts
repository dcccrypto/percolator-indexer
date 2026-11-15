import { lotExpOfMarketV22 } from "@percolatorct/sdk";
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

// lot_exp is immutable per (market, asset), so a value read once is good for the life of the process.
const newCache = () => new BoundedTtlMap<string, number>(200_000, 24 * 60 * 60 * 1000);
let lotExpCache = newCache();

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
  for (let a = 0; a < 64; a++) {
    let lot: number;
    try {
      lot = lotExpOfMarketV22(data, a);
    } catch (err) {
      // A lot_exp byte above the cap is one bad slot (skip it); anything else means no more slots / not a market.
      if (err instanceof RangeError && /lot_exp byte/.test(err.message)) continue;
      return;
    }
    lotExpCache.set(`${slabAddress}:${a}`, lot);
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
