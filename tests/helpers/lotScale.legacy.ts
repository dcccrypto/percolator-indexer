/**
 * FROZEN copy of noteLotExps before the geometry-once rewrite (IDX-PERF-NOTELOTEXPS). Used only by the differential
 * test and the bench as the reference. Returns the entries the old function would have written to the cache.
 */
import { lotExpOfMarketV22 } from "@percolatorct/sdk";

export function legacyNoteLotExps(slabAddress: string, data: Uint8Array): Map<string, number> {
  const out = new Map<string, number>();
  for (let a = 0; a < 64; a++) {
    let lot: number;
    try {
      lot = lotExpOfMarketV22(data, a);
    } catch (err) {
      if (err instanceof RangeError && /lot_exp byte/.test(err.message)) continue;
      return out;
    }
    out.set(`${slabAddress}:${a}`, lot);
  }
  return out;
}
