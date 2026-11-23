/**
 * Per-market lot scale (percolator-prog docs/fixes/tiny-price-format.md): the engine price is quote-E6 per
 * 10^lot_exp base units, so the per-token price is price_e6 / 1e6 / 10^lot_exp. A default (lot_exp = 0) market
 * must come out exactly as before (price_e6 / 1e6).
 */
import { describe, it, expect, beforeEach, vi } from "vitest";
import { knownLotExp, resolveLotExp, cachedLotExp, lotExpOfSlabData, noteLotExps, perLotPrice, perTokenPrice, resetLotExpCache } from "../../src/parsers/lotScale.js";
import { readAssetEffectivePriceE6 } from "../../src/parsers/markPrice.js";
import { buildMarket, LAYOUT_V21, LAYOUT_V22, REAL_MARKET_V21 } from "../helpers/v22Fixtures.js";

type Layout = typeof LAYOUT_V22;
const slotOff = (l: Layout, i: number) => l.marketGroupOff + l.marketGroupLen + i * l.assetSlotStride;

/** A market whose asset i has effective_price prices[i] and lot_exp lots[i]. */
function market(layout: Layout, prices: bigint[], lots: number[], version?: number): Uint8Array {
  const d = buildMarket(layout, { slots: prices.length }, version);
  const dv = new DataView(d.buffer, d.byteOffset, d.byteLength);
  const slot = (i: number) => layout.marketGroupOff + layout.marketGroupLen + i * layout.assetSlotStride;
  prices.forEach((p, i) => {
    dv.setBigUint64(slot(i) + layout.wrapperSlotLen + layout.assetState.effectivePrice, p, true);
    d[slot(i) + layout.wrapperSlot.profileLotExp] = lots[i]!;
  });
  return d;
}

/** What the indexer shows for asset `a`: the engine price scaled by the market's own lot_exp. */
function shown(d: Uint8Array, a: number): number {
  return perTokenPrice(readAssetEffectivePriceE6(d, a)!, lotExpOfSlabData(d, a));
}

beforeEach(() => resetLotExpCache());

describe("perTokenPrice", () => {
  it("lot 0 is exactly price_e6 / 1_000_000, bit for bit (default markets are unchanged)", () => {
    for (const p of [1n, 33n, 85_187_279n, 13_614_586n, 999_999_999_999n, 1_500_000n]) {
      expect(perTokenPrice(p, 0)).toBe(Number(p) / 1_000_000);
    }
  });

  it("a non-default lot divides by a further 10^lot_exp (design note: $3e-7 at lot 6 publishes 300_000)", () => {
    expect(perTokenPrice(300_000n, 6)).toBeCloseTo(3e-7, 20);
    expect(perTokenPrice(500_000, 6)).toBeCloseTo(5e-7, 20);
    expect(perTokenPrice(1_666_666, 6)).toBeCloseTo(1.666666e-6, 18);
    expect(perTokenPrice(1_000_000n, 15)).toBe(1e-15);
  });

  it("a negative, fractional or NaN lot is treated as 0 rather than producing NaN or a scaled-up price", () => {
    expect(perTokenPrice(1_500_000, -1)).toBe(1.5);
    expect(perTokenPrice(1_500_000, 1.5)).toBe(1.5);
    expect(perTokenPrice(1_500_000, Number.NaN)).toBe(1.5);
  });

  it("lot 1 is the first scaled lot (not treated as 0)", () => {
    expect(perTokenPrice(1_500_000n, 1)).toBe(0.15);
    expect(perTokenPrice(1_500_000, 1)).toBe(0.15);
    expect(perLotPrice(0.15, 1)).toBe(1.5);
  });

  it("non-default lots are ONE division by 10^(6+lot): bit-exact, not just close", () => {
    // A two-step price / 1e6 / 10^lot gives 9.999999999999999e-10, 3.3000000000000004e-8, 6.999999999999999e-21 here.
    expect(perTokenPrice(1n, 3)).toBe(1e-9);
    expect(perTokenPrice(33n, 3)).toBe(3.3e-8);
    expect(perTokenPrice(33, 9)).toBe(3.3e-14);
    expect(perTokenPrice(7n, 15)).toBe(7e-21);
  });

  it("a lot above the program cap (16) is NOT clamped or zeroed here: it divides by 10^22", () => {
    // Today's arithmetic, not a policy: every reader caps first (the SDK throws, lotExpOfSlabData says 0), so
    // perTokenPrice never sees lot > 15 in practice. Pinned so a silent clamp to 15 is noticed.
    expect(perTokenPrice(1_500_000n, 16)).toBe(1_500_000 / 1e22);
  });
});

describe("lot_exp read from the market account", () => {
  it("a non-default lot_exp market shows a per-token price; a default market is unchanged (v2.2)", () => {
    // asset 0: default scale, $150.00; asset 1: lot_exp 6 with 300_000 atoms == $3e-7 per token; asset 2: lot 15.
    const d = market(LAYOUT_V22, [150_000_000n, 300_000n, 2_500_000n], [0, 6, 15]);
    expect(shown(d, 0)).toBe(150);
    expect(shown(d, 1)).toBeCloseTo(3e-7, 20);
    expect(shown(d, 2)).toBeCloseTo(2.5e-15, 28);
    // BEFORE this change every asset was shown as atoms / 1e6:
    expect(readAssetEffectivePriceE6(d, 1)! / 1_000_000).toBe(0.3);
  });

  it("v2.1 (VERSION 18) default markets are unchanged", () => {
    const d = market(LAYOUT_V21, [85_187_279n, 7_000_000n], [0, 0]);
    expect(shown(d, 0)).toBe(85.187279);
    expect(shown(d, 1)).toBe(7);
  });

  it("reads 0 for an unreadable account, an unknown VERSION or a missing asset", () => {
    expect(lotExpOfSlabData(new Uint8Array(32), 0)).toBe(0);
    expect(lotExpOfSlabData(market(LAYOUT_V22, [1n], [3], 77), 0)).toBe(0);
    expect(lotExpOfSlabData(market(LAYOUT_V22, [1n], [3]), 5)).toBe(0);
  });

  it("a lot_exp byte above the program's cap is not trusted (reads 0)", () => {
    expect(lotExpOfSlabData(market(LAYOUT_V22, [1_000_000n], [16]), 0)).toBe(0);
  });

  it("a lot_exp byte >= 128 is read unsigned: reads 0, caches nothing, and resolveLotExp defers", async () => {
    for (const b of [128, 200, 255]) {
      resetLotExpCache();
      const d = market(LAYOUT_V22, [1_000_000n, 1n], [b, 2]);
      expect(lotExpOfSlabData(d, 0)).toBe(0);
      noteLotExps("HI", d);
      expect(knownLotExp("HI", 0)).toBeUndefined();
      expect(knownLotExp("HI", 1)).toBe(2);
      resetLotExpCache();
      expect(await resolveLotExp("HI", 0, async () => d)).toBeNull();
    }
  });

  it("the first asset past the last slot reads 0 (not undefined)", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 2 });
    d[slotOff(LAYOUT_V22, 1) + LAYOUT_V22.wrapperSlot.profileLotExp] = 4;
    expect(lotExpOfSlabData(d, 1)).toBe(4);
    expect(lotExpOfSlabData(d, 2)).toBe(0);
  });

  it("a negative asset index reads 0 even when the bytes before slot 0 look like a lot_exp", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    // The byte slotOff(-1) + profileLotExp would land on (inside the market group / config).
    d[slotOff(LAYOUT_V22, 0) - LAYOUT_V22.assetSlotStride + LAYOUT_V22.wrapperSlot.profileLotExp] = 9;
    expect(lotExpOfSlabData(d, -1)).toBe(0);
  });

  it("8 trailing bytes after a market account do not hide lot 6", () => {
    const base = buildMarket(LAYOUT_V22, { slots: 1 });
    const d = new Uint8Array(base.length + 8);
    d.set(base);
    d[slotOff(LAYOUT_V22, 0) + LAYOUT_V22.wrapperSlot.profileLotExp] = 6;
    expect(lotExpOfSlabData(d, 0)).toBe(6);
  });
});

describe("VERSION 18 accounts use the v2.1 slot geometry", () => {
  it("synthetic: a sentinel at the v2.2 offset of a v2.1 market is ignored", () => {
    const d = buildMarket(LAYOUT_V21, { slots: 2 });
    for (const i of [0, 1]) {
      const v22At = slotOff(LAYOUT_V22, i) + LAYOUT_V22.wrapperSlot.profileLotExp;
      if (v22At < d.length) d[v22At] = 7;
    }
    expect(lotExpOfSlabData(d, 0)).toBe(0);
    expect(lotExpOfSlabData(d, 1)).toBe(0);
  });

  it("real devnet VERSION 18 market: every asset is lot 0, cached, and resolveLotExp never defers", async () => {
    const n = Math.floor((REAL_MARKET_V21.length - LAYOUT_V21.marketGroupOff - LAYOUT_V21.marketGroupLen) / LAYOUT_V21.assetSlotStride);
    expect(n).toBe(14);
    for (let a = 0; a < n; a++) expect(lotExpOfSlabData(REAL_MARKET_V21, a)).toBe(0);
    noteLotExps("V18", REAL_MARKET_V21);
    for (let a = 0; a < n; a++) expect(knownLotExp("V18", a)).toBe(0);
    resetLotExpCache();
    expect(await resolveLotExp("V18b", 0, async () => REAL_MARKET_V21)).toBe(0);
  });
});

describe("lot_exp cache for matcher-CPI fills (which never read the slab)", () => {
  it("is empty (lot 0) until a market is seen, then serves every asset's lot_exp", () => {
    expect(cachedLotExp("SLAB", 1)).toBe(0);
    noteLotExps("SLAB", market(LAYOUT_V22, [150_000_000n, 300_000n], [0, 6]));
    expect(cachedLotExp("SLAB", 0)).toBe(0);
    expect(cachedLotExp("SLAB", 1)).toBe(6);
    expect(cachedLotExp("OTHER", 1)).toBe(0);
    // A CPI fill at 300_000 atoms on that asset is shown per token:
    expect(perTokenPrice(300_000n, cachedLotExp("SLAB", 1))).toBeCloseTo(3e-7, 20);
  });

  it("an unreadable account records nothing and never throws", () => {
    noteLotExps("BAD", new Uint8Array(10));
    expect(cachedLotExp("BAD", 0)).toBe(0);
  });
});

describe("fees stay in per-lot units", () => {
  it("perLotPrice undoes perTokenPrice, so size(lots) x price keeps its meaning; lot 0 is the identity", () => {
    expect(perLotPrice(1.5, 0)).toBe(1.5);
    expect(perLotPrice(perTokenPrice(300_000n, 6), 6)).toBeCloseTo(0.3, 12);
  });

  it("perLotPrice treats a negative, fractional or NaN lot as 0 (the identity), like perTokenPrice", () => {
    expect(perLotPrice(1.5, -1)).toBe(1.5);
    expect(perLotPrice(1.5, 1.5)).toBe(1.5);
    expect(perLotPrice(1.5, Number.NaN)).toBe(1.5);
  });

  it("a bad lot_exp byte on one slot does not hide the later slots", () => {
    const d = market(LAYOUT_V22, [1n, 1n, 1n], [3, 16, 7]);
    noteLotExps("S", d);
    expect(cachedLotExp("S", 0)).toBe(3);
    expect(cachedLotExp("S", 1)).toBe(0);
    expect(cachedLotExp("S", 2)).toBe(7);
  });
});

describe("resolveLotExp (never a guessed lot 0)", () => {
  const SLAB = "SlabAddr1111111111111111111111111111111111";
  it("a cache hit makes no read", async () => {
    noteLotExps(SLAB, market(LAYOUT_V22, [1n], [4]));
    const read = vi.fn();
    expect(await resolveLotExp(SLAB, 0, read)).toBe(4);
    expect(read).not.toHaveBeenCalled();
  });
  it("a miss reads the market once, fills the cache for every slot, then serves from cache", async () => {
    const d = market(LAYOUT_V22, [1n, 1n], [3, 5]);
    const read = vi.fn(async () => d);
    expect(await resolveLotExp(SLAB, 1, read)).toBe(5);
    expect(await resolveLotExp(SLAB, 0, read)).toBe(3);
    expect(read).toHaveBeenCalledTimes(1);
  });
  it("null when the market cannot be read (throw, null, undefined), and nothing is cached", async () => {
    expect(await resolveLotExp(SLAB, 0, async () => { throw new Error("x"); })).toBeNull();
    expect(await resolveLotExp(SLAB, 0, async () => null)).toBeNull();
    expect(await resolveLotExp(SLAB, 0, async () => undefined)).toBeNull();
    expect(knownLotExp(SLAB, 0)).toBeUndefined();
  });
  it("a market that was read but has no v2.2 slot is lot 0 (as before) and is not cached", async () => {
    expect(await resolveLotExp(SLAB, 0, async () => new Uint8Array(64))).toBe(0);
    expect(knownLotExp(SLAB, 0)).toBeUndefined();
  });
  it("lot_exp is immutable: the cache entry does not expire (no 24 h TTL)", async () => {
    vi.useFakeTimers();
    try {
      noteLotExps(SLAB, market(LAYOUT_V22, [1n], [7]));
      vi.setSystemTime(Date.now() + 30 * 24 * 60 * 60 * 1000);
      expect(knownLotExp(SLAB, 0)).toBe(7);
    } finally { vi.useRealTimers(); }
  });
});

describe("readMarkPriceE6 mark-EWMA fallback records lot_exp (consumers divide the per-lot EWMA the same way as effective_price)", () => {
  it("effective_price 0, lot 6: returns the per-lot EWMA and the cache knows lot 6", async () => {
    const { readMarkPriceE6 } = await import("../../src/parsers/markPrice.js");
    const d = buildMarket(LAYOUT_V22, { slots: 2, markEwmaE6: 300_000n });
    d[LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlot.profileLotExp] = 6;
    const e6 = await readMarkPriceE6({ getAccountInfo: async () => ({ data: Buffer.from(d) }) } as any, "So11111111111111111111111111111111111111112", 0);
    expect(e6).toBe(300_000);
    expect(perTokenPrice(e6!, cachedLotExp("So11111111111111111111111111111111111111112", 0))).toBeCloseTo(3e-7, 20);
  });
});
