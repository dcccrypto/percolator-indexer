/**
 * A slot whose lot_exp byte is above the SDK cap (lotExpOfMarketV22 throws RangeError "lot_exp byte N is above 15")
 * must DEFER a fill (resolveLotExp -> null), never price it as lot 0 (the insert's ignoreDuplicates would keep that forever).
 */
import { describe, it, expect, beforeEach } from "vitest";
import { lotExpOfMarketV22 } from "@percolatorct/sdk";
import { resolveLotExp, resetLotExpCache, knownLotExp, perTokenPrice } from "../src/parsers/lotScale.js";
import { buildMarket, LAYOUT_V22 } from "./helpers/v22Fixtures.js";

function marketWithBadSlot1(): Uint8Array {
  const d = buildMarket(LAYOUT_V22, { slots: 3 });
  const base = LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen;
  d[base + 0 * LAYOUT_V22.assetSlotStride + 19] = 3;
  d[base + 1 * LAYOUT_V22.assetSlotStride + 19] = 200;
  d[base + 2 * LAYOUT_V22.assetSlotStride + 19] = 5;
  return d;
}

describe("lotScale bad lot_exp slot", () => {
  beforeEach(() => resetLotExpCache());

  it("SDK throws on the bad byte (premise)", () => {
    const d = marketWithBadSlot1();
    expect(lotExpOfMarketV22(d, 0)).toBe(3);
    expect(() => lotExpOfMarketV22(d, 1)).toThrow(/lot_exp byte 200 is above/);
    expect(lotExpOfMarketV22(d, 2)).toBe(5);
  });

  it("defers (null) for the bad slot, still resolves its neighbours", async () => {
    const d = marketWithBadSlot1();
    expect(await resolveLotExp("S", 1, async () => d)).toBeNull();
    expect(knownLotExp("S", 1)).toBeUndefined();
    expect(await resolveLotExp("S", 0, async () => d)).toBe(3);
    expect(await resolveLotExp("S", 2, async () => d)).toBe(5);
  });

  it("still returns 0 for an asset index that is not present (legacy / short market)", async () => {
    const d = buildMarket(LAYOUT_V22, { slots: 2 });
    expect(await resolveLotExp("T", 40, async () => d)).toBe(0);
  });

  it("bigint price above 2^53 stays within 1 ulp of exact (lead 3: not a bug)", () => {
    const p = 2n ** 60n + 12345n;
    const exact = Number(p / 1_000_000n) + Number(p % 1_000_000n) / 1e6;
    expect(Math.abs(perTokenPrice(p, 0) - exact) / exact).toBeLessThan(2 ** -51);
  });
});
