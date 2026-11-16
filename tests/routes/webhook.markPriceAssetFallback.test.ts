/**
 * Follow-up to #302: the webhook's parseMarkPriceE6FromAccountBytes (post-state and fresh-RPC strategies)
 * must not answer a request for asset N > 0 with the market-wide config mark EWMA.
 */
import { describe, it, expect, vi } from "vitest";

vi.mock("@percolatorct/shared", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import { parseMarkPriceE6FromAccountBytes as parse } from "../../src/routes/webhook.js";
import { buildMarket, LAYOUT_V21, LAYOUT_V22 } from "../helpers/v22Fixtures.js";

function market(layout: typeof LAYOUT_V22, prices: bigint[], ewma: bigint): Uint8Array {
  const d = buildMarket(layout, { slots: prices.length, markEwmaE6: ewma });
  const dv = new DataView(d.buffer, d.byteOffset, d.byteLength);
  prices.forEach((p, i) => {
    dv.setBigUint64(layout.marketGroupOff + layout.marketGroupLen + i * layout.assetSlotStride + layout.wrapperSlotLen + layout.assetState.effectivePrice, p, true);
  });
  return d;
}

describe.each([["v2.2", LAYOUT_V22], ["v2.1", LAYOUT_V21]])("webhook parseMarkPriceE6FromAccountBytes (%s)", (_n, layout) => {
  it("asset 1 own effective price wins over the EWMA", () => {
    expect(parse(market(layout, [150_000_000n, 2_500_000n], 149_000_000n), 1)).toBe(2.5);
  });
  it("asset 1 slot inactive: 0, not the market-wide EWMA", () => {
    expect(parse(market(layout, [150_000_000n, 0n], 149_000_000n), 1)).toBe(0);
  });
  it("asset index beyond the slot count: 0, not the EWMA", () => {
    expect(parse(market(layout, [150_000_000n, 2_500_000n], 149_000_000n), 5)).toBe(0);
  });
  it("asset 0 keeps its EWMA fallback (unchanged, as in #302)", () => {
    expect(parse(market(layout, [0n, 2_500_000n], 149_000_000n), 0)).toBe(149);
  });
});
