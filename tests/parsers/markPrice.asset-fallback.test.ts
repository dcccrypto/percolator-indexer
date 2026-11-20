/**
 * A per-asset price request on a real v2.2 (VERSION 19) layout must never be answered with another
 * asset's price. `readMarkEwmaE6` is the MARKET-WIDE oracle-profile EWMA, so it is not asset N's price.
 */
import { describe, it, expect, vi } from "vitest";

const mockGetAccountInfo = vi.fn();
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  withRetry: vi.fn(async (fn: () => unknown) => fn()),
}));
vi.mock("@solana/web3.js", () => ({
  Connection: class {},
  PublicKey: class { constructor(public key: string) {} },
}));

import { readMarkPriceE6 } from "../../src/parsers/markPrice.js";
import { buildMarket, LAYOUT_V21, LAYOUT_V22 } from "../helpers/v22Fixtures.js";

const conn = { getAccountInfo: mockGetAccountInfo } as never;

function market(layout: typeof LAYOUT_V22, prices: bigint[], ewma: bigint): Uint8Array {
  const d = buildMarket(layout, { slots: prices.length, markEwmaE6: ewma });
  const dv = new DataView(d.buffer, d.byteOffset, d.byteLength);
  prices.forEach((p, i) => {
    dv.setBigUint64(layout.marketGroupOff + layout.marketGroupLen + i * layout.assetSlotStride + layout.wrapperSlotLen + layout.assetState.effectivePrice, p, true);
  });
  return d;
}
const serve = (d: Uint8Array) => mockGetAccountInfo.mockResolvedValue({ data: Buffer.from(d) });

describe("readMarkPriceE6 with assetIndex on a v2.2 market", () => {
  it.each([[LAYOUT_V22], [LAYOUT_V21]])("returns the asset's own effective price (layout %#)", async (layout) => {
    serve(market(layout, [150_000_000n, 2_500_000n], 149_000_000n));
    expect(await readMarkPriceE6(conn, "S", 1)).toBe(2_500_000);
  });

  it("asset 1 slot inactive (effective 0): null, not the market-wide EWMA", async () => {
    serve(market(LAYOUT_V22, [150_000_000n, 0n], 149_000_000n));
    expect(await readMarkPriceE6(conn, "S", 1)).toBeNull();
  });

  it("asset 2 effective price out of range (>= 1e12): null, not the EWMA", async () => {
    serve(market(LAYOUT_V22, [150_000_000n, 2_500_000n, 1_000_000_000_000n], 149_000_000n));
    expect(await readMarkPriceE6(conn, "S", 2)).toBeNull();
  });

  it("asset index beyond the slot count: null, not the EWMA", async () => {
    serve(market(LAYOUT_V22, [150_000_000n, 2_500_000n], 149_000_000n));
    expect(await readMarkPriceE6(conn, "S", 5)).toBeNull();
  });

  it("no assetIndex keeps the market-wide mark; asset 0 keeps its fallback", async () => {
    serve(market(LAYOUT_V22, [0n, 2_500_000n], 149_000_000n));
    expect(await readMarkPriceE6(conn, "S")).toBe(149_000_000);
    expect(await readMarkPriceE6(conn, "S", 0)).toBe(149_000_000);
  });
});
