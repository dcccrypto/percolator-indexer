/**
 * Follow-up to #302: TradeIndexer.readMarkPriceFromSlab must not record the market-wide config mark EWMA
 * as the fill price of asset N > 0 when that asset's own effective_price slot is unreadable. Real v2.1/v2.2
 * layouts (no layout mocks).
 */
import { describe, it, expect, vi } from "vitest";

vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(), insertTradeRows: vi.fn(), tradeKey: vi.fn() }));
vi.mock("../../src/db/marketRows.js", () => ({ getMarketRowsCached: vi.fn(async () => []) }));
vi.mock("../../src/db/tradeCursor.js", () => ({ loadTradeCursors: vi.fn(async () => new Map()), saveTradeCursor: vi.fn() }));
vi.mock("../../src/services/v22LogEvents.js", () => ({ recordV22LogEvents: vi.fn() }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn() }));
vi.mock("@percolatorct/shared", () => ({
  config: { allProgramIds: [] },
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getConnection: vi.fn(),
  eventBus: { on: vi.fn(), off: vi.fn(), publish: vi.fn() },
  decodeBase58: vi.fn(),
  withRetry: vi.fn(async (fn: () => unknown) => fn()),
  captureException: vi.fn(),
}));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";
import { buildMarket, LAYOUT_V21, LAYOUT_V22 } from "../helpers/v22Fixtures.js";

const read = (data: Uint8Array, asset: number): Promise<number> =>
  (new TradeIndexerPolling() as unknown as { readMarkPriceFromSlab(c: unknown, s: string, a: number): Promise<number> })
    .readMarkPriceFromSlab({ getAccountInfo: async () => ({ data: Buffer.from(data) }) }, "11111111111111111111111111111111", asset);

function market(layout: typeof LAYOUT_V22, prices: bigint[], ewma: bigint): Uint8Array {
  const d = buildMarket(layout, { slots: prices.length, markEwmaE6: ewma });
  const dv = new DataView(d.buffer, d.byteOffset, d.byteLength);
  prices.forEach((p, i) => {
    dv.setBigUint64(layout.marketGroupOff + layout.marketGroupLen + i * layout.assetSlotStride + layout.wrapperSlotLen + layout.assetState.effectivePrice, p, true);
  });
  return d;
}

describe.each([["v2.2", LAYOUT_V22], ["v2.1", LAYOUT_V21]])("TradeIndexer.readMarkPriceFromSlab (%s)", (_n, layout) => {
  it("asset 1 own effective price wins over the EWMA", async () => {
    expect(await read(market(layout, [150_000_000n, 2_500_000n], 149_000_000n), 1)).toBe(2.5);
  });
  it("asset 1 slot inactive: 0, not the market-wide EWMA", async () => {
    expect(await read(market(layout, [150_000_000n, 0n], 149_000_000n), 1)).toBe(0);
  });
  it("asset index beyond the slot count: 0, not the EWMA", async () => {
    expect(await read(market(layout, [150_000_000n, 2_500_000n], 149_000_000n), 5)).toBe(0);
  });
  it("asset 0 keeps its EWMA fallback (unchanged, as in #302)", async () => {
    expect(await read(market(layout, [0n, 2_500_000n], 149_000_000n), 0)).toBe(149);
  });
});
