import { beforeEach, describe, expect, it } from "vitest";
import { ASSET_STATE_MARKET_ID_OFF, assetGenerationOf, marketVersionOf, noteMarketLayout, resetMarketLayoutNotes } from "../../src/layout/marketVersions.js";
import { registrationSliceLen } from "../../src/layout/resolve.js";
import { LAYOUT_V21, LAYOUT_V22, REAL_MARKET_V21, buildMarket, stampHeader } from "../helpers/v22Fixtures.js";

beforeEach(() => resetMarketLayoutNotes());

/** Stamp asset `i`'s generation (`market_id`, u64 at the start of its engine slot). */
function withGeneration(d: Uint8Array, layout: typeof LAYOUT_V22, i: number, gen: bigint): Uint8Array {
  const off = layout.marketGroupOff + layout.marketGroupLen + i * layout.assetSlotStride + layout.wrapperSlotLen + ASSET_STATE_MARKET_ID_OFF;
  new DataView(d.buffer, d.byteOffset).setBigUint64(off, gen, true);
  return d;
}

describe("noteMarketLayout / marketVersionOf / assetGenerationOf", () => {
  it("the generation sits at the engine slot start: asset 0 of a v2.2 market is at 2,422 (layout-v22.json: asset0_asset_state_field_abs.market_id)", () => {
    expect(LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlotLen).toBe(2_422);
    const d = withGeneration(buildMarket(LAYOUT_V22, { slots: 2 }), LAYOUT_V22, 0, 7n);
    withGeneration(d, LAYOUT_V22, 1, 9n);
    noteMarketLayout("S", d);
    expect(marketVersionOf("S")).toBe(19);
    expect([assetGenerationOf("S", 0), assetGenerationOf("S", 1), assetGenerationOf("S", 2)]).toEqual([7n, 9n, null]);
    expect(new DataView(d.buffer).getBigUint64(2_422, true)).toBe(7n); // the independent byte check
  });

  it("v2.1: VERSION 18 recorded from a REAL devnet capture; its generations come from ITS slot geometry", () => {
    noteMarketLayout("R", REAL_MARKET_V21);
    expect(marketVersionOf("R")).toBe(LAYOUT_V21.version);
    expect(assetGenerationOf("R", 0)).not.toBeNull();
  });

  it("a light (sliced) read records the VERSION but keeps the generations an earlier full read recorded", () => {
    const d = withGeneration(buildMarket(LAYOUT_V22, { slots: 1 }), LAYOUT_V22, 0, 5n);
    noteMarketLayout("S", d);
    noteMarketLayout("S", d.subarray(0, registrationSliceLen()));
    expect(marketVersionOf("S")).toBe(19);
    expect(assetGenerationOf("S", 0)).toBe(5n);
    // a full read replaces them
    noteMarketLayout("S", withGeneration(buildMarket(LAYOUT_V22, { slots: 1 }), LAYOUT_V22, 0, 6n));
    expect(assetGenerationOf("S", 0)).toBe(6n);
  });

  it("not a market (wrong magic / kind / unknown VERSION / short): nothing recorded, never throws", () => {
    noteMarketLayout("A", new Uint8Array(10));
    noteMarketLayout("B", stampHeader(new Uint8Array(4200), 2, 19));
    noteMarketLayout("C", buildMarket(LAYOUT_V22, {}, 20));
    noteMarketLayout("D", new TextEncoder().encode("PERCOLAT........ legacy v1 slab ........"));
    for (const s of ["A", "B", "C", "D"]) expect(marketVersionOf(s)).toBeNull();
  });
});
