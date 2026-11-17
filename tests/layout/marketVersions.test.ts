import { beforeEach, describe, expect, it } from "vitest";
import { ASSET_STATE_MARKET_ID_OFF, MAX_MARKET_NOTES, assetGenerationOf, marketNotesEvicted, marketVersionOf, noteMarketLayout, resetMarketLayoutNotes } from "../../src/layout/marketVersions.js";
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

describe("noteMarketLayout edge branches", () => {
  const slotOff = (i: number) => LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + i * LAYOUT_V22.assetSlotStride + LAYOUT_V22.wrapperSlotLen + ASSET_STATE_MARKET_ID_OFF;

  it("a buffer cut inside a later slot records only the slots the geometry counts as whole", () => {
    const d = withGeneration(buildMarket(LAYOUT_V22, { slots: 2 }), LAYOUT_V22, 0, 3n);
    withGeneration(d, LAYOUT_V22, 1, 4n);
    noteMarketLayout("T", d.subarray(0, slotOff(1) + 4)); // 4 of slot 1's 8 bytes
    expect(marketVersionOf("T")).toBe(19);
    expect(assetGenerationOf("T", 0)).toBe(3n);
    expect(assetGenerationOf("T", 1)).toBeNull();
  });

  it("a light read of a slab never read before records the VERSION with no generations", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    noteMarketLayout("L", d.subarray(0, registrationSliceLen()));
    expect(marketVersionOf("L")).toBe(19);
    expect(assetGenerationOf("L", 0)).toBeNull();
  });

  it("an unknown slab reads as null for VERSION and generation", () => {
    expect(marketVersionOf("never")).toBeNull();
    expect(assetGenerationOf("never", 0)).toBeNull();
    expect(assetGenerationOf("never", -1)).toBeNull();
  });

  it("each mapped VERSION (18 -> v2.1, 19 -> v2.2) is recorded; 0, 17 and 20 (out of range) are not", () => {
    noteMarketLayout("v18", REAL_MARKET_V21);
    noteMarketLayout("v19", buildMarket(LAYOUT_V22, { slots: 1 }));
    expect([marketVersionOf("v18"), marketVersionOf("v19")]).toEqual([18, 19]);
    for (const v of [0, 1, 17, 20, 255]) {
      noteMarketLayout(`bad${v}`, buildMarket(LAYOUT_V22, {}, v));
      expect(marketVersionOf(`bad${v}`)).toBeNull();
    }
  });

  it("at the cap an already-noted slab is refreshed in place (no eviction) and kept as most recent", () => {
    const d = withGeneration(buildMarket(LAYOUT_V22, { slots: 1 }), LAYOUT_V22, 0, 1n);
    for (let i = 0; i < 20_000; i++) noteMarketLayout(`cap${i}`, d);
    noteMarketLayout("cap0", d);
    expect(marketVersionOf("cap0")).toBe(19);
    expect(marketNotesEvicted()).toBe(0);
    noteMarketLayout("overflow", d);
    expect(marketVersionOf("cap0")).toBe(19);
    expect(marketVersionOf("cap1")).toBeNull();
    noteMarketLayout("cap0", withGeneration(buildMarket(LAYOUT_V22, { slots: 1 }), LAYOUT_V22, 0, 2n));
    expect(assetGenerationOf("cap0", 0)).toBe(2n);
  });
});


describe("IDX-BUG-MARKETVERSIONSCAP leads", () => {
  const slotOff = (i: number) => LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + i * LAYOUT_V22.assetSlotStride + LAYOUT_V22.wrapperSlotLen + ASSET_STATE_MARKET_ID_OFF;

  it("lead 2: a partial read holding fewer slots keeps the generations of the higher slots from an earlier full read", () => {
    const d = withGeneration(buildMarket(LAYOUT_V22, { slots: 3 }), LAYOUT_V22, 0, 3n);
    withGeneration(d, LAYOUT_V22, 1, 4n);
    withGeneration(d, LAYOUT_V22, 2, 5n);
    noteMarketLayout("P", d);
    const partial = withGeneration(buildMarket(LAYOUT_V22, { slots: 3 }), LAYOUT_V22, 0, 8n);
    noteMarketLayout("P", partial.subarray(0, slotOff(1) - 4)); // slot 0 whole only
    expect(assetGenerationOf("P", 0)).toBe(8n);
    expect(assetGenerationOf("P", 1)).toBe(4n);
    expect(assetGenerationOf("P", 2)).toBe(5n);
  });

  it("lead 3: a light read carrying a different VERSION does not keep generations of the old layout", () => {
    noteMarketLayout("V", REAL_MARKET_V21);
    expect(assetGenerationOf("V", 0)).not.toBeNull();
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    noteMarketLayout("V", d.subarray(0, registrationSliceLen()));
    expect(marketVersionOf("V")).toBe(19);
    expect(assetGenerationOf("V", 0)).toBeNull();
  });

  it("lead 1: at the cap a NEW slab is still noted (oldest evicted) and the eviction is counted", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    for (let i = 0; i < MAX_MARKET_NOTES; i++) noteMarketLayout(`s${i}`, d);
    expect(marketNotesEvicted()).toBe(0);
    noteMarketLayout("fresh", d);
    expect(marketVersionOf("fresh")).toBe(19);
    expect(marketVersionOf("s0")).toBeNull();
    expect(marketVersionOf("s1")).toBe(19);
    expect(marketNotesEvicted()).toBe(1);
  });
});
