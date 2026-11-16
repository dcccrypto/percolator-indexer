import { afterEach, describe, expect, it, vi } from "vitest";

/**
 * The SDK derives slotCount from whole strides, so with the real geometry every market_id fits. The in-loop bounds
 * guard in noteMarketLayout is the backstop for a geometry that over-reports slots; pin it with a stubbed geometry.
 */
vi.mock("@percolatorct/sdk", () => ({
  resolveMarketGeometry: () => ({
    slotCount: 3,
    layout: { version: 19 },
    engineOff: (i: number) => i * 16, // slot 0 -> 0, slot 1 -> 16, slot 2 -> 32 (past a 24-byte buffer)
  }),
}));

import { assetGenerationOf, marketVersionOf, noteMarketLayout, resetMarketLayoutNotes } from "../../src/layout/marketVersions.js";

afterEach(() => resetMarketLayoutNotes());

describe("noteMarketLayout bounds guard", () => {
  it("stops at the first slot whose market_id would run past the buffer, keeping the earlier ones", () => {
    const d = new Uint8Array(24);
    const dv = new DataView(d.buffer);
    dv.setBigUint64(0, 11n, true);
    dv.setBigUint64(16, 22n, true); // slot 1: 16 + 8 = 24, exactly fits
    noteMarketLayout("G", d);
    expect(marketVersionOf("G")).toBe(19);
    expect([assetGenerationOf("G", 0), assetGenerationOf("G", 1), assetGenerationOf("G", 2)]).toEqual([11n, 22n, null]);
  });

  it("a buffer too short even for slot 1 records slot 0 only", () => {
    const d = new Uint8Array(23);
    new DataView(d.buffer).setBigUint64(0, 5n, true);
    noteMarketLayout("H", d);
    expect([assetGenerationOf("H", 0), assetGenerationOf("H", 1)]).toEqual([5n, null]);
  });
});
