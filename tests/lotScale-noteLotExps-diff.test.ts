/**
 * Differential: the geometry-once noteLotExps writes exactly the cache entries the frozen per-slot SDK loop did,
 * for every market fixture shape and for random-byte buffers.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { LAYOUT_V21, LAYOUT_V22, LOT_EXP_MAX_V22 } from "@percolatorct/sdk";
import { noteLotExps, resetLotExpCache, knownLotExp } from "../src/parsers/lotScale.js";
import { legacyNoteLotExps } from "./helpers/lotScale.legacy.js";
import { buildMarket, REAL_MARKET_V21, REAL_PORTFOLIO_V21, stampHeader } from "./helpers/v22Fixtures.js";

const ADDR = "DiffMarket";
beforeEach(() => resetLotExpCache());

function actual(data: Uint8Array): Map<string, number> {
  resetLotExpCache();
  noteLotExps(ADDR, data);
  const m = new Map<string, number>();
  for (let a = 0; a < 70; a++) { const v = knownLotExp(ADDR, a); if (v !== undefined) m.set(`${ADDR}:${a}`, v); }
  return m;
}
function same(data: Uint8Array): void {
  const exp = legacyNoteLotExps(ADDR, data);
  expect([...actual(data)].sort()).toEqual([...exp].sort());
}
function withLot(layout: typeof LAYOUT_V22, slots: number, f: (a: number) => number, extraTail = 0): Uint8Array {
  const d0 = buildMarket(layout, { slots });
  const d = new Uint8Array(d0.length + extraTail);
  d.set(d0);
  const base = layout.marketGroupOff + layout.marketGroupLen;
  for (let a = 0; a < slots; a++) d[base + a * layout.assetSlotStride + layout.wrapperSlot.profileLotExp] = f(a);
  return d;
}

describe("noteLotExps == legacy per-slot loop", () => {
  it("fixtures: 0,1,2,4,63,64,65,70 slots, both layouts", () => {
    for (const layout of [LAYOUT_V22, LAYOUT_V21]) for (const s of [0, 1, 2, 4, 63, 64, 65, 70]) {
      same(withLot(layout, s, (a) => a % (LOT_EXP_MAX_V22 + 1)));
    }
  });
  it("bad lot_exp byte above cap is skipped, others kept", () => {
    const d = withLot(LAYOUT_V22, 5, (a) => (a === 2 ? LOT_EXP_MAX_V22 + 1 : a === 3 ? 255 : a));
    same(d);
    resetLotExpCache(); noteLotExps(ADDR, d);
    expect(knownLotExp(ADDR, 2)).toBeUndefined();
    expect(knownLotExp(ADDR, 3)).toBeUndefined();
    expect(knownLotExp(ADDR, 4)).toBe(4);
  });
  it("partial trailing slot, real v2.1 market, non-market, short buffers", () => {
    same(withLot(LAYOUT_V22, 3, (a) => a, LAYOUT_V22.assetSlotStride - 1));
    same(REAL_MARKET_V21);
    same(REAL_PORTFOLIO_V21);
    same(new Uint8Array(0));
    same(new Uint8Array(15));
    same(new Uint8Array(4096));
    same(stampHeader(new Uint8Array(LAYOUT_V22.marketGroupOff + 4), 1, LAYOUT_V22.version));
  });
  it("header/length edges: unknown version, cross-layout header, lengths around slotsBase and slot multiples", () => {
    const base = LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen;
    for (const len of [base - 1, base, base + 1, base + LAYOUT_V22.assetSlotStride - 1, base + LAYOUT_V22.assetSlotStride, base + LAYOUT_V22.assetSlotStride + 1]) {
      const d = new Uint8Array(len).fill(3);
      stampHeader(d, 1, LAYOUT_V22.version);
      same(d);
    }
    for (const v of [0, 20, 65535]) same(stampHeader(new Uint8Array(base + 3 * LAYOUT_V22.assetSlotStride).fill(2), 1, v));
    same(stampHeader(new Uint8Array(base + 3 * LAYOUT_V22.assetSlotStride).fill(2), 1, LAYOUT_V21.version));
    same(stampHeader(new Uint8Array(base + 3 * LAYOUT_V22.assetSlotStride).fill(2), 3, LAYOUT_V22.version));
    // SDK-mirroring note: noteLotExps copies resolveMarketGeometry/lotExpOfMarketV22 offsets; this file is the drift guard.
  });
  it("random-byte buffers (random, and valid header + random body)", () => {
    let s = 0x9e3779b9;
    const rnd = () => ((s = (Math.imul(s, 1664525) + 1013904223) >>> 0) >>> 0);
    for (let i = 0; i < 300; i++) {
      const layout = i % 2 ? LAYOUT_V22 : LAYOUT_V21;
      const slots = rnd() % 8;
      const len = layout.marketGroupOff + layout.marketGroupLen + slots * layout.assetSlotStride + (rnd() % 3 === 0 ? rnd() % layout.assetSlotStride : 0);
      const d = new Uint8Array(len);
      for (let j = 0; j < len; j++) d[j] = rnd() & 0xff;
      if (i % 3) stampHeader(d, 1, layout.version);
      // keep most lot bytes plausible so both branches (kept / skipped) are hit
      const base = layout.marketGroupOff + layout.marketGroupLen;
      for (let a = 0; a < slots; a++) if (rnd() % 4) d[base + a * layout.assetSlotStride + layout.wrapperSlot.profileLotExp] = rnd() % (LOT_EXP_MAX_V22 + 1);
      same(d);
    }
  });
});
