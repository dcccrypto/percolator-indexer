/**
 * Branch coverage for src/parsers/markPrice.ts on REAL slab bytes (no SDK / reader mocks): the wrapper-magic mark
 * path (zero / out-of-range EWMA, unknown VERSION), a non-Error throw, and the lot_exp side effect.
 * v2.1 uses the real devnet capture; v2.2 is stamped from the SDK's LAYOUT_V22 table.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readAssetEffectivePriceE6, readMarkPriceE6 } from "../../src/parsers/markPrice.js";
import { cachedLotExp, knownLotExp, resetLotExpCache } from "../../src/parsers/lotScale.js";
import { getUnknownLayoutCount, resetUnknownLayoutState } from "../../src/layout/resolve.js";
import { buildMarket, LAYOUT_V22, REAL_MARKET_V21 } from "../helpers/v22Fixtures.js";

const SLAB = "Ar6khqrJVfPDx1KGmSP6Q4hxre6NNm66GpDJPHG1oF6N";
const connWith = (data: Uint8Array) => ({ getAccountInfo: vi.fn(async () => ({ data: Buffer.from(data) })) }) as any;

function setEffective(d: Uint8Array, layout: typeof LAYOUT_V22, asset: number, v: bigint): void {
  const off = layout.marketGroupOff + layout.marketGroupLen + asset * layout.assetSlotStride + layout.wrapperSlotLen + layout.assetState.effectivePrice;
  new DataView(d.buffer, d.byteOffset, d.byteLength).setBigUint64(off, v, true);
}

beforeEach(() => {
  resetLotExpCache();
  resetUnknownLayoutState();
});

describe("readAssetEffectivePriceE6 edges (real bytes)", () => {
  it("short buffer, out-of-range index and a non-wrapper buffer are null", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 2 });
    setEffective(d, LAYOUT_V22, 0, 5_000_000n);
    expect(readAssetEffectivePriceE6(d.slice(0, 5), 0)).toBeNull();
    expect(readAssetEffectivePriceE6(d, 2)).toBeNull();
    expect(readAssetEffectivePriceE6(new Uint8Array(d.length), 0)).toBeNull();
    expect(readAssetEffectivePriceE6(d, 0)).toBe(5_000_000);
  });

  it("a market truncated inside slot 0 has no whole slot (index >= slotCount)", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    setEffective(d, LAYOUT_V22, 0, 5_000_000n);
    const cut = LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlotLen + LAYOUT_V22.assetState.effectivePrice + 4;
    expect(readAssetEffectivePriceE6(d.slice(0, cut), 0)).toBeNull();
  });

  it("zero and >= 1e12 effective prices are null, 1e12-1 is kept", () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    setEffective(d, LAYOUT_V22, 0, 0n);
    expect(readAssetEffectivePriceE6(d, 0)).toBeNull();
    setEffective(d, LAYOUT_V22, 0, 1_000_000_000_000n);
    expect(readAssetEffectivePriceE6(d, 0)).toBeNull();
    setEffective(d, LAYOUT_V22, 0, 999_999_999_999n);
    expect(readAssetEffectivePriceE6(d, 0)).toBe(999_999_999_999);
  });
});

describe("readMarkPriceE6 wrapper-magic path (real bytes)", () => {
  it("v2.2: zero mark EWMA and no readable effective price -> null, no lot_exp recorded", async () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1, markEwmaE6: 0n });
    expect(await readMarkPriceE6(connWith(d), SLAB, 0)).toBeNull();
    expect(knownLotExp(SLAB, 0)).toBeUndefined();
  });

  it("v2.2: out-of-range mark EWMA -> null", async () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1, markEwmaE6: 1_000_000_000_000n });
    expect(await readMarkPriceE6(connWith(d), SLAB)).toBeNull();
  });

  it("v2.2: a valid mark EWMA is returned; lot_exp is recorded only when a fill's assetIndex is given", async () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1, markEwmaE6: 42_000_000n });
    expect(await readMarkPriceE6(connWith(d), SLAB)).toBe(42_000_000);
    expect(knownLotExp(SLAB, 0)).toBeUndefined();
    // asset 5 has no slot -> effective is null -> falls back to the mark and still notes lot_exp
    expect(await readMarkPriceE6(connWith(d), SLAB, 5)).toBe(42_000_000);
    expect(knownLotExp(SLAB, 0)).toBe(0);
  });

  it("lot_exp path: an effective price read records the market's real lot_exp", async () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1 });
    setEffective(d, LAYOUT_V22, 0, 77_000_000n);
    expect(await readMarkPriceE6(connWith(d), SLAB, 0)).toBe(77_000_000);
    expect(cachedLotExp(SLAB, 0)).toBe(0);
    expect(knownLotExp(SLAB, 0)).toBe(0);
  });

  it("real v2.1 devnet capture: effective price is read and lot_exp is recorded", async () => {
    const p = await readMarkPriceE6(connWith(REAL_MARKET_V21), SLAB, 0);
    expect(p).not.toBeNull();
    expect(p).toBe(readAssetEffectivePriceE6(REAL_MARKET_V21, 0));
    expect(typeof knownLotExp(SLAB, 0)).toBe("number");
  });

  it("unknown VERSION with wrapper magic -> null and reportUnknownLayout fires once per market", async () => {
    const d = buildMarket(LAYOUT_V22, { slots: 1, markEwmaE6: 42_000_000n }, 77);
    expect(await readMarkPriceE6(connWith(d), SLAB)).toBeNull();
    expect(getUnknownLayoutCount()).toBe(1);
    expect(await readMarkPriceE6(connWith(d), SLAB, 0)).toBeNull();
    expect(getUnknownLayoutCount()).toBe(2); // counted on every call
  });
});

describe("readMarkPriceE6 failure handling", () => {
  it("a non-Error throw is stringified and yields null", async () => {
    const conn = { getAccountInfo: vi.fn(async () => ({ get data(): Uint8Array { throw "boom"; } })) } as any;
    expect(await readMarkPriceE6(conn, SLAB)).toBeNull();
  });

  it("an account with no data is null", async () => {
    const conn = { getAccountInfo: vi.fn(async () => ({ data: undefined })) } as any;
    expect(await readMarkPriceE6(conn, SLAB)).toBeNull();
  });

  it("a short non-wrapper account falls through to the layout check and returns null", async () => {
    expect(await readMarkPriceE6(connWith(new Uint8Array(100)), SLAB)).toBeNull();
  });
});
