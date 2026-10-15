/**
 * #221 reader on a VERSION 19 (v2.2) market: slot stride and asset-state offset come from the SDK table, so
 * effective_price is read at the right place for v2.2 and v2.1 alike, and an unknown VERSION returns null
 * rather than garbage. v2.2 accounts are stamped from LAYOUT_V22 (no v2.2 account exists on any cluster yet).
 */
import { describe, it, expect } from "vitest";
import { readAssetEffectivePriceE6 } from "../../src/parsers/markPrice.js";
import { buildMarket, LAYOUT_V21, LAYOUT_V22 } from "../helpers/v22Fixtures.js";

function withEffective(layout: typeof LAYOUT_V22, slots: number, prices: bigint[], version?: number): Uint8Array {
  const d = buildMarket(layout, { slots }, version);
  const dv = new DataView(d.buffer, d.byteOffset, d.byteLength);
  prices.forEach((p, i) => {
    dv.setBigUint64(layout.marketGroupOff + layout.marketGroupLen + i * layout.assetSlotStride + layout.wrapperSlotLen + layout.assetState.effectivePrice, p, true);
  });
  return d;
}

describe("readAssetEffectivePriceE6, VERSION-keyed", () => {
  it("reads each asset's effective_price on a v2.2 (VERSION 19) market", () => {
    const d = withEffective(LAYOUT_V22, 3, [150_000_000n, 2_500_000n, 91_000_000n]);
    expect(readAssetEffectivePriceE6(d, 0)).toBe(150_000_000);
    expect(readAssetEffectivePriceE6(d, 1)).toBe(2_500_000);
    expect(readAssetEffectivePriceE6(d, 2)).toBe(91_000_000);
    expect(readAssetEffectivePriceE6(d, 3)).toBeNull();
  });

  it("v2.1 (VERSION 18) still reads at the v2.1 offsets", () => {
    const d = withEffective(LAYOUT_V21, 2, [85_187_279n, 7_000_000n]);
    expect(readAssetEffectivePriceE6(d, 0)).toBe(85_187_279);
    expect(readAssetEffectivePriceE6(d, 1)).toBe(7_000_000);
  });

  it("an unknown VERSION returns null instead of reading at some other VERSION's offset", () => {
    const d = withEffective(LAYOUT_V22, 2, [150_000_000n, 1n], 77);
    expect(readAssetEffectivePriceE6(d, 0)).toBeNull();
  });
});
