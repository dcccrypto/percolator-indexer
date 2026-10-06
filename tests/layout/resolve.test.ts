import { beforeEach, describe, expect, it } from "vitest";
import { UnknownLayoutError } from "@percolatorct/sdk";
import {
  getUnknownLayoutCount, hasWrapperMagic, isWrapperKind, readMarkEwmaE6, readMarketGroupFields, reportUnknownLayout, resetUnknownLayoutState,
} from "../../src/layout/resolve.js";
import { LAYOUT_V21, LAYOUT_V22, REAL_MARKET_V21, buildMarket, pk, stampHeader } from "../helpers/v22Fixtures.js";

const u128 = (d: Uint8Array, o: number): bigint => { const v = new DataView(d.buffer, d.byteOffset); return v.getBigUint64(o, true) | (v.getBigUint64(o + 8, true) << 64n); };

beforeEach(() => resetUnknownLayoutState());

describe("readMarketGroupFields: VERSION-keyed", () => {
  it("v2.1 REAL devnet capture: identical to what the old literals (592 + 285/301/317, header 758) read", () => {
    const f = readMarketGroupFields(REAL_MARKET_V21, "t");
    expect(f.layout.version).toBe(18);
    expect(f.vault).toBe(u128(REAL_MARKET_V21, 592 + 285));
    expect(f.insurance).toBe(u128(REAL_MARKET_V21, 592 + 301));
    expect(f.cTot).toBe(u128(REAL_MARKET_V21, 592 + 317));
    expect(f.asset0ProfileOff).toBe(592 + 758); // 1350
    expect(f.geometry.slotCount).toBe(14); // 33,900 = 1,350 + 14 x 2,325
  });

  it("v2.2: header 806, slot 0 at 1,398, vault at +333 (LITERALS from the combination ledger, not the table)", () => {
    const d = buildMarket(LAYOUT_V22, { vault: 777_000n, insurance: 55n, cTot: 9n, materialized: 3n, slots: 2 });
    const f = readMarketGroupFields(d, "t");
    expect(f.layout.version).toBe(19);
    expect(f.asset0ProfileOff).toBe(1398);
    expect(f.geometry.slotOff(1)).toBe(1398 + 2629);
    expect(u128(d, 592 + 333)).toBe(777_000n); // the independent byte check
    expect([f.vault, f.insurance, f.cTot, f.materializedPortfolioCount]).toEqual([777_000n, 55n, 9n, 3n]);
  });

  it("NEGATIVE CONTROL: v2.2 bytes read with the v2.1 literals (+285) do NOT give the vault: the old code was wrong for v2.2", () => {
    const d = buildMarket(LAYOUT_V22, { vault: 777_000n });
    expect(u128(d, 592 + 285)).not.toBe(777_000n);
    expect(readMarketGroupFields(d, "t").vault).toBe(777_000n);
  });

  it("an unknown VERSION is a typed error, never a guessed layout", () => {
    const d = buildMarket(LAYOUT_V22, { vault: 1n }, 20);
    expect(() => readMarketGroupFields(d, "t")).toThrow(UnknownLayoutError);
    try { readMarketGroupFields(d, "t"); } catch (e) { expect((e as UnknownLayoutError).code).toBe("UNKNOWN_VERSION"); expect((e as UnknownLayoutError).version).toBe(20); }
  });

  it("a kind other than market is refused", () => {
    const d = stampHeader(new Uint8Array(4200), 2, 19);
    expect(() => readMarketGroupFields(d, "t")).toThrow(UnknownLayoutError);
  });

  it("a market with no whole slot present still reads its header and reports no slot 0", () => {
    const d = buildMarket(LAYOUT_V22, { vault: 5n, slots: 0 });
    const f = readMarketGroupFields(d, "t");
    expect([f.vault, f.asset0ProfileOff]).toEqual([5n, null]);
  });
});

describe("magic / kind helpers", () => {
  it("hasWrapperMagic: wrapper accounts of any VERSION yes, legacy v1 slab and short buffers no", () => {
    expect(hasWrapperMagic(REAL_MARKET_V21)).toBe(true);
    expect(hasWrapperMagic(buildMarket(LAYOUT_V22, {}, 99))).toBe(true);
    expect(hasWrapperMagic(new TextEncoder().encode("PERCOLAT........ legacy v1 slab ........"))).toBe(false);
    expect(hasWrapperMagic(new Uint8Array(5))).toBe(false);
    expect(isWrapperKind(REAL_MARKET_V21, 1)).toBe(true);
    expect(isWrapperKind(REAL_MARKET_V21, 2)).toBe(false);
  });
});

describe("readMarkEwmaE6", () => {
  it("reads the config block for both known VERSIONs and refuses an unknown one", () => {
    expect(readMarkEwmaE6(buildMarket(LAYOUT_V21, { markEwmaE6: 123_456n }), "t")).toBe(123_456n);
    expect(readMarkEwmaE6(buildMarket(LAYOUT_V22, { markEwmaE6: 654_321n }), "t")).toBe(654_321n);
    expect(() => readMarkEwmaE6(buildMarket(LAYOUT_V22, { markEwmaE6: 1n }, 20), "t")).toThrow(UnknownLayoutError);
  });
});

describe("reportUnknownLayout", () => {
  it("handles UnknownLayoutError (counted, deduped per account+code+version), returns false for anything else", () => {
    const err = (() => { try { readMarketGroupFields(buildMarket(LAYOUT_V22, {}, 20), "t"); } catch (e) { return e; } return null; })();
    const a = pk().toBase58();
    expect(reportUnknownLayout(a, err, "t")).toBe(true);
    expect(reportUnknownLayout(a, err, "t")).toBe(true);
    expect(getUnknownLayoutCount()).toBe(2); // every skip is counted; the log/Sentry is once per account
    expect(reportUnknownLayout(a, new Error("rpc 429"), "t")).toBe(false);
    expect(getUnknownLayoutCount()).toBe(2);
  });
});
