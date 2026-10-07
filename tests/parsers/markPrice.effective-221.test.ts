/**
 * #221: a fill is priced at its asset's `effective_price` — the price the engine
 * booked it at — not the wrapper's mark EWMA, which a matcher fill nudges toward
 * the matcher's quote (so it reads high after a long and low after a short).
 *
 * Uses the REAL SDK and the REAL bytes of a live devnet v18 market (LINK), so the
 * slot arithmetic is checked against the deployed layout, not a hand-built buffer.
 */
import { describe, it, expect, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";
import { parseWrapperConfigV17, V17_HEADER_LEN } from "@percolatorct/sdk";
import { readAssetEffectivePriceE6, readMarkPriceE6 } from "../../src/parsers/markPrice.js";

const fixture = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, "../fixtures/v18-market-link.json"), "utf8"),
) as { dataBase64: string };
const LIVE = new Uint8Array(Buffer.from(fixture.dataBase64, "base64"));

const BOOKED_E6 = 13_614_586; // asset[0].effective_price on the live account
const EWMA_E6 = 13_625_479; // WrapperConfigV17.markEwmaE6 — what was stored before #221
const ASSET1_E6 = 13_810_000; // asset[1]: a separate, never-cranked slot on the same account
const SLOTS = 14; // (33_900 - (592 + 758)) / 2325

/** Copy of the live account with asset 0's effective_price overwritten. */
function withEffective(valueE6: bigint): Uint8Array {
  const copy = LIVE.slice();
  new DataView(copy.buffer).setBigUint64(2399, valueE6, true); // 592 + 758 + 1024 + 25
  return copy;
}

describe("readAssetEffectivePriceE6 (#221)", () => {
  it("returns the booked effective_price of a live v18 market", () => {
    expect(readAssetEffectivePriceE6(LIVE, 0)).toBe(BOOKED_E6);
  });

  it("is NOT the mark EWMA the indexer used to store for a matcher fill", () => {
    const ewma = Number(parseWrapperConfigV17(LIVE, V17_HEADER_LEN).markEwmaE6);
    expect(ewma).toBe(EWMA_E6);
    expect(readAssetEffectivePriceE6(LIVE, 0)).not.toBe(ewma);
  });

  it("slot math matches the deployed layout: every slot's market_id is 1..N in order", () => {
    const dv = new DataView(LIVE.buffer, LIVE.byteOffset, LIVE.byteLength);
    for (let i = 0; i < SLOTS; i++) {
      // AssetStateV16Account.market_id is the first field after the 1024-byte wrapper.
      expect(dv.getBigUint64(592 + 758 + i * 2325 + 1024, true)).toBe(BigInt(i + 1));
    }
  });

  it("reads each asset's OWN slot", () => {
    expect(readAssetEffectivePriceE6(LIVE, 1)).toBe(ASSET1_E6);
  });

  it("returns null for an asset index past the account's capacity", () => {
    expect(readAssetEffectivePriceE6(LIVE, SLOTS)).toBeNull();
    expect(readAssetEffectivePriceE6(LIVE, 10_000)).toBeNull();
  });

  it("returns null for an invalid asset index", () => {
    expect(readAssetEffectivePriceE6(LIVE, -1)).toBeNull();
    expect(readAssetEffectivePriceE6(LIVE, 0.5)).toBeNull();
    expect(readAssetEffectivePriceE6(LIVE, Number.NaN)).toBeNull();
  });

  it("returns null for bytes that are not a v18 market account", () => {
    expect(readAssetEffectivePriceE6(new Uint8Array(LIVE.length), 0)).toBeNull();
    expect(readAssetEffectivePriceE6(new Uint8Array(8), 0)).toBeNull();
  });

  it("returns null for a truncated account", () => {
    expect(readAssetEffectivePriceE6(LIVE.slice(0, 2400), 0)).toBeNull();
  });

  it("returns null for a zero or out-of-range price", () => {
    expect(readAssetEffectivePriceE6(withEffective(0n), 0)).toBeNull();
    expect(readAssetEffectivePriceE6(withEffective(1_000_000_000_000n), 0)).toBeNull();
    expect(readAssetEffectivePriceE6(withEffective(42n), 0)).toBe(42);
  });
});

describe("layout pin (real account bytes)", () => {
  const dv = new DataView(LIVE.buffer, LIVE.byteOffset, LIVE.byteLength);
  const BASE = 592 + 758 + 1024; // asset 0's AssetStateV16Account
  it("reads the field at +25, and NOT any off-by-n neighbour (real bytes: those are shifted, out-of-range or other fields)", () => {
    const at = (rel: number) => dv.getBigUint64(BASE + rel, true);
    expect(at(25)).toBe(BigInt(BOOKED_E6));
    expect(readAssetEffectivePriceE6(LIVE, 0)).toBe(Number(at(25)));
    for (const rel of [9, 24, 26, 41]) {
      expect(at(rel)).not.toBe(BigInt(BOOKED_E6)); // an offset mutation to any of these returns another value or null
    }
    // NOTE: on every live devnet slab (47 markets x 14 slots) the adjacent u64s at +17, +25 and +33
    // (raw oracle target, effective price, next field) are IDENTICAL, so no real account can tell
    // those three offsets apart; the independent confirmation of +25 is the real-tx cross-check in
    // tests/matcher-fill-xcheck.test.ts (the wrapper's own oracle_price_e6 == this field).
  });
  it("market_id sits at +0 and lifecycle at +16 of the same slot (the struct the +25 comes from)", () => {
    expect(dv.getBigUint64(BASE, true)).toBe(1n);
    expect(LIVE[BASE + 16]).toBeLessThan(8);
  });
  it("only the v18 layout version is read: version 17 or 19 in the header returns null even if the rest looks valid", () => {
    for (const v of [17, 19]) {
      const copy = LIVE.slice();
      new DataView(copy.buffer).setUint16(8, v, true);
      expect(readAssetEffectivePriceE6(copy, 0)).toBeNull();
    }
    expect(readAssetEffectivePriceE6(LIVE, 0)).toBe(BOOKED_E6);
  });
});

describe("readMarkPriceE6 with an assetIndex (#221)", () => {
  const conn = { getAccountInfo: vi.fn(async () => ({ data: Buffer.from(LIVE) })) } as any;
  const SLAB = "Ar6khqrJVfPDx1KGmSP6Q4hxre6NNm66GpDJPHG1oF6N";

  it("prices a fill at the asset's booked effective_price", async () => {
    expect(await readMarkPriceE6(conn, SLAB, 0)).toBe(BOOKED_E6);
  });

  it("prices each asset from its own slot", async () => {
    expect(await readMarkPriceE6(conn, SLAB, 1)).toBe(ASSET1_E6);
  });

  it("falls back to the mark when the asset's slot can't be read", async () => {
    expect(await readMarkPriceE6(conn, SLAB, SLOTS)).toBe(EWMA_E6);
  });

  it("keeps the original mark-only behaviour when no assetIndex is given", async () => {
    expect(await readMarkPriceE6(conn, SLAB)).toBe(EWMA_E6);
  });
});
