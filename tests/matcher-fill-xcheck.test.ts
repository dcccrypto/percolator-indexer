/**
 * Cross-check on REAL data: the `oracle_price_e6` the wrapper hands the matcher (what we read from
 * the CPI) equals the slab's `asset[i].effective_price` (what the engine books the fill at, and what
 * `readAssetEffectivePriceE6` reads). The slab was read after the tx while its effective_price still
 * equalled the call's oracle price (see the fixture note: it is a latest-state read, not a post-state).
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { parsePercolatorFills } from "../src/parsers/percolatorTxParser.js";
import { resolveCpiLeg } from "../src/parsers/matcherFill.js";
import { readAssetEffectivePriceE6 } from "../src/parsers/markPrice.js";

const f = JSON.parse(readFileSync(new URL("./fixtures/tradecpi/xcheck-oracle-vs-effective.json", import.meta.url), "utf8"));
const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";

describe("matcher-call oracle price == slab effective_price", () => {
  it("same value, same asset", async () => {
    const [fill] = parsePercolatorFills(f.tx, f.sig, [PROGRAM]);
    expect(fill.slabAddress).toBe(f.slab);
    const r = await resolveCpiLeg({ evidence: fill.cpi!, assetIndex: fill.assetIndex, side: fill.side, wireSizeAbs: fill.sizeAbs, legPos: 0, readContext: async () => null, policy: "request" });
    const slab = new Uint8Array(Buffer.from(f.slabDataBase64, "base64"));
    const eff = readAssetEffectivePriceE6(slab, fill.assetIndex);
    expect(eff).toBe(Number(f.slabEffectivePriceE6));
    expect(r).toMatchObject({ kind: "fill" });
    expect((r as { priceE6: bigint }).priceE6).toBe(BigInt(eff!));
  });
  it("negative control: another asset's slot is not this price", () => {
    const slab = new Uint8Array(Buffer.from(f.slabDataBase64, "base64"));
    expect(readAssetEffectivePriceE6(slab, 1)).not.toBe(Number(f.slabEffectivePriceE6));
  });
});
