/**
 * LIQ-3 (#300): the trades key is (tx_signature, asset_index, leg_index) with no slab, so a sweep liquidating asset 0 on two
 * markets in one tx used to give both markers (sig, 0, 1000) and the second was dropped as a duplicate.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { decodeV22LogEvents } from "../src/parsers/v22FillEvents.js";
import { rawInstructionsFromParsedTx } from "../src/parsers/v22Events.js";
import { liquidationMarkers } from "../src/services/v22LogEvents.js";

const W = "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM";
const FX = JSON.parse(readFileSync(new URL("./fixtures/synth-sweep-2mkt.json", import.meta.url), "utf8"));

describe("LIQ-3 (#300): liquidation markers of one tx must have distinct trades keys", () => {
  it("two liquidations on two markets, same asset, one tx -> two distinct (tx_signature, asset_index, leg_index)", () => {
    const rows = decodeV22LogEvents(
      { err: null, logMessages: FX.tx.meta.logMessages },
      rawInstructionsFromParsedTx(FX.tx, [W]),
      new Set([W]),
      { isKnownMarket: () => true, versionOf: () => null, generationOf: () => null },
      { signature: FX.signature, slot: FX.tx.slot, blockTimeSec: FX.tx.blockTime },
    ).rows;
    const markers = liquidationMarkers(rows);
    expect(markers.map((m) => m.slab_address).sort()).toEqual(["6TfAsNSi8dNdNCr4FQnXUmV9aUHD8ciz25gLR1MVVpUa", "7f52gFQcdhGBorp1DmuThCQ1wiPVMozEW7mTVq37ULbj"]);
    const keys = new Set(markers.map((m) => `${m.tx_signature}|${m.asset_index}|${m.leg_index}`));
    expect(keys.size).toBe(markers.length); // before the fix both were (sig, 0, 1000)
  });
});
