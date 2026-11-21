/**
 * DRAFT (report idx-liquidations-vs-chain-20261120, finding LIQ-3, against OPEN PR #300 a979e3e, not base).
 * Drop into tests/ with fixtures/ next to it, on #300's branch.
 *
 * #300 writes one is_liquidation trades row per REDUCE reason-3 event with `leg_index = 1000 + ord`, where `ord` counts per
 * (slab, asset) (#300 src/services/v22LogEvents.ts:34-52; its own test asserts "independent of other slabs"). The trades
 * dedup key is (tx_signature, asset_index, leg_index) with NO slab (supabase/migrations/20260903120000_trades_dedup_nulls_and_backfill.sql:58,
 * insertTradeRow onConflict src/db/insertTradeRow.ts:172, duplicates ignored). A sweep liquidating asset 0 on two markets in one
 * tx gives both markers (sig, 0, 1000): the second is dropped as a "duplicate". Real-path proof: scratch-PG run "outsynth300"
 * in the report (poll: 1 marker of 2, the 7f52gF liquidation missing).
 *
 * Skipped on base (no liquidationMarkers export). Fails on #300.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { decodeV22LogEvents } from "../src/parsers/v22FillEvents.js";
import { rawInstructionsFromParsedTx } from "../src/parsers/v22Events.js";

const W = "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM";
const FX = JSON.parse(readFileSync(new URL("./fixtures/synth-sweep-2mkt.json", import.meta.url), "utf8"));
const P = "../src/services/v22LogEvents.js";
const mod: any = await import(/* @vite-ignore */ P).catch(() => null);
const has = typeof mod?.liquidationMarkers === "function";

describe("LIQ-3 (#300): liquidation markers of one tx must have distinct trades keys", () => {
  it.skipIf(!has)("two liquidations on two markets, same asset, one tx -> two distinct (tx_signature, asset_index, leg_index)", () => {
    const rows = decodeV22LogEvents(
      { err: null, logMessages: FX.tx.meta.logMessages },
      rawInstructionsFromParsedTx(FX.tx, [W]),
      new Set([W]),
      { isKnownMarket: () => true, versionOf: () => null, generationOf: () => null },
      { signature: FX.signature, slot: FX.tx.slot, blockTimeSec: FX.tx.blockTime },
    ).rows;
    const markers = mod.liquidationMarkers(rows) as Array<{ tx_signature: string; asset_index: number; leg_index: number; slab_address: string }>;
    expect(markers.map((m) => m.slab_address).sort()).toEqual(["6TfAsNSi8dNdNCr4FQnXUmV9aUHD8ciz25gLR1MVVpUa", "7f52gFQcdhGBorp1DmuThCQ1wiPVMozEW7mTVq37ULbj"]);
    const keys = new Set(markers.map((m) => `${m.tx_signature}|${m.asset_index}|${m.leg_index}`));
    expect(keys.size).toBe(markers.length); // FAILS on #300: both are (sig, 0, 1000)
  });
});
