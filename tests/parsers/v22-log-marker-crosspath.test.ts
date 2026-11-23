/**
 * Regression for LOGORD-1 (report J-20261123-IDX-REPORT-LOGORDINALCROSSPATHREAL).
 *
 * An `events_unknown` marker's dedup key (signature, -1, inner_index) used to number markets by their position among the
 * markets the path's gate KNOWS, so in a truncated multi-market tx two markets' markers collided on (sig, -1, 1_000_000)
 * and `insertV22Events`' ignoreDuplicates silently dropped one, leaving that market's lost events unflagged.
 * The fix (src/parsers/v22FillEvents.ts `markerOrdinal`) keys a marker on its market address alone.
 *
 * Fixture is SYNTHETIC (real-derived): real TradeCpi 4HMi5a7n… on 6YppKd… + real TradeCpi 35nYdrnS… on 6fonYS…,
 * concatenated (sig SYNTHCP002, slot 510327470); logs cut after the first wrapper data line ("Log truncated").
 *
 * Failed 2/2 on 831a5d4 (cases 1 and 2).
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { LOG_EVENT_INNER_BASE, decodeV22LogEvents, markerOrdinal } from "../../src/parsers/v22FillEvents.js";
import { rawInstructionsFromParsedTx } from "../../src/parsers/v22Events.js";

const W = "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM";
const W18 = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const FX = JSON.parse(readFileSync(new URL("./fixtures/synth-trade-2mkt.json", import.meta.url), "utf8"));
const A = "6YppKdUiMmQSie7DrpJgQ9ofmCxvEhanKoRw6e9Yzh4T";
const B = "6fonYScxUpeN8782wm1DQMPJVZj5vUGXnKg75GTRSBUF";
const lm: string[] = FX.meta.logMessages;
const cut = [...lm.slice(0, lm.findIndex((l) => l.startsWith("Program data: ")) + 1), "Log truncated"];

const markers = (known: Set<string>, wrappers: string[] = [W]) =>
  decodeV22LogEvents(
    { err: null, logMessages: cut },
    rawInstructionsFromParsedTx(FX, wrappers),
    new Set(wrappers),
    { isKnownMarket: (s) => known.has(s), versionOf: (s) => (known.has(s) ? 19 : null), generationOf: () => null },
    { signature: FX.transaction.signatures[0], slot: FX.slot, blockTimeSec: FX.blockTime },
  ).rows.map((r) => ({ slab: r.slab_address, key: `${r.ix_index}|${r.inner_index}`, kind: r.kind }));

describe("LOGORD-1: an events_unknown marker's key must not depend on the path's market gate", () => {
  it("market B's marker has the same key under the poll gate {B} and the registry gate {A, B}", () => {
    const poll = markers(new Set([B]));
    const registry = markers(new Set([A, B]));
    expect(poll).toEqual([expect.objectContaining({ slab: B, kind: "events_unknown" })]);
    expect(registry.find((r) => r.slab === B)?.key).toBe(poll[0].key);
  });

  it("the poll path's two slab passes give the two markets' markers different keys", () => {
    const a = markers(new Set([A]));
    const b = markers(new Set([B]));
    expect(a[0].key).not.toBe(b[0].key);
  });

  it("the key is the same whatever wrapper ids the path passes (stream {6kpg} vs poll {6kpg, ETDLAd})", () => {
    expect(markers(new Set([A, B]), [W, W18])).toEqual(markers(new Set([A, B]), [W]));
  });

  it("the registry gives both markers, keyed by address and inside the int4 inner_index column", () => {
    const registry = markers(new Set([A, B]));
    expect(registry.map((r) => r.slab)).toEqual([A, B].sort());
    expect(registry.map((r) => r.key)).toEqual([A, B].sort().map((m) => `-1|${LOG_EVENT_INNER_BASE + markerOrdinal(m)}`));
    for (const m of [A, B, "1".repeat(32), "z".repeat(44)]) {
      expect(Number.isInteger(markerOrdinal(m))).toBe(true);
      expect(markerOrdinal(m)).toBeGreaterThanOrEqual(0);
      expect(LOG_EVENT_INNER_BASE + markerOrdinal(m)).toBeLessThanOrEqual(2 ** 31 - 1);
    }
  });
});
