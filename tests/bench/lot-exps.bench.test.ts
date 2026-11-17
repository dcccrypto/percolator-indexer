/**
 * noteLotExps bench: us/call, SDK lotExpOfMarketV22 calls/call and throws/call for markets with 1, 4 and 64 slots
 * (64 = the loop cap), plus the frozen legacy implementation as reference. No timing assertions (numbers are printed).
 *
 * Run: npx vitest run --maxWorkers=4 tests/bench/lot-exps.bench.test.ts
 * Env: BENCH_CALLS (default 5000)
 */
import { it, expect, vi } from "vitest";

const counts = { calls: 0, throws: 0 };
vi.mock("@percolatorct/sdk", async (orig) => {
  const mod = await orig<typeof import("@percolatorct/sdk")>();
  return {
    ...mod,
    lotExpOfMarketV22: (data: Uint8Array, a?: number) => {
      counts.calls++;
      try { return mod.lotExpOfMarketV22(data, a); } catch (e) { counts.throws++; throw e; }
    },
  };
});

import { LAYOUT_V22 } from "@percolatorct/sdk";
import { noteLotExps, resetLotExpCache, knownLotExp } from "../../src/parsers/lotScale.js";
import { legacyNoteLotExps } from "../helpers/lotScale.legacy.js";
import { buildMarket } from "../helpers/v22Fixtures.js";

const N = Number(process.env.BENCH_CALLS ?? 5000);

function market(slots: number): Uint8Array {
  const d = buildMarket(LAYOUT_V22, { slots });
  const base = LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen;
  for (let a = 0; a < slots; a++) d[base + a * LAYOUT_V22.assetSlotStride + LAYOUT_V22.wrapperSlot.profileLotExp] = a % 7;
  return d;
}

function run(label: string, fn: (d: Uint8Array) => void, d: Uint8Array): void {
  for (let i = 0; i < 200; i++) fn(d); // warm-up
  counts.calls = 0; counts.throws = 0;
  const t0 = performance.now();
  for (let i = 0; i < N; i++) fn(d);
  const us = ((performance.now() - t0) * 1000) / N;
  console.log(`${label.padEnd(26)} ${us.toFixed(2).padStart(8)} us/call  sdkCalls/call=${(counts.calls / N).toFixed(2)}  throws/call=${(counts.throws / N).toFixed(2)}`);
}

it("noteLotExps bench", () => {
  for (const slots of [1, 4, 64]) {
    const d = market(slots);
    run(`current   ${slots} slot(s)`, (x) => noteLotExps("BenchMarket", x), d);
    run(`legacy    ${slots} slot(s)`, (x) => legacyNoteLotExps("BenchMarket", x), d);
    resetLotExpCache();
    noteLotExps("BenchMarket", d);
    expect(knownLotExp("BenchMarket", slots - 1)).toBe((slots - 1) % 7);
  }
}, 120_000);
