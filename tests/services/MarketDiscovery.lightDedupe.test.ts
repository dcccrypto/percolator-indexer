import { describe, it, expect, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";

const PROGRAMS = ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"];
const found = vi.hoisted(() => ({ byProgram: new Map<string, unknown[]>() }));

vi.mock("../../src/v17/discovery.js", () => ({
  discoverV17Markets: vi.fn(async (_c: unknown, pid: { toBase58(): string }) => found.byProgram.get(pid.toBase58()) ?? []),
}));
vi.mock("@percolatorct/sdk", () => ({ discoverMarkets: vi.fn(), getMarketsByAddress: vi.fn(), isV17Account: vi.fn(() => false) }));
vi.mock("@percolatorct/shared", () => ({
  config: { allProgramIds: ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"] },
  createLogger: () => ({ info() {}, warn() {}, error() {}, debug() {} }),
  captureException: vi.fn(),
  sendCriticalAlert: vi.fn(),
  getPrimaryConnection: () => ({}),
  getFallbackConnection: () => ({}),
}));

import { MarketDiscovery } from "../../src/services/MarketDiscovery.js";

function rng(seed: number) {
  let s = seed >>> 0;
  return () => ((s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 2 ** 32);
}
const slab = (i: number) => {
  const b = new Uint8Array(32);
  b[0] = i & 255; b[1] = i >> 8; b[31] = 9;
  return new PublicKey(b);
};

/** The pre-change algorithm, verbatim: O(n^2) `some` scan, key re-encoded at the swap. */
function reference(known: Map<string, unknown>, lists: any[][]) {
  const fresh: any[] = [];
  for (const found of lists) {
    for (const m of found) {
      const key = m.slabAddress.toBase58();
      if (!known.has(key) && !fresh.some((f) => f.slabAddress.toBase58() === key)) fresh.push(m);
    }
  }
  const next = new Map(known);
  if (fresh.length > 0) for (const m of fresh) next.set(m.slabAddress.toBase58(), { market: m });
  return { fresh, next };
}

describe("discoverLight dedupe: differential vs the O(n^2) original", () => {
  it("random found lists with duplicates across and within programs match exactly", async () => {
    let mismatches = 0;
    let withDupes = 0;
    for (let seed = 1; seed <= 40; seed++) {
      const r = rng(seed);
      const pool = 5 + Math.floor(r() * 60);
      const lists = PROGRAMS.map((p) =>
        Array.from({ length: Math.floor(r() * 40) }, () => ({ slabAddress: slab(Math.floor(r() * pool)), programId: new PublicKey(p) })),
      );
      PROGRAMS.forEach((p, i) => found.byProgram.set(p, lists[i]));

      const d = new MarketDiscovery();
      const known = new Map<string, unknown>();
      for (let i = 0; i < pool; i++) if (r() < 0.25) known.set(slab(i).toBase58(), { market: { slabAddress: slab(i), preexisting: true } });
      (d as any).markets = new Map(known);

      const exp = reference(known, lists);
      const got = await d.discoverLight();
      const gotMap: Map<string, unknown> = (d as any).markets;

      if (exp.fresh.length < lists.flat().length) withDupes++;
      const sameFresh = got.length === exp.fresh.length && got.every((m, i) => m === exp.fresh[i]);
      const sameMap = gotMap.size === exp.next.size && [...exp.next].every(([k, v]) => (gotMap.get(k) as any)?.market === (v as any).market);
      const sameOrder = JSON.stringify([...gotMap.keys()]) === JSON.stringify([...exp.next.keys()]);
      if (!sameFresh || !sameMap || !sameOrder) mismatches++;
    }
    expect(withDupes).toBeGreaterThan(20); // the generator really produces duplicates
    expect(mismatches).toBe(0);
  });

  it("empty scans leave the map object untouched", async () => {
    PROGRAMS.forEach((p) => found.byProgram.set(p, []));
    const d = new MarketDiscovery();
    const before = (d as any).markets;
    expect(await d.discoverLight()).toEqual([]);
    expect((d as any).markets).toBe(before);
  });
});
