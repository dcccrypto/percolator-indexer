/**
 * Light-discovery dedupe cost: MarketDiscovery.discoverLight() over 3 program ids with N found markets in total
 * (some duplicated across programs). Median of 20 runs per N.
 *
 * Run:  npx vitest run --config bench/vitest.bench.config.ts bench/discoveryDedupe.bench.test.ts
 * Prints: DISCOVERY_DEDUPE {"n":2000,"medianMs":...}
 */
import { describe, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";

const PROGRAMS = [
  "11111111111111111111111111111111",
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
  "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
];
const found = vi.hoisted(() => ({ byProgram: new Map<string, unknown[]>() }));

vi.mock("../src/v17/discovery.js", () => ({
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

import { MarketDiscovery } from "../src/services/MarketDiscovery.js";

function slabKey(i: number): PublicKey {
  const b = new Uint8Array(32);
  b[0] = i & 255; b[1] = (i >> 8) & 255; b[2] = (i >> 16) & 255; b[31] = 7;
  return new PublicKey(b);
}

describe("discoverLight dedupe bench", () => {
  for (const n of [500, 2000, 10000].filter((x) => (process.env.SKIP_N ?? "").split(",").indexOf(String(x)) < 0)) {
    it(`N=${n}`, async () => {
      const per = Math.ceil(n / 3);
      PROGRAMS.forEach((p, pi) => {
        found.byProgram.set(p, Array.from({ length: per }, (_, i) => ({ slabAddress: slabKey(i + pi * Math.floor(per * 0.9)), programId: new PublicKey(p) })));
      });
      const times: number[] = [];
      let count = 0;
      const runs = Number(process.env[`RUNS_${n}`] ?? 20); // override per N with RUNS_<n> (the quadratic pre-fix code is very slow at 2k/10k)
      for (let r = 0; r < runs; r++) {
        const d = new MarketDiscovery();
        // CPU time (user+system), not wall clock: the dev box is often heavily loaded and wall time is then noise.
        const c0 = process.cpuUsage();
        const fresh = await d.discoverLight();
        const c1 = process.cpuUsage(c0);
        times.push((c1.user + c1.system) / 1000);
        count = fresh.length;
      }
      times.sort((a, b) => a - b);
      const medianMs = runs % 2 === 0 ? (times[runs / 2 - 1] + times[runs / 2]) / 2 : times[(runs - 1) / 2];
      process.stderr.write("DISCOVERY_DEDUPE " + JSON.stringify({ n, fresh: count, runs, medianMs: +medianMs.toFixed(2) }) + "\n");
    }, 600_000);
  }
});
