/**
 * decodeV22LogEvents bench over the v22Fixtures-based doc examples. Run: node --import tsx bench/v22FillEvents.bench.ts
 * Env: BENCH_N (iterations per run, default 50000), BENCH_RUNS (default 5). Prints median ops/s and an output hash.
 */
import { createHash } from "node:crypto";
import { decodeV22LogEvents } from "../src/parsers/v22FillEvents.js";
import { fixtureTxs, gate, WRAPPERS } from "../tests/helpers/v22FillLogs.js";

const N = Number(process.env.BENCH_N ?? 50_000);
const RUNS = Number(process.env.BENCH_RUNS ?? 5);
const txs = fixtureTxs();
const run = (n: number): void => {
  for (let i = 0; i < n; i++) {
    const t = txs[i % txs.length];
    decodeV22LogEvents({ err: null, logMessages: t.logs }, t.ixs, WRAPPERS, gate, { signature: "sig", slot: 1, blockTimeSec: 1_790_000_000 });
  }
};
const h = createHash("sha256");
for (const t of txs) h.update(JSON.stringify(decodeV22LogEvents({ err: null, logMessages: t.logs }, t.ixs, WRAPPERS, gate, { signature: "sig", slot: 1 }), (_k, v) => (typeof v === "bigint" ? v.toString() : v)));
run(5000);
const rates: number[] = [];
for (let r = 0; r < RUNS; r++) {
  const t0 = process.hrtime.bigint();
  run(N);
  rates.push(N / (Number(process.hrtime.bigint() - t0) / 1e9));
}
rates.sort((a, b) => a - b);
console.log(`median ${Math.round(rates[Math.floor(RUNS / 2)])} ops/s  runs=${rates.map((x) => Math.round(x)).join(",")}  hash=${h.digest("hex").slice(0, 16)}`);
