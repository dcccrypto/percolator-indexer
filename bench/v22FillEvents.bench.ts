/**
 * decodeV22LogEvents bench over the v22Fixtures-based doc examples. Run: node --import tsx bench/v22FillEvents.bench.ts
 * Env: BENCH_N (iterations per run, default 50000), BENCH_RUNS (default 5), BENCH_IMPL=new|ref|both (default both: interleaved so machine load hits both equally). Prints median ops/s and an output hash.
 */
import { createHash } from "node:crypto";
import { decodeV22LogEvents as newImpl } from "../src/parsers/v22FillEvents.js";
import { decodeV22LogEvents as refImpl } from "../tests/helpers/v22FillEventsRef.js";
import { fixtureTxs, gate, WRAPPERS } from "../tests/helpers/v22FillLogs.js";

const N = Number(process.env.BENCH_N ?? 50_000);
const RUNS = Number(process.env.BENCH_RUNS ?? 5);
const txs = fixtureTxs();
const which = process.env.BENCH_IMPL ?? "both";
const impls: Array<[string, typeof newImpl]> = [];
if (which !== "new") impls.push(["ref", refImpl]);
if (which !== "ref") impls.push(["new", newImpl]);
const ctx = { signature: "sig", slot: 1, blockTimeSec: 1_790_000_000 };
const run = (fn: typeof newImpl, n: number): void => {
  for (let i = 0; i < n; i++) {
    const t = txs[i % txs.length];
    fn({ err: null, logMessages: t.logs }, t.ixs, WRAPPERS, gate, ctx);
  }
};
const hash = (fn: typeof newImpl): string => {
  const h = createHash("sha256");
  for (const t of txs) h.update(JSON.stringify(fn({ err: null, logMessages: t.logs }, t.ixs, WRAPPERS, gate, ctx), (_k, v) => (typeof v === "bigint" ? v.toString() : v)));
  return h.digest("hex").slice(0, 16);
};
const rates: Record<string, number[]> = {};
for (const [name, fn] of impls) { rates[name] = []; run(fn, 3000); }
for (let r = 0; r < RUNS; r++) {
  for (const [name, fn] of impls) {
    const t0 = process.hrtime.bigint();
    run(fn, N);
    rates[name].push(N / (Number(process.hrtime.bigint() - t0) / 1e9));
  }
}
const med: Record<string, number> = {};
for (const [name, fn] of impls) {
  const a = [...rates[name]].sort((x, y) => x - y);
  med[name] = a[Math.floor(RUNS / 2)];
  console.log(`${name}: median ${Math.round(med[name])} ops/s  runs=${rates[name].map((x) => Math.round(x)).join(",")}  hash=${hash(fn)}`);
}
if (med.ref && med.new) console.log(`speedup x${(med.new / med.ref).toFixed(2)} (${((med.new / med.ref - 1) * 100).toFixed(0)}%)`);
