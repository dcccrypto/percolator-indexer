/**
 * decodeBase58Fast micro-bench: >=50k realistic inputs (32-byte keys, 64-byte signatures, seeded), median of 5 runs.
 * Run:  NETWORK=devnet PROGRAM_ID=5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe npx tsx bench/fastBase58.bench.ts
 * Env:  BENCH_N (default 60000), BENCH_RUNS (default 5), BENCH_IMPL (module path, default the src decoder;
 *       bench/fastBase58.old.ts is the pre-change copy for before/after pairs).
 * Prints ms per pass over all inputs and a hash of every output so a before/after pair can show identical output.
 */
import { createHash } from "node:crypto";
import { encodeBase58, rng } from "../tests/parsers/percolatorTxParser.fixtures.js";

const N = Number(process.env.BENCH_N ?? 60000);
const RUNS = Number(process.env.BENCH_RUNS ?? 5);
const impls = (process.env.BENCH_IMPL ?? "../src/parsers/fastBase58.js").split(",");

const r = rng(12345);
const inputs: string[] = [];
for (let i = 0; i < N; i++) {
  const len = i % 4 === 0 ? 64 : 32; // 3:1 keys to signatures, as in a transaction
  inputs.push(encodeBase58(Uint8Array.from({ length: len }, () => Math.floor(r() * 256))));
}

for (const path of impls) {
  const { decodeBase58Fast } = (await import(path)) as { decodeBase58Fast: (s: string) => Uint8Array | null };
  const hash = createHash("sha256");
  for (const s of inputs) hash.update(decodeBase58Fast(s)!);
  let sink = 0;
  const times: number[] = [];
  for (let run = 0; run < RUNS + 1; run++) {
    const t0 = performance.now();
    for (let i = 0; i < inputs.length; i++) sink += decodeBase58Fast(inputs[i])!.length;
    const ms = performance.now() - t0;
    if (run > 0) times.push(ms); // run 0 = warmup
  }
  times.sort((a, b) => a - b);
  console.log(JSON.stringify({ impl: path, inputs: N, runsMs: times.map((t) => +t.toFixed(1)), medianMs: +times[Math.floor(times.length / 2)].toFixed(1), outputSha256: hash.digest("hex").slice(0, 16), sink }));
}
process.exit(0);
