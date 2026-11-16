/**
 * Matcher-fill parse bench: 1,000 transactions built from tests/fixtures/tradecpi/*.json, parsed through
 * parsePercolatorFills (-> cpiEvidence -> decodeBase58 + decodeMatcherCall) and then resolveCpiLeg
 * (returnMatches + counters) with a stub context read. Prints median ms over 20 runs, plus a sha256 of
 * every parsed fill + resolution so a before/after pair proves the output is identical.
 *
 * Run:  npx tsx bench/matcher-fill-parse.ts
 * Env:  BENCH_TX (default 1000), BENCH_RUNS (default 20)
 */
import { readFileSync, readdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { parsePercolatorFills } from "../src/parsers/percolatorTxParser.js";
import { decodeMatcherReturn, resolveCpiLeg, type ContextRead } from "../src/parsers/matcherFill.js";

const TX = Number(process.env.BENCH_TX ?? 1000);
const RUNS = Number(process.env.BENCH_RUNS ?? 20);
const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const dir = new URL("../tests/fixtures/tradecpi/", import.meta.url);

type Fx = { sig: string; tx: any; ctxReturnHex?: string };
const fixtures: Fx[] = readdirSync(dir)
  .filter((n) => n.endsWith(".json"))
  .sort()
  .map((n) => JSON.parse(readFileSync(new URL(n, dir), "utf8")) as Fx)
  .filter((f) => f.tx && f.sig && parsePercolatorFills(f.tx, f.sig, [PROGRAM]).length > 0);

const txs = Array.from({ length: TX }, (_, i) => fixtures[i % fixtures.length]);
const ctxOf = (f: Fx) => (f.ctxReturnHex ? decodeMatcherReturn(Uint8Array.from(Buffer.from(f.ctxReturnHex, "hex"))) : null);
const json = (v: unknown) => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x));

async function pass(hash?: ReturnType<typeof createHash>): Promise<number> {
  let fills = 0;
  for (const f of txs) {
    const out = parsePercolatorFills(f.tx, f.sig, [PROGRAM]);
    fills += out.length;
    const ctx = ctxOf(f);
    const read = async (): Promise<ContextRead> => ({ kind: "ok", ret: ctx });
    for (const fill of out) {
      let res = null;
      if (fill.cpi) {
        res = await resolveCpiLeg({ evidence: fill.cpi, assetIndex: fill.assetIndex, side: fill.side, wireSizeAbs: fill.sizeAbs, legPos: fill.legPos ?? 0, readContext: read, policy: "request" });
      }
      if (hash) hash.update(json([fill, res]));
    }
  }
  return fills;
}

const h = createHash("sha256");
const fills = await pass(h);
for (let i = 0; i < 3; i++) await pass();
const times: number[] = [];
for (let r = 0; r < RUNS; r++) {
  const t0 = performance.now();
  await pass();
  times.push(performance.now() - t0);
}
times.sort((a, b) => a - b);
const mid = Math.floor(RUNS / 2);
const median = RUNS % 2 ? times[mid] : (times[mid - 1] + times[mid]) / 2;
console.log(`fixtures=${fixtures.length} tx=${TX} fills/pass=${fills} runs=${RUNS}`);
console.log(`median ${median.toFixed(1)} ms per ${TX} tx   (min ${times[0].toFixed(1)}, max ${times[RUNS - 1].toFixed(1)})`);
console.log(`output sha256 ${h.digest("hex")}`);

process.exit(0);
