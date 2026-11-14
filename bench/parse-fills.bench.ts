/**
 * Parse-path bench: parsePercolatorFills over the RECORDED v2.2 devnet fixture (no network, no DB).
 *
 * The 167 recorded transactions are rehydrated into the shape getParsedTransaction returns (PublicKey objects),
 * then parsed repeatedly. Reports ms per 1,000 transactions parsed, median of 5 runs, plus a hash of every
 * parsed fill so a before/after pair can prove the output is identical.
 *
 * Run:  NETWORK=devnet PROGRAM_ID=5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe npx tsx bench/parse-fills.bench.ts
 * Env:  BENCH_ROUNDS (passes over the fixture per run, default 30), BENCH_RUNS (default 5)
 */
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import { PublicKey } from "@solana/web3.js";
import { parsePercolatorFills } from "../src/parsers/percolatorTxParser.js";

const ROUNDS = Number(process.env.BENCH_ROUNDS ?? 30);
const RUNS = Number(process.env.BENCH_RUNS ?? 5);
const WRAPPER = "5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe";

const fx = JSON.parse(gunzipSync(readFileSync(new URL("./fixtures/v22-6markets-20261113.json.gz", import.meta.url))).toString("utf8")) as {
  txs: Record<string, any>;
};

const pk = (s: string) => new PublicKey(s);
function rehydrateIx(ix: any): any {
  if (ix.parsed) return { ...ix, programId: pk(ix.programId) };
  return { ...ix, programId: pk(ix.programId), accounts: (ix.accounts ?? []).map(pk) };
}
function rehydrate(tx: any): any {
  return {
    ...tx,
    meta: tx.meta && {
      ...tx.meta,
      innerInstructions: (tx.meta.innerInstructions ?? []).map((g: any) => ({ ...g, instructions: g.instructions.map(rehydrateIx) })),
    },
    transaction: {
      ...tx.transaction,
      message: { ...tx.transaction.message, instructions: tx.transaction.message.instructions.map(rehydrateIx) },
    },
  };
}

const entries = Object.entries(fx.txs).filter(([, t]) => t).map(([sig, t]) => [sig, rehydrate(t)] as const);
const programIds = [WRAPPER];
const json = (v: unknown) => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x));

function pass(hash?: ReturnType<typeof createHash>): number {
  let fills = 0;
  for (const [sig, tx] of entries) {
    const out = parsePercolatorFills(tx, sig, programIds);
    fills += out.length;
    if (hash) hash.update(json(out));
  }
  return fills;
}

const h = createHash("sha256");
const fillsPerPass = pass(h);
for (let i = 0; i < 3; i++) pass(); // warm up

const per1000: number[] = [];
for (let r = 0; r < RUNS; r++) {
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < ROUNDS; i++) pass();
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  per1000.push((ms / (ROUNDS * entries.length)) * 1000);
}
const sorted = [...per1000].sort((a, b) => a - b);
console.log(
  "BENCH " +
    JSON.stringify({
      txs: entries.length,
      fillsPerPass,
      runs_ms_per_1000_tx: per1000.map((x) => +x.toFixed(2)),
      median_ms_per_1000_tx: +sorted[Math.floor(sorted.length / 2)].toFixed(2),
      outputHash: h.digest("hex").slice(0, 16),
    }),
);
// @percolatorct/shared opens handles on import (logger/DB client); exit explicitly so the bench ends.
process.exit(0);
