/**
 * v22Events bench: both ingestion adapters + decodeV22Events over 10,000 SDK-built v2.2 transactions
 * (every event kind, inner instructions, some non-event noise). Reports the FROZEN legacy module vs the current one,
 * median of 5 runs, in the same process.
 *
 * Run: npx vitest run --config bench/vitest.bench.config.ts bench/v22Events.bench.test.ts
 * Env: BENCH_TXS (default 10000), BENCH_RUNS (default 5)
 */
import { it, expect } from "vitest";
import { createHash } from "node:crypto";
import { BackstopMode, buildBondDepositIxV22, buildBondExecuteWithdrawIxV22, buildBondRequestWithdrawIxV22, buildEvictAndTradeCpiIxV22,
  buildInitInsuranceUnitsIxV22, buildInsuranceBackstopDrawIxV22, buildRescueDepositIxV22, buildSettleHoldingRentIxV22, buildSweepBandDustLegIxV22, encodeTradeCpi } from "@percolatorct/sdk";
import { TransactionInstruction } from "@solana/web3.js";
import * as cur from "../src/parsers/v22Events.js";
import * as old from "../tests/helpers/v22Events.legacy.js";
import { enhancedTx, parsedTx, pk } from "../tests/helpers/v22Fixtures.js";

const N = Number(process.env.BENCH_TXS ?? 10_000);
const RUNS = Number(process.env.BENCH_RUNS ?? 5);
const PROGRAM = pk();
const market = pk(), lp = pk(), user = pk(), keeper = pk(), portfolio = pk(), ata = pk(), vault = pk(), victim = pk(), taker = pk();
const m = { programId: PROGRAM, market, registryDomain: 0, lpPortfolio: lp };

function tradeCpi(): TransactionInstruction {
  const data = encodeTradeCpi({ accountAPortfolioId: 1n, accountAPositionEpoch: 2n, accountBPortfolioId: 3n, accountBPositionEpoch: 4n, accountBMatcherSequence: 5n,
    assetIndex: 4, marketId: 9n, sizeQ: -1_000_000n, feeBps: 5n, limitPrice: 100_000_000n, backingFeeCapBps: 0 });
  return new TransactionInstruction({ programId: PROGRAM, keys: [{ pubkey: taker, isSigner: true, isWritable: true }, { pubkey: market, isSigner: false, isWritable: true }], data: Buffer.from(data) });
}
const ixs = [
  buildBondDepositIxV22(m, user, ata, vault, 5_000_000n, 4_900_000n), buildBondRequestWithdrawIxV22(m, user, 777n), buildBondExecuteWithdrawIxV22(m, user, ata, vault, 123n, 1),
  buildRescueDepositIxV22(m, user, ata, ata, vault, 100_000_000n, 90_000_000n), buildInitInsuranceUnitsIxV22(m, keeper),
  buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Propose, 0n), buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Draw, 2n ** 128n - 1n),
  buildSettleHoldingRentIxV22(m, keeper, portfolio, 3, 123_456_789n), buildSweepBandDustLegIxV22(m, keeper, portfolio, 2), buildEvictAndTradeCpiIxV22(tradeCpi(), victim),
  new TransactionInstruction({ programId: PROGRAM, keys: [], data: Buffer.from([105, 0, 0]) }), // non-event noise
];
const parsed = Array.from({ length: N }, (_, i) => parsedTx([ixs[i % ixs.length], ixs[(i * 7 + 3) % ixs.length]], { inner: i % 3 === 0 ? [ixs[(i + 5) % ixs.length]] : undefined }));
const enhanced = Array.from({ length: N }, (_, i) => enhancedTx([ixs[i % ixs.length], ixs[(i * 7 + 3) % ixs.length]], i % 3 === 0 ? { nested: [ixs[(i + 5) % ixs.length]] } : {}));
const ids = [PROGRAM.toBase58()];
const idSet = new Set(ids);
const ctx = { signature: "benchSig", slot: 1, blockTimeSec: 1_790_000_000 };

type Mod = typeof cur;
function pass(mod: Mod): string {
  const h = createHash("sha256");
  for (let i = 0; i < N; i++) {
    const a = mod.decodeV22Events(mod.rawInstructionsFromParsedTx(parsed[i] as never, ids), ctx);
    const b = mod.decodeV22Events(mod.rawInstructionsFromEnhancedTx(enhanced[i] as never, idSet), ctx);
    h.update(JSON.stringify([a, b]));
  }
  return h.digest("hex");
}
// timing pass without the hash (hashing would dominate)
function timed(mod: Mod): number {
  let sink = 0;
  const t = performance.now();
  for (let i = 0; i < N; i++) {
    sink += mod.decodeV22Events(mod.rawInstructionsFromParsedTx(parsed[i] as never, ids), ctx).length;
    sink += mod.decodeV22Events(mod.rawInstructionsFromEnhancedTx(enhanced[i] as never, idSet), ctx).length;
  }
  const dt = performance.now() - t;
  if (sink < 0) throw new Error("unreachable");
  return dt;
}
const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

it(`v22Events bench: ${N} txs x 2 shapes, median of ${RUNS}`, () => {
  expect(pass(cur)).toBe(pass(old)); // identical output before any timing claim
  timed(old); timed(cur); // warm up
  const o: number[] = [], c: number[] = [];
  for (let r = 0; r < RUNS; r++) { o.push(timed(old)); c.push(timed(cur)); }
  const mo = median(o), mc = median(c);
  console.log(`BENCH legacy runs=${o.map((x) => x.toFixed(0)).join(",")} median=${mo.toFixed(1)}ms`);
  console.log(`BENCH current runs=${c.map((x) => x.toFixed(0)).join(",")} median=${mc.toFixed(1)}ms`);
  console.log(`BENCH speedup=${(((mo - mc) / mo) * 100).toFixed(1)}% faster (${(mo / mc).toFixed(2)}x)`);
}, 600_000);
