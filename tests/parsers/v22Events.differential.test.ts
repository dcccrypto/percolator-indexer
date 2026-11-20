/**
 * Differential: the current src/parsers/v22Events.ts vs the FROZEN pre-speedup copy (tests/helpers/v22Events.legacy.ts).
 * Every SDK-built fixture plus 2,000 seeded random/malformed inputs must give deep-equal output and identical errors.
 */
import { isDeepStrictEqual } from "node:util";
import { describe, expect, it } from "vitest";
import { BackstopMode, buildBondDepositIxV22, buildBondExecuteWithdrawIxV22, buildBondRequestWithdrawIxV22, buildEvictAndTradeCpiIxV22,
  buildInitInsuranceUnitsIxV22, buildInsuranceBackstopDrawIxV22, buildRescueDepositIxV22, buildSettleHoldingRentIxV22, buildSweepBandDustLegIxV22, encodeTradeCpi } from "@percolatorct/sdk";
import { TransactionInstruction } from "@solana/web3.js";
import * as cur from "../../src/parsers/v22Events.js";
import * as old from "../helpers/v22Events.legacy.js";
import { encodeBase58 } from "../../src/lib/base58.js";
import { enhancedTx, parsedTx, pk } from "../helpers/v22Fixtures.js";

const PROGRAM = pk(), market = pk(), lp = pk(), user = pk(), keeper = pk(), portfolio = pk(), ata = pk(), vault = pk(), victim = pk(), taker = pk();
const m = { programId: PROGRAM, market, registryDomain: 0, lpPortfolio: lp };
const P = PROGRAM.toBase58();
const ctx = { signature: "sig", slot: 7, blockTimeSec: 1_790_000_000 };

const tradeCpi = (): TransactionInstruction => new TransactionInstruction({
  programId: PROGRAM, keys: [{ pubkey: taker, isSigner: true, isWritable: true }, { pubkey: market, isSigner: false, isWritable: true }],
  data: Buffer.from(encodeTradeCpi({ accountAPortfolioId: 1n, accountAPositionEpoch: 2n, accountBPortfolioId: 3n, accountBPositionEpoch: 4n, accountBMatcherSequence: 5n,
    assetIndex: 4, marketId: 9n, sizeQ: -1_000_000n, feeBps: 5n, limitPrice: 100_000_000n, backingFeeCapBps: 0 })),
});
const fixtures: TransactionInstruction[] = [
  buildBondDepositIxV22(m, user, ata, vault, 5_000_000n, 4_900_000n), buildBondRequestWithdrawIxV22(m, user, 777n), buildBondRequestWithdrawIxV22(m, user, 0n),
  buildBondExecuteWithdrawIxV22(m, user, ata, vault, 123n, 1), buildRescueDepositIxV22(m, user, ata, ata, vault, 100_000_000n, 90_000_000n),
  buildInitInsuranceUnitsIxV22(m, keeper), buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Propose, 0n),
  buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Draw, 2n ** 128n - 1n), buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Restore, 55n),
  buildSettleHoldingRentIxV22(m, keeper, portfolio, 3, 123_456_789n), buildSweepBandDustLegIxV22(m, keeper, portfolio, 2), buildEvictAndTradeCpiIxV22(tradeCpi(), victim),
];

type Out = { ok: true; v: unknown } | { ok: false; e: string };
const run = (f: () => unknown): Out => {
  try { return { ok: true, v: f() }; } catch (e) { return { ok: false, e: `${(e as Error)?.name}: ${(e as Error)?.message}` }; }
};
// toEqual deep-compares bigint and Uint8Array too, so any output or error difference fails the test
// Fast path: node's isDeepStrictEqual (strictly stricter than toEqual, ~10x cheaper than expect().toEqual); only a strict
// mismatch falls through to toEqual, so pass/fail semantics and failure diffs are unchanged.
const same = (a: Out, b: Out): void => { if (!isDeepStrictEqual(a, b)) expect(b).toEqual(a); };

function both(parsed: unknown, enhanced: unknown, ids: string[]): void {
  const set = new Set(ids);
  for (const [a, b] of [[old, cur]] as const) {
    const ra = run(() => a.rawInstructionsFromParsedTx(parsed as never, ids)), rb = run(() => b.rawInstructionsFromParsedTx(parsed as never, ids));
    same(ra, rb);
    if (ra.ok) same(run(() => a.decodeV22Events(ra.v as never, ctx)), run(() => b.decodeV22Events(rb.ok ? (rb.v as never) : [], ctx)));
    const ea = run(() => a.rawInstructionsFromEnhancedTx(enhanced as never, set)), eb = run(() => b.rawInstructionsFromEnhancedTx(enhanced as never, set));
    same(ea, eb);
    if (ea.ok) same(run(() => a.decodeV22Events(ea.v as never, ctx)), run(() => b.decodeV22Events(eb.ok ? (eb.v as never) : [], ctx)));
  }
}

describe("v22Events differential (old vs new)", () => {
  it("every SDK-built fixture, top-level and inner, both shapes", () => {
    for (const ix of fixtures) {
      both(parsedTx([ix]), enhancedTx([ix]), [P]);
      both(parsedTx([fixtures[0]], { inner: [ix] }), enhancedTx([fixtures[0]], { nested: [ix] }), [P]);
      both(parsedTx([ix], { err: { InstructionError: [0, "x"] } }), enhancedTx([ix]), [P]);
      both(parsedTx([ix]), enhancedTx([ix]), []);
      const out = cur.decodeV22Events(cur.rawInstructionsFromParsedTx(parsedTx([ix]) as never, [P]), ctx);
      expect(out.length).toBeGreaterThan(0); // the fixture really is decoded (not a vacuous pass)
    }
  });

  it("2,000 seeded random / malformed inputs: deep-equal output, identical errors", () => {
    let s = 0x9e3779b9;
    const rnd = (): number => { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 2 ** 32; };
    const ri = (n: number): number => Math.floor(rnd() * n);
    const pick = <T,>(xs: readonly T[]): T => xs[ri(xs.length)];
    const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    const keys = [P, market.toBase58(), user.toBase58(), victim.toBase58(), taker.toBase58()];
    const tags = [106, 108, 109, 110, 111, 112, 116, 118, 119, 105, 0, 255, 21, 10];

    const randData = (): unknown => {
      const r = rnd();
      if (r < 0.45) { // a fixture, possibly mutated
        const base = new Uint8Array(pick(fixtures).data);
        const mode = ri(5);
        if (mode === 1 && base.length > 1) return encodeBase58(base.subarray(0, ri(base.length)));
        if (mode === 2) base[ri(base.length)] = ri(256);
        if (mode === 3) base[0] = pick(tags);
        if (mode === 4) return encodeBase58(Uint8Array.from([...base, ...Array.from({ length: ri(8) }, () => ri(256))]));
        return encodeBase58(base);
      }
      if (r < 0.65) { const b = Uint8Array.from({ length: ri(40) }, () => ri(256)); if (b.length) b[0] = pick(tags); return encodeBase58(b); }
      if (r < 0.75) return Array.from({ length: ri(30) }, () => rnd() < 0.1 ? "1" : pick([...B58])).join(""); // raw base58 text, leading 1s
      if (r < 0.85) return Array.from({ length: ri(12) }, () => pick(["0", "O", "I", "l", "+", "/", " ", "é", "😀", ...B58])).join(""); // invalid chars
      return pick([undefined, null, "", 0, 5, 1n, true, {}, [], ["a"], { length: 3 }, 12345]);
    };
    const randKey = (): unknown => {
      const r = rnd();
      if (r < 0.7) return pick(keys);
      return pick(["", null, undefined, { toBase58: () => "viaMethod" }, { pubkey: pick(keys) }, { pubkey: { pubkey: "deep" } }, { pubkey: null }, 0, 7, {}, { toBase58: () => { throw new Error("boom"); } }]);
    };
    const randIx = (): unknown => {
      const r = rnd();
      if (r < 0.05) return pick([null, undefined, 0, "str", [], { parsed: {}, programId: P }]);
      const o: Record<string, unknown> = { programId: rnd() < 0.85 ? pick(keys) : pick([null, undefined, "", { toBase58: () => P }, 3]), data: randData() };
      if (rnd() < 0.95) o.accounts = rnd() < 0.9 ? Array.from({ length: ri(14) }, randKey) : pick([undefined, null, "notarray", {}]);
      return o;
    };
    const randParsed = (): unknown => {
      const r = rnd();
      if (r < 0.04) return pick([{}, { meta: null }, { meta: { err: 1 } }, { transaction: {}, meta: {} }, { meta: { innerInstructions: [{}] } }]);
      const inner = rnd() < 0.5 ? [{ index: ri(3), instructions: rnd() < 0.9 ? Array.from({ length: ri(4) }, randIx) : undefined }] : undefined;
      return {
        transaction: rnd() < 0.97 ? { message: { instructions: rnd() < 0.97 ? Array.from({ length: ri(5) }, randIx) : undefined } } : undefined,
        meta: { err: rnd() < 0.1 ? { InstructionError: [0, "x"] } : null, innerInstructions: inner },
      };
    };
    const randEnh = (): unknown => {
      if (rnd() < 0.03) return pick([{}, { instructions: null }, { instructions: [null] }]);
      const mk = (): Record<string, unknown> => ({ programId: rnd() < 0.85 ? pick(keys) : pick([undefined, "", null, 9]), data: randData(), accounts: rnd() < 0.9 ? Array.from({ length: ri(14) }, () => rnd() < 0.9 ? pick(keys) : pick(["", null, undefined, 5])) : pick([undefined, "x", {}]) });
      return {
        instructions: Array.from({ length: ri(5) }, () => ({ ...mk(), innerInstructions: rnd() < 0.4 ? Array.from({ length: ri(3) }, mk) : undefined })),
        innerInstructions: rnd() < 0.3 ? [{ instructions: Array.from({ length: ri(3) }, mk) }, {}] : undefined,
      };
    };
    // identical inputs to old and new: the objects are only read, never mutated, so sharing them is safe
    for (let i = 0; i < 2000; i++) {
      const ids = rnd() < 0.9 ? [P] : pick([[], [P, keys[1]], ["x"]]);
      both(randParsed(), randEnh(), ids);
    }
    // ~2-5 s of pure CPU alone (2,000 cases x 4 comparisons); 4-10x slower on the shared Mac mini (load 30-80), so the 10 s default is not a safe bound.
  }, 60_000);

  it("decodeV22Event on random byte strings (all tags, every length) is identical", () => {
    let s = 12345;
    const rnd = (): number => { s = (Math.imul(s, 1103515245) + 12345) >>> 0; return s / 2 ** 32; };
    const accounts = [market, user, victim, taker, portfolio, ata, vault, keeper, lp].map((k) => k.toBase58());
    for (const tag of [106, 108, 109, 110, 111, 112, 116, 118, 119, 105]) {
      for (let len = 0; len <= 40; len++) {
        const data = Uint8Array.from({ length: len }, () => Math.floor(rnd() * 256));
        if (len) data[0] = tag;
        for (const n of [0, 1, 2, 5, 9]) {
          const ix = { programId: P, accounts: accounts.slice(0, n), data, ixIndex: 1, innerIndex: -1 };
          same(run(() => old.decodeV22Event(ix, ctx)), run(() => cur.decodeV22Event(ix, ctx)));
        }
      }
    }
  });
});
