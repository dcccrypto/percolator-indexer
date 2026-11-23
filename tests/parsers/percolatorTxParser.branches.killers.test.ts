/**
 * Mutation killers for src/parsers/percolatorTxParser.ts, next to percolatorTxParser.branches.test.ts.
 * Each block names the mutant id(s) it kills (mutant list: IDX-REPORT-TXPARSERBRANCHESTESTMUT). The
 * "rules" block at the end states M01/M10/M24/M27 directly, so they no longer rely only on the diff test
 * against the frozen reference copy. M34 (BatchTradeCpi evidence read from the wrong outer index) is
 * pinned on the outer envelope of a real devnet tx (tests/fixtures/tradecpi/batchLike_multi.json).
 */
import { readFileSync } from "node:fs";
import { describe, it, expect } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { IX_TAG, encodeTradeNoCpi, encodeTradeCpi, encodeBatchTradeNoCpi, encodeBatchTradeCpi } from "@percolatorct/sdk";
import { parsePercolatorFills, decodeV18BatchLegs } from "../../src/parsers/percolatorTxParser.js";

const PERC = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";
const OTHER = "11111111111111111111111111111111";
const COMPUTE_BUDGET = "ComputeBudget111111111111111111111111111111";
const TRADER = PublicKey.unique().toBase58();
const SLAB = PublicKey.unique().toBase58();
const MATCHER = PublicKey.unique().toBase58();
const CTX = PublicKey.unique().toBase58();

function b58(bytes: Uint8Array): string {
  const A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const d: number[] = [];
  for (let i = zeros; i < bytes.length; i++) {
    let c = bytes[i];
    for (let j = 0; j < d.length; j++) { c += d[j] << 8; d[j] = c % 58; c = (c / 58) | 0; }
    while (c > 0) { d.push(c % 58); c = (c / 58) | 0; }
  }
  let out = "1".repeat(zeros);
  for (let i = d.length - 1; i >= 0; i--) out += A[d[i]];
  return out;
}

const noCpi = (sizeQ: bigint, feeBps = 30n, assetIndex = 1) =>
  encodeTradeNoCpi({
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    assetIndex, marketId: 9n, sizeQ, execPrice: 50_000_000_000n, feeBps, backingFeeCapBps: 0,
  });
const cpi = (sizeQ: bigint, feeBps = 30n) =>
  encodeTradeCpi({
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    accountBMatcherSequence: 7n, assetIndex: 0, marketId: 9n, sizeQ, feeBps, limitPrice: 51_000_000_000n, backingFeeCapBps: 0,
  });
const batchNo = (legs: Array<{ s: bigint; fee?: bigint }>) =>
  encodeBatchTradeNoCpi({
    legs: legs.map((l, i) => ({ assetIndex: i, marketId: 9n, sizeQ: l.s, execPrice: 50_000_000_000n + BigInt(i), feeBps: l.fee ?? 30n })),
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
  });
const batchCpi = (legs: Array<{ s: bigint; fee?: bigint }>) =>
  encodeBatchTradeCpi({
    legs: legs.map((l, i) => ({ assetIndex: i, marketId: 9n, sizeQ: l.s, feeBps: l.fee ?? 30n, limitPrice: 51_000_000_000n + BigInt(i) })),
    maxSlippageAtoms: 0n, maxFeeAtoms: 0n,
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    accountBMatcherSequence: 7n,
  });

/** Matcher single call (tag 0, 43 B): reqId@1, asset u16@9, lp@11, oracle price@19, req i128@27. */
function matcherSingle(reqSize: bigint, priceE6: bigint): Uint8Array {
  const d = new Uint8Array(43);
  const v = new DataView(d.buffer);
  d[0] = 0;
  v.setBigUint64(1, 7n, true);
  v.setBigUint64(19, priceE6, true);
  v.setBigUint64(27, BigInt.asUintN(64, reqSize), true);
  v.setBigInt64(35, reqSize >> 64n, true);
  return d;
}
/** Matcher batch call (tag 3): n@1, reqId@2, lp@10, legs@18 x26 (asset u16, price u64@+2, req i128@+10). */
function matcherBatch(legs: Array<{ req: bigint; price: bigint }>): Uint8Array {
  const d = new Uint8Array(18 + legs.length * 26);
  const v = new DataView(d.buffer);
  d[0] = 3; d[1] = legs.length;
  v.setBigUint64(2, 7n, true);
  legs.forEach((l, i) => {
    const o = 18 + i * 26;
    v.setUint16(o, i, true);
    v.setBigUint64(o + 2, l.price, true);
    v.setBigUint64(o + 10, BigInt.asUintN(64, l.req), true);
    v.setBigInt64(o + 18, l.req >> 64n, true);
  });
  return d;
}

const ix = (data: Uint8Array, accounts: string[], programId = PERC) => ({ programId, accounts, data: b58(data) });
const txOf = (ixs: any[], meta: any = { err: null }): any => ({ transaction: { message: { instructions: ixs } }, meta });
const CPI_ACCTS = [TRADER, SLAB, "a", "b", MATCHER, CTX];
const NOCPI_ACCTS = [TRADER, "x", SLAB];

describe("KILLERS: tx-level filters (M01, M02, M03)", () => {
  it("M01: a FAILED tx carrying valid trades yields no fills (sibling test used an empty ix list)", () => {
    const ixs = [ix(noCpi(1_000_000n), NOCPI_ACCTS), ix(cpi(-2_000_000n), CPI_ACCTS), ix(batchNo([{ s: 3n }]), NOCPI_ACCTS)];
    expect(parsePercolatorFills(txOf(ixs), "s", [PERC])).toHaveLength(3); // positive control
    expect(parsePercolatorFills(txOf(ixs, { err: { InstructionError: [0, { Custom: 6 }] } }), "s", [PERC])).toEqual([]);
  });

  it("M02: identical trade bytes under a foreign program id are ignored; programIds selects", () => {
    const tx = txOf([ix(noCpi(1_000_000n), NOCPI_ACCTS, OTHER), ix(cpi(5n), CPI_ACCTS, PERC)]);
    const fills = parsePercolatorFills(tx, "s", [PERC]);
    expect(fills).toHaveLength(1);
    expect(fills[0]).toMatchObject({ programId: PERC, sizeAbs: 5n });
    expect(parsePercolatorFills(tx, "s", [OTHER]).map((f) => f.programId)).toEqual([OTHER]);
  });

  it("M03: a jsonParsed instruction is skipped even when it carries Percolator trade bytes", () => {
    const parsed = { ...ix(noCpi(1_000_000n), NOCPI_ACCTS), parsed: { type: "x" } };
    expect(parsePercolatorFills(txOf([parsed]), "s", [PERC])).toEqual([]);
  });
});

describe("KILLERS: side + i128 size for every trade variant (M04-M10)", () => {
  const SIZES: bigint[] = [
    1n, -1n, 1_000_000n, -1_000_000n,
    (1n << 64n) + 3n, -((1n << 64n) + 3n), // high word in use
    (1n << 64n) - 1n, -((1n << 64n) - 1n), // low word only, all ones
    (1n << 120n) + 5n, -((1n << 120n) + 5n),
  ];
  const abs = (s: bigint) => (s < 0n ? -s : s);
  const sideOf = (s: bigint) => (s < 0n ? "short" : "long");

  it.each(SIZES.map((s) => [s]))("TradeNoCpi / TradeCpi size %s -> exact sizeAbs + side", (s) => {
    for (const [data, accts] of [[noCpi(s), NOCPI_ACCTS], [cpi(s), CPI_ACCTS]] as const) {
      const [f] = parsePercolatorFills(txOf([ix(data, accts)]), "s", [PERC]);
      expect(f.sizeAbs).toBe(abs(s));
      expect(f.side).toBe(sideOf(s));
    }
  });

  it("BatchTradeNoCpi / BatchTradeCpi: per-leg exact sizeAbs + side across mixed signs", () => {
    const legs = SIZES.map((s) => ({ s }));
    for (const [data, accts] of [[batchNo(legs), NOCPI_ACCTS], [batchCpi(legs), CPI_ACCTS]] as const) {
      const fills = parsePercolatorFills(txOf([ix(data, accts)]), "s", [PERC]);
      expect(fills.map((f) => [f.assetIndex, f.sizeAbs, f.side])).toEqual(SIZES.map((s, i) => [i, abs(s), sideOf(s)]));
    }
  });
});

describe("KILLERS: fee_bps comes from its own leg (M17-M19, M24) and own variant (M20, M21)", () => {
  it("BatchTradeNoCpi and BatchTradeCpi legs keep distinct fees in order", () => {
    const legs = [{ s: 1n, fee: 11n }, { s: -2n, fee: 22n }, { s: 3n, fee: 33n }];
    for (const tag of [IX_TAG.BatchTradeNoCpi, IX_TAG.BatchTradeCpi]) {
      const data = tag === IX_TAG.BatchTradeNoCpi ? batchNo(legs) : batchCpi(legs);
      expect(decodeV18BatchLegs(tag, data).map((l) => l.feeBps)).toEqual([11, 22, 33]);
      const fills = parsePercolatorFills(txOf([ix(data, tag === IX_TAG.BatchTradeNoCpi ? NOCPI_ACCTS : CPI_ACCTS)]), "s", [PERC]);
      expect(fills.map((f) => f.feeBps)).toEqual([11, 22, 33]);
    }
  });

  it("single TradeNoCpi / TradeCpi carry their own non-default fee", () => {
    expect(parsePercolatorFills(txOf([ix(noCpi(1n, 7n), NOCPI_ACCTS)]), "s", [PERC])[0].feeBps).toBe(7);
    expect(parsePercolatorFills(txOf([ix(cpi(1n, 9n), CPI_ACCTS)]), "s", [PERC])[0].feeBps).toBe(9);
  });
});

describe("KILLERS: the request price never becomes the fill price (M26, M27)", () => {
  it("no variant sets priceE6 (exec_price/limit_price are caller-chosen)", () => {
    const tx = txOf([
      ix(noCpi(1n), NOCPI_ACCTS), ix(cpi(1n), CPI_ACCTS),
      ix(batchNo([{ s: 1n }, { s: 2n }]), NOCPI_ACCTS), ix(batchCpi([{ s: 1n }]), CPI_ACCTS),
    ]);
    const fills = parsePercolatorFills(tx, "s", [PERC]);
    expect(fills).toHaveLength(5);
    for (const f of fills) expect(f.priceE6).toBeUndefined();
  });
});

describe("KILLERS: inner instructions pair with their OWN outer index (M31-M34, M37)", () => {
  // A real TradeCpi tx starts with ComputeBudget ixs, so the wrapper ix is NOT at index 0.
  const cb = { programId: COMPUTE_BUDGET, accounts: [], data: b58(new Uint8Array([2, 0, 0, 0, 0])) };

  it("TradeCpi at outer index 1 reads the matcher call under inner group 1 only", () => {
    const call = { programId: MATCHER, data: b58(matcherSingle(-400n, 123_000_000n)) };
    const at = (index: number) => parsePercolatorFills(txOf([cb, ix(cpi(-500n), CPI_ACCTS)], { err: null, innerInstructions: [{ index, instructions: [call] }] }), "s", [PERC])[0].cpi;
    const ev = at(1);
    expect(ev?.kind).toBe("call");
    expect(ev?.kind === "call" && ev.call.legs[0]).toEqual({ assetIndex: 0, oraclePriceE6: 123_000_000n, reqSize: -400n });
    expect(ev?.kind === "call" && ev.matcherContext).toBe(CTX);
    expect(at(0)).toEqual({ kind: "no-matcher-call" });
    expect(at(2)).toEqual({ kind: "no-matcher-call" });
  });

  it("two TradeCpi ixs each get their own matcher call (no cross-pairing)", () => {
    const g = (index: number, req: bigint) => ({ index, instructions: [{ programId: MATCHER, data: b58(matcherSingle(req, 1_000_000n)) }] });
    const fills = parsePercolatorFills(
      txOf([cb, ix(cpi(10n), CPI_ACCTS), ix(cpi(20n), CPI_ACCTS)], { err: null, innerInstructions: [g(2, 20n), g(1, 10n)] }),
      "s", [PERC],
    );
    expect(fills.map((f) => f.cpi?.kind === "call" && f.cpi.call.legs[0].reqSize)).toEqual([10n, 20n]);
  });

  it("BatchTradeCpi at outer index 1 reads the batch call under inner group 1 (legPos per leg)", () => {
    const call = { programId: MATCHER, data: b58(matcherBatch([{ req: 5n, price: 2_000_000n }, { req: -6n, price: 3_000_000n }])) };
    const run = (index: number) => parsePercolatorFills(txOf([cb, ix(batchCpi([{ s: 5n }, { s: -6n }]), CPI_ACCTS)], { err: null, innerInstructions: [{ index, instructions: [call] }] }), "s", [PERC]);
    const fills = run(1);
    expect(fills.map((f) => f.legPos)).toEqual([0, 1]);
    for (const f of fills) expect(f.cpi?.kind).toBe("call");
    expect(run(2)[0].cpi).toEqual({ kind: "unknown", batch: true });
  });

  it("EvictAndTradeCpi (119) through parsePercolatorFills: matcher found at unwrapped [4]/[5]", () => {
    const data = cpi(-500n);
    const wrapped = new Uint8Array(data); wrapped[0] = 119;
    const VICTIM = PublicKey.unique().toBase58();
    const call = { programId: MATCHER, data: b58(matcherSingle(-500n, 1_000_000n)) };
    const [f] = parsePercolatorFills(txOf([ix(wrapped, [VICTIM, ...CPI_ACCTS])], { err: null, innerInstructions: [{ index: 0, instructions: [call] }] }), "s", [PERC]);
    expect(f).toMatchObject({ trader: TRADER, slabAddress: SLAB, sizeAbs: 500n, side: "short", legPos: 0 });
    expect(f.cpi?.kind).toBe("call");
    expect(f.cpi?.kind === "call" && f.cpi.matcherContext).toBe(CTX);
  });
});

describe("KILLERS: batch leg count (M16)", () => {
  it("every leg of an N-leg batch is returned (last leg too)", () => {
    for (const n of [1, 2, 5]) {
      const legs = Array.from({ length: n }, (_, i) => ({ s: BigInt(i + 1) }));
      expect(parsePercolatorFills(txOf([ix(batchNo(legs), NOCPI_ACCTS)]), "s", [PERC]).map((f) => f.sizeAbs)).toEqual(legs.map((l) => l.s));
    }
  });
});

describe("KILLERS: BatchTradeCpi on a real devnet tx envelope pairs each batch with its OWN matcher call (M34)", () => {
  // Real devnet tx BUuTJP8E… (wrapper ETDLAd…): 3 ComputeBudget ixs, then the wrapper ixs at outer 3/4/5, each with
  // its own inner group holding one matcher CPI (matcher EDKKgR… at accounts[4], context ADDwHG… at accounts[5]).
  // Devnet has no real BatchTradeCpi, so the wrapper bodies and matcher call bytes are swapped for batch encodings;
  // the outer layout, program ids, account lists and inner-ix envelope are the recorded ones.
  const real = JSON.parse(readFileSync(new URL("../fixtures/tradecpi/batchLike_multi.json", import.meta.url), "utf8"));
  const realIxs: any[] = real.tx.transaction.message.instructions;
  const realInner: any[] = real.tx.meta.innerInstructions;
  const WRAPPER: string = realIxs[3].programId;
  const REAL_ACCTS: string[] = realIxs[3].accounts;
  const REAL_MATCHER: string = REAL_ACCTS[4];
  const REAL_CTX: string = REAL_ACCTS[5];

  const A = [{ req: 5n, price: 2_000_000n }, { req: -6n, price: 3_000_000n }];
  const B = [{ req: -7n, price: 4_000_000n }, { req: 8n, price: 5_000_000n }, { req: 9n, price: 6_000_000n }];
  const innerAt = (index: number, legs: typeof A) => ({
    index,
    instructions: [{ ...realInner[0].instructions[0], data: b58(matcherBatch(legs)) }],
  });
  const batchIx = (legs: typeof A) => ({ ...realIxs[3], data: b58(batchCpi(legs.map((l) => ({ s: l.req })))) });
  const realTx = (innerInstructions: any[]) => ({
    ...real.tx,
    transaction: { message: { instructions: [realIxs[0], realIxs[1], realIxs[2], batchIx(A), batchIx(B)] } },
    meta: { err: null, logMessages: [], innerInstructions, returnData: null },
  });

  it("the envelope is the real one this test claims (matcher at [4] is the inner ix program)", () => {
    expect(realIxs.slice(0, 3).every((i) => i.programId === COMPUTE_BUDGET)).toBe(true);
    expect(realInner[0].index).toBe(3);
    expect(realInner[0].instructions[0].programId).toBe(REAL_MATCHER);
  });

  it("batch at outer 3 reads group 3, batch at outer 4 reads group 4: every leg gets its own call leg", () => {
    const fills = parsePercolatorFills(realTx([innerAt(4, B), innerAt(3, A)]), real.sig, [WRAPPER]);
    expect(fills.map((f) => [f.legPos, f.sizeAbs, f.side])).toEqual([
      [0, 5n, "long"], [1, 6n, "short"],
      [0, 7n, "short"], [1, 8n, "long"], [2, 9n, "long"],
    ]);
    const expected = [...A, ...B];
    fills.forEach((f, i) => {
      expect(f.cpi?.kind).toBe("call");
      if (f.cpi?.kind !== "call") return;
      expect(f.cpi.matcherContext).toBe(REAL_CTX);
      expect(f.cpi.call.batch).toBe(true);
      expect(f.cpi.call.legs).toHaveLength(i < 2 ? 2 : 3);
      expect(f.cpi.call.legs[f.legPos!]).toMatchObject({ reqSize: expected[i].req, oraclePriceE6: expected[i].price });
    });
  });

  it("a batch whose own inner group is missing is unknown, never paired with the next ix's call", () => {
    const fills = parsePercolatorFills(realTx([innerAt(4, B)]), real.sig, [WRAPPER]);
    expect(fills.slice(0, 2).map((f) => f.cpi)).toEqual([{ kind: "unknown", batch: true }, { kind: "unknown", batch: true }]);
    expect(fills.slice(2).every((f) => f.cpi?.kind === "call")).toBe(true);
  });
});

describe("RULES: stated directly, not only via the frozen-reference diff (M01, M10, M24, M27)", () => {
  it("M01 rule: a reverted tx (any truthy meta.err) stores no fill from ANY trade variant", () => {
    const variants: Array<[string, any]> = [
      ["TradeNoCpi", ix(noCpi(1_000_000n), NOCPI_ACCTS)],
      ["TradeCpi", ix(cpi(-2_000_000n), CPI_ACCTS)],
      ["BatchTradeNoCpi", ix(batchNo([{ s: 3n }, { s: -4n }]), NOCPI_ACCTS)],
      ["BatchTradeCpi", ix(batchCpi([{ s: 5n }]), CPI_ACCTS)],
    ];
    const errs = [{ InstructionError: [0, { Custom: 6 }] }, "AccountNotFound", { InsufficientFundsForRent: { account_index: 0 } }];
    for (const [name, v] of variants) {
      expect(parsePercolatorFills(txOf([v]), "s", [PERC]).length, `${name} ok tx`).toBeGreaterThan(0);
      for (const err of errs) expect(parsePercolatorFills(txOf([v], { err }), "s", [PERC]), `${name} err ${JSON.stringify(err)}`).toEqual([]);
    }
  });

  it("M10 rule: each batch leg's side is the sign of THAT leg's size_q (alternating signs, both batch kinds)", () => {
    const legs = [{ s: 1n }, { s: -2n }, { s: 3n }, { s: -4n }];
    for (const [tag, data, accts] of [
      [IX_TAG.BatchTradeNoCpi, batchNo(legs), NOCPI_ACCTS],
      [IX_TAG.BatchTradeCpi, batchCpi(legs), CPI_ACCTS],
    ] as const) {
      const fills = parsePercolatorFills(txOf([ix(data, accts)]), "s", [PERC]);
      expect(fills.map((f) => f.side)).toEqual(["long", "short", "long", "short"]);
      expect(fills.map((f) => f.side)).toEqual(decodeV18BatchLegs(tag, data).map((l) => l.side));
    }
  });

  it("M24 rule: each batch leg's feeBps is THAT leg's fee_bps, not leg 0's", () => {
    const legs = [{ s: 1n, fee: 5n }, { s: 2n, fee: 40n }, { s: -3n, fee: 100n }];
    for (const [data, accts] of [[batchNo(legs), NOCPI_ACCTS], [batchCpi(legs), CPI_ACCTS]] as const) {
      const fills = parsePercolatorFills(txOf([ix(data, accts)]), "s", [PERC]);
      fills.forEach((f, i) => expect(f.feeBps, `leg ${i}`).toBe(Number(legs[i].fee)));
    }
  });

  it("M27 rule: a batch leg never takes its caller-chosen exec_price as the fill price (priceE6 stays unset)", () => {
    const data = batchNo([{ s: 1n }, { s: -2n }, { s: 3n }]); // exec_price 50_000_000_000 + legIndex, all nonzero
    for (const f of parsePercolatorFills(txOf([ix(data, NOCPI_ACCTS)]), "s", [PERC])) expect(f.priceE6).toBeUndefined();
  });
});
