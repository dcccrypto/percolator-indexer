import { describe, it, expect } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { IX_TAG, encodeTradeNoCpi, encodeTradeCpi, encodeBatchTradeNoCpi, encodeBatchTradeCpi } from "@percolatorct/sdk";
import {
  parsePercolatorFills,
  parsePercolatorLiquidations,
  cpiEvidenceFromParsed,
  decodeV18SingleFill,
  decodeV18BatchLegs,
  decodeRebalanceReduce,
  rebalanceReduceFill,
  isRebalanceReduceTag,
} from "../../src/parsers/percolatorTxParser.js";

const PERC = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";
const OTHER = "11111111111111111111111111111111";
const TRADER = PublicKey.unique().toBase58();
const SLAB = PublicKey.unique().toBase58();
const MATCHER_PK = PublicKey.unique().toBase58();

function b58(bytes: Uint8Array): string {
  const A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const d: number[] = [];
  for (let i = zeros; i < bytes.length; i++) {
    let c = bytes[i];
    for (let j = 0; j < d.length; j++) {
      c += d[j] << 8;
      d[j] = c % 58;
      c = (c / 58) | 0;
    }
    while (c > 0) {
      d.push(c % 58);
      c = (c / 58) | 0;
    }
  }
  let out = "1".repeat(zeros);
  for (let i = d.length - 1; i >= 0; i--) out += A[d[i]];
  return out;
}

const noCpi = (sizeQ: bigint, feeBps = 30n) =>
  encodeTradeNoCpi({
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    assetIndex: 1, marketId: 9n, sizeQ, execPrice: 50_000_000_000n, feeBps, backingFeeCapBps: 0,
  });
const cpi = (sizeQ: bigint) =>
  encodeTradeCpi({
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    accountBMatcherSequence: 7n, assetIndex: 0, marketId: 9n, sizeQ, feeBps: 30n, limitPrice: 51_000_000_000n, backingFeeCapBps: 0,
  });
const batchNo = (sizes: bigint[]) =>
  encodeBatchTradeNoCpi({
    legs: sizes.map((s, i) => ({ assetIndex: i, marketId: 9n, sizeQ: s, execPrice: 50_000_000_000n, feeBps: 30n })),
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
  });
const batchCpi = (sizes: bigint[]) =>
  encodeBatchTradeCpi({
    legs: sizes.map((s, i) => ({ assetIndex: i, marketId: 9n, sizeQ: s, feeBps: 30n, limitPrice: 51_000_000_000n })),
    maxSlippageAtoms: 0n, maxFeeAtoms: 0n,
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    accountBMatcherSequence: 7n,
  });

function reduceBytes(q: bigint, asset = 3, tag: number = IX_TAG.RebalanceReduce): Uint8Array {
  const b = new Uint8Array(35);
  b[0] = tag;
  const dv = new DataView(b.buffer);
  dv.setUint16(17, asset, true);
  dv.setBigUint64(19, q & ((1n << 64n) - 1n), true);
  dv.setBigUint64(27, q >> 64n, true);
  return b;
}

const txOf = (ixs: any[], meta: any = { err: null }): any => ({ transaction: { message: { instructions: ixs } }, meta });

describe("decoders — edge branches", () => {
  it("decodeV18SingleFill rejects unknown tag and short buffers", () => {
    expect(decodeV18SingleFill(999, noCpi(1n))).toBeNull();
    expect(decodeV18SingleFill(IX_TAG.TradeNoCpi, noCpi(1n).slice(0, 40))).toBeNull();
  });

  it("feeBps beyond MAX_SAFE_INTEGER reads as undefined", () => {
    const d = decodeV18SingleFill(IX_TAG.TradeNoCpi, noCpi(1_000_000n, (1n << 63n)));
    expect(d?.feeBps).toBeUndefined();
  });

  it("zero exec_price reads as undefined", () => {
    const bytes = noCpi(1_000_000n);
    // exec_price @59 (u64) -> zero it
    bytes.fill(0, 59, 67);
    expect(decodeV18SingleFill(IX_TAG.TradeNoCpi, bytes)?.execPriceE6).toBeUndefined();
    expect(decodeV18SingleFill(IX_TAG.TradeCpi, cpi(1_000_000n))?.execPriceE6).toBeUndefined();
  });

  it("decodeV18BatchLegs: short header, truncated leg, zero-size leg skipped", () => {
    expect(decodeV18BatchLegs(IX_TAG.BatchTradeNoCpi, new Uint8Array(1))).toEqual([]);
    const full = batchNo([1_000_000n, 2_000_000n]);
    // truncate mid second leg -> only first leg decoded
    const trunc = full.slice(0, 2 + 42 + 10);
    expect(decodeV18BatchLegs(IX_TAG.BatchTradeNoCpi, trunc)).toHaveLength(1);
    const withZero = batchNo([0n, 5_000_000n]);
    const legs = decodeV18BatchLegs(IX_TAG.BatchTradeNoCpi, withZero);
    expect(legs).toHaveLength(1);
    expect(legs[0].legIndex).toBe(1);
  });

  it("decodeV18BatchLegs: NoCpi reads exec_price+fee, Cpi reads fee only (trailer slot swap)", () => {
    const [n] = decodeV18BatchLegs(IX_TAG.BatchTradeNoCpi, batchNo([1_000_000n]));
    expect(n.feeBps).toBe(30);
    expect(n.execPriceE6).toBe(50_000_000_000n);
    const [c] = decodeV18BatchLegs(IX_TAG.BatchTradeCpi, batchCpi([1_000_000n]));
    expect(c.feeBps).toBe(30);
    expect(c.execPriceE6).toBeUndefined();
  });

  it("decodeRebalanceReduce: wrong tag, short, zero, 128-bit value", () => {
    expect(isRebalanceReduceTag(IX_TAG.RebalanceReduce)).toBe(true);
    expect(decodeRebalanceReduce(new Uint8Array(10))).toBeNull();
    expect(decodeRebalanceReduce(reduceBytes(5n, 3, IX_TAG.TradeCpi))).toBeNull();
    expect(decodeRebalanceReduce(reduceBytes(0n))).toBeNull();
    const big = (5n << 64n) | 7n;
    expect(decodeRebalanceReduce(reduceBytes(big))).toEqual({ assetIndex: 3, reduceQ: big });
  });

  it("rebalanceReduceFill: no position, bad reduce, caps at position, side opposite", () => {
    expect(rebalanceReduceFill(5n, 0n)).toBeNull();
    expect(rebalanceReduceFill(0n, 5n)).toBeNull();
    expect(rebalanceReduceFill(10n, 4n)).toEqual({ side: "short", sizeValue: 4n });
    expect(rebalanceReduceFill(3n, -4n)).toEqual({ side: "long", sizeValue: 3n });
    expect(rebalanceReduceFill(10n, -4n)).toEqual({ side: "long", sizeValue: 4n });
  });
});

describe("parsePercolatorFills — branch coverage", () => {
  const ix = (data: Uint8Array | string, accounts: any[], programId: any = PERC) => ({
    programId, accounts, data: typeof data === "string" ? data : b58(data),
  });

  it("returns [] for missing meta, empty instructions, missing message", () => {
    expect(parsePercolatorFills({ transaction: {} } as any, "s", [PERC])).toEqual([]);
    expect(parsePercolatorFills(txOf([], { err: null }), "s", [PERC])).toEqual([]);
    expect(parsePercolatorFills({ meta: { err: null } } as any, "s", [PERC])).toEqual([]);
  });

  it("skips bad programId, undecodable data and empty data", () => {
    const fills = parsePercolatorFills(
      txOf([
        { programId: undefined, accounts: [TRADER, SLAB, SLAB], data: b58(noCpi(1_000_000n)) },
        ix("0OIl", [TRADER]), // invalid base58 chars
        ix(new Uint8Array(0), [TRADER]),
      ]),
      "s",
      [PERC],
    );
    expect(fills).toEqual([]);
  });

  it("skips non-trade tags, missing trader, and undecodable single fills", () => {
    const other = new Uint8Array([IX_TAG.PermissionlessCrank ?? 5, 0, 0]);
    const out = parsePercolatorFills(
      txOf([
        ix(other, [TRADER]),
        ix(noCpi(1_000_000n), []), // no trader
        ix(noCpi(0n), [TRADER, SLAB, SLAB]), // zero size
      ]),
      "s",
      [PERC],
    );
    expect(out).toEqual([]);
  });

  it("TradeNoCpi takes slab from accounts[2]; accounts missing -> undefined slab", () => {
    const [a] = parsePercolatorFills(txOf([ix(noCpi(1_000_000n), [TRADER, "x", SLAB])]), "s", [PERC]);
    expect(a.slabAddress).toBe(SLAB);
    expect(a.cpi).toBeUndefined();
    expect(a.feeBps).toBe(30);
    const [b] = parsePercolatorFills(txOf([ix(noCpi(1_000_000n), [TRADER])]), "s", [PERC]);
    expect(b.slabAddress).toBeUndefined();
  });

  it("TradeCpi attaches cpi evidence (unknown without inner ix) and legPos 0", () => {
    const [f] = parsePercolatorFills(txOf([ix(cpi(1_000_000n), [TRADER, SLAB])]), "s", [PERC]);
    expect(f.slabAddress).toBe(SLAB);
    expect(f.legPos).toBe(0);
    expect(f.cpi).toEqual({ kind: "unknown" });
  });

  it("BatchTradeCpi attaches batch cpi evidence per leg with legPos", () => {
    const fills = parsePercolatorFills(txOf([ix(batchCpi([1_000_000n, 2_000_000n]), [TRADER, SLAB])]), "s", [PERC]);
    expect(fills.map((f) => f.legPos)).toEqual([0, 1]);
    expect(fills[0].cpi).toEqual({ kind: "unknown", batch: true });
  });

  it("BatchTradeNoCpi has no cpi/legPos", () => {
    const fills = parsePercolatorFills(txOf([ix(batchNo([1_000_000n]), [TRADER, "x", SLAB])]), "s", [PERC]);
    expect(fills).toHaveLength(1);
    expect(fills[0].cpi).toBeUndefined();
    expect(fills[0].legPos).toBeUndefined();
  });

  it("RebalanceReduce emits a placeholder fill when owner and payload are valid", () => {
    const [f] = parsePercolatorFills(txOf([ix(reduceBytes(9n), [TRADER, SLAB])]), "s", [PERC]);
    expect(f).toMatchObject({ trader: TRADER, assetIndex: 3, sizeAbs: 9n, rebalanceReduce: true, slabAddress: SLAB });
  });

  it("RebalanceReduce with zero size or no owner is dropped", () => {
    expect(parsePercolatorFills(txOf([ix(reduceBytes(0n), [TRADER, SLAB])]), "s", [PERC])).toEqual([]);
    expect(parsePercolatorFills(txOf([ix(reduceBytes(9n), [])]), "s", [PERC])).toEqual([]);
  });

  it("accepts pubkeys as PublicKey, {pubkey} wrappers, and toBase58-only objects", () => {
    const pk = new PublicKey(TRADER);
    const wrapped = { pubkey: new PublicKey(SLAB) };
    const noBytes = { toBase58: () => SLAB };
    const badBytes = { toBase58: () => SLAB, toBytes: () => new Uint8Array(3) };
    const [a] = parsePercolatorFills(txOf([ix(noCpi(1_000_000n), [pk, 0, wrapped])]), "s", [PERC]);
    expect(a.trader).toBe(TRADER);
    expect(a.slabAddress).toBe(SLAB);
    const [b] = parsePercolatorFills(txOf([ix(noCpi(1_000_000n), [noBytes, 0, badBytes])]), "s", [PERC]);
    expect(b.trader).toBe(SLAB);
    expect(b.slabAddress).toBe(SLAB);
    // unusable key objects resolve to undefined -> no trader -> skipped
    expect(parsePercolatorFills(txOf([ix(noCpi(1_000_000n), [{}])]), "s", [PERC])).toEqual([]);
    expect(parsePercolatorFills(txOf([ix(noCpi(1_000_000n), [{ pubkey: {} }])]), "s", [PERC])).toEqual([]);
  });

  it("parseReturnData shapes do not break fill parsing (array data, non-string, bad programId)", () => {
    const base = ix(cpi(1_000_000n), [TRADER, SLAB]);
    for (const rd of [
      { programId: OTHER, data: ["AAAA", "base64"] },
      { programId: OTHER, data: 5 },
      { programId: undefined, data: "AAAA" },
      null,
    ]) {
      const out = parsePercolatorFills(txOf([base], { err: null, returnData: rd }), "s", [PERC]);
      expect(out).toHaveLength(1);
    }
  });

  it("inner instructions are matched only to their own outer index", () => {
    const out = parsePercolatorFills(
      txOf([ix(cpi(1_000_000n), [TRADER, SLAB, "a", "b", MATCHER_PK, "ctx"])], {
        err: null,
        innerInstructions: [{ index: 5, instructions: [{ programId: MATCHER_PK, data: "abc" }] }],
      }),
      "s",
      [PERC],
    );
    // matcher IS at accounts[4], but its inner call sits under another outer index -> none seen
    expect(out[0].cpi).toEqual({ kind: "no-matcher-call" });
  });
});

describe("cpiEvidenceFromParsed", () => {
  const MATCHER = PublicKey.unique().toBase58();
  const accts = [TRADER, SLAB, "a", "b", MATCHER, "ctx"];

  it("null inner groups -> unknown (batch flag follows isBatch)", () => {
    expect(cpiEvidenceFromParsed({ accounts: accts }, 0, null, null, false)).toEqual({ kind: "unknown" });
    expect(cpiEvidenceFromParsed({ accounts: accts }, 0, undefined, null, true)).toEqual({ kind: "unknown", batch: true });
  });

  it("empty inner list with a matcher account -> no-matcher-call for single", () => {
    expect(cpiEvidenceFromParsed({ accounts: accts }, 0, [], null, false)).toEqual({ kind: "no-matcher-call" });
    expect(cpiEvidenceFromParsed({ accounts: accts }, 0, [], null, true)).toEqual({ kind: "unknown", batch: true });
  });

  it("filters other indexes, missing instructions, invalid pids and non-string data", () => {
    const groups = [
      { index: 1, instructions: [{ programId: MATCHER, data: "x" }] },
      { index: 0 }, // no instructions
      { index: 0, instructions: [{ programId: undefined, data: "x" }, { programId: MATCHER, data: 5 }, null] },
    ];
    expect(cpiEvidenceFromParsed({ accounts: accts }, 0, groups as any, null, false)).toEqual({ kind: "no-matcher-call" });
  });

  it("matcher inner ix that does not decode -> unknown; uses accountsOverride and missing accounts", () => {
    const groups = [{ index: 0, instructions: [{ programId: MATCHER, data: "1" }] }];
    expect(cpiEvidenceFromParsed({ accounts: [] }, 0, groups, null, false, accts)).toEqual({ kind: "unknown" });
    // override supplies the matcher account: with no inner calls that is "no-matcher-call", not "unknown"
    expect(cpiEvidenceFromParsed({ accounts: [] }, 0, [], null, false, accts)).toEqual({ kind: "no-matcher-call" });
    expect(cpiEvidenceFromParsed({}, 0, groups, null, false)).toEqual({ kind: "unknown" });
  });
});

describe("parsePercolatorLiquidations", () => {
  const ix = { programId: PERC, accounts: [TRADER], data: b58(new Uint8Array([5, 1, 0, 0])) };

  it("returns [] for missing meta, err, or no instructions", () => {
    expect(parsePercolatorLiquidations({ transaction: {} } as any, "s", [PERC])).toEqual([]);
    expect(parsePercolatorLiquidations({ meta: { err: "x" } } as any, "s", [PERC])).toEqual([]);
    expect(parsePercolatorLiquidations(txOf([]), "s", [PERC])).toEqual([]);
  });

  it("walks instructions, skipping parsed / foreign / undecodable ones (v18: never emits a marker)", () => {
    const out = parsePercolatorLiquidations(
      txOf([
        { parsed: {}, programId: PERC },
        { programId: undefined, data: ix.data },
        { programId: OTHER, data: ix.data, accounts: [] },
        { programId: PERC, data: "0OIl" },
        { programId: PERC, data: b58(new Uint8Array(0)) },
        ix,
        { programId: PERC, data: ix.data }, // accounts omitted
      ]),
      "s",
      [PERC],
    );
    expect(out).toEqual([]);
  });
});
