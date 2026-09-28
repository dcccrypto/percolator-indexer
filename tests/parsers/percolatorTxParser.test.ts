import { describe, it, expect } from "vitest";
import { PublicKey } from "@solana/web3.js";
import {
  IX_TAG,
  encodeTradeNoCpi,
  encodeTradeCpi,
  encodeBatchTradeNoCpi,
  encodeBatchTradeCpi,
} from "@percolatorct/sdk";
import { parsePercolatorFills, decodeV18SingleFill, decodeV18BatchLegs } from "../../src/parsers/percolatorTxParser.js";

const PERC = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";
const TRADER = "11111111111111111111111111111111";

/** Tiny base58 encoder (avoids pulling bs58 as a new dep — mirrors decodeBase58's alphabet). */
function encodeBase58(bytes: Uint8Array): string {
  const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  // Count leading zeros.
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;

  // Convert base256 to base58.
  const digits: number[] = [];
  for (let i = zeros; i < bytes.length; i++) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8;
      digits[j] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }

  let out = "";
  for (let i = 0; i < zeros; i++) out += "1";
  for (let i = digits.length - 1; i >= 0; i--) out += ALPHABET[digits[i]];
  return out;
}

/**
 * v18 fixture builders — these wrap the REAL SDK encoders (encodeTradeNoCpi /
 * encodeTradeCpi / encodeBatchTradeNoCpi / encodeBatchTradeCpi) rather than
 * hand-rolling instruction bytes. This anchors every fixture to the SDK's own
 * ground truth, so a parser/encoder offset drift fails here instead of only
 * showing up against a real on-chain v18 TradeCpi (see also the empirical
 * decode proof against v18-wire.ts's live devnet reads).
 */
function tradeNoCpiIxData(sizeQ: bigint, assetIndex = 0): string {
  const bytes = encodeTradeNoCpi({
    accountAPortfolioId: 1n,
    accountAPositionEpoch: 0n,
    accountBPortfolioId: 2n,
    accountBPositionEpoch: 0n,
    assetIndex,
    marketId: 9n,
    sizeQ,
    execPrice: 50_000_000_000n,
    feeBps: 30n,
    backingFeeCapBps: 0,
  });
  expect(bytes.length).toBe(77); // TradeNoCpi is fixed-size — a length drift is a wire break
  return encodeBase58(bytes);
}

function tradeCpiIxData(sizeQ: bigint, assetIndex = 0): string {
  const bytes = encodeTradeCpi({
    accountAPortfolioId: 1n,
    accountAPositionEpoch: 0n,
    accountBPortfolioId: 2n,
    accountBPositionEpoch: 0n,
    accountBMatcherSequence: 7n,
    assetIndex,
    marketId: 9n,
    sizeQ,
    feeBps: 30n,
    limitPrice: 51_000_000_000n,
    backingFeeCapBps: 0,
  });
  expect(bytes.length).toBe(85); // TradeCpi is fixed-size — a length drift is a wire break
  return encodeBase58(bytes);
}

function batchTradeNoCpiIxData(legs: Array<{ assetIndex: number; sizeQ: bigint }>): string {
  const bytes = encodeBatchTradeNoCpi({
    legs: legs.map((l) => ({
      assetIndex: l.assetIndex,
      marketId: 9n,
      sizeQ: l.sizeQ,
      execPrice: 50_000_000_000n,
      feeBps: 30n,
    })),
    accountAPortfolioId: 1n,
    accountAPositionEpoch: 0n,
    accountBPortfolioId: 2n,
    accountBPositionEpoch: 0n,
  });
  expect(bytes.length).toBe(2 + legs.length * 42 + 32); // header + N legs*42B + 32B trailer
  return encodeBase58(bytes);
}

function batchTradeCpiIxData(legs: Array<{ assetIndex: number; sizeQ: bigint }>): string {
  const bytes = encodeBatchTradeCpi({
    legs: legs.map((l) => ({
      assetIndex: l.assetIndex,
      marketId: 9n,
      sizeQ: l.sizeQ,
      feeBps: 30n,
      limitPrice: 51_000_000_000n,
    })),
    maxSlippageAtoms: 0n,
    maxFeeAtoms: 0n,
    accountAPortfolioId: 1n,
    accountAPositionEpoch: 0n,
    accountBPortfolioId: 2n,
    accountBPositionEpoch: 0n,
    accountBMatcherSequence: 7n,
  });
  expect(bytes.length).toBe(2 + legs.length * 42 + 72); // header + N legs*42B + 72B trailer
  return encodeBase58(bytes);
}

describe("decodeV18SingleFill — byte-exact against the SDK encoders", () => {
  it("round-trips TradeNoCpi (tag 6, 77B) — asset_index@33, size_q@43", () => {
    const bytes = encodeTradeNoCpi({
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
      assetIndex: 2,
      marketId: 9n,
      sizeQ: 1_000_000n,
      execPrice: 50_000_000_000n,
      feeBps: 30n,
      backingFeeCapBps: 0,
    });
    const decoded = decodeV18SingleFill(IX_TAG.TradeNoCpi, bytes);
    expect(decoded).toEqual({ assetIndex: 2, sizeValue: 1_000_000n, side: "long" });
  });

  it("round-trips TradeCpi (tag 10, 85B) — asset_index@41, size_q@51, negative size = short", () => {
    const bytes = encodeTradeCpi({
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
      accountBMatcherSequence: 7n,
      assetIndex: 5,
      marketId: 9n,
      sizeQ: -500_000n,
      feeBps: 30n,
      limitPrice: 51_000_000_000n,
      backingFeeCapBps: 0,
    });
    const decoded = decodeV18SingleFill(IX_TAG.TradeCpi, bytes);
    expect(decoded).toEqual({ assetIndex: 5, sizeValue: 500_000n, side: "short" });
  });

  it("returns null for a zero-size fill", () => {
    const bytes = encodeTradeNoCpi({
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
      assetIndex: 0,
      marketId: 9n,
      sizeQ: 0n,
      execPrice: 0n,
      feeBps: 0n,
      backingFeeCapBps: 0,
    });
    expect(decodeV18SingleFill(IX_TAG.TradeNoCpi, bytes)).toBeNull();
  });

  it("does NOT misread a TradeCpi buffer using TradeNoCpi's offsets (the two tags diverge in v18)", () => {
    // TradeCpi's asset_index(41)/size_q(51) sit 8 bytes later than TradeNoCpi's
    // (33/43) because of the extra accountBMatcherSequence field. Decoding a
    // TradeCpi buffer with the TradeNoCpi tag must NOT silently succeed.
    const cpiBytes = encodeTradeCpi({
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
      accountBMatcherSequence: 7n,
      assetIndex: 5,
      marketId: 9n,
      sizeQ: 1_000_000n,
      feeBps: 30n,
      limitPrice: 51_000_000_000n,
      backingFeeCapBps: 0,
    });
    const wrongTagDecode = decodeV18SingleFill(IX_TAG.TradeNoCpi, cpiBytes);
    // TradeNoCpi's asset_index offset (33) inside a TradeCpi buffer reads part
    // of accountBMatcherSequence/marketId, not the real assetIndex (5).
    expect(wrongTagDecode?.assetIndex).not.toBe(5);
  });
});

describe("decodeV18BatchLegs — byte-exact against the SDK encoders", () => {
  it("decodes BatchTradeNoCpi legs (42B/leg, market_id inserted before size_q)", () => {
    const bytes = encodeBatchTradeNoCpi({
      legs: [
        { assetIndex: 0, marketId: 1n, sizeQ: 100n, execPrice: 50_000_000_000n, feeBps: 30n },
        { assetIndex: 1, marketId: 2n, sizeQ: -200n, execPrice: 40_000_000_000n, feeBps: 30n },
      ],
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
    });
    const legs = decodeV18BatchLegs(bytes);
    expect(legs).toEqual([
      { assetIndex: 0, sizeValue: 100n, side: "long", legIndex: 0 },
      { assetIndex: 1, sizeValue: 200n, side: "short", legIndex: 1 },
    ]);
  });

  it("decodes BatchTradeCpi legs (42B/leg, feeBps+limitPrice trailer)", () => {
    const bytes = encodeBatchTradeCpi({
      legs: [{ assetIndex: 3, marketId: 4n, sizeQ: 999n, feeBps: 30n, limitPrice: 51_000_000_000n }],
      maxSlippageAtoms: 0n,
      maxFeeAtoms: 0n,
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
      accountBMatcherSequence: 7n,
    });
    const legs = decodeV18BatchLegs(bytes);
    expect(legs).toEqual([{ assetIndex: 3, sizeValue: 999n, side: "long", legIndex: 0 }]);
  });

  it("would misdecode under the old v17 34B/leg stride (regression guard)", () => {
    // Sanity-check that the v18 fixture is NOT accidentally parseable under the
    // retired v17 stride (34B/leg, no market_id) — if this ever passed, the v18
    // decoder constants silently regressed back to v17.
    const bytes = encodeBatchTradeNoCpi({
      legs: [{ assetIndex: 7, marketId: 1n, sizeQ: 42n, execPrice: 1n, feeBps: 1n }],
      accountAPortfolioId: 1n,
      accountAPositionEpoch: 0n,
      accountBPortfolioId: 2n,
      accountBPositionEpoch: 0n,
    });
    // Under the old (wrong) v17 34B stride, leg 0's "size" would be read from
    // bytes [2:18] — which under the v18 layout is [asset_index(2)=7][market_id
    // low 6 bytes of 8]. That is NOT 42n, proving the two layouts disagree.
    const legacyOffsetSize = new DataView(bytes.buffer, bytes.byteOffset + 2, 16);
    const legacyLow64 = legacyOffsetSize.getBigUint64(0, true);
    expect(legacyLow64).not.toBe(42n);
  });
});

describe("parsePercolatorFills", () => {
  it("extracts a fill from TradeNoCpi with asset_index (v18 wire format)", () => {
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: tradeNoCpiIxData(1_000_000n, /* assetIndex */ 2),
            },
          ],
        },
      },
      meta: {
        err: null,
        logMessages: ["Program log: mark_price=42000000000"],
      },
    };

    const fills = parsePercolatorFills(tx, "signature123", [PERC]);
    expect(fills).toHaveLength(1);
    expect(fills[0]).toMatchObject({
      signature: "signature123",
      trader: TRADER,
      programId: PERC,
      assetIndex: 2,
      sizeAbs: 1_000_000n,
      side: "long",
    });
    // Post-refactor (2026-04-20): parser NEVER pulls price from logs — the old
    // `mark_price=<n>` regex was bogus on real program output. Callers must
    // resolve price via slab state (see readMarkPriceE6).
    expect(fills[0].priceE6).toBeUndefined();
  });

  it("extracts a fill from TradeCpi with asset_index=0 (default)", () => {
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: tradeCpiIxData(500_000n, 0),
            },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
    };
    const fills = parsePercolatorFills(tx, "sig2", [PERC]);
    expect(fills).toHaveLength(1);
    expect(fills[0].assetIndex).toBe(0);
    expect(fills[0].sizeAbs).toBe(500_000n);
  });

  it("ignores log-derived prices completely (even when logs include a valid mark_price line)", () => {
    // Real program logs contain sol_log_64-style lines like `Program log: 1, 84123456, 0, 0, 0`
    // which the old fuzzy parser would grab — producing the wrong price. Verify the new
    // parser ignores logs entirely.
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: tradeCpiIxData(1_000_000n),
            },
          ],
        },
      },
      meta: {
        err: null,
        logMessages: [
          "Program log: mark_price=84000000",
          "Program log: 1, 13153290, 0, 0, 0",
        ],
      },
    };
    const fills = parsePercolatorFills(tx, "sig", [PERC]);
    expect(fills).toHaveLength(1);
    expect(fills[0].priceE6).toBeUndefined();
  });

  it("expands BatchTradeNoCpi into per-leg fills with correct assetIndex", () => {
    // Batch with 2 legs: asset 0 long 100, asset 1 long 200
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: batchTradeNoCpiIxData([
                { assetIndex: 0, sizeQ: 100n },
                { assetIndex: 1, sizeQ: 200n },
              ]),
            },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
    };
    const fills = parsePercolatorFills(tx, "batchsig", [PERC]);
    expect(fills).toHaveLength(2);
    expect(fills[0]).toMatchObject({ assetIndex: 0, sizeAbs: 100n });
    expect(fills[1]).toMatchObject({ assetIndex: 1, sizeAbs: 200n });
    // All fills share the same signature and trader
    expect(fills[0].signature).toBe("batchsig");
    expect(fills[1].signature).toBe("batchsig");
    expect(fills[0].trader).toBe(TRADER);
  });

  it("expands BatchTradeCpi legs correctly", () => {
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: batchTradeCpiIxData([{ assetIndex: 3, sizeQ: 999n }]),
            },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
    };
    const fills = parsePercolatorFills(tx, "batchcpisig", [PERC]);
    expect(fills).toHaveLength(1);
    expect(fills[0].assetIndex).toBe(3);
    expect(fills[0].sizeAbs).toBe(999n);
  });

  it("skips batch instruction when n_legs=0", () => {
    // The SDK's own encoder refuses to build a zero-leg batch instruction
    // ("at least one leg is required"), so this malformed-wire case (a
    // hand-built buffer, not encoder output) has to be constructed directly —
    // it exercises the parser's defensive handling of on-chain bytes that
    // don't match any legitimate encoder output.
    const emptyBatch = new Uint8Array([IX_TAG.BatchTradeNoCpi, 0]);
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: encodeBase58(emptyBatch),
            },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
    };
    expect(parsePercolatorFills(tx, "sig", [PERC])).toEqual([]);
  });

  it("returns empty array when tx.meta.err is set", () => {
    const tx: any = {
      transaction: { message: { instructions: [] } },
      meta: { err: "InsufficientFunds", logMessages: [] },
    };
    expect(parsePercolatorFills(tx, "sig", [PERC])).toEqual([]);
  });

  it("skips instructions for unrelated programs", () => {
    const OTHER = "11111111111111111111111111111112";
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(OTHER),
              accounts: [new PublicKey(TRADER)],
              data: tradeNoCpiIxData(100n),
            },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
    };
    expect(parsePercolatorFills(tx, "sig", [PERC])).toEqual([]);
  });

  it("skips parsed instructions (system/token)", () => {
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              parsed: { type: "transfer" },
              programId: new PublicKey(PERC),
            },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
    };
    expect(parsePercolatorFills(tx, "sig", [PERC])).toEqual([]);
  });
});
