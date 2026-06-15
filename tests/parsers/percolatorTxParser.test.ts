import { describe, it, expect } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { IX_TAG } from "@percolatorct/sdk";
import { parsePercolatorFills } from "../../src/parsers/percolatorTxParser.js";

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
 * Build a synthetic v17 single-fill instruction data buffer.
 * v17 layout: tag(1) + asset_index(u16 LE=2) + size_q(i128 LE=16) = 19 bytes min.
 *
 * BREAKING CHANGE vs v12: size is at bytes [3:19], not [5:21].
 * The old v12 format was: tag(1)+lpIdx(2)+userIdx(2)+size(16) = 21 bytes.
 */
function makeTradeIxData(tag: number, size: bigint, assetIndex = 0): string {
  const buf = new Uint8Array(19);
  const dv = new DataView(buf.buffer);
  dv.setUint8(0, tag);
  dv.setUint16(1, assetIndex, true); // asset_index (u16 LE) — was lpIdx in v12
  // i128 LE — write low i64 at bytes 3..11; bytes 11..19 remain 0 (positive values only here).
  dv.setBigInt64(3, size, true);     // size_q starts at byte 3 in v17 (was byte 5 in v12)
  return encodeBase58(buf);
}

/**
 * Build a synthetic v17 batch-fill instruction data buffer.
 * v17 layout: tag(1)+n_legs(u8=1)+[asset_index(u16=2)+size_q(i128=16)+exec_price(u64=8)+fee_bps_or_limit(u64=8)]*n
 * Each leg = 34 bytes; total = 2 + n*34 bytes (matches v16_program.rs tags 66/67 and SDK encoders).
 */
function makeBatchTradeIxData(
  tag: number,
  legs: Array<{ assetIndex: number; size: bigint }>,
): string {
  const legLen = 34;
  const buf = new Uint8Array(2 + legs.length * legLen);
  const dv = new DataView(buf.buffer);
  dv.setUint8(0, tag);
  dv.setUint8(1, legs.length);
  for (let i = 0; i < legs.length; i++) {
    const off = 2 + i * legLen;
    dv.setUint16(off, legs[i].assetIndex, true);     // asset_index
    dv.setBigInt64(off + 2, legs[i].size, true);     // size_q low i64
    // bytes off+10 .. off+33: high i64 of i128 + exec_price(8) + fee_bps/limit(8) — all zero
  }
  return encodeBase58(buf);
}

describe("parsePercolatorFills", () => {
  it("extracts a fill from TradeNoCpi with asset_index (v17 wire format)", () => {
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: makeTradeIxData(IX_TAG.TradeNoCpi, 1_000_000n, /* assetIndex */ 2),
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
      assetIndex: 2,         // v17: asset_index from bytes [1:3] of instruction data
      sizeAbs: 1_000_000n,
      side: expect.stringMatching(/long|short/),
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
              data: makeTradeIxData(IX_TAG.TradeCpi, 500_000n, 0),
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
              data: makeTradeIxData(IX_TAG.TradeCpi, 1_000_000n),
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
              data: makeBatchTradeIxData(IX_TAG.BatchTradeNoCpi, [
                { assetIndex: 0, size: 100n },
                { assetIndex: 1, size: 200n },
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
              data: makeBatchTradeIxData(IX_TAG.BatchTradeCpi, [
                { assetIndex: 3, size: 999n },
              ]),
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
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              // Empty leg list
              data: makeBatchTradeIxData(IX_TAG.BatchTradeNoCpi, []),
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
              data: makeTradeIxData(IX_TAG.TradeNoCpi, 100n),
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
