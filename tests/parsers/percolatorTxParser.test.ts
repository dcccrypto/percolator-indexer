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
 * Build a synthetic tx.transaction.message.instructions entry for a trade tx.
 * Layout: tag(1) + lpIdx(u16=2) + userIdx(u16=2) + size(i128 LE=16) = 21 bytes.
 * Only the low i64 of the size is populated — sufficient for sizes < 2^63.
 */
function makeTradeIxData(tag: number, size: bigint): string {
  const buf = new Uint8Array(21);
  const dv = new DataView(buf.buffer);
  dv.setUint8(0, tag);
  dv.setUint16(1, 0, true); // lpIdx
  dv.setUint16(3, 0, true); // userIdx
  // i128 LE — write low i64 at bytes 5..13; bytes 13..21 remain 0 (positive values only here).
  dv.setBigInt64(5, size, true);
  return encodeBase58(buf);
}

describe("parsePercolatorFills", () => {
  it("extracts a fill from TradeNoCpi", () => {
    const tx: any = {
      transaction: {
        message: {
          instructions: [
            {
              programId: new PublicKey(PERC),
              accounts: [new PublicKey(TRADER)],
              data: makeTradeIxData(IX_TAG.TradeNoCpi, 1_000_000n),
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
      sizeAbs: 1_000_000n,
      side: expect.stringMatching(/long|short/),
    });
    // Post-refactor (2026-04-20): parser NEVER pulls price from logs — the old
    // `mark_price=<n>` regex was bogus on real program output. Callers must
    // resolve price via slab state (see readMarkPriceE6).
    expect(fills[0].priceE6).toBeUndefined();
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
