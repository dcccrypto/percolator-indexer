import { describe, it, expect, vi } from "vitest";
import { Keypair } from "@solana/web3.js";
import { parsePercolatorFills } from "../../src/parsers/percolatorTxParser.js";

// Pubkeys are re-encoded to base58 on every toBase58() call; the parser memoises them by raw bytes.
describe("parsePercolatorFills pubkey encoding", () => {
  it("encodes a repeated program id once, and still compares the right string", () => {
    const wrapper = Keypair.generate().publicKey;
    const other = Keypair.generate().publicKey;
    const mkTx = () => ({
      transaction: {
        message: {
          instructions: [
            { programId: other, accounts: [], data: "1" },
            { programId: wrapper, accounts: [], data: "" },
          ],
        },
      },
      meta: { err: null, logMessages: [], innerInstructions: [] },
    });
    const spy = vi.spyOn(wrapper.constructor.prototype as { toBase58(): string }, "toBase58");
    try {
      expect(parsePercolatorFills(mkTx(), "sig1", [wrapper.toBase58()])).toEqual([]);
      const first = spy.mock.calls.length;
      expect(parsePercolatorFills(mkTx(), "sig2", [wrapper.toBase58()])).toEqual([]);
      // Second parse of the same accounts: only the test's own wrapper.toBase58() call, none from the parser.
      expect(spy.mock.calls.length - first).toBe(1);
    } finally {
      spy.mockRestore();
    }
  });
});
