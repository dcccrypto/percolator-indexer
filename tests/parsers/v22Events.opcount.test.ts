/**
 * Pins the v22Events speedup itself (not a timing): the instruction-data base58 decode must no longer go through the
 * slow `decodeBase58` of @percolatorct/shared (array `unshift` + `indexOf` per char). Deterministic call counts, and
 * the decoded bytes must still equal the shared function's output.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ shared: 0 }));
vi.mock("@percolatorct/shared", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    decodeBase58: (s: string) => {
      calls.shared++;
      return actual.decodeBase58(s);
    },
  };
});

import { decodeBase58 as shared } from "@percolatorct/shared";
import { rawInstructionsFromEnhancedTx, rawInstructionsFromParsedTx } from "../../src/parsers/v22Events.js";
import { encodeBase58 } from "../../src/lib/base58.js";
import { pk } from "../helpers/v22Fixtures.js";

const PROGRAM = pk().toBase58();
const payloads = Array.from({ length: 50 }, (_, i) => Uint8Array.from({ length: 40 + i }, (_, k) => (k * 7 + i) & 0xff));
payloads[0] = Uint8Array.from([0, 0, 1, 2, 3]); // leading zero bytes

describe("v22Events base58 decode path (op-count pin)", () => {
  beforeEach(() => {
    calls.shared = 0;
  });

  it("enhanced adapter: zero calls into shared decodeBase58 for string data; bytes equal shared output", () => {
    const tx = { instructions: payloads.map((p) => ({ programId: PROGRAM, accounts: [], data: encodeBase58(p) })) };
    const out = rawInstructionsFromEnhancedTx(tx, new Set([PROGRAM]));
    expect(out).toHaveLength(payloads.length);
    out.forEach((o, i) => {
      expect(Array.from(o.data)).toEqual(Array.from(payloads[i]));
      expect(Array.from(o.data)).toEqual(Array.from(shared(encodeBase58(payloads[i]))!)); // this call is counted below
    });
    expect(calls.shared).toBe(payloads.length); // only the explicit reference calls above, none from the parser
  });

  it("parsed adapter: zero calls into shared decodeBase58 for string data", () => {
    const tx = {
      transaction: { message: { instructions: payloads.map((p) => ({ programId: PROGRAM, accounts: [], data: encodeBase58(p) })) } },
      meta: { err: null, innerInstructions: [] },
    };
    const out = rawInstructionsFromParsedTx(tx, [PROGRAM]);
    expect(out).toHaveLength(payloads.length);
    expect(calls.shared).toBe(0);
  });
});
