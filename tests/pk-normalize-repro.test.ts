/**
 * Regression test (BUG-1).
 * ALL_PROGRAM_IDS is split on "," but never trimmed (@percolatorct/shared getProgramIds()), unlike
 * MARKETS_FILTER and INDEXER_BLOCKED_SLABS, which both trim. The same program id then arrives from two sources:
 * the env entry " B" and the Helius/RPC payload's "B".
 * The env must be set before @percolatorct/shared is first evaluated: it is an external dep, so vi.resetModules()
 * does not re-run it. Hence vi.hoisted.
 */
import { describe, it, expect, vi } from "vitest";
const { A, B } = vi.hoisted(() => {
  const A = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
  const B = "7RXTVmGcJMDqqTCFu5ADQRyLDvVZBi3r5U5WXzoULHJV";
  process.env.ALL_PROGRAM_IDS = `${A}, ${B}`; // human-formatted list: space after the comma
  return { A, B };
});
import { PublicKey } from "@solana/web3.js";
import { getProgramIds } from "../src/lib/programIds.js";
import { rawInstructionsFromEnhancedTx } from "../src/parsers/v22Events.js";

describe("ALL_PROGRAM_IDS='A, B'", () => {
  it("getProgramIds() (via getProgramIds) is the two canonical ids", () => {
    expect(getProgramIds()).toEqual([A, B]);
  });

  it("webhook: a Helius ix on program B is recognised (webhook.ts:50 PROGRAM_IDS -> :569/:596/:733)", () => {
    const PROGRAM_IDS = new Set(getProgramIds()); // exactly webhook.ts:50
    const tx = { instructions: [{ programId: B, accounts: [], data: "2" }] }; // "2" = base58 [0x01]
    expect(rawInstructionsFromEnhancedTx(tx, PROGRAM_IDS)).toHaveLength(1);
  });

  it("poller: a parsed-tx ix on program B passes the program filter (TradeIndexer.ts:426 -> :863/:944/:1347)", () => {
    const programIds = new Set(getProgramIds()); // exactly TradeIndexer.ts:426
    expect(programIds.has(new PublicKey(B).toBase58())).toBe(true);
  });

  it("discovery: every program id builds a PublicKey (MarketDiscovery.ts:104/167/174)", () => {
    for (const id of getProgramIds()) expect(() => new PublicKey(id)).not.toThrow();
  });
});
