/**
 * v2.2 EvictAndTradeCpi (119), end to end through the real call sites. A 119 is `[119]` + a TradeCpi body with ONE
 * victim account prepended. Built here from a REAL TradeCpi transaction (tests/fixtures/tradecpi/partial.json):
 * data[0] -> 119 and the victim prepended. The matcher call under it is untouched.
 *
 *  - parsePercolatorFills must hand the UNWRAPPED account list to cpiEvidenceFromParsed (else the matcher program is
 *    looked up one account off: the call is not found and the fill is read as a zero fill / unverified).
 *  - TradeIndexer's reduce-leg dry walk must count the 119 as a fill (else a tag 44 after it is numbered 0 instead of 1
 *    and the stored-leg check drops the 119's own fill).
 * Each assertion was proven to FAIL with its call-site fix removed (see the PR body).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";
import { decodeBase58 } from "@percolatorct/shared";
import { IX_TAG } from "@percolatorct/sdk";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const getAccountInfo = vi.fn();
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] }, getConnection: vi.fn(() => ({ getAccountInfo })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
const storedLegs: { rows: any[] | null } = { rows: [] };
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => storedLegs.rows),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
import { insertTradeRow } from "../src/db/insertTradeRow.js";
import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";
import { parsePercolatorFills } from "../src/parsers/percolatorTxParser.js";
import { encodeBase58 } from "../src/lib/base58.js";

const fx = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const pk = (s: string) => new PublicKey(s);
const VICTIM = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const isWrapper = (i: any) => i.programId === PROGRAM && i.data.length > 100;

/** The fixture tx with its wrapper instruction rewritten as an EvictAndTradeCpi (jsonParsed shape, string keys). */
function asEvict(f: any, extra: any[] = []) {
  const tx = JSON.parse(JSON.stringify(f.tx));
  const ixs = tx.transaction.message.instructions;
  const w = ixs.find(isWrapper);
  const bytes = new Uint8Array(decodeBase58(w.data)!);
  bytes[0] = 119;
  w.data = encodeBase58(bytes);
  w.accounts = [VICTIM, ...w.accounts];
  ixs.push(...extra);
  return tx;
}
function asWeb3(tx: any) {
  return {
    slot: tx.slot, blockTime: tx.blockTime,
    meta: { err: null, logMessages: [], innerInstructions: tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map((i: any) => ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) })) },
    transaction: { message: { instructions: tx.transaction.message.instructions.map((i: any) => (i.parsed ? i : { ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) } },
  };
}
const wrapperOf = (f: any) => f.tx.transaction.message.instructions.find(isWrapper);

describe("119 through parsePercolatorFills", () => {
  it("the wrapped TradeCpi's matcher call is found (cpi.kind === call) and the fill is attributed to the entrant and market, not the victim", () => {
    const f = fx("partial");
    const tx = asEvict(f);
    const fills = parsePercolatorFills({ transaction: tx.transaction, meta: { err: null, innerInstructions: tx.meta.innerInstructions, returnData: tx.meta.returnData } }, f.sig, [PROGRAM]);
    expect(fills).toHaveLength(1);
    expect(fills[0].cpi?.kind).toBe("call");
    expect(fills[0].trader).toBe(wrapperOf(f).accounts[0]);
    expect(fills[0].slabAddress).toBe(wrapperOf(f).accounts[1]);
    expect(fills[0].trader).not.toBe(VICTIM);
  });
});

describe("119 through TradeIndexer (poll path)", () => {
  beforeEach(() => { storedLegs.rows = []; vi.mocked(insertTradeRow).mockClear(); getAccountInfo.mockReset(); });
  const poll = (f: any, tx: any) => (new TradeIndexerPolling() as any).processTransaction(asWeb3(tx), f.sig, wrapperOf(f).accounts[1], new Set([PROGRAM]));

  it("matcher evidence is read from the unwrapped list: the exact executed size 483 at the booked price is written", async () => {
    const f = fx("partial");
    getAccountInfo.mockResolvedValue({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f, asEvict(f))).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0])).toEqual([expect.objectContaining({ size: "483", price: 13.861751, side: "short", leg_index: 0, trader: wrapperOf(f).accounts[0] })]);
  });

  it("reduce-leg numbering: [119, tag 44] numbers the tag 44 as leg 1, so its stored row does not hide the 119's own fill", async () => {
    const f = fx("partial");
    const trader = wrapperOf(f).accounts[0], slab = wrapperOf(f).accounts[1];
    const reduce = new Uint8Array(35);
    reduce[0] = IX_TAG.RebalanceReduce; // asset_index @17 = 0
    new DataView(reduce.buffer).setBigUint64(19, 1_000_000n, true); // reduce_q
    const reduceIx = { programId: PROGRAM, accounts: [trader, slab, VICTIM], data: encodeBase58(reduce) };
    // The tag 44's own inferred row is already stored at leg 1 (same trader, same side as the fill).
    storedLegs.rows = [{ slab_address: slab, asset_index: 0, leg_index: 1, trader, side: "short", size: "1", is_liquidation: false }];
    getAccountInfo.mockResolvedValue({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f, asEvict(f, [reduceIx]))).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0])).toEqual([expect.objectContaining({ size: "483", leg_index: 0 })]);
  });
});
