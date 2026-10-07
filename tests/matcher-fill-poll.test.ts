/** #213 / #221 on the poll path (TradeIndexerPolling.processTransaction), real transactions. */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const getAccountInfo = vi.fn();
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, getConnection: vi.fn(() => ({ getAccountInfo })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
import { insertTradeRow } from "../src/db/insertTradeRow.js";
import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";

const fx = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const pk = (s: string) => new PublicKey(s);
function asWeb3(f: any) {
  const tx = f.tx;
  return {
    slot: tx.slot, blockTime: tx.blockTime,
    meta: { err: null, logMessages: [], innerInstructions: tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map((i: any) => ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) })) },
    transaction: { message: { instructions: tx.transaction.message.instructions.map((i: any) => i.parsed ? i : ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) } },
  };
}
const market = (f: any) => f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1] as string;

async function poll(f: any) {
  const idx: any = new TradeIndexerPolling();
  return idx.processTransaction(asWeb3(f), f.sig, market(f), new Set([PROGRAM]));
}

describe("poll path: TradeCpi rows", () => {
  beforeEach(() => { vi.mocked(insertTradeRow).mockClear(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("partial fill: executed size, booked price, fee on the executed notional", async () => {
    const f = fx("partial");
    getAccountInfo.mockResolvedValue({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f)).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0])).toEqual([expect.objectContaining({ size: "483", price: 13.861751, side: "short", leg_index: 0 })]);
    expect(getAccountInfo).toHaveBeenCalledTimes(1);
  });

  it("zero fill: no row, no RPC", async () => {
    expect(await poll(fx("zeroNoMatcherCall"))).toBe(false);
    expect(insertTradeRow).not.toHaveBeenCalled();
    expect(getAccountInfo).not.toHaveBeenCalled();
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("#213 tx, context since overwritten: matcher-requested size at the booked price, not recorded as skipped", async () => {
    const f = fx("issue213Partial");
    getAccountInfo.mockResolvedValue({ data: Buffer.from(fx("full").ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f)).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0])).toEqual([expect.objectContaining({ size: "822500", price: 121.580511 })]);
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("strict mode: no row, signature recorded", async () => {
    process.env.TRADECPI_UNVERIFIED_SIZE = "skip";
    try {
      getAccountInfo.mockResolvedValue(null);
      expect(await poll(fx("issue213Partial"))).toBe(false);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: fx("issue213Partial").sig })]);
    } finally { delete process.env.TRADECPI_UNVERIFIED_SIZE; }
  });
});
