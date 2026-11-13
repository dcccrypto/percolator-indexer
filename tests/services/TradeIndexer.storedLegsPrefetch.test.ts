/**
 * Poll path: the batched stored-legs query only covers transactions that can read stored legs (a fill or a tag 44 of
 * ours). Keeper-crank-only batches cost no DB query, and per-leg dedup still uses the batched rows.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { Keypair, PublicKey } from "@solana/web3.js";
import { IX_TAG, IX_TAG_V22 } from "@percolatorct/sdk";
import { encodeBase58 } from "../../src/lib/base58.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const txBySig = new Map<string, unknown>();
const sigLists = new Map<string, { signature: string; err: null }[]>();
const connection = {
  getSignaturesForAddress: vi.fn(async (pk: PublicKey) => sigLists.get(pk.toBase58()) ?? []),
  getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map((s) => txBySig.get(s) ?? null)),
  getParsedTransaction: vi.fn(async (s: string) => txBySig.get(s) ?? null),
  getAccountInfo: vi.fn(async () => null),
};
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] },
    getConnection: () => connection,
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: noop,
  };
});
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
const stored = { rows: new Map<string, unknown[]>() };
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async (sigs: string[]) => new Map(sigs.map((s) => [s, stored.rows.get(s) ?? []]))),
}));
import { insertTradeRow } from "../../src/db/insertTradeRow.js";
import { fetchStoredLegs, fetchStoredLegsMany } from "../../src/db/storedLegs.js";
import { TradeIndexerPolling, mayReadStoredLegs } from "../../src/services/TradeIndexer.js";

const pk = (s: string) => new PublicKey(s);
const full = JSON.parse(readFileSync(new URL("../fixtures/tradecpi/full.json", import.meta.url), "utf8"));
function asWeb3(f: any) {
  const tx = f.tx;
  return {
    slot: tx.slot, blockTime: tx.blockTime,
    meta: { err: null, logMessages: [], innerInstructions: tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map((i: any) => ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) })) },
    transaction: { message: { instructions: tx.transaction.message.instructions.map((i: any) => i.parsed ? i : ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) } },
  };
}
const tradeIx = full.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
const MARKET: string = tradeIx.accounts[1];
const TRADER: string = tradeIx.accounts[0];
const wrapperTx = (tag: number, err: unknown = null) => ({
  slot: 1, blockTime: 1, meta: { err, logMessages: [], innerInstructions: [] },
  transaction: { message: { instructions: [{ programId: pk(PROGRAM), accounts: [pk(MARKET), pk(MARKET)], data: encodeBase58(Uint8Array.from([tag, 0, 0, 0])) }] } },
});
const sig = (n: string) => n.padEnd(64, "1");
const list = (...s: string[]) => s.map((signature) => ({ signature, err: null as null }));

describe("stored-legs prefetch only for transactions that can use it", () => {
  beforeEach(() => {
    txBySig.clear(); sigLists.clear(); stored.rows.clear();
    vi.mocked(fetchStoredLegsMany).mockClear(); vi.mocked(fetchStoredLegs).mockClear(); vi.mocked(insertTradeRow).mockClear();
    connection.getAccountInfo.mockReset();
  });

  it("a batch of keeper cranks costs no stored-legs query; the cursor still commits", async () => {
    const [c1, c2] = [sig("C1"), sig("C2")];
    txBySig.set(c1, wrapperTx(IX_TAG.KeeperCrank)); txBySig.set(c2, wrapperTx(IX_TAG.KeeperCrank));
    sigLists.set(MARKET, list(c2, c1));
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(fetchStoredLegsMany).not.toHaveBeenCalled();
    expect(fetchStoredLegs).not.toHaveBeenCalled();
    expect(idx.lastSignature.get(MARKET)).toBe(c2);
  });

  it("a mixed batch queries only the trade's signature, and an already stored leg is not written again", async () => {
    const c = sig("C");
    txBySig.set(c, wrapperTx(IX_TAG.KeeperCrank)); txBySig.set(full.sig, asWeb3(full));
    sigLists.set(MARKET, list(full.sig, c));
    stored.rows.set(full.sig, [{ slab_address: MARKET, asset_index: 0, leg_index: 0, trader: TRADER, side: "long", size: "1", is_liquidation: false }]);
    connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(vi.mocked(fetchStoredLegsMany).mock.calls).toEqual([[[full.sig]]]);
    expect(fetchStoredLegs).not.toHaveBeenCalled(); // the batched rows were used
    expect(insertTradeRow).not.toHaveBeenCalled(); // already stored
    expect(idx.lastSignature.get(MARKET)).toBe(full.sig);
  });

  it("negative control: the same trade NOT stored is written", async () => {
    txBySig.set(full.sig, asWeb3(full));
    sigLists.set(MARKET, list(full.sig));
    connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(vi.mocked(fetchStoredLegsMany).mock.calls).toEqual([[[full.sig]]]);
    expect(insertTradeRow).toHaveBeenCalledTimes(1);
  });

  it("predicate: trade tags, tag 119 (wrapped TradeCpi) and tag 44 qualify; cranks, other programs and failed txs do not", () => {
    const ids = new Set([PROGRAM]);
    for (const t of [IX_TAG.TradeNoCpi, IX_TAG.TradeCpi, IX_TAG.BatchTradeNoCpi, IX_TAG.BatchTradeCpi, IX_TAG_V22.EvictAndTradeCpi, IX_TAG.RebalanceReduce]) {
      expect(mayReadStoredLegs(wrapperTx(t), ids)).toBe(true);
    }
    expect(mayReadStoredLegs(wrapperTx(IX_TAG.KeeperCrank), ids)).toBe(false);
    expect(mayReadStoredLegs(wrapperTx(IX_TAG.TradeCpi), new Set([Keypair.generate().publicKey.toBase58()]))).toBe(false);
    expect(mayReadStoredLegs(wrapperTx(IX_TAG.TradeCpi, { InstructionError: [0, "Custom"] }), ids)).toBe(false);
  });

  it("predicate over several instructions: a trade after a crank or a parsed ix qualifies; a trade only in inner instructions does not", () => {
    const ids = new Set([PROGRAM]);
    const ixOf = (tag: number) => wrapperTx(tag).transaction.message.instructions[0];
    const parsed = { programId: pk("11111111111111111111111111111111"), parsed: { type: "transfer" }, program: "system" };
    const multi = (...ixs: unknown[]) => ({ ...wrapperTx(IX_TAG.KeeperCrank), transaction: { message: { instructions: ixs } } });
    expect(mayReadStoredLegs(multi(ixOf(IX_TAG.KeeperCrank), ixOf(IX_TAG.TradeCpi)) as never, ids)).toBe(true);
    expect(mayReadStoredLegs(multi(parsed, ixOf(IX_TAG.RebalanceReduce)) as never, ids)).toBe(true);
    const innerOnly = { ...wrapperTx(IX_TAG.KeeperCrank), meta: { err: null, logMessages: [], innerInstructions: [{ index: 0, instructions: [ixOf(IX_TAG.TradeCpi)] }] } };
    expect(mayReadStoredLegs(innerOnly as never, ids)).toBe(false);
  });
});
