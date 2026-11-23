/**
 * Stored-legs read-ahead pin (mutation run IDX-REPORT-PREFETCHDECISIONMUTSTRENGTH, 2026-11-23). INDEXER_TX_BATCH_SIZE=30, which does not divide
 * IN_CHUNK=100, so a batch straddles the read-ahead horizon. Kills K02 (cache check on batch[0] only) and S02
 * (read-ahead end off by one); both survive the whole suite at 06554cc, where every test runs batch size 10.
 * Needs its own file: TX_BATCH_SIZE is read once at import.
 */
import { describe, it, expect, vi, afterAll } from "vitest";
const prevBatchSize = vi.hoisted(() => { const prev = process.env.INDEXER_TX_BATCH_SIZE; process.env.INDEXER_TX_BATCH_SIZE = "30"; return prev; });
afterAll(() => {
  if (prevBatchSize === undefined) delete process.env.INDEXER_TX_BATCH_SIZE;
  else process.env.INDEXER_TX_BATCH_SIZE = prevBatchSize;
});
import { signedAs } from "../helpers/signedTx.js";
import { PublicKey } from "@solana/web3.js";
import { IX_TAG } from "@percolatorct/sdk";
import { encodeBase58 } from "../../src/lib/base58.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const MARKET = new PublicKey(new Uint8Array(32).fill(7)).toBase58();
const txBySig = new Map<string, unknown>();
const sigLists = new Map<string, { signature: string; err: null }[]>();
const connection = {
  getSignaturesForAddress: vi.fn(async (pk: PublicKey) => sigLists.get(pk.toBase58()) ?? []),
  getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map((s) => signedAs(s, txBySig.get(s) ?? null))),
  getParsedTransaction: vi.fn(async (s: string) => signedAs(s, txBySig.get(s) ?? null)),
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
vi.mock("../../src/db/insertTradeRow.js", () => ({
  insertTradeRow: vi.fn(async () => true),
  insertTradeRows: vi.fn(async (rows: unknown[]) => rows),
  tradeKey: (r: { tx_signature: string | null; asset_index: number; leg_index: number }) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`,
}));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async (sigs: string[]) => new Map(sigs.map((s) => [s, []]))),
}));
import { fetchStoredLegsMany } from "../../src/db/storedLegs.js";
import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

const pk = (s: string) => new PublicKey(s);
const wrapperTx = (tag: number) => ({
  slot: 1, blockTime: 1, meta: { err: null, logMessages: [], innerInstructions: [] },
  transaction: { message: { instructions: [{ programId: pk(PROGRAM), accounts: [pk(MARKET), pk(MARKET)], data: encodeBase58(Uint8Array.from([tag, 0, 0, 0])) }] } },
});
const sig = (n: string) => n.padEnd(64, "1");

describe("stored-legs read-ahead when the tx batch straddles IN_CHUNK (batch size 30)", () => {
  it("130 sigs, trade txs at 0, 95 and 105: two reads, [0..99] then exactly [100..129]", async () => {
    const sigs = Array.from({ length: 130 }, (_, n) => sig(`X${String(n).padStart(3, "0")}Z`));
    sigs.forEach((s, n) => txBySig.set(s, wrapperTx([0, 95, 105].includes(n) ? IX_TAG.TradeNoCpi : IX_TAG.KeeperCrank)));
    sigLists.set(MARKET, [...sigs].reverse().map((signature) => ({ signature, err: null as null })));
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 1000);
    expect(vi.mocked(fetchStoredLegsMany).mock.calls).toEqual([[sigs.slice(0, 100)], [sigs.slice(100)]]);
  });
});
