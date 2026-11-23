/**
 * Stored-legs prefetch decision pins (mutation run IDX-REPORT-PREFETCHDECISIONMUTSTRENGTH, 2026-11-23).
 * Default INDEXER_TX_BATCH_SIZE (10). Same harness as tests/services/TradeIndexer.storedLegsPrefetch.test.ts.
 * Kills F01 (failed batched read treated as "nothing stored"), K04 (prefetched leg_index ignored) and S02
 * (read-ahead end off by one); all three survived the whole suite at 06554cc.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { signedAs } from "../helpers/signedTx.js";
import { readFileSync } from "node:fs";
import { Keypair, PublicKey } from "@solana/web3.js";
import { IX_TAG } from "@percolatorct/sdk";
import { encodeBase58 } from "../../src/lib/base58.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
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
vi.mock("../../src/db/insertTradeRow.js", () => {
  const insertTradeRow = vi.fn(async (_row?: unknown) => true);
  const insertTradeRows = vi.fn(async (rows: unknown[]) => { const out: unknown[] = []; for (const r of rows) if (await insertTradeRow(r)) out.push(r); return out; });
  const tradeKey = (r: { tx_signature: string | null; asset_index: number; leg_index: number }) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`;
  return { insertTradeRow, insertTradeRows, tradeKey };
});
const stored = { rows: new Map<string, unknown[]>() };
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async (sigs: string[]) => new Map(sigs.map((s) => [s, stored.rows.get(s) ?? []]))),
}));
import { insertTradeRow } from "../../src/db/insertTradeRow.js";
import { fetchStoredLegs, fetchStoredLegsMany } from "../../src/db/storedLegs.js";
import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

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
const OTHER = Keypair.generate().publicKey.toBase58();
const wrapperTx = (tag: number) => ({
  slot: 1, blockTime: 1, meta: { err: null, logMessages: [], innerInstructions: [] },
  transaction: { message: { instructions: [{ programId: pk(PROGRAM), accounts: [pk(MARKET), pk(MARKET)], data: encodeBase58(Uint8Array.from([tag, 0, 0, 0])) }] } },
});
const sig = (n: string) => n.padEnd(64, "1");
const list = (...s: string[]) => s.map((signature) => ({ signature, err: null as null }));
const ctx = () => connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);

describe("stored-legs prefetch: killers for the survivors of the 2026-11-23 mutation run", () => {
  beforeEach(() => {
    txBySig.clear(); sigLists.clear(); stored.rows.clear();
    vi.mocked(fetchStoredLegsMany).mockClear(); vi.mocked(fetchStoredLegs).mockClear(); vi.mocked(insertTradeRow).mockClear();
    vi.mocked(fetchStoredLegs).mockImplementation(async () => []);
    connection.getAccountInfo.mockReset();
  });

  // F01: a failed batched read must NOT be treated as "nothing stored". The tx falls back to its own read, which
  // finds the fill stored under an older leg number (same trader/side); writing it again is a second row with a
  // different (sig, asset, leg) key, so the unique index does not stop it (volume and PnL double).
  it("a failed batched read falls back to the per-tx read; a fill stored under another leg number is not re-written", async () => {
    txBySig.set(full.sig, asWeb3(full));
    sigLists.set(MARKET, list(full.sig));
    vi.mocked(fetchStoredLegsMany).mockResolvedValueOnce(null);
    vi.mocked(fetchStoredLegs).mockResolvedValueOnce([{ slab_address: MARKET, asset_index: 0, leg_index: 5, trader: TRADER, side: "long", size: "1", is_liquidation: false }]);
    ctx();
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(fetchStoredLegsMany).toHaveBeenCalledTimes(1);
    expect(vi.mocked(fetchStoredLegs).mock.calls).toEqual([[full.sig]]);
    expect(insertTradeRow).not.toHaveBeenCalled();
  });

  // K04: the prefetched rows keep their own leg_index. Another trader's row at leg 1 of the same tx must not make
  // this tx's leg 0 look stored (if it did, the new fill is silently dropped).
  it("a stored row at another leg (other trader/side) does not mark leg 0 as stored", async () => {
    txBySig.set(full.sig, asWeb3(full));
    sigLists.set(MARKET, list(full.sig));
    stored.rows.set(full.sig, [{ slab_address: MARKET, asset_index: 0, leg_index: 1, trader: OTHER, side: "short", size: "1", is_liquidation: false }]);
    ctx();
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(fetchStoredLegs).not.toHaveBeenCalled();
    expect(insertTradeRow).toHaveBeenCalledTimes(1);
    expect(vi.mocked(insertTradeRow).mock.calls[0][0]).toMatchObject({ tx_signature: full.sig, leg_index: 0 });
  });

  // S02: the read-ahead covers exactly IN_CHUNK (100) signatures from the first batch that needs it: a 100-sig
  // window with trade txs at positions 0 and 99 costs ONE query holding all 100 signatures.
  it("a 100-signature window costs one batched read covering all 100 signatures", async () => {
    const sigs = Array.from({ length: 100 }, (_, n) => sig(`W${String(n).padStart(3, "0")}Z`));
    sigs.forEach((s, n) => txBySig.set(s, wrapperTx(n === 0 || n === 99 ? IX_TAG.TradeNoCpi : IX_TAG.KeeperCrank)));
    sigLists.set(MARKET, list(...[...sigs].reverse())); // newest-first, like the RPC
    const idx: any = new TradeIndexerPolling(); idx._running = true;
    await idx.indexTradesForSlab(MARKET, 1000);
    expect(vi.mocked(fetchStoredLegsMany).mock.calls).toEqual([[sigs]]);
  });
});
