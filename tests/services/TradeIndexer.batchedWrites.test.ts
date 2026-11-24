/**
 * Poll path: the fills of a window are written in one batched upsert, not one insert per fill. Duplicates are
 * skipped, events fire only for rows really written, and a failed batch falls back to per-row inserts.
 * The second block pins the flush threshold, the final flush on a read hold, the cursor order around the final flush,
 * the per-row fallback, event order, and that a batched write counts as "DB up" for the poison-row hold.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { signedAs } from "../helpers/signedTx.js";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const txBySig = new Map<string, unknown>();
const sigLists = new Map<string, { signature: string; err: null }[]>();
const published: unknown[] = [];
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
    eventBus: { on: noop, off: noop, publish: (...a: unknown[]) => published.push(a) },
  };
});
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/tradeCursor.js", () => ({ loadTradeCursors: vi.fn(async () => new Map()), saveTradeCursor: vi.fn(async () => undefined) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async (sigs: string[]) => new Map(sigs.map((s) => [s, []]))),
}));
vi.mock("../../src/db/insertTradeRow.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/insertTradeRow.js")>()),
  insertTradeRow: vi.fn(async () => true),
  insertTradeRows: vi.fn(async (rows: { tx_signature: string; asset_index: number; leg_index: number }[]) => rows),
}));
vi.mock("../../src/services/v22LogEvents.js", () => ({ recordV22LogEvents: vi.fn(async () => ({ status: "not_applicable", rows: 0 })) }));
import { insertTradeRow, insertTradeRows } from "../../src/db/insertTradeRow.js";
import { recordV22LogEvents } from "../../src/services/v22LogEvents.js";
import { TradeIndexerPolling, MAX_WRITE_HOLDS } from "../../src/services/TradeIndexer.js";
import { encodeBase58 } from "../../src/lib/base58.js";

const pk = (s: string) => new PublicKey(s);
const full = JSON.parse(readFileSync(new URL("../fixtures/tradecpi/full.json", import.meta.url), "utf8"));
const hydrate = (i: any) => (i.parsed ? i : { ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) });
const mkTx = (n: number) => ({
  slot: 1 + n, blockTime: 1 + n,
  meta: { err: null, logMessages: [], innerInstructions: full.tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map(hydrate) })) },
  transaction: { message: { instructions: full.tx.transaction.message.instructions.map(hydrate) } },
});
const MARKET: string = full.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1];
// 2-byte counter so more than 255 distinct signatures are possible (the 200-row flush threshold needs 201)
const sigOf = (n: number) => encodeBase58(Uint8Array.from({ length: 64 }, (_, k) => (k === 0 ? 2 : k === 1 ? (n & 0xff) + 1 : k === 2 ? (n >> 8) + 1 : (k * 13) % 255 + 1)));

/** Store n transactions with signatures from+1..from+n (oldest first) and return their signatures. */
function load(n: number, from = 0) {
  const sigs = Array.from({ length: n }, (_, i) => sigOf(from + i + 1));
  sigs.forEach((s, i) => txBySig.set(s, mkTx(from + i)));
  connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);
  return sigs;
}
/** The RPC lists signatures newest-first. */
const list = (sigs: string[]) => sigLists.set(MARKET, [...sigs].reverse().map((signature) => ({ signature, err: null })));
const newIndexer = () => { const idx: any = new TradeIndexerPolling(); idx._running = true; return idx; };

async function run(n: number) {
  const sigs = load(n); list(sigs);
  const idx = newIndexer();
  await idx.indexTradesForSlab(MARKET, 100);
  return { idx, sigs };
}
const eventSigs = () => published.map((p: any) => p[2].signature);

beforeEach(() => {
  txBySig.clear(); sigLists.clear(); published.length = 0;
  vi.mocked(insertTradeRow).mockReset().mockResolvedValue(true);
  vi.mocked(insertTradeRows).mockReset().mockImplementation((async (rows: any[]) => rows) as never);
  vi.mocked(recordV22LogEvents).mockReset().mockResolvedValue({ status: "not_applicable", rows: 0 } as never);
});

describe("poll path batches its trade writes", () => {
  it("25 single-fill transactions are one upsert, oldest-first, with one event each", async () => {
    const { idx, sigs } = await run(25);
    expect(insertTradeRow).not.toHaveBeenCalled();
    expect(insertTradeRows).toHaveBeenCalledTimes(1);
    const rows = vi.mocked(insertTradeRows).mock.calls[0][0];
    expect(rows.map((r) => r.tx_signature)).toEqual(sigs);
    expect(published).toHaveLength(25);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[24]); // cursor committed after the flush
  });

  it("a leg the upsert skipped (already stored) fires no event", async () => {
    vi.mocked(insertTradeRows).mockImplementation((async (rows: any[]) => rows.slice(1)) as never);
    await run(3);
    expect(published).toHaveLength(2);
  });

  it("a failed batch falls back to one insert per row", async () => {
    vi.mocked(insertTradeRows).mockRejectedValue(new Error("boom"));
    const { idx, sigs } = await run(3);
    expect(insertTradeRow).toHaveBeenCalledTimes(3);
    expect(published).toHaveLength(3);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });
});

describe("poll path batched writes: threshold, final flush, cursor, fallback", () => {
  it("201 fills are two upserts of exactly 200 then 1, oldest-first, one event each, nothing re-sent", async () => {
    const sigs = load(201); list(sigs);
    const idx = newIndexer();
    await idx.indexTradesForSlab(MARKET, 1000);
    const calls = vi.mocked(insertTradeRows).mock.calls.map((c) => c[0].map((r) => r.tx_signature));
    expect(calls.map((c) => c.length)).toEqual([200, 1]);
    expect(calls.flat()).toEqual(sigs);
    expect(published).toHaveLength(201);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[200]);
  });

  it("a null-tx read hold still writes the fills processed before it, and holds the cursor", async () => {
    const sigs = load(3); txBySig.delete(sigs[2]); list(sigs); // newest reads as null (confirmed) -> hold
    const idx = newIndexer();
    await idx.indexTradesForSlab(MARKET, 100);
    expect(insertTradeRows).toHaveBeenCalledTimes(1);
    expect(vi.mocked(insertTradeRows).mock.calls[0][0].map((r) => r.tx_signature)).toEqual(sigs.slice(0, 2));
    expect(published).toHaveLength(2);
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
  });

  it("busy slab whose only successful writes are batched: a poison signature is skipped after MAX_WRITE_HOLDS polls", async () => {
    const all = load(3);
    const poison = all[1];
    vi.mocked(recordV22LogEvents).mockImplementation((async (i: any) =>
      i.signature === poison ? { status: "write_failed", rows: 1, slab: MARKET } : { status: "not_applicable", rows: 0 }) as never);
    list(all);
    const idx = newIndexer();
    for (let p = 1; p <= MAX_WRITE_HOLDS; p++) {
      await idx.indexTradesForSlab(MARKET, 100);
      expect(idx.lastSignature.get(MARKET)).toBeUndefined(); // held, not committed
      // a fresh fill per poll (the fake RPC ignores `until`, so every held poll re-sends the whole window through the
      // batch, which reports every row written): the batch is the only DB write that succeeds on this slab
      all.push(...load(1, 20 + p)); list(all);
    }
    await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).toBe(all[all.length - 1]);
  });

  it("failed batch + one failing fallback row: later rows still written, no event for it, cursor held", async () => {
    const sigs = load(3); list(sigs);
    vi.mocked(insertTradeRows).mockRejectedValue(new Error("batch down"));
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => { if (r.tx_signature === sigs[1]) throw new Error("row down"); return true; }) as never);
    const idx = newIndexer();
    await idx.indexTradesForSlab(MARKET, 100);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0].tx_signature)).toEqual(sigs);
    expect(eventSigs()).toEqual([sigs[0], sigs[2]]);
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
  });

  it("failed batch, the fallback reports a duplicate: no event for it, cursor committed", async () => {
    const sigs = load(3); list(sigs);
    vi.mocked(insertTradeRows).mockRejectedValue(new Error("batch down"));
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => r.tx_signature !== sigs[0]) as never);
    const idx = newIndexer();
    await idx.indexTradesForSlab(MARKET, 100);
    expect(eventSigs()).toEqual(sigs.slice(1));
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });

  it("events fire oldest-first, in the batch's own order", async () => {
    const sigs = load(5); list(sigs);
    await newIndexer().indexTradesForSlab(MARKET, 100);
    expect(eventSigs()).toEqual(sigs);
  });
});
