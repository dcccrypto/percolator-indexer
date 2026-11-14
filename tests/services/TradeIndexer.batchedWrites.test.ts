/**
 * Poll path: the fills of a window are written in one batched upsert, not one insert per fill. Duplicates are
 * skipped, events fire only for rows really written, and a failed batch falls back to per-row inserts.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const txBySig = new Map<string, unknown>();
const sigLists = new Map<string, { signature: string; err: null }[]>();
const published: unknown[] = [];
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
import { insertTradeRow, insertTradeRows } from "../../src/db/insertTradeRow.js";
import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";
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
const sigOf = (n: number) => encodeBase58(Uint8Array.from({ length: 64 }, (_, k) => (k === 0 ? 1 : k === 1 ? n + 1 : (k * 13) % 255 + 1)));

async function run(n: number) {
  const sigs = Array.from({ length: n }, (_, i) => sigOf(i + 1));
  sigs.forEach((s, i) => txBySig.set(s, mkTx(i)));
  sigLists.set(MARKET, sigs.map((signature) => ({ signature, err: null })).reverse());
  connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);
  const idx: any = new TradeIndexerPolling(); idx._running = true;
  await idx.indexTradesForSlab(MARKET, 100);
  return { idx, sigs };
}

describe("poll path batches its trade writes", () => {
  beforeEach(() => {
    txBySig.clear(); sigLists.clear(); published.length = 0;
    vi.mocked(insertTradeRow).mockReset().mockResolvedValue(true);
    vi.mocked(insertTradeRows).mockReset().mockImplementation((async (rows: any[]) => rows) as never);
  });

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
