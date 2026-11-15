/**
 * Data-loss regression (IDX-FILLLOSS): a failed fallback fill insert was log-only and the cursor still
 * committed, so the fill was never stored. The cursor is now held (capped) so the signatures are retried.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";
const captureSpy = vi.hoisted(() => vi.fn());
const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const txBySig = new Map<string, unknown>();
const sigLists = new Map<string, { signature: string; err: null }[]>();
const connection = {
  getSignaturesForAddress: vi.fn(async (pk: PublicKey, o?: any) => {
    const all = sigLists.get(pk.toBase58()) ?? [];
    if (o?.until) { const i = all.findIndex((s) => s.signature === o.until); return i < 0 ? all : all.slice(0, i); }
    return all;
  }),
  getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map((s) => txBySig.get(s) ?? null)),
  getParsedTransaction: vi.fn(async (s: string) => txBySig.get(s) ?? null),
  getAccountInfo: vi.fn(async () => null),
};
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return { ...actual, config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] }, getConnection: () => connection,
    withRetry: async (fn: () => Promise<unknown>) => fn(), createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: captureSpy, eventBus: { on: noop, off: noop, publish: noop } };
});
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/tradeCursor.js", () => ({ loadTradeCursors: vi.fn(async () => new Map()), saveTradeCursor: vi.fn(async () => undefined) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({ ...(await orig<any>()),
  fetchStoredLegs: vi.fn(async () => []), fetchStoredLegsMany: vi.fn(async (s: string[]) => new Map(s.map((x) => [x, []]))) }));
const store = new Set<string>();
vi.mock("../../src/db/insertTradeRow.js", async (orig) => ({ ...(await orig<any>()), insertTradeRow: vi.fn(), insertTradeRows: vi.fn() }));
import { insertTradeRow, insertTradeRows } from "../../src/db/insertTradeRow.js";
import { TradeIndexerPolling, MAX_WRITE_HOLDS } from "../../src/services/TradeIndexer.js";
import { encodeBase58 } from "../../src/lib/base58.js";
const pk = (s: string) => new PublicKey(s);
const full = JSON.parse(readFileSync(new URL("../fixtures/tradecpi/full.json", import.meta.url), "utf8"));
const hydrate = (i: any) => (i.parsed ? i : { ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) });
const mkTx = (n: number) => ({ slot: 1 + n, blockTime: 1 + n,
  meta: { err: null, logMessages: [], innerInstructions: full.tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map(hydrate) })) },
  transaction: { message: { instructions: full.tx.transaction.message.instructions.map(hydrate) } } });
const MARKET: string = full.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1];
const sigOf = (n: number) => encodeBase58(Uint8Array.from({ length: 64 }, (_, k) => (k === 0 ? 1 : k === 1 ? n + 1 : (k * 13) % 255 + 1)));
const key = (r: any) => r.tx_signature;

const setup = () => {
  const sigs = [1, 2, 3].map(sigOf);
  sigs.forEach((s, i) => txBySig.set(s, mkTx(i)));
  sigLists.set(MARKET, [...sigs].reverse().map((signature) => ({ signature, err: null })));
  connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);
  vi.mocked(insertTradeRows).mockRejectedValue(new Error("batch down"));
  const idx: any = new TradeIndexerPolling(); idx._running = true;
  return { sigs, idx };
};

describe("poll path: a failed fill write must not lose the fill", () => {
  beforeEach(() => { store.clear(); vi.clearAllMocks(); captureSpy.mockClear(); });

  it("fill 2 fails once: cursor is held, next poll stores it", async () => {
    const { sigs, idx } = setup();
    let failed = false;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1] && !failed) { failed = true; throw new Error("transient db"); }
      store.add(key(r)); return true; }) as never);
    await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).not.toBe(sigs[2]);
    await idx.indexTradesForSlab(MARKET, 100);
    expect(sigs.map((s) => store.has(s))).toEqual([true, true, true]);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });

  it("hold cap: a poison row holds the cursor MAX_WRITE_HOLDS polls, then the indexer moves on", async () => {
    const { sigs, idx } = setup();
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1]) throw new Error("poison row");
      store.add(key(r)); return true; }) as never);
    for (let p = 1; p <= MAX_WRITE_HOLDS; p++) {
      await idx.indexTradesForSlab(MARKET, 100);
      expect(idx.lastSignature.get(MARKET)).not.toBe(sigs[2]);
    }
    await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
    expect(captureSpy).toHaveBeenCalledTimes(1);
    expect(store.has(sigs[1])).toBe(false);
    expect(store.has(sigs[0]) && store.has(sigs[2])).toBe(true);
  });

  it("a clean poll resets the hold count (no alert after fail, succeed, fail)", async () => {
    const { sigs, idx } = setup();
    let bad = true;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1] && bad) throw new Error("db");
      store.add(key(r)); return true; }) as never);
    for (let p = 0; p < MAX_WRITE_HOLDS; p++) await idx.indexTradesForSlab(MARKET, 100);
    bad = false;
    await idx.indexTradesForSlab(MARKET, 100);
    bad = true; store.clear();
    idx.lastSignature.delete(MARKET);
    for (let p = 0; p < MAX_WRITE_HOLDS; p++) await idx.indexTradesForSlab(MARKET, 100);
    expect(captureSpy).not.toHaveBeenCalled();
    expect(idx.lastSignature.get(MARKET)).not.toBe(sigs[2]);
  });
});
