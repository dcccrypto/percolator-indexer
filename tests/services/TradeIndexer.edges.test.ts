/**
 * TradeIndexer edge paths: partial tx-cache fetches (mass skip, failure, merge), liquidation markers,
 * a throwing v22 log recorder, a batched insert that fails with a non-Error, mayReadStoredLegs on empty data.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";

const m = vi.hoisted(() => ({
  fetchTolerant: vi.fn(),
  liqs: [] as unknown[],
  insertTradeRow: vi.fn(async (_r?: unknown) => true),
  insertTradeRows: vi.fn(),
  logEvents: vi.fn(async (_a?: unknown) => undefined),
}));

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] },
    getConnection: () => ({}),
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    eventBus: { on: noop, off: noop, publish: noop },
  };
});
vi.mock("../../src/lib/tolerantTxFetch.js", async (orig) => ({ ...(await orig<typeof import("../../src/lib/tolerantTxFetch.js")>()), fetchParsedTxsTolerant: m.fetchTolerant }));
vi.mock("../../src/parsers/percolatorTxParser.js", async (orig) => ({
  ...(await orig<typeof import("../../src/parsers/percolatorTxParser.js")>()),
  parsePercolatorLiquidations: () => m.liqs,
}));
vi.mock("../../src/services/v22LogEvents.js", () => ({ recordV22LogEvents: m.logEvents }));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: m.insertTradeRow, insertTradeRows: m.insertTradeRows, tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}` }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));

import { TradeIndexerPolling, mayReadStoredLegs } from "../../src/services/TradeIndexer.js";

const SLAB = "So11111111111111111111111111111111111111112";
const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const SIG = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";
const mk = (): any => new TradeIndexerPolling();
const tx = (n: number): any => ({ slot: n, blockTime: n, meta: { err: null, logMessages: [], innerInstructions: [] }, transaction: { message: { instructions: [] } } });

beforeEach(() => {
  vi.clearAllMocks();
  m.liqs = [];
  m.logEvents.mockResolvedValue(undefined);
  m.insertTradeRows.mockImplementation(async (rows: unknown[]) => rows);
  m.fetchTolerant.mockReset();
});

describe("fetchBatchCached partial paths", () => {
  it("serves cached txs and merges the fetched misses in order", async () => {
    const idx = mk();
    idx.txCache.set("a", tx(1));
    idx.txCache.set("c", tx(3));
    m.fetchTolerant.mockResolvedValueOnce({ txs: [tx(2)], skipped: [], failed: false });
    const r = await idx.fetchBatchCached({}, ["a", "b", "c"], true);
    expect(m.fetchTolerant.mock.calls[0][1]).toEqual(["b"]);
    expect(r.txs.map((t: any) => t.slot)).toEqual([1, 2, 3]);
    expect(r.failed).toBe(false);
  });

  it("everything cached: no fetch at all", async () => {
    const idx = mk();
    idx.txCache.set("a", tx(1));
    const r = await idx.fetchBatchCached({}, ["a"], true);
    expect(m.fetchTolerant).not.toHaveBeenCalled();
    expect(r.txs).toHaveLength(1);
  });

  it("a mass skip on the misses-only fetch re-fetches the whole batch", async () => {
    const idx = mk();
    idx.txCache.set("a", tx(1));
    m.fetchTolerant
      .mockResolvedValueOnce({ txs: [null], skipped: ["b"], failed: false, massSkip: true })
      .mockResolvedValueOnce({ txs: [tx(1), tx(2)], skipped: [], failed: false });
    const r = await idx.fetchBatchCached({}, ["a", "b"], true);
    expect(m.fetchTolerant.mock.calls.map((c: any[]) => c[1])).toEqual([["b"], ["a", "b"]]);
    expect(r.txs).toHaveLength(2);
  });

  it("a failed misses-only fetch fails the batch with no txs (cursor must hold)", async () => {
    const idx = mk();
    idx.txCache.set("a", tx(1));
    m.fetchTolerant.mockResolvedValueOnce({ txs: [null], skipped: [], failed: true });
    const r = await idx.fetchBatchCached({}, ["a", "b"], true);
    expect(r.failed).toBe(true);
    expect(r.txs).toEqual([null, null]);
  });

  it("not partial, or all misses: fetches the whole batch", async () => {
    const idx = mk();
    idx.txCache.set("a", tx(1));
    m.fetchTolerant.mockResolvedValue({ txs: [tx(1), tx(2)], skipped: [], failed: false });
    await idx.fetchBatchCached({}, ["a", "b"], false);
    expect(m.fetchTolerant.mock.calls[0][1]).toEqual(["a", "b"]);
    await idx.fetchBatchCached({}, ["x", "y"], true);
    expect(m.fetchTolerant.mock.calls[1][1]).toEqual(["x", "y"]);
  });
});

describe("processTransaction side paths", () => {
  const run = (t: any, pass?: any) => mk().processTransaction(t, SIG, SLAB, new Set([PROGRAM]), pass);

  it("writes a liquidation marker per liquidation of this slab only, leg 1000+i", async () => {
    m.liqs = [
      { slabAddress: SLAB, portfolio: "P1", assetIndex: 0 },
      { slabAddress: "otherslab", portfolio: "P2", assetIndex: 0 },
      { slabAddress: SLAB, portfolio: "P3", assetIndex: 1 },
    ];
    const pass: any = { earlier: new Set(), incomplete: false, pending: [], newCount: 0 };
    await run(tx(1), pass);
    expect(m.insertTradeRow.mock.calls.map((c: any[]) => [c[0].trader, c[0].leg_index, c[0].is_liquidation])).toEqual([["P1", 1000, true], ["P3", 1001, true]]);
    expect(pass.writesOk).toBe(2);
  });

  it("a failing liquidation insert marks the signature failed (cursor held), the tx still returns", async () => {
    m.liqs = [{ slabAddress: SLAB, portfolio: "P1", assetIndex: 0 }];
    m.insertTradeRow.mockRejectedValueOnce(new Error("db"));
    const pass: any = { earlier: new Set(), incomplete: false, pending: [], newCount: 0 };
    await run(tx(1), pass);
    expect(pass.incomplete).toBe(true);
    expect([...(pass.failedSigs ?? [])]).toEqual([SIG]);
  });

  it("a throwing v22 log recorder never fails the trade path", async () => {
    m.logEvents.mockRejectedValueOnce(new Error("log sink down"));
    m.liqs = [{ slabAddress: SLAB, portfolio: "P1", assetIndex: 0 }];
    await run(tx(1));
    expect(m.logEvents).toHaveBeenCalledTimes(1);
    expect(m.insertTradeRow).toHaveBeenCalledTimes(1);
  });
});

describe("flushPending", () => {
  const row = (leg: number): any => ({ slab_address: SLAB, trader: "T", side: "long", size: "1", price: 1, fee: 0, tx_signature: SIG, asset_index: 0, leg_index: leg });

  it("batch failure falls back per row: a non-Error throw marks the signature failed, a duplicate is skipped, a new row counts", async () => {
    const idx = mk();
    m.insertTradeRows.mockRejectedValueOnce(new Error("batch down"));
    m.insertTradeRow
      .mockRejectedValueOnce("string failure") // not an Error instance
      .mockResolvedValueOnce(false) // duplicate leg
      .mockResolvedValueOnce(true);
    const onNew = vi.fn();
    const pass: any = { earlier: new Set(), incomplete: false, pending: [{ row: row(0), onNew }, { row: row(1), onNew }, { row: row(2), onNew }], newCount: 0 };
    await idx.flushPending(pass);
    expect(pass.incomplete).toBe(true);
    expect([...pass.failedSigs]).toEqual([SIG]);
    expect(pass.newCount).toBe(1);
    expect(onNew).toHaveBeenCalledTimes(1);
    expect(pass.pending).toEqual([]);
  });

  it("an empty queue is a no-op", async () => {
    const pass: any = { pending: [] };
    await mk().flushPending(pass);
    await mk().flushPending({});
    expect(m.insertTradeRows).not.toHaveBeenCalled();
  });
});

describe("mayReadStoredLegs", () => {
  it("skips instructions with empty data and non-trade tags", () => {
    const pk = (s: string) => new PublicKey(s);
    const t: any = { meta: { err: null }, transaction: { message: { instructions: [
      { programId: pk(PROGRAM), accounts: [], data: "" },
      { programId: pk(PROGRAM), accounts: [], data: "2" }, // tag 1, not a trade
    ] } } };
    expect(mayReadStoredLegs(t, new Set([PROGRAM]))).toBe(false);
  });
});
