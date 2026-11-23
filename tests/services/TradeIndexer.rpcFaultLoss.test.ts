/**
 * RPC faults that lost trades / v22 events for good in TradeIndexerPolling.indexTradesForSlab (RPC fault replay,
 * J-20261120-IDX-REPORT-RPCFAULTINJECTREPLAY, measured on feat/v22-indexer 9e8e192). These failed there.
 *
 * Loss 1 (fault "nulltx"): getParsedTransactions answers null for a signature that getSignaturesForAddress listed as
 *   confirmed and successful (err null). The window loop did `if (!tx) { pass.incomplete = true; continue; }` and then
 *   committed the cursor to the window's newest signature, so the null one was never read again. Now the cursor is
 *   held (as LpVaultIndexer.syncRegistry does), bounded by MAX_NULL_TX_HOLDS polls, then recorded in skipped_signatures.
 * Loss 2 (fault "shortpage"): with a cursor, fetchSignatureWindow treated any page shorter than `limit` as "the cursor
 *   was reached". A node that cuts a page short ended the walk early; the cursor then committed to the newest, and the
 *   older remainder was never polled. Now a short page costs one extra `before` call.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { signedAs } from "../helpers/signedTx.js";

type Sig = { signature: string; err: unknown; slot: number };
let chain: Sig[] = []; // newest-first, like the RPC
let nullFor = new Set<string>(); // getParsedTransactions answers null for these (node has not got them yet)
let shortFirstPage = false; // the first page of a window is cut to half
const calls: { limit?: number; before?: string; until?: string }[] = [];

const getSignaturesForAddress = vi.fn(async (_pk: unknown, opts: { limit?: number; before?: string; until?: string } = {}) => {
  calls.push({ ...opts });
  let all = chain;
  if (opts.before) { const b = all.findIndex((s) => s.signature === opts.before); all = b >= 0 ? all.slice(b + 1) : []; }
  const cut = opts.until ? all.findIndex((s) => s.signature === opts.until) : -1;
  let page = (cut >= 0 ? all.slice(0, cut) : all).slice(0, opts.limit ?? 1000);
  if (shortFirstPage && !opts.before && page.length >= 2) page = page.slice(0, Math.floor(page.length / 2));
  return page;
});
const tx = (slot: number) => ({ slot, blockTime: 0, meta: { err: null, logMessages: [] }, transaction: { message: { instructions: [], accountKeys: [] } } });
const getParsedTransactions = vi.fn(async (sigs: string[]) => sigs.map((s) => (nullFor.has(s) ? null : signedAs(s, tx(Number(s.slice(3)))))));

vi.mock("@percolatorct/shared", () => ({
  config: { allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"] },
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getConnection: () => ({ getSignaturesForAddress, getParsedTransactions, getParsedTransaction: async () => null }),
  eventBus: { on: vi.fn(), off: vi.fn(), publish: vi.fn() },
  decodeBase58: () => null,
  withRetry: async (fn: () => Promise<unknown>) => fn(),
  captureException: vi.fn(),
}));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: async () => [],
  fetchStoredLegsMany: async () => new Map(),
}));
const recordSkippedSignatures = vi.fn(async (_rows: unknown[]) => undefined);
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (rows: unknown[]) => recordSkippedSignatures(rows) }));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

const SLAB = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const makeChain = (n: number): Sig[] => Array.from({ length: n + 1 }, (_, i) => ({ signature: `sig${n - i}`, err: null, slot: n - i }));

describe("TradeIndexerPolling: RPC faults must not lose a confirmed signature for good", () => {
  let indexer: any;
  let processed: string[];
  beforeEach(() => {
    calls.length = 0;
    recordSkippedSignatures.mockClear();
    nullFor = new Set();
    shortFirstPage = false;
    indexer = new TradeIndexerPolling();
    indexer._running = true;
    processed = [];
    vi.spyOn(indexer, "processTransaction").mockImplementation(async (...a: unknown[]) => { processed.push(a[1] as string); return false; });
  });

  it("loss 1: a confirmed signature whose transaction reads as null is processed once the node returns it", async () => {
    chain = makeChain(10);
    indexer.lastSignature.set(SLAB, "sig0");
    nullFor = new Set(["sig4"]); // listed with err null, but the answering node returns null for it this poll
    await indexer.indexTradesForSlab(SLAB);
    nullFor = new Set(); // next poll: the node has it
    chain = [{ signature: "sig11", err: null, slot: 11 }, ...chain];
    await indexer.indexTradesForSlab(SLAB);
    // 9e8e192: sig4 is never processed (the first poll committed the cursor to sig10 past it)
    expect(processed).toContain("sig4");
  });

  it("loss 1 (cursor): the cursor does not move past a confirmed signature that read as null", async () => {
    chain = makeChain(10);
    indexer.lastSignature.set(SLAB, "sig0");
    nullFor = new Set(["sig4"]);
    await indexer.indexTradesForSlab(SLAB);
    // 9e8e192: 'sig10'
    expect(indexer.lastSignature.get(SLAB)).not.toBe("sig10");
  });

  it("loss 2: a page cut short by the node does not end the window walk above the cursor", async () => {
    chain = makeChain(20); // 20 signatures above the cursor, under one page of 50
    indexer.lastSignature.set(SLAB, "sig0");
    shortFirstPage = true; // the node returns the newest 10 only
    await indexer.indexTradesForSlab(SLAB);
    shortFirstPage = false;
    await indexer.indexTradesForSlab(SLAB);
    // 9e8e192: sig1..sig10 are never processed (the walk stopped at the short page, the cursor went to sig20)
    expect([...new Set(processed)].sort()).toEqual(Array.from({ length: 20 }, (_, i) => `sig${i + 1}`).sort());
  });

  it("loss 1 (bound): a signature that never resolves holds the cursor 5 polls, then is recorded and passed", async () => {
    chain = makeChain(10);
    indexer.lastSignature.set(SLAB, "sig0");
    nullFor = new Set(["sig4"]);
    for (let poll = 1; poll <= 5; poll++) {
      await indexer.indexTradesForSlab(SLAB);
      expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
    }
    expect(recordSkippedSignatures).not.toHaveBeenCalled();
    await indexer.indexTradesForSlab(SLAB); // 6th poll: over MAX_NULL_TX_HOLDS
    expect(indexer.lastSignature.get(SLAB)).toBe("sig10");
    expect(recordSkippedSignatures).toHaveBeenCalledWith([expect.objectContaining({ signature: "sig4", slab: SLAB, source: "trade-indexer" })]);
    expect(processed).toContain("sig10");
  });

  it("loss 1 (bound per slab): one null signature in two slabs' windows holds each slab its own 5 polls", async () => {
    const SLAB2 = "11111111111111111111111111111111"; // a crank tx is in several slabs' windows
    chain = makeChain(10);
    indexer.lastSignature.set(SLAB, "sig0");
    indexer.lastSignature.set(SLAB2, "sig0");
    nullFor = new Set(["sig4"]);
    for (let poll = 1; poll <= 5; poll++) {
      await indexer.indexTradesForSlab(SLAB);
      await indexer.indexTradesForSlab(SLAB2);
    }
    expect([indexer.lastSignature.get(SLAB), indexer.lastSignature.get(SLAB2)]).toEqual(["sig0", "sig0"]);
    expect(recordSkippedSignatures).not.toHaveBeenCalled();
    nullFor = new Set(); // the node catches up: both slabs read it, nothing is skipped
    await indexer.indexTradesForSlab(SLAB);
    await indexer.indexTradesForSlab(SLAB2);
    expect([indexer.lastSignature.get(SLAB), indexer.lastSignature.get(SLAB2)]).toEqual(["sig10", "sig10"]);
    expect(recordSkippedSignatures).not.toHaveBeenCalled();
  });

  it("loss 2 (cost): a window that ends on a short page costs exactly one extra before-call, which answers empty", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    await indexer.indexTradesForSlab(SLAB);
    expect(calls).toEqual([{ limit: 50, until: "sig0" }, { limit: 50, before: "sig1", until: "sig0" }]);
    expect(indexer.lastSignature.get(SLAB)).toBe("sig3");
  });
});
