/**
 * DRAFT failing tests (J-20261120-IDX-REPORT-RPCFAULTINJECTREPLAY): permanent trade / v22 losses the RPC fault replay
 * measured in TradeIndexerPolling.indexTradesForSlab on feat/v22-indexer 9e8e192. Each test is what a fix must make
 * pass; on 9e8e192 they FAIL. Drop into tests/services/ to run:
 *   vitest run tests/services/TradeIndexer.rpcFaultLoss.draft.test.ts --maxWorkers=4
 *
 * Loss 1 (fault "nulltx"): getParsedTransactions answers null for a signature that getSignaturesForAddress listed as
 *   confirmed and successful (err null). TradeIndexer.ts (window loop) does `if (!tx) { pass.incomplete = true;
 *   continue; }` and then commits the cursor to the window's newest signature, so the null one is never read again.
 *   (LpVaultIndexer treats the same null as an error and holds its cursor; the trade path should too, bounded like
 *   the other holds.)
 * Loss 2 (fault "shortpage"): with a cursor, fetchSignatureWindow treats any page shorter than `limit` as "the cursor
 *   was reached". A node that cuts a page short (fewer than `limit` while older signatures above the cursor remain)
 *   ends the walk early; the cursor then commits to the newest, and the older remainder is never polled.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

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
const getParsedTransactions = vi.fn(async (sigs: string[]) => sigs.map((s) => (nullFor.has(s) ? null : tx(Number(s.slice(3))))));

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
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: async () => undefined }));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

const SLAB = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const makeChain = (n: number): Sig[] => Array.from({ length: n + 1 }, (_, i) => ({ signature: `sig${n - i}`, err: null, slot: n - i }));

describe("TradeIndexerPolling: RPC faults must not lose a confirmed signature for good", () => {
  let indexer: any;
  let processed: string[];
  beforeEach(() => {
    calls.length = 0;
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
});
