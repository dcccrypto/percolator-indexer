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
 * Thrown faults (J-20261123-IDX-FOLLOW-RPCFAULTTHROWKILLERS): the RPC call itself throws, as a real web3 Connection does
 *   on -32603 / a socket reset / an HTTP 503 (error shapes from the real-Connection stub run, IDX-REPORT-RPCFAULTLOSSREALRPC).
 *   A getParsedTransactions batch that still fails after its retries must hold the cursor (`batchFailed`); dropping that
 *   flag commits the cursor past every later batch (39 of 59 real rows lost on the stub path). A throwing
 *   getSignaturesForAddress page must hold it too.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { SolanaJSONRPCError } from "@solana/web3.js";
import { signedAs } from "../helpers/signedTx.js";

type Sig = { signature: string; err: unknown; slot: number };
let chain: Sig[] = []; // newest-first, like the RPC
let nullFor = new Set<string>(); // getParsedTransactions answers null for these (node has not got them yet)
let shortFirstPage = false; // the first page of a window is cut to half
const calls: { limit?: number; before?: string; until?: string }[] = [];
// Thrown faults. `throws` answers whether this call throws; `left` counts the remaining throws (Infinity = until cleared).
type Fault = { err: () => unknown; left: number };
let gptFault: (Fault & { fromCall: number }) | null = null; // getParsedTransactions: throw from the Nth call of the poll
let gsfaFault: (Fault & { before: string }) | null = null; // getSignaturesForAddress: throw on the page `before` this sig
let gptCalls = 0;
const hit = (f: Fault): boolean => { if (f.left <= 0) return false; f.left--; return true; };

const getSignaturesForAddress = vi.fn(async (_pk: unknown, opts: { limit?: number; before?: string; until?: string } = {}) => {
  calls.push({ ...opts });
  if (gsfaFault && opts.before === gsfaFault.before && hit(gsfaFault)) throw gsfaFault.err();
  let all = chain;
  if (opts.before) { const b = all.findIndex((s) => s.signature === opts.before); all = b >= 0 ? all.slice(b + 1) : []; }
  const cut = opts.until ? all.findIndex((s) => s.signature === opts.until) : -1;
  let page = (cut >= 0 ? all.slice(0, cut) : all).slice(0, opts.limit ?? 1000);
  if (shortFirstPage && !opts.before && page.length >= 2) page = page.slice(0, Math.floor(page.length / 2));
  return page;
});
const tx = (slot: number) => ({ slot, blockTime: 0, meta: { err: null, logMessages: [] }, transaction: { message: { instructions: [], accountKeys: [] } } });
const getParsedTransactions = vi.fn(async (sigs: string[]) => {
  gptCalls++;
  if (gptFault && gptCalls >= gptFault.fromCall && hit(gptFault)) throw gptFault.err();
  return sigs.map((s) => (nullFor.has(s) ? null : signedAs(s, tx(Number(s.slice(3))))));
});

vi.mock("@percolatorct/shared", () => ({
  config: { allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"] },
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getConnection: () => ({ getSignaturesForAddress, getParsedTransactions, getParsedTransaction: async () => null }),
  eventBus: { on: vi.fn(), off: vi.fn(), publish: vi.fn() },
  decodeBase58: () => null,
  // Like shared withRetry (maxRetries + 1 attempts, every error retried) without the backoff sleep.
  withRetry: async (fn: () => Promise<unknown>, opts: { maxRetries?: number } = {}) => {
    for (let attempt = 0; ; attempt++) {
      try { return await fn(); } catch (err) { if (attempt >= (opts.maxRetries ?? 3)) throw err; }
    }
  },
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
    gptFault = null;
    gsfaFault = null;
    gptCalls = 0;
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

  it("loss 2 (cost): a window that ends on a short page costs one extra before-call, and the cursor commits on the second agreeing read", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    await indexer.indexTradesForSlab(SLAB);
    const window = [{ limit: 50, until: "sig0" }, { limit: 50, before: "sig1", until: "sig0" }];
    expect(calls).toEqual(window);
    // EMPTY-PAGE-1: that empty answer may be false (draft falseEmptyPage test), so the cursor is held for one poll ...
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
    await indexer.indexTradesForSlab(SLAB);
    // ... and the re-read of the same window (same two calls) confirms it.
    expect(calls).toEqual([...window, ...window]);
    expect(indexer.lastSignature.get(SLAB)).toBe("sig3");
  });

  // Thrown faults. 60 signatures above the cursor: gSFA pages sig60..sig11, then before=sig11 sig10..sig1, then empty;
  // getParsedTransactions batches of 10 oldest-first, so call 3 is sig21..sig30 (no prefetch on a cold cache).
  const ALL60 = Array.from({ length: 60 }, (_, i) => `sig${i + 1}`).sort();
  const seen = () => [...new Set(processed)].sort();
  // Error shapes a real web3 Connection throws for each fault (real-Connection stub run).
  const faults: [string, (what: string) => unknown][] = [
    ["-32603 Internal error", (what) => new SolanaJSONRPCError({ code: -32603, message: "Internal error" }, `failed to get ${what}`)],
    ["socket reset", () => Object.assign(new TypeError("fetch failed"), { cause: Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }) })],
    ["HTTP 503", () => new Error("503 Service Unavailable: <html>\r\n<head><title>503 Service Temporarily Unavailable</title></head>\r\n</html>")],
  ];

  describe.each(faults)("thrown fault %s", (_name, make) => {
    it("getParsedTransactions throws once: retried in the same poll, every row written, cursor to the newest", async () => {
      chain = makeChain(60);
      indexer.lastSignature.set(SLAB, "sig0");
      gptFault = { err: () => make("transactions"), left: 1, fromCall: 3 };
      expect(await indexer.indexTradesForSlab(SLAB)).toBe(true);
      expect(gptFault.left).toBe(0); // the throw was hit
      expect(indexer.lastSignature.get(SLAB)).toBe("sig60");
      expect(seen()).toEqual(ALL60);
    });

    it("getParsedTransactions batch 3 fails every retry of one poll: cursor held, the next poll writes every row", async () => {
      chain = makeChain(60);
      indexer.lastSignature.set(SLAB, "sig0");
      gptFault = { err: () => make("transactions"), left: 3, fromCall: 3 }; // TX_FETCH_RETRIES 2 = 3 attempts
      const ok = await indexer.indexTradesForSlab(SLAB);
      expect(gptFault.left).toBe(0);
      expect(indexer.lastSignature.get(SLAB)).toBe("sig0"); // dropping batchFailed: 'sig60'
      expect(ok).toBe(false); // a failed read is not loop progress (/health)
      expect(seen()).toEqual(ALL60.filter((s) => Number(s.slice(3)) <= 20)); // batches 1-2 only, nothing past the failed one
      expect(await indexer.indexTradesForSlab(SLAB)).toBe(true);
      expect(indexer.lastSignature.get(SLAB)).toBe("sig60");
      expect(seen()).toEqual(ALL60); // dropping batchFailed: sig21..sig60 never processed
      expect(recordSkippedSignatures).not.toHaveBeenCalled();
    });

    it("getParsedTransactions fails persistently for 3 polls: cursor held each poll, recovery writes every row", async () => {
      chain = makeChain(60);
      indexer.lastSignature.set(SLAB, "sig0");
      gptFault = { err: () => make("transactions"), left: Infinity, fromCall: 3 };
      for (let poll = 1; poll <= 3; poll++) {
        const ok = await indexer.indexTradesForSlab(SLAB);
        expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
        expect(ok).toBe(false);
      }
      gptFault = null;
      expect(await indexer.indexTradesForSlab(SLAB)).toBe(true);
      expect(indexer.lastSignature.get(SLAB)).toBe("sig60");
      expect(seen()).toEqual(ALL60);
      expect(recordSkippedSignatures).not.toHaveBeenCalled();
    });

    it("getSignaturesForAddress page 2 throws once: retried in the same poll, every row written", async () => {
      chain = makeChain(60);
      indexer.lastSignature.set(SLAB, "sig0");
      gsfaFault = { err: () => make("signatures for address"), left: 1, before: "sig11" };
      expect(await indexer.indexTradesForSlab(SLAB)).toBe(true);
      expect(gsfaFault.left).toBe(0);
      expect(calls.filter((c) => c.before === "sig11")).toHaveLength(2); // the same page retried, not the window re-walked
      expect(indexer.lastSignature.get(SLAB)).toBe("sig60");
      expect(seen()).toEqual(ALL60);
    });

    it("getSignaturesForAddress page 2 throws persistently: cursor held, nothing processed, the next poll writes every row", async () => {
      chain = makeChain(60);
      indexer.lastSignature.set(SLAB, "sig0");
      gsfaFault = { err: () => make("signatures for address"), left: Infinity, before: "sig11" };
      expect(await indexer.indexTradesForSlab(SLAB)).toBe(false);
      expect(calls.filter((c) => c.before === "sig11")).toHaveLength(4); // gSFA maxRetries 3 = 4 attempts
      expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
      expect(processed).toEqual([]); // a partial window is never processed
      gsfaFault = null;
      expect(await indexer.indexTradesForSlab(SLAB)).toBe(true);
      expect(indexer.lastSignature.get(SLAB)).toBe("sig60");
      expect(seen()).toEqual(ALL60);
    });
  });
});
