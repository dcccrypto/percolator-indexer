/**
 * TradeIndexerPolling lifecycle: start/stop, startup backfill (empty, partial failure, retry/backoff,
 * exhaustion, in-flight join), poll loop (excluded/blocked markets, per-market error, markets read failure,
 * re-entrancy) and the window paths that skip or hold the cursor.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { Keypair } from "@solana/web3.js";

const captureSpy = vi.hoisted(() => vi.fn());
const markets = vi.hoisted(() => ({ rows: [] as any[], fail: null as Error | null }));
const blocked = vi.hoisted(() => new Set<string>());

type Sig = { signature: string; err: unknown };
const sigLists = new Map<string, Sig[]>();
const failSigsFor = new Set<string>();
const minimalTx = (sig: string) => ({ slot: 1, blockTime: 1, meta: { err: null, logMessages: [], innerInstructions: [] }, transaction: { signatures: [sig], message: { accountKeys: [], instructions: [] } } });
const connection = {
  getSignaturesForAddress: vi.fn(async (pk: { toBase58(): string }) => {
    if (failSigsFor.has(pk.toBase58())) throw new Error("rpc down");
    return sigLists.get(pk.toBase58()) ?? [];
  }),
  getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map(minimalTx)),
  getParsedTransaction: vi.fn(async (sig: string) => minimalTx(sig)),
  getAccountInfo: vi.fn(async () => null),
};

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe"] },
    getConnection: () => connection,
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: captureSpy,
    eventBus: { on: noop, off: noop, publish: noop },
  };
});
vi.mock("../../src/db/marketRows.js", () => ({
  getMarketRowsCached: vi.fn(async () => { if (markets.fail) throw markets.fail; return markets.rows; }),
}));
vi.mock("../../src/blocklist.js", () => ({ isBlockedSlab: (s: string) => blocked.has(s) }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/insertTradeRow.js", () => {
  const insertTradeRow = vi.fn(async () => true);
  const insertTradeRows = vi.fn(async (rows: unknown[]) => rows);
  const tradeKey = (r: { tx_signature: string | null; asset_index: number; leg_index: number }) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`;
  return { insertTradeRow, insertTradeRows, tradeKey };
});
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
vi.mock("../../src/db/tradeCursor.js", () => ({
  loadTradeCursors: vi.fn(async () => new Map()),
  saveTradeCursor: vi.fn(async () => undefined),
}));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";
import { saveTradeCursor } from "../../src/db/tradeCursor.js";

const slabA = Keypair.generate().publicKey.toBase58();
const slabB = Keypair.generate().publicKey.toBase58();
const mk = (): any => { const i: any = new TradeIndexerPolling(); i._running = true; return i; };

beforeEach(() => {
  vi.clearAllMocks();
  markets.rows = []; markets.fail = null;
  blocked.clear(); sigLists.clear(); failSigsFor.clear();
});
afterEach(() => { vi.useRealTimers(); });

describe("start / stop", () => {
  it("start is idempotent, schedules polling + the startup backfill, stop clears the timer", async () => {
    vi.useFakeTimers();
    const idx: any = new TradeIndexerPolling();
    const poll = vi.spyOn(idx, "pollAllMarkets").mockResolvedValue(undefined);
    const bf = vi.spyOn(idx, "backfill").mockResolvedValue(undefined);
    idx.start();
    idx.start(); // second call: no second timer
    expect(idx.pollTimer).not.toBeNull();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(bf).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(poll).toHaveBeenCalledTimes(1);
    idx.stop();
    expect(idx.pollTimer).toBeNull();
    expect(idx._running).toBe(false);
    idx.stop(); // stopping twice is harmless
  });
});

describe("backfill", () => {
  it("is a no-op when already done or not running", async () => {
    const idx = mk();
    const run = vi.spyOn(idx, "runBackfill");
    idx.hasBackfilled = true;
    await idx.backfill();
    idx.hasBackfilled = false; idx._running = false;
    await idx.backfill();
    expect(run).not.toHaveBeenCalled();
  });

  it("joins an in-flight backfill instead of starting a second", async () => {
    const idx = mk();
    let release!: () => void;
    const run = vi.spyOn(idx, "runBackfill").mockImplementation(() => new Promise<void>((r) => { release = r; }));
    const a = idx.backfill();
    const b = idx.backfill();
    expect(b).toBe(a);
    expect(run).toHaveBeenCalledTimes(1);
    release(); await a;
    expect(idx.backfillInFlight).toBeNull();
  });

  it("no markets yet: does not mark the backfill complete", async () => {
    const idx = mk();
    await idx.backfill();
    expect(idx.hasBackfilled).toBe(false);
  });

  it("a per-market failure is reported and the rest still run, then it completes", async () => {
    markets.rows = [{ slab_address: slabA }, { slab_address: slabB }];
    const idx = mk();
    const seen: string[] = [];
    vi.spyOn(idx, "indexTradesForSlab").mockImplementation(async (s: string) => { seen.push(s); if (s === slabA) throw new Error("boom"); });
    await idx.backfill();
    expect(seen).toEqual([slabA, slabB]);
    expect(captureSpy).toHaveBeenCalledWith(expect.any(Error), { tags: { context: "trade-indexer-backfill", slabAddress: slabA } });
    expect(idx.hasBackfilled).toBe(true);
  });

  it("stops walking markets once the indexer is stopped mid-backfill", async () => {
    markets.rows = [{ slab_address: slabA }, { slab_address: slabB }];
    const idx = mk();
    const seen: string[] = [];
    vi.spyOn(idx, "indexTradesForSlab").mockImplementation(async (s: string) => { seen.push(s); idx._running = false; });
    await idx.runBackfill();
    expect(seen).toEqual([slabA]);
  });

  it("market read failure schedules a backoff retry, then gives up after 3 attempts", async () => {
    vi.useFakeTimers();
    markets.fail = new Error("db down");
    const idx = mk();
    const bf = vi.spyOn(idx, "backfill").mockResolvedValue(undefined);
    await idx.runBackfill(); // attempt 1 -> retry in 20s
    expect(idx.hasBackfilled).toBe(false);
    expect(bf).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(19_999);
    expect(bf).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(bf).toHaveBeenCalledTimes(1);
    await idx.runBackfill(); // attempt 2 -> retry in 40s
    expect(idx.hasBackfilled).toBe(false);
    await vi.advanceTimersByTimeAsync(39_999);
    expect(bf).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(bf).toHaveBeenCalledTimes(2);
    await idx.runBackfill(); // attempt 3 -> exhausted
    expect(idx.hasBackfilled).toBe(true);
    expect(captureSpy).toHaveBeenCalledTimes(3);
  });

  it("a stopped indexer does not schedule a retry after a failed backfill", async () => {
    markets.fail = new Error("db down");
    const idx = mk();
    idx._running = false;
    await idx.runBackfill();
    expect(idx.hasBackfilled).toBe(true); // falls to the exhausted branch, poll covers the gap
  });
});

describe("poll loop", () => {
  it("does nothing when stopped or while a pass is in flight", async () => {
    const idx = mk();
    const once = vi.spyOn(idx, "pollAllMarketsOnce").mockResolvedValue(undefined);
    idx._running = false;
    await idx.pollAllMarkets();
    idx._running = true; idx.pollInFlight = true;
    await idx.pollAllMarkets();
    expect(once).not.toHaveBeenCalled();
    idx.pollInFlight = false;
    await idx.pollAllMarkets();
    expect(once).toHaveBeenCalledTimes(1);
    expect(idx.pollInFlight).toBe(false);
  });

  it("re-triggers the backfill first when it has not completed", async () => {
    const idx = mk();
    const bf = vi.spyOn(idx, "backfill").mockResolvedValue(undefined);
    await idx.pollAllMarketsOnce();
    expect(bf).toHaveBeenCalledTimes(1);
  });

  it("skips indexer_excluded and blocklisted markets, and survives a per-market error", async () => {
    markets.rows = [
      { slab_address: slabA },
      { slab_address: slabB },
      { slab_address: Keypair.generate().publicKey.toBase58(), indexer_excluded: true },
      { slab_address: "blockedslab", indexer_excluded: false },
    ];
    blocked.add("blockedslab");
    const idx = mk(); idx.hasBackfilled = true;
    const seen: string[] = [];
    vi.spyOn(idx, "indexTradesForSlab").mockImplementation(async (s: string) => { seen.push(s); if (s === slabA) throw new Error("x"); });
    await idx.pollAllMarketsOnce();
    expect(seen).toEqual([slabA, slabB]);
  });

  it("stops mid-pass when the indexer is stopped", async () => {
    markets.rows = [{ slab_address: slabA }, { slab_address: slabB }];
    const idx = mk(); idx.hasBackfilled = true;
    const seen: string[] = [];
    vi.spyOn(idx, "indexTradesForSlab").mockImplementation(async (s: string) => { seen.push(s); idx._running = false; });
    await idx.pollAllMarketsOnce();
    expect(seen).toEqual([slabA]);
  });

  it("a markets read failure is reported, not thrown", async () => {
    markets.fail = new Error("db down");
    const idx = mk(); idx.hasBackfilled = true;
    await expect(idx.pollAllMarketsOnce()).resolves.toBeUndefined();
    expect(captureSpy).toHaveBeenCalledWith(expect.any(Error), { tags: { context: "trade-indexer-poll" } });
  });
});

describe("indexTradesForSlab window skips", () => {
  it("a signature fetch failure leaves the cursor untouched", async () => {
    failSigsFor.add(slabA);
    const idx = mk();
    await idx.indexTradesForSlab(slabA, 10);
    expect(idx.lastSignature.get(slabA)).toBeUndefined();
    expect(saveTradeCursor).not.toHaveBeenCalled();
  });

  it("an empty page is a no-op", async () => {
    sigLists.set(slabA, []);
    const idx = mk();
    await idx.indexTradesForSlab(slabA, 10);
    expect(idx.lastSignature.get(slabA)).toBeUndefined();
    expect(connection.getParsedTransactions).not.toHaveBeenCalled();
  });

  it("a window of only errored transactions advances the cursor to the newest signature without fetching", async () => {
    sigLists.set(slabA, [{ signature: "e2", err: { InstructionError: [0, "x"] } }, { signature: "e1", err: { InstructionError: [0, "x"] } }]);
    const idx = mk();
    await idx.indexTradesForSlab(slabA, 10);
    expect(idx.lastSignature.get(slabA)).toBe("e2");
    expect(connection.getParsedTransactions).not.toHaveBeenCalled();
  });
});

// ---- Hung-RPC liveness regression (J-20261121-IDX-REPORT-HUNGRPCSTALL draft, fixed by J-20261121-IDX-FIX-HUNGRPCLIVENESS) ----
// Live finding: an RPC that accepts the request and never answers leaves web3.js waiting (no RPC deadline in the indexer),
// pollAllMarkets keeps pollInFlight=true, and every later setInterval tick returns at the guard. No trade is indexed
// while the RPC stays hung (5 min per held call, forever for a trickling server), while /health (getSlot, 5 s timeout) can still say ok.
// Measured bound (e708fc5, web3.js 1.99 on Node's undici fetch): a silent / headers-only RPC rejects after undici's
// 300 s headers/body timeout; a server that trickles bytes is never cut off. The never-settling mock models the trickle
// case, and the expectation (a service deadline well under 300 s) is also what makes the 300 s case cheap.
describe("HUNG-RPC: poll pass blocked on a never-settling RPC read", () => {
  it("a later poll pass reads the market again once the RPC answers (a hung read must not wedge pollInFlight)", async () => {
    vi.useFakeTimers();
    markets.rows = [{ slab_address: slabA }];
    const idx: any = new TradeIndexerPolling();
    idx.hasBackfilled = true; // poll path only
    connection.getSignaturesForAddress.mockImplementationOnce(() => new Promise(() => {})); // the hung request
    idx.start();
    await vi.advanceTimersByTimeAsync(5 * 60_000 + 1_000); // first poll pass starts and hangs
    expect(connection.getSignaturesForAddress).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(30 * 60_000); // six more intervals; the RPC answers every NEW request
    idx.stop();
    // Before the fix (e708fc5 / 3061d3e): still 1. Expected: the hung read is abandoned after a deadline and later passes run.
    expect(connection.getSignaturesForAddress.mock.calls.length).toBeGreaterThan(1);
  });
});
