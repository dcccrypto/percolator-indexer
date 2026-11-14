/**
 * Shared parsed-transaction cache on the poll path: a signature in several slabs' windows is fetched once, and the
 * skip / failure / cursor outcome of every batch is the same as without the cache.
 */
import { RpcLimiter } from "../../src/lib/rpcLimiter.js";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { Keypair } from "@solana/web3.js";

const fetchedBatches: string[][] = [];
const fetchedSingles: string[] = [];
const sigLists = new Map<string, { signature: string; err: null }[]>();
const poison = new Set<string>();
const transient = new Set<string>();
const withRetrySpy = vi.fn(async (fn: () => Promise<unknown>) => fn());

const minimalTx = (sig: string) => ({ slot: 1, blockTime: 1, meta: { err: null, logMessages: [], innerInstructions: [] }, transaction: { signatures: [sig], message: { accountKeys: [], instructions: [] } } });
const versionErr = () => Object.assign(new Error("Transaction version (2) is not supported by the requesting client"), { code: -32015 });

const connection = {
  getSignaturesForAddress: vi.fn(async (pk: { toBase58(): string }) => sigLists.get(pk.toBase58()) ?? []),
  getParsedTransactions: vi.fn(async (sigs: string[]) => {
    fetchedBatches.push([...sigs]);
    if (sigs.some((s) => transient.has(s))) throw Object.assign(new Error("429 Too Many Requests"), { code: 429 });
    if (sigs.some((s) => poison.has(s))) throw versionErr();
    return sigs.map(minimalTx);
  }),
  getParsedTransaction: vi.fn(async (sig: string) => {
    fetchedSingles.push(sig);
    if (poison.has(sig)) throw versionErr();
    return minimalTx(sig);
  }),
  getAccountInfo: vi.fn(async () => null),
};

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe"] },
    getConnection: () => connection,
    withRetry: (fn: () => Promise<unknown>) => withRetrySpy(fn),
    createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: noop,
  };
});
const recordSkipped = vi.fn(async (_rows: { signature: string }[]) => undefined);
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (rows: { signature: string }[]) => recordSkipped(rows), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/insertTradeRow.js", () => {
  // Poll windows write through insertTradeRows; delegate to the insertTradeRow mock so assertions on it still hold.
  const insertTradeRow = vi.fn(async (_row?: unknown) => true);
  const insertTradeRows = vi.fn(async (rows: unknown[]) => { const out: unknown[] = []; for (const r of rows) if (await insertTradeRow(r)) out.push(r); return out; });
  const tradeKey = (r: { tx_signature: string | null; asset_index: number; leg_index: number }) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`;
  return { insertTradeRow, insertTradeRows, tradeKey };
});
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

/** The cache flag is read at module load: the enabled case uses the static import, the disabled control re-imports. */
async function freshIndexer(disabled = false): Promise<any> {
  let Ctor: typeof TradeIndexerPolling = TradeIndexerPolling;
  if (disabled) {
    vi.resetModules();
    process.env.INDEXER_TX_CACHE_DISABLED = "true";
    try { Ctor = (await import("../../src/services/TradeIndexer.js")).TradeIndexerPolling; } finally { delete process.env.INDEXER_TX_CACHE_DISABLED; }
  }
  const idx: any = new Ctor();
  idx.rpcLimiter = new RpcLimiter({ minIntervalMs: 0, baseBackoffMs: 1, maxBackoffMs: 1, maxRateLimitRetries: 1 });
  idx._running = true;
  return idx;
}

const slab = () => Keypair.generate().publicKey.toBase58();
const sig = (n: string) => n.padEnd(64, "1"); // the poll path does not validate the format here
const list = (...s: string[]) => s.map((signature) => ({ signature, err: null }));
const allFetched = () => [...fetchedBatches.flat(), ...fetchedSingles];

describe("TradeIndexer shared tx cache", () => {
  beforeEach(() => {
    fetchedBatches.length = 0; fetchedSingles.length = 0; sigLists.clear(); poison.clear(); transient.clear();
    recordSkipped.mockClear(); withRetrySpy.mockClear(); connection.getParsedTransactions.mockClear(); connection.getParsedTransaction.mockClear();
  });

  it("a signature shared by two slabs is fetched once; both cursors commit", async () => {
    const idx = await freshIndexer();
    const [A, B] = [slab(), slab()];
    const [a, b, c, d] = ["A", "B", "C", "D"].map(sig);
    sigLists.set(A, list(c, b, a));
    sigLists.set(B, list(d, c, b, a));
    await idx.indexTradesForSlab(A, 100);
    await idx.indexTradesForSlab(B, 100);
    expect(allFetched().sort()).toEqual([a, b, c, d].sort());
    expect(fetchedBatches.at(-1)).toEqual([d]); // B: only its miss
    expect(idx.lastSignature.get(A)).toBe(c);
    expect(idx.lastSignature.get(B)).toBe(d);
  });

  it("negative control: INDEXER_TX_CACHE_DISABLED=true fetches every window in full", async () => {
    const idx = await freshIndexer(true);
    const [A, B] = [slab(), slab()];
    const [a, b, c, d] = ["A", "B", "C", "D"].map(sig);
    sigLists.set(A, list(c, b, a));
    sigLists.set(B, list(d, c, b, a));
    await idx.indexTradesForSlab(A, 100);
    await idx.indexTradesForSlab(B, 100);
    expect(allFetched()).toHaveLength(7);
  }, 60_000); // cold re-import of the module graph

  it("all cached: no RPC transaction fetch at all", async () => {
    const idx = await freshIndexer();
    const [A, B] = [slab(), slab()];
    const [a, b] = ["A", "B"].map(sig);
    sigLists.set(A, list(b, a));
    sigLists.set(B, list(b, a));
    await idx.indexTradesForSlab(A, 100);
    const n = allFetched().length;
    await idx.indexTradesForSlab(B, 100);
    expect(allFetched().length).toBe(n);
    expect(idx.lastSignature.get(B)).toBe(b);
  });

  it("a poison signature next to a CACHED sibling is skipped exactly as without the cache (full-batch refetch)", async () => {
    const idx = await freshIndexer();
    const [A, B] = [slab(), slab()];
    const [a, p] = ["A", "P"].map(sig);
    poison.add(p);
    sigLists.set(A, list(a));
    sigLists.set(B, list(p, a)); // newest first: p is newer than a
    await idx.indexTradesForSlab(A, 100);
    await idx.indexTradesForSlab(B, 100);
    // Misses-only [p] has no fetched sibling and trips the breaker; the whole batch [a, p] is then fetched as before.
    expect(fetchedBatches.at(-1)).toEqual([a, p]);
    expect(recordSkipped).toHaveBeenCalledTimes(1);
    expect(recordSkipped.mock.calls[0][0]).toEqual([expect.objectContaining({ signature: p })]);
    expect(idx.lastSignature.get(B)).toBe(p); // advanced, same as the uncached outcome
  });

  it("a poison signature alone (no sibling anywhere) still trips the breaker and holds the cursor", async () => {
    const idx = await freshIndexer();
    const A = slab();
    const p = sig("P");
    poison.add(p);
    sigLists.set(A, list(p));
    await idx.indexTradesForSlab(A, 100);
    expect(recordSkipped).not.toHaveBeenCalled();
    expect(idx.lastSignature.has(A)).toBe(false);
  });

  it("a transient failure on the misses holds the cursor and caches nothing from the failed call", async () => {
    const idx = await freshIndexer();
    const [A, B] = [slab(), slab()];
    const [a, q] = ["A", "Q"].map(sig);
    sigLists.set(A, list(a));
    sigLists.set(B, list(q, a));
    await idx.indexTradesForSlab(A, 100);
    transient.add(q);
    await idx.indexTradesForSlab(B, 100);
    expect(idx.lastSignature.has(B)).toBe(false);
    expect(recordSkipped).not.toHaveBeenCalled();
    transient.clear();
    fetchedBatches.length = 0;
    await idx.indexTradesForSlab(B, 100);
    expect(fetchedBatches.flat()).toEqual([q]); // q re-fetched (never cached), a still cached
    expect(idx.lastSignature.get(B)).toBe(q);
  });

  it("prefetch packs a window's scattered misses into one call instead of one per window batch", async () => {
    const idx = await freshIndexer();
    const [A, B] = [slab(), slab()];
    const shared = Array.from({ length: 20 }, (_, i) => sig("S" + String(i).padStart(2, "0")));
    const [m1, m2] = [sig("M1"), sig("M2")];
    sigLists.set(A, list(...shared));
    const bList = [...shared];
    bList[3] = m1; bList[15] = m2; // one miss in each of B's two window batches of 10
    sigLists.set(B, list(...bList));
    await idx.indexTradesForSlab(A, 100);
    fetchedBatches.length = 0;
    await idx.indexTradesForSlab(B, 100);
    expect(fetchedBatches).toEqual([[m2, m1]]); // oldest-first window order, one round trip
    expect(idx.lastSignature.get(B)).toBe(bList[0]);
  });

  it("a transient failure during prefetch: one attempt, no retry cycle, then the plain full-batch fetch", async () => {
    const idx = await freshIndexer();
    const [A, B] = [slab(), slab()];
    const [a, q] = ["A", "Q"].map(sig);
    sigLists.set(A, list(a));
    sigLists.set(B, list(q, a));
    await idx.indexTradesForSlab(A, 100);
    transient.add(q);
    fetchedBatches.length = 0; withRetrySpy.mockClear();
    await idx.indexTradesForSlab(B, 100);
    // prefetch [q] once (no 429 retry), then the window batch in full as without the cache (no misses-only step);
    // that one is retried once by the limiter (maxRateLimitRetries 1 above) before the failure holds the cursor
    expect(fetchedBatches).toEqual([[q], [a, q], [a, q]]);
    expect(withRetrySpy).toHaveBeenCalledTimes(2); // getSignaturesForAddress + the full-batch fetch; none from prefetch
    expect(idx.lastSignature.has(B)).toBe(false);
  });
});
