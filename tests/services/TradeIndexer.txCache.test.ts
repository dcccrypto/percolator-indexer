/**
 * Shared parsed-transaction cache on the poll path: a signature in several slabs' windows is fetched once, and the
 * skip / failure / cursor outcome of every batch is the same as without the cache.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { Keypair } from "@solana/web3.js";

const fetchedBatches: string[][] = [];
const fetchedSingles: string[] = [];
const sigLists = new Map<string, { signature: string; err: null }[]>();
const poison = new Set<string>();
const transient = new Set<string>();

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
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: noop,
  };
});
const recordSkipped = vi.fn(async (_rows: { signature: string }[]) => undefined);
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (rows: { signature: string }[]) => recordSkipped(rows), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));

async function freshIndexer(disabled = false): Promise<any> {
  vi.resetModules();
  if (disabled) process.env.INDEXER_TX_CACHE_DISABLED = "true";
  else delete process.env.INDEXER_TX_CACHE_DISABLED;
  const { TradeIndexerPolling } = await import("../../src/services/TradeIndexer.js");
  const idx: any = new TradeIndexerPolling();
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
    recordSkipped.mockClear(); connection.getParsedTransactions.mockClear(); connection.getParsedTransaction.mockClear();
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
  });

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
});
