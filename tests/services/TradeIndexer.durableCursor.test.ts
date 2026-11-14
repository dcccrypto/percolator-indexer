import { describe, it, expect, vi, beforeEach } from "vitest";
import { Keypair } from "@solana/web3.js";

type Sig = { signature: string; err: null };
const sigLists = new Map<string, Sig[]>();
const failTx = new Set<string>();
const fetched: string[] = [];
const untilSeen: (string | undefined)[] = [];
const table = new Map<string, string>(); // the durable cursor table, survives a "restart"
const saves: [string, string][] = [];

const minimalTx = (sig: string) => ({ slot: 1, blockTime: 1, meta: { err: null, logMessages: [], innerInstructions: [] }, transaction: { signatures: [sig], message: { accountKeys: [], instructions: [] } } });
const connection = {
  getSignaturesForAddress: vi.fn(async (pk: { toBase58(): string }, opts: { until?: string; before?: string; limit?: number } = {}) => {
    untilSeen.push(opts.until);
    let all = sigLists.get(pk.toBase58()) ?? [];
    if (opts.before) all = all.slice(all.findIndex((s) => s.signature === opts.before) + 1);
    const cut = opts.until ? all.findIndex((s) => s.signature === opts.until) : -1;
    return (cut >= 0 ? all.slice(0, cut) : all).slice(0, opts.limit ?? 1000);
  }),
  getParsedTransactions: vi.fn(async (sigs: string[]) => {
    fetched.push(...sigs);
    if (sigs.some((s) => failTx.has(s))) throw Object.assign(new Error("429 Too Many Requests"), { code: 429 });
    return sigs.map(minimalTx);
  }),
  getParsedTransaction: vi.fn(async (sig: string) => {
    fetched.push(sig);
    if (failTx.has(sig)) throw Object.assign(new Error("429 Too Many Requests"), { code: 429 });
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
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
vi.mock("../../src/db/tradeCursor.js", () => ({
  loadTradeCursors: vi.fn(async () => new Map(table)),
  saveTradeCursor: vi.fn(async (slab: string, sig: string) => { saves.push([slab, sig]); table.set(slab, sig); }),
}));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

const slab = Keypair.generate().publicKey.toBase58();
const mk = (...names: string[]): Sig[] => names.map((signature) => ({ signature, err: null })); // newest first
const newIndexer = (): any => { const i: any = new TradeIndexerPolling(); i._running = true; return i; };

beforeEach(() => {
  sigLists.clear(); failTx.clear(); fetched.length = 0; untilSeen.length = 0; table.clear(); saves.length = 0;
});

describe("TradeIndexerPolling durable cursor", () => {
  it("a restart resumes from the stored cursor: nothing is re-fetched, and signatures that arrived meanwhile are not missed", async () => {
    sigLists.set(slab, mk("s3", "s2", "s1"));
    await newIndexer().indexTradesForSlab(slab, 50);
    expect(fetched.sort()).toEqual(["s1", "s2", "s3"]);
    expect(table.get(slab)).toBe("s3");

    fetched.length = 0;
    const restarted = newIndexer(); // in-memory cursors gone
    await restarted.indexTradesForSlab(slab, 50);
    expect(fetched).toEqual([]); // 0 re-fetched
    expect(untilSeen.at(-1)).toBe("s3");

    sigLists.set(slab, mk("s5", "s4", "s3", "s2", "s1")); // two new transactions after the restart
    await restarted.indexTradesForSlab(slab, 50);
    expect(fetched.sort()).toEqual(["s4", "s5"]); // only the new ones, none missed
    expect(table.get(slab)).toBe("s5");
  });

  it("loads the durable cursors once per process, not once per slab or poll", async () => {
    const { loadTradeCursors } = await import("../../src/db/tradeCursor.js");
    vi.mocked(loadTradeCursors).mockClear();
    const idx = newIndexer();
    sigLists.set(slab, mk("a1"));
    await idx.indexTradesForSlab(slab, 50);
    await idx.indexTradesForSlab(slab, 50);
    expect(loadTradeCursors).toHaveBeenCalledTimes(1);
  });

  it("a failed batch does not move the in-memory or the stored cursor", async () => {
    table.set(slab, "s1");
    sigLists.set(slab, mk("s3", "s2", "s1"));
    failTx.add("s2");
    const idx = newIndexer();
    await idx.indexTradesForSlab(slab, 50);
    expect(saves).toEqual([]);
    expect(table.get(slab)).toBe("s1");
    expect(idx.lastSignature.get(slab)).toBe("s1");

    failTx.clear(); // the next poll retries the same window and then commits
    fetched.length = 0;
    await idx.indexTradesForSlab(slab, 50);
    expect(fetched.sort()).toEqual(["s2", "s3"]);
    expect(table.get(slab)).toBe("s3");
  });
});
