/**
 * EMPTY-PAGE-1 (RPC fault replay on the real path, J-20261123-IDX-REPORT-RPCFAULTLOSSREALRPC): fetchSignatureWindow ended
 * the window walk on the first `before` page that brought nothing new, and the poll then committed the cursor to the
 * window's newest signature. A node that cuts pages short AND answers one `before` page EMPTY while signatures remain
 * above the cursor ended the walk early, and the rest was passed for good, with no log, no Sentry event and no
 * skipped_signatures row (stub.mjs + real web3 Connection, 61 real devnet A8Xs txs: 1 of 59 rows lost at 9db52dd).
 *
 * Now a window that had a short page and ended on an empty `before` page commits the cursor only when a second read of
 * it agrees (bounded by MAX_EMPTY_PAGE_HOLDS = 5 polls). The first test failed at 6b70cc4 (sig1 never processed).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { signedAs } from "../helpers/signedTx.js";

type Sig = { signature: string; err: unknown; slot: number };
let chain: Sig[] = [];
let falseEmptyOnce = false; // the next `before` page answers [] although older signatures above `until` remain
let falseEmptyAlways = false; // every `before` page answers []
const calls: { limit?: number; before?: string; until?: string }[] = [];

const getSignaturesForAddress = vi.fn(async (_pk: unknown, opts: { limit?: number; before?: string; until?: string } = {}) => {
  calls.push({ ...opts });
  let all = chain;
  if (opts.before) {
    if (falseEmptyAlways) return [];
    if (falseEmptyOnce) { falseEmptyOnce = false; return []; }
    const b = all.findIndex((s) => s.signature === opts.before); all = b >= 0 ? all.slice(b + 1) : [];
  }
  const cut = opts.until ? all.findIndex((s) => s.signature === opts.until) : -1;
  let page = (cut >= 0 ? all.slice(0, cut) : all).slice(0, opts.limit ?? 1000);
  if (!opts.before) page = page.slice(0, 2); // a short first page (the node cut it)
  return page;
});
const tx = (slot: number) => ({ slot, blockTime: 0, meta: { err: null, logMessages: [] }, transaction: { message: { instructions: [], accountKeys: [] } } });
let nullTxs = false; // getParsedTransactions answers null for every signature (a lagging node)
const getParsedTransactions = vi.fn(async (sigs: string[]) => sigs.map((s) => (nullTxs ? null : signedAs(s, tx(Number(s.slice(3)))))));
const captureException = vi.fn();

vi.mock("@percolatorct/shared", () => ({
  config: { allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"] },
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getConnection: () => ({ getSignaturesForAddress, getParsedTransactions, getParsedTransaction: async () => null }),
  eventBus: { on: vi.fn(), off: vi.fn(), publish: vi.fn() },
  decodeBase58: () => null,
  withRetry: async (fn: () => Promise<unknown>) => fn(),
  captureException: (...a: unknown[]) => captureException(...a),
}));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: async () => [],
  fetchStoredLegsMany: async () => new Map(),
}));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined) }));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";

const SLAB = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const makeChain = (n: number): Sig[] => Array.from({ length: n + 1 }, (_, i) => ({ signature: `sig${n - i}`, err: null, slot: n - i }));

describe("TradeIndexerPolling: a falsely empty `before` page must not pass the rest of the window (EMPTY-PAGE-1)", () => {
  let indexer: any;
  let processed: string[];
  beforeEach(() => {
    calls.length = 0;
    falseEmptyOnce = false;
    falseEmptyAlways = false;
    nullTxs = false;
    captureException.mockClear();
    indexer = new TradeIndexerPolling();
    indexer._running = true;
    processed = [];
    vi.spyOn(indexer, "processTransaction").mockImplementation(async (...a: unknown[]) => { processed.push(a[1] as string); return false; });
  });

  it("short first page, then one empty `before` page with sig1 still above the cursor: sig1 is processed by the next poll", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    falseEmptyOnce = true;
    await indexer.indexTradesForSlab(SLAB); // pages: [sig3, sig2], then before=sig2 -> [] (sig1 remains)
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0"); // held: the empty answer is not trusted yet
    await indexer.indexTradesForSlab(SLAB); // the node answers correctly now
    // 6b70cc4: the first poll committed the cursor to sig3; sig1 was never processed
    expect(processed).toContain("sig1");
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0"); // that read disagreed with the first one: held again
    await indexer.indexTradesForSlab(SLAB); // a third read agrees with the second
    expect(indexer.lastSignature.get(SLAB)).toBe("sig3");
    expect(captureException).not.toHaveBeenCalled();
  });

  it("a later read that comes back SHORTER than the earlier one does not confirm it (the first read was the honest one)", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    await indexer.indexTradesForSlab(SLAB); // honest: [sig3, sig2], [sig1], []
    falseEmptyOnce = true;
    await indexer.indexTradesForSlab(SLAB); // false: [sig3, sig2], [] (a subset of the first read)
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
    await indexer.indexTradesForSlab(SLAB); // honest again, disagrees with the false one
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
    await indexer.indexTradesForSlab(SLAB); // two honest reads in a row agree
    expect(indexer.lastSignature.get(SLAB)).toBe("sig3");
  });

  it("new signatures that arrive between the two reads do not block the confirmation; the cursor moves to the newest", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    await indexer.indexTradesForSlab(SLAB);
    chain = makeChain(5); // sig4, sig5 arrive
    await indexer.indexTradesForSlab(SLAB);
    expect(indexer.lastSignature.get(SLAB)).toBe("sig5");
    expect(new Set(processed)).toEqual(new Set(["sig1", "sig2", "sig3", "sig4", "sig5"]));
  });

  it("a confirmation is not used up by a pass that holds for another reason (null tx): the cursor commits on the next clean pass", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    await indexer.indexTradesForSlab(SLAB); // first read: held, unconfirmed
    indexer.txCache = new (indexer.txCache.constructor)(10, 60_000); // empty cache: the next read goes to the node
    nullTxs = true;
    await indexer.indexTradesForSlab(SLAB); // agrees, but every tx reads null: null-tx hold
    nullTxs = false;
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
    await indexer.indexTradesForSlab(SLAB); // clean pass, still agreeing: commits now, not one poll later
    expect(indexer.lastSignature.get(SLAB)).toBe("sig3");
  });

  it("bounded: a window that reads differently on every poll holds the cursor 5 polls, then commits with an alert", async () => {
    chain = makeChain(3);
    indexer.lastSignature.set(SLAB, "sig0");
    falseEmptyAlways = true; // every read: the newest 2 only, and the chain grows by one each poll
    for (let poll = 1; poll <= 5; poll++) {
      await indexer.indexTradesForSlab(SLAB);
      expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
      chain = makeChain(3 + poll);
    }
    expect(captureException).not.toHaveBeenCalled();
    await indexer.indexTradesForSlab(SLAB); // 6th disagreeing read: over MAX_EMPTY_PAGE_HOLDS
    expect(indexer.lastSignature.get(SLAB)).toBe("sig8");
    expect(captureException).toHaveBeenCalledWith(expect.any(Error), expect.objectContaining({ tags: { context: "trade-indexer-window-unconfirmed" } }));
  });

  it("with a cursor even a one-signature window is held once; without a cursor (no walk) the look-back page commits at once", async () => {
    chain = makeChain(1); // [sig1] short, then before=sig1 -> []: still a short-then-empty window, so held once
    indexer.lastSignature.set(SLAB, "sig0");
    await indexer.indexTradesForSlab(SLAB);
    expect(indexer.lastSignature.get(SLAB)).toBe("sig0");
    // Without a cursor there is no walk (one bounded look-back page) and nothing to confirm.
    const fresh = new TradeIndexerPolling() as any;
    fresh._running = true;
    vi.spyOn(fresh, "processTransaction").mockImplementation(async () => false);
    await fresh.indexTradesForSlab(SLAB);
    expect(fresh.lastSignature.get(SLAB)).toBe("sig1");
  });
});
