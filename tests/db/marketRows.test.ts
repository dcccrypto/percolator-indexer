import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const calls: Array<{ select: string; network: string; after: string | null; limit: number }> = [];
/** The fake `markets` table, as slab addresses. Kept sorted; tests may mutate it between pages. */
let table: string[] = [];
/** Runs after each page is served (to simulate a concurrent write between page reads). */
let afterPage: ((pageIndex: number) => void) | null = null;
/** When set, the next page read resolves to this error / null data instead of the table. */
let nextResult: { data: null; error: unknown } | null = null;
/** When set, each page read waits on it before answering (to hold a read in flight). */
let gate: Promise<void> | null = null;
const slab = (i: number) => `slab${String(i).padStart(6, "0")}`;
const setTotal = (n: number) => {
  table = Array.from({ length: n }, (_, i) => slab(i));
};

vi.mock("@percolatorct/shared", () => ({
  getNetwork: () => "devnet",
  getSupabase: () => ({
    from: () => {
      const q: Record<string, unknown> = {};
      let select = "";
      let network = "";
      let after: string | null = null;
      q.select = (cols: string) => ((select = cols), q);
      q.eq = (_col: string, v: string) => ((network = v), q);
      q.gt = (_col: string, v: string) => ((after = v), q);
      q.order = () => q;
      q.limit = async (n: number) => {
        calls.push({ select, network, after, limit: n });
        if (gate) await gate;
        if (nextResult) {
          const r = nextResult;
          nextResult = null;
          return r;
        }
        const page = [...table]
          .sort()
          .filter((a) => after === null || a > after)
          .slice(0, n)
          .map((slab_address) => ({ slab_address, indexer_excluded: false, keeper_status: null }));
        afterPage?.(calls.length - 1);
        return { data: page, error: null };
      };
      return q;
    },
  }),
}));

import {
  getMarketRows,
  getMarketRowsCached,
  resetMarketRowsCache,
  MARKET_ROWS_PAGE,
  MARKET_ROWS_TTL_MS,
} from "../../src/db/marketRows.js";

describe("marketRows", () => {
  beforeEach(() => {
    calls.length = 0;
    afterPage = null;
    nextResult = null;
    gate = null;
    resetMarketRowsCache();
  });
  afterEach(() => vi.useRealTimers());

  it("reads only the three columns the indexer uses, for this network", async () => {
    setTotal(3);
    const rows = await getMarketRows();
    expect(rows).toHaveLength(3);
    expect(calls).toHaveLength(1);
    expect(calls[0].select).toBe("slab_address,indexer_excluded,keeper_status");
    expect(calls[0].network).toBe("devnet");
  });

  it("pages past PostgREST max_rows instead of silently truncating at 1000", async () => {
    setTotal(2 * MARKET_ROWS_PAGE + 5);
    const rows = await getMarketRows();
    expect(rows).toHaveLength(table.length);
    expect(new Set(rows.map((r) => r.slab_address)).size).toBe(table.length);
    expect(calls.map((c) => c.after)).toEqual([null, slab(MARKET_ROWS_PAGE - 1), slab(2 * MARKET_ROWS_PAGE - 1)]);
    expect(calls.every((c) => c.limit === MARKET_ROWS_PAGE)).toBe(true);
  });

  it("a page that is exactly full triggers one more (empty) read, then stops", async () => {
    setTotal(MARKET_ROWS_PAGE);
    expect(await getMarketRows()).toHaveLength(MARKET_ROWS_PAGE);
    expect(calls).toHaveLength(2);
  });

  it("concurrent readers share one read", async () => {
    setTotal(4);
    const [a, b] = await Promise.all([getMarketRows(), getMarketRows()]);
    expect(a).toBe(b);
    expect(calls).toHaveLength(1);
  });

  it("the cached variant reuses a fresh read and re-reads after the TTL", async () => {
    vi.useFakeTimers();
    setTotal(2);
    await getMarketRows();
    await getMarketRowsCached();
    expect(calls).toHaveLength(1);
    vi.advanceTimersByTime(MARKET_ROWS_TTL_MS + 1);
    await getMarketRowsCached();
    expect(calls).toHaveLength(2);
  });

  it("the uncached variant always re-reads (registration must never act on stale rows)", async () => {
    setTotal(2);
    await getMarketRows();
    await getMarketRows();
    expect(calls).toHaveLength(2);
  });

  it("a row inserted below the cursor between pages does not make a market read twice", async () => {
    // Offset paging: page 2 starts at OFFSET 1000, so the old row 999 (now 1000) is read again.
    setTotal(2 * MARKET_ROWS_PAGE + 5);
    afterPage = (i) => {
      if (i === 0) table.push("slab-new-0"); // sorts before every slab000… ("-" < "0")
    };
    const rows = (await getMarketRows()).map((r) => r.slab_address);
    expect(new Set(rows).size).toBe(rows.length);
    expect(rows).toHaveLength(2 * MARKET_ROWS_PAGE + 5);
  });

  it("a row deleted below the cursor between pages does not make a market go missing", async () => {
    // Offset paging: the old row 1000 (now 999) falls between the pages and is never read.
    setTotal(2 * MARKET_ROWS_PAGE + 5);
    const deleted = slab(10);
    afterPage = (i) => {
      if (i === 0) table.splice(table.indexOf(deleted), 1);
    };
    const rows = new Set((await getMarketRows()).map((r) => r.slab_address));
    for (let i = 0; i < 2 * MARKET_ROWS_PAGE + 5; i++) {
      if (slab(i) !== deleted) expect(rows.has(slab(i))).toBe(true);
    }
  });

  it("a rejected read clears inFlight and leaves the cache unwritten", async () => {
    setTotal(2);
    nextResult = { data: null, error: new Error("pg down") };
    await expect(getMarketRows()).rejects.toThrow("pg down");
    // Not cached: the cached variant must hit the DB again, and a new read must start (inFlight cleared).
    expect(await getMarketRowsCached()).toHaveLength(2);
    expect(calls).toHaveLength(2);
  });

  it("a cached read exactly at the TTL is stale (strict < comparison)", async () => {
    vi.useFakeTimers();
    setTotal(2);
    await getMarketRows();
    vi.advanceTimersByTime(MARKET_ROWS_TTL_MS - 1);
    await getMarketRowsCached();
    expect(calls).toHaveLength(1);
    vi.advanceTimersByTime(1);
    await getMarketRowsCached();
    expect(calls).toHaveLength(2);
  });

  it("null data with no error is treated as an empty table", async () => {
    nextResult = { data: null, error: null };
    expect(await getMarketRows()).toEqual([]);
  });

  it("reset during an in-flight read: the old run neither writes the cache nor clears a newer inFlight", async () => {
    setTotal(2);
    let release!: () => void;
    gate = new Promise<void>((r) => (release = r));
    const old = getMarketRows();
    resetMarketRowsCache();
    // A newer read starts after the reset; it must not be clobbered by the old run settling.
    const fresh = getMarketRows();
    release();
    gate = null;
    await Promise.all([old, fresh]);
    const before = calls.length;
    // Old run settled first: cache must hold only the newer run's result, and inFlight must be clear.
    await getMarketRowsCached();
    expect(calls.length).toBe(before); // newer run's cache is fresh
  });

  it("reset during an in-flight read: the stale run's result is not cached", async () => {
    setTotal(2);
    let release!: () => void;
    gate = new Promise<void>((r) => (release = r));
    const old = getMarketRows();
    resetMarketRowsCache();
    table = [slab(0), slab(1), slab(2)]; // DB changed after the reset
    release();
    await old;
    gate = null;
    const before = calls.length;
    await getMarketRowsCached(); // cache must be empty -> re-read
    expect(calls.length).toBe(before + 1);
  });

  it("reset during an in-flight read: a newer inFlight survives the old run's finally", async () => {
    setTotal(2);
    let releaseOld!: () => void;
    gate = new Promise<void>((r) => (releaseOld = r));
    const old = getMarketRows();
    resetMarketRowsCache();
    let releaseNew!: () => void;
    gate = new Promise<void>((r) => (releaseNew = r));
    const fresh = getMarketRows();
    releaseOld();
    await old;
    // fresh is still pending; a concurrent caller must share it, not start a third read.
    const callsBefore = calls.length;
    const shared = getMarketRows();
    expect(shared).toBe(fresh);
    expect(calls.length).toBe(callsBefore);
    releaseNew();
    await fresh;
  });
});
