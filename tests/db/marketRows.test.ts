import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const calls: Array<{ select: string; network: string; from: number; to: number }> = [];
let total = 0;

vi.mock("@percolatorct/shared", () => ({
  getNetwork: () => "devnet",
  getSupabase: () => ({
    from: () => {
      const q: Record<string, unknown> = {};
      let select = "";
      let network = "";
      q.select = (cols: string) => ((select = cols), q);
      q.eq = (_col: string, v: string) => ((network = v), q);
      q.order = () => q;
      q.range = async (from: number, to: number) => {
        calls.push({ select, network, from, to });
        const n = Math.max(0, Math.min(to + 1, total) - from);
        const data = Array.from({ length: n }, (_, i) => ({
          slab_address: `slab${from + i}`,
          indexer_excluded: false,
          keeper_status: null,
        }));
        return { data, error: null };
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
    resetMarketRowsCache();
  });
  afterEach(() => vi.useRealTimers());

  it("reads only the three columns the indexer uses, for this network", async () => {
    total = 3;
    const rows = await getMarketRows();
    expect(rows).toHaveLength(3);
    expect(calls).toHaveLength(1);
    expect(calls[0].select).toBe("slab_address,indexer_excluded,keeper_status");
    expect(calls[0].network).toBe("devnet");
  });

  it("pages past PostgREST max_rows instead of silently truncating at 1000", async () => {
    total = 2 * MARKET_ROWS_PAGE + 5;
    const rows = await getMarketRows();
    expect(rows).toHaveLength(total);
    expect(new Set(rows.map((r) => r.slab_address)).size).toBe(total);
    expect(calls.map((c) => c.from)).toEqual([0, MARKET_ROWS_PAGE, 2 * MARKET_ROWS_PAGE]);
  });

  it("a page that is exactly full triggers one more (empty) read, then stops", async () => {
    total = MARKET_ROWS_PAGE;
    expect(await getMarketRows()).toHaveLength(MARKET_ROWS_PAGE);
    expect(calls).toHaveLength(2);
  });

  it("concurrent readers share one read", async () => {
    total = 4;
    const [a, b] = await Promise.all([getMarketRows(), getMarketRows()]);
    expect(a).toBe(b);
    expect(calls).toHaveLength(1);
  });

  it("the cached variant reuses a fresh read and re-reads after the TTL", async () => {
    vi.useFakeTimers();
    total = 2;
    await getMarketRows();
    await getMarketRowsCached();
    expect(calls).toHaveLength(1);
    vi.advanceTimersByTime(MARKET_ROWS_TTL_MS + 1);
    await getMarketRowsCached();
    expect(calls).toHaveLength(2);
  });

  it("the uncached variant always re-reads (registration must never act on stale rows)", async () => {
    total = 2;
    await getMarketRows();
    await getMarketRows();
    expect(calls).toHaveLength(2);
  });
});
