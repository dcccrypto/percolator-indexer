import { describe, it, expect, vi } from "vitest";

// A PostgREST stand-in that enforces max_rows = 1000 (supabase/config.toml) on every read, like the real server:
// the cap applies to the response size regardless of .limit()/.range().
const MAX_ROWS = 1000;
const TOTAL = 2500;
const table = Array.from({ length: TOTAL }, (_, i) => ({ slab_address: `slab${String(i).padStart(5, "0")}`, signature: `sig${i}` }));
const state = { failOnCall: null as number | null, calls: 0 };

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() }),
  getNetwork: () => "devnet",
  getSupabase: () => ({
    from: () => ({
      select: () => {
        let lo = 0;
        let hi = Number.MAX_SAFE_INTEGER;
        const q: Record<string, unknown> = {
          eq: () => q,
          order: () => q,
          limit: (n: number) => { hi = Math.min(hi, lo + n - 1); return q; },
          range: (a: number, b: number) => { lo = a; hi = b; return q; },
          then: (res: (v: unknown) => unknown) => {
            state.calls++;
            if (state.failOnCall === state.calls) return res({ data: null, error: { message: "boom" } });
            const upper = Math.min(hi, lo + MAX_ROWS - 1);
            return res({ data: table.slice(lo, upper + 1), error: null });
          },
        };
        return q;
      },
    }),
  }),
}));

import { loadTradeCursors } from "../../src/db/tradeCursor.js";

describe("loadTradeCursors vs PostgREST max_rows", () => {
  it("loads every cursor, not just the first max_rows", async () => {
    state.calls = 0;
    state.failOnCall = null;
    const m = await loadTradeCursors();
    expect(m.size).toBe(TOTAL);
    expect(m.get("slab02499")).toBe("sig2499");
  });

  it("keeps the pages it already read when a later page fails", async () => {
    state.calls = 0;
    state.failOnCall = 2;
    const m = await loadTradeCursors();
    expect(m.size).toBe(1000);
  });
});
