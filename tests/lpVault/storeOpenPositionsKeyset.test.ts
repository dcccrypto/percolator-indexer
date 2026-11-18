import { describe, it, expect } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { SupabaseLpVaultStore } from "../../src/lpVault/store.js";

type Row = Record<string, unknown>;

/**
 * Fake PostgREST for lp_vault_positions: order/range/gt, max_rows cap, and a hook that runs
 * before each query is answered (to mutate the table between pages, like a concurrent writePosition).
 */
function fakeDb(all: Row[], opts: { maxRows?: number; beforeQuery?: (n: number, rows: Row[]) => void; source?: (n: number) => Row[]; ignoreGt?: boolean } = {}) {
  let n = 0;
  const db = {
    from() {
      let from = 0, to = Infinity;
      let gt: [string, string] | null = null;
      const q: Record<string, unknown> = {};
      for (const op of ["select", "eq", "or"]) q[op] = () => q;
      q.order = () => q;
      q.gt = (c: string, v: string) => ((gt = [c, v]), q);
      q.range = (a: number, b: number) => ((from = a), (to = b), q);
      q.limit = (k: number) => ((to = from + k - 1), q);
      q.then = (res: (v: unknown) => unknown) => {
        opts.beforeQuery?.(n, all);
        let rows = opts.source ? opts.source(n) : [...all];
        n++;
        rows.sort((x, y) => String(x.user_wallet).localeCompare(String(y.user_wallet)));
        if (gt && !opts.ignoreGt) rows = rows.filter((r) => String(r[gt![0]]) > gt![1]);
        rows = rows.slice(from, Math.min(to, from + (opts.maxRows ?? 1e9) - 1) + 1);
        return Promise.resolve({ data: rows, error: null }).then(res);
      };
      return q;
    },
  };
  return db as unknown as SupabaseClient;
}
const pos = (i: number): Row => ({ user_wallet: `u${String(i).padStart(8, "0")}`, lp_shares: "1" });

describe("listOpenPositions under concurrent change", () => {
  it("does not skip a still-open position when an earlier row closes between pages", async () => {
    const all = Array.from({ length: 2500 }, (_, i) => pos(i));
    const db = fakeDb(all, {
      beforeQuery: (n, rows) => {
        if (n === 1) rows.splice(5, 1); // u00005 zeroed (leaves the lp_shares>0 filter) after page 1
      },
    });
    const out = await new SupabaseLpVaultStore(db).listOpenPositions("devnet", "R");
    const got = new Set(out.map((r) => r.user_wallet));
    // every position that was open throughout must be returned
    for (let i = 0; i < 2500; i++) if (i !== 5) expect(got.has(`u${String(i).padStart(8, "0")}`)).toBe(true);
  });

  it("refuses a page source that never advances instead of looping forever", async () => {
    const db = fakeDb([], { source: () => [pos(1), pos(2)], ignoreGt: true });
    await expect(new SupabaseLpVaultStore(db).listOpenPositions("devnet", "R")).rejects.toThrow();
  });

  it("refuses an endless stream of distinct pages", async () => {
    let k = 0;
    const db = fakeDb([], { source: () => Array.from({ length: 1000 }, () => pos(k++)) });
    await expect(new SupabaseLpVaultStore(db).listOpenPositions("devnet", "R")).rejects.toThrow();
  });
});
