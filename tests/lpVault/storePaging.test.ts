import { describe, it, expect } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { SupabaseLpVaultStore } from "../../src/lpVault/store.js";

/** Fake PostgREST: honours order/limit/range and silently caps every response at max_rows. */
function cappingDb(table: string, all: Record<string, unknown>[], maxRows: number) {
  const calls: Array<{ op: string; args: unknown[] }> = [];
  const db = {
    from(t: string) {
      let orders: Array<[string, boolean]> = [];
      let from = 0;
      let to = Number.POSITIVE_INFINITY;
      const q: Record<string, unknown> = {};
      for (const op of ["select", "eq", "or"]) q[op] = () => q;
      q.order = (col: string, o?: { ascending?: boolean }) => {
        orders.push([col, o?.ascending !== false]);
        calls.push({ op: "order", args: [col] });
        return q;
      };
      q.limit = (n: number) => {
        to = Math.min(to, from + n - 1);
        calls.push({ op: "limit", args: [n] });
        return q;
      };
      q.range = (a: number, b: number) => {
        from = a;
        to = b;
        calls.push({ op: "range", args: [a, b] });
        return q;
      };
      q.then = (res: (v: unknown) => unknown) => {
        let rows = t === table ? [...all] : [];
        rows.sort((x, y) => {
          for (const [c, asc] of orders) {
            const a = x[c] as number | string, b = y[c] as number | string;
            if (a !== b) return (a < b ? -1 : 1) * (asc ? 1 : -1);
          }
          return 0;
        });
        rows = rows.slice(from, Math.min(to, from + maxRows - 1) + 1);
        return Promise.resolve({ data: rows, error: null }).then(res);
      };
      return q;
    },
  };
  return { db: db as unknown as SupabaseClient, calls };
}

const evRow = (i: number) => ({
  signature: `sig${String(i).padStart(5, "0")}`, ix_index: 0, kind: "deposit", slot: i,
  market_slab: "S", lp_mint: "M", collateral_atoms: "1", lp_amount: "1",
});
const posRow = (i: number) => ({ user_wallet: `u${String(i).padStart(5, "0")}`, lp_shares: "1" });

describe("SupabaseLpVaultStore under PostgREST max_rows", () => {
  it("loadEvents returns the full history when max_rows < page size", async () => {
    const { db } = cappingDb("lp_vault_events", Array.from({ length: 2500 }, (_, i) => evRow(i)), 1000);
    const out = await new SupabaseLpVaultStore(db).loadEvents("devnet", "R", "U");
    expect(out).toHaveLength(2500);
    expect(out[2499]!.slot).toBe(2499);
  });

  it("loadEvents orders by slot, signature, ix_index so pages are stable", async () => {
    const { db, calls } = cappingDb("lp_vault_events", [evRow(1)], 1000);
    await new SupabaseLpVaultStore(db).loadEvents("devnet", "R", "U");
    expect(calls.filter((c) => c.op === "order").map((c) => c.args[0]).slice(0, 3)).toEqual(["slot", "signature", "ix_index"]);
  });

  it("listOpenPositions returns every open position when max_rows < 5000", async () => {
    const { db } = cappingDb("lp_vault_positions", Array.from({ length: 2500 }, (_, i) => posRow(i)), 1000);
    const out = await new SupabaseLpVaultStore(db).listOpenPositions("devnet", "R");
    expect(out).toHaveLength(2500);
  });
});

describe("loadEvents row cap", () => {
  it("still refuses a history of >= 10000 events", async () => {
    const { db } = cappingDb("lp_vault_events", Array.from({ length: 10_000 }, (_, i) => evRow(i)), 1000);
    await expect(new SupabaseLpVaultStore(db).loadEvents("devnet", "R", "U")).rejects.toThrow("fold refused");
  });
});
