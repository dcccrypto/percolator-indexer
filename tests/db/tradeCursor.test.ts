import { describe, it, expect, vi, beforeEach } from "vitest";

const state = {
  network: "devnet" as string | null,
  selectResult: { data: [] as unknown, error: null as null | { message: string } },
  selectThrows: null as null | unknown,
  upsertResult: { error: null as null | { message: string } },
  upsertThrows: null as null | unknown,
  upserts: [] as { row: Record<string, unknown>; opts: unknown }[],
  eqCalls: [] as [string, string][],
  errorLogs: [] as unknown[][],
};

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ error: (...a: unknown[]) => state.errorLogs.push(a), warn: vi.fn(), info: vi.fn(), debug: vi.fn() }),
  getNetwork: () => {
    if (state.network === null) throw new Error("NETWORK not set");
    return state.network;
  },
  getSupabase: () => ({
    from: (table: string) => {
      expect(table).toBe("trade_indexer_cursor");
      return {
        select: () => ({
          eq: (col: string, val: string) => {
            state.eqCalls.push([col, val]);
            return {
              limit: async () => {
                if (state.selectThrows) throw state.selectThrows;
                return state.selectResult;
              },
            };
          },
        }),
        upsert: async (row: Record<string, unknown>, opts: unknown) => {
          if (state.upsertThrows) throw state.upsertThrows;
          state.upserts.push({ row, opts });
          return state.upsertResult;
        },
      };
    },
  }),
}));

import { loadTradeCursors, saveTradeCursor, resetTradeCursorWarnings } from "../../src/db/tradeCursor.js";

beforeEach(() => {
  resetTradeCursorWarnings();
  state.network = "devnet";
  state.selectResult = { data: [], error: null };
  state.selectThrows = null;
  state.upsertResult = { error: null };
  state.upsertThrows = null;
  state.upserts = [];
  state.eqCalls = [];
  state.errorLogs = [];
});

describe("loadTradeCursors", () => {
  it("maps slab -> signature for the current network", async () => {
    state.selectResult = { data: [{ slab_address: "slabA", signature: "sig1" }, { slab_address: "slabB", signature: "sig2" }], error: null };
    const m = await loadTradeCursors();
    expect(m.get("slabA")).toBe("sig1");
    expect(m.get("slabB")).toBe("sig2");
    expect(m.size).toBe(2);
    expect(state.eqCalls).toEqual([["network", "devnet"]]);
  });

  it("treats a null data payload as no cursors", async () => {
    state.selectResult = { data: null, error: null };
    expect((await loadTradeCursors()).size).toBe(0);
  });

  it("returns empty without querying when the network is unset", async () => {
    state.network = null;
    expect((await loadTradeCursors()).size).toBe(0);
    expect(state.eqCalls).toEqual([]);
  });

  it("returns empty and logs once when supabase reports an error (missing table)", async () => {
    state.selectResult = { data: null, error: { message: "relation does not exist" } };
    expect((await loadTradeCursors()).size).toBe(0);
    expect((await loadTradeCursors()).size).toBe(0);
    expect(state.errorLogs).toHaveLength(1);
    expect(state.errorLogs[0][1]).toMatchObject({ op: "load", error: "relation does not exist" });
  });

  it("returns empty when the client throws an Error, and stringifies non-Errors", async () => {
    state.selectThrows = new Error("boom");
    expect((await loadTradeCursors()).size).toBe(0);
    expect(state.errorLogs[0][1]).toMatchObject({ error: "boom" });
    resetTradeCursorWarnings();
    state.selectThrows = "plain string";
    expect((await loadTradeCursors()).size).toBe(0);
    expect(state.errorLogs[1][1]).toMatchObject({ error: "plain string" });
  });
});

describe("saveTradeCursor", () => {
  it("upserts one row keyed on network+slab", async () => {
    await saveTradeCursor("slabA", "sig9");
    expect(state.upserts).toHaveLength(1);
    const { row, opts } = state.upserts[0];
    expect(row).toMatchObject({ network: "devnet", slab_address: "slabA", signature: "sig9" });
    expect(Number.isNaN(Date.parse(row.updated_at as string))).toBe(false);
    expect(opts).toEqual({ onConflict: "network,slab_address" });
  });

  it("does nothing when the network is unset", async () => {
    state.network = null;
    await saveTradeCursor("slabA", "sig9");
    expect(state.upserts).toEqual([]);
  });

  it("never throws on a supabase error and warns only once per op", async () => {
    state.upsertResult = { error: { message: "permission denied" } };
    await expect(saveTradeCursor("slabA", "s1")).resolves.toBeUndefined();
    await expect(saveTradeCursor("slabA", "s2")).resolves.toBeUndefined();
    expect(state.errorLogs).toHaveLength(1);
    expect(state.errorLogs[0][1]).toMatchObject({ op: "save", error: "permission denied" });
  });

  it("never throws when the client throws, including non-Error values", async () => {
    state.upsertThrows = new Error("network down");
    await expect(saveTradeCursor("a", "b")).resolves.toBeUndefined();
    resetTradeCursorWarnings();
    state.upsertThrows = 42;
    await expect(saveTradeCursor("a", "b")).resolves.toBeUndefined();
    expect(state.errorLogs[1][1]).toMatchObject({ error: "42" });
  });

  it("warns separately for load and save", async () => {
    state.selectResult = { data: null, error: { message: "x" } };
    state.upsertResult = { error: { message: "y" } };
    await loadTradeCursors();
    await saveTradeCursor("a", "b");
    expect(state.errorLogs).toHaveLength(2);
  });
});
