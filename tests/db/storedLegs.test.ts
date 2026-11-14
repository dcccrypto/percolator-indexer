import { describe, it, expect, vi, beforeEach } from "vitest";

const state = {
  result: { data: [] as unknown, error: null as null | { message: string } },
  throws: null as null | unknown,
  warns: [] as unknown[][],
  inCalls: [] as string[][],
  chunkResults: [] as Array<{ data: unknown; error: null | { message: string } }>,
};

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ warn: (...a: unknown[]) => state.warns.push(a), error: vi.fn(), info: vi.fn(), debug: vi.fn() }),
  getNetwork: () => "devnet",
  getSupabase: () => ({
    from: () => ({
      select: () => {
        const q: Record<string, unknown> = {};
        q.eq = () => q;
        q.in = (_c: string, vals: string[]) => {
          state.inCalls.push(vals);
          return q;
        };
        // `.eq("network", ...)` is the last call; make the builder awaitable
        q.then = (resolve: (v: unknown) => void, reject: (e: unknown) => void) => run().then(resolve, reject);
        return q;
      },
    }),
  }),
}));

async function run() {
  if (state.throws) throw state.throws;
  if (state.chunkResults.length) return state.chunkResults.shift()!;
  return state.result;
}

import {
  fetchStoredLegs,
  fetchStoredLegsMany,
  fillAlreadyStored,
  reduceAlreadyStored,
  cpiFillAlreadyStored,
  IN_CHUNK,
  type StoredLeg,
} from "../../src/db/storedLegs.js";

const leg = (o: Partial<StoredLeg> = {}): StoredLeg => ({
  slab_address: "S", asset_index: 0, leg_index: 0, trader: "T", side: "long", size: "100", is_liquidation: false, ...o,
});

beforeEach(() => {
  state.result = { data: [], error: null };
  state.throws = null;
  state.warns = [];
  state.inCalls = [];
  state.chunkResults = [];
});

describe("fetchStoredLegs", () => {
  it("returns rows, and [] when data is null", async () => {
    state.result = { data: [leg()], error: null };
    expect(await fetchStoredLegs("sig")).toEqual([leg()]);
    state.result = { data: null, error: null };
    expect(await fetchStoredLegs("sig")).toEqual([]);
  });

  it("returns null and warns on a DB error", async () => {
    state.result = { data: null, error: { message: "boom" } };
    expect(await fetchStoredLegs("sigsigsigsigsig")).toBeNull();
    expect(state.warns).toHaveLength(1);
  });

  it("returns null when the client throws a non-Error", async () => {
    state.throws = "weird";
    expect(await fetchStoredLegs("sig")).toBeNull();
    expect(JSON.stringify(state.warns[0])).toContain("weird");
  });
});

describe("fetchStoredLegsMany", () => {
  it("gives every signature an entry, de-duplicated, and groups rows", async () => {
    state.result = { data: [{ ...leg(), tx_signature: "a" }, { ...leg({ leg_index: 1 }), tx_signature: "a" }, { ...leg(), tx_signature: "zzz" }], error: null };
    const m = await fetchStoredLegsMany(["a", "b", "a"]);
    expect(m).not.toBeNull();
    expect([...m!.keys()]).toEqual(["a", "b"]);
    expect(m!.get("a")).toHaveLength(2);
    expect(m!.get("b")).toEqual([]);
  });

  it("chunks at IN_CHUNK signatures per query", async () => {
    const sigs = Array.from({ length: IN_CHUNK * 2 + 1 }, (_, i) => `s${i}`);
    const m = await fetchStoredLegsMany(sigs);
    expect(m!.size).toBe(sigs.length);
    expect(state.inCalls.map((c) => c.length)).toEqual([IN_CHUNK, IN_CHUNK, 1]);
  });

  it("makes no query for an empty list", async () => {
    const m = await fetchStoredLegsMany([]);
    expect(m!.size).toBe(0);
    expect(state.inCalls).toEqual([]);
  });

  it("returns null if any chunk fails (error result)", async () => {
    const sigs = Array.from({ length: IN_CHUNK + 1 }, (_, i) => `s${i}`);
    state.chunkResults = [{ data: [], error: null }, { data: null, error: { message: "nope" } }];
    expect(await fetchStoredLegsMany(sigs)).toBeNull();
    expect(state.warns).toHaveLength(1);
  });

  it("returns null when the client throws a non-Error", async () => {
    state.throws = 42;
    expect(await fetchStoredLegsMany(["a"])).toBeNull();
  });

  it("treats null data as no rows", async () => {
    state.result = { data: null, error: null };
    expect((await fetchStoredLegsMany(["a"]))!.get("a")).toEqual([]);
  });
});

describe("fillAlreadyStored", () => {
  const f = { slab: "S", assetIndex: 0, legIndex: 5, trader: "T", side: "long" as const, size: "100", ordinal: 1 };

  it("is false when nothing was read", () => {
    expect(fillAlreadyStored(null, f)).toBe(false);
  });

  it("is true for a row at the exact leg_index", () => {
    expect(fillAlreadyStored([leg({ leg_index: 5, size: "1" })], f)).toBe(true);
  });

  it("matches an identical fill at another leg number, with fractional / bigint / number sizes", () => {
    expect(fillAlreadyStored([leg({ leg_index: 0, size: "100.7" })], f)).toBe(true);
    expect(fillAlreadyStored([leg({ leg_index: 0, size: 100n })], f)).toBe(true);
    expect(fillAlreadyStored([leg({ leg_index: 0, size: 100.9 })], f)).toBe(true);
  });

  it("does not match an unparseable size", () => {
    expect(fillAlreadyStored([leg({ size: null }), leg({ size: Number.NaN, leg_index: 1 })], f)).toBe(false);
  });

  it("needs as many identical stored fills as the ordinal (split orders keep their later legs)", () => {
    const stored = [leg({ leg_index: 0 })];
    expect(fillAlreadyStored(stored, { ...f, ordinal: 1 })).toBe(true);
    expect(fillAlreadyStored(stored, { ...f, ordinal: 2 })).toBe(false);
    expect(fillAlreadyStored([...stored, leg({ leg_index: 1 })], { ...f, ordinal: 2 })).toBe(true);
  });

  it("ignores liquidation rows, other slabs/assets, other traders, sides and sizes", () => {
    const stored = [
      leg({ is_liquidation: true }),
      leg({ slab_address: "X" }),
      leg({ asset_index: 1 }),
      leg({ trader: "U" }),
      leg({ side: "short" }),
      leg({ size: "99" }),
    ];
    expect(fillAlreadyStored(stored, f)).toBe(false);
  });

  it("does not count a tag-44 row as an identical TradeCpi fill", () => {
    const stored = [leg({ leg_index: 2 })];
    expect(fillAlreadyStored(stored, f, new Set([2]))).toBe(false);
    expect(fillAlreadyStored(stored, f)).toBe(true);
  });
});

describe("reduceAlreadyStored", () => {
  const r = { slab: "S", assetIndex: 0, legIndex: 3, trader: "T", reduceQ: 100n };

  it("is false when nothing was read", () => {
    expect(reduceAlreadyStored(null, r)).toBe(false);
  });

  it("is true for a same-trader row at the leg with size <= reduce_q", () => {
    expect(reduceAlreadyStored([leg({ leg_index: 3, size: "100" })], r)).toBe(true);
    expect(reduceAlreadyStored([leg({ leg_index: 3, size: "40" })], r)).toBe(true);
  });

  it("is false when the stored size exceeds reduce_q or is not an integer string", () => {
    expect(reduceAlreadyStored([leg({ leg_index: 3, size: "101" })], r)).toBe(false);
    expect(reduceAlreadyStored([leg({ leg_index: 3, size: null })], r)).toBe(false);
  });

  it("is false for a liquidation row or a different slab, asset, leg or trader", () => {
    const base = { leg_index: 3, size: "1" };
    expect(reduceAlreadyStored([leg({ ...base, is_liquidation: true })], r)).toBe(false);
    expect(reduceAlreadyStored([leg({ ...base, slab_address: "X" })], r)).toBe(false);
    expect(reduceAlreadyStored([leg({ ...base, asset_index: 1 })], r)).toBe(false);
    expect(reduceAlreadyStored([leg({ ...base, leg_index: 4 })], r)).toBe(false);
    expect(reduceAlreadyStored([leg({ ...base, trader: "U" })], r)).toBe(false);
  });
});

describe("cpiFillAlreadyStored", () => {
  const f = { slab: "S", assetIndex: 0, legIndex: 5, trader: "T", side: "long" as const, ordinal: 1 };

  it("is false when nothing was read", () => {
    expect(cpiFillAlreadyStored(null, f)).toBe(false);
  });

  it("is true at the exact leg, whatever the size", () => {
    expect(cpiFillAlreadyStored([leg({ leg_index: 5, size: "7" })], f)).toBe(true);
  });

  it("matches same (trader, side) elsewhere regardless of size, up to the ordinal", () => {
    const stored = [leg({ leg_index: 0, size: "1" })];
    expect(cpiFillAlreadyStored(stored, f)).toBe(true);
    expect(cpiFillAlreadyStored(stored, { ...f, ordinal: 2 })).toBe(false);
  });

  it("excludes tag-44 legs and liquidations, other trader or side", () => {
    expect(cpiFillAlreadyStored([leg({ leg_index: 2 })], f, new Set([2]))).toBe(false);
    expect(cpiFillAlreadyStored([leg({ is_liquidation: true })], f)).toBe(false);
    expect(cpiFillAlreadyStored([leg({ trader: "U" }), leg({ side: "short", leg_index: 1 })], f)).toBe(false);
  });
});
