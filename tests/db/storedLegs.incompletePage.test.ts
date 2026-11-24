// A stored-leg read must be complete or null, never a partial list. A short list makes stored
// legs look new and they are inserted again. Before this, fetchStoredLegsMany stopped and
// returned what it had on a 2xx page with data:null and no error, on an empty page before all
// rows were read, and on a row with no id. fetchStoredLegs did not page at all, so it returned
// max_rows rows for a bigger transaction.
// The fake below acts like PostgREST: in/eq/gt/order(id)/limit, a max_rows cap, count=exact
// (Content-Range total) when asked, and a fault on query number k.
import { describe, it, expect, vi, beforeEach } from "vitest";

type Row = { id: string; tx_signature: string; network: string; slab_address: string; asset_index: number; leg_index: number; trader: string; side: string; size: string; is_liquidation: boolean };
type Fault = "nullNoError" | "emptyPage" | "noId" | "noCount" | "starCount" | "ignoreGt";

const db = {
  rows: [] as Row[],
  maxRows: 1000,
  queries: 0,
  fault: null as null | { page: number; mode: Fault },
};

function serve(st: { inVals?: string[]; eqs: Array<[string, unknown]>; after?: string; lim?: number; count: boolean }) {
  const k = db.queries++;
  const mode = db.fault && db.fault.page === k ? db.fault.mode : null;
  let rows = db.rows.filter((r) => (st.inVals ? st.inVals.includes(r.tx_signature) : true));
  for (const [c, v] of st.eqs) rows = rows.filter((r) => (r as Record<string, unknown>)[c] === v);
  rows = [...rows].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const total = rows.length; // PostgREST counts the filtered rows before gt/limit only when gt is absent (first page)
  if (st.after !== undefined && mode !== "ignoreGt") rows = rows.filter((r) => r.id > st.after!);
  let data: unknown[] | null = rows.slice(0, Math.min(db.maxRows, st.lim ?? Infinity)).map((r) => ({ ...r }));
  if (mode === "nullNoError") data = null;
  if (mode === "emptyPage") data = [];
  if (mode === "noId") data = data!.map((r, i) => (i === data!.length - 1 ? { ...(r as Row), id: undefined } : r));
  // starCount: Content-Range "0-n/*" (Prefer header stripped) -> postgrest-js parseInt("*") = NaN
  const count = !st.count || mode === "noCount" ? null : mode === "starCount" ? Number.NaN : total;
  return { data, error: null, count };
}

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ warn: vi.fn(), error: vi.fn(), info: vi.fn(), debug: vi.fn() }),
  getNetwork: () => "devnet",
  getSupabase: () => ({
    from: () => ({
      select: (_cols: string, opts?: { count?: string }) => {
        const st: Parameters<typeof serve>[0] = { eqs: [], count: opts?.count === "exact" };
        const q: Record<string, unknown> = {};
        q.eq = (c: string, v: unknown) => (st.eqs.push([c, v]), q);
        q.in = (_c: string, v: string[]) => ((st.inVals = v), q);
        q.gt = (_c: string, v: string) => ((st.after = v), q);
        q.order = () => q;
        q.limit = (n: number) => ((st.lim = n), q);
        q.then = (res: (v: unknown) => void, rej: (e: unknown) => void) => Promise.resolve().then(() => serve(st)).then(res, rej);
        return q;
      },
    }),
  }),
}));

import { fetchStoredLegs, fetchStoredLegsMany } from "../../src/db/storedLegs.js";

const id = (n: number) => `00000000-0000-4000-a000-${String(n).padStart(12, "0")}`;
let next = 0;
function seed(legsPerSig: number[], maxRows: number) {
  db.rows = [];
  db.maxRows = maxRows;
  const sigs = legsPerSig.map((_, i) => `sig${i}`);
  legsPerSig.forEach((n, i) => {
    for (let l = 0; l < n; l++) {
      db.rows.push({ id: id(next++), tx_signature: sigs[i], network: "devnet", slab_address: "S", asset_index: 0, leg_index: l, trader: "T", side: "long", size: "100", is_liquidation: false });
    }
  });
  return sigs;
}

beforeEach(() => {
  db.queries = 0;
  db.fault = null;
  next = 0;
});

describe("fetchStoredLegsMany: an incomplete read returns null, never a partial map", () => {
  it("reads every row with no fault (2 chunks, max_rows 50)", async () => {
    const sigs = seed(Array.from({ length: 150 }, (_, i) => i % 3), 50);
    const m = await fetchStoredLegsMany(sigs);
    expect(m).not.toBeNull();
    for (const [i, s] of sigs.entries()) expect(m!.get(s)!.length).toBe(i % 3);
  });

  it("nothing stored -> every signature present and empty", async () => {
    seed([], 1000);
    const m = await fetchStoredLegsMany(["a", "b"]);
    expect(m && [...m.entries()]).toEqual([["a", []], ["b", []]]);
  });

  it("page 0 is data:null with no error (empty 2xx body), legs=[1] -> null", async () => {
    const sigs = seed([1], 50);
    db.fault = { page: 0, mode: "nullNoError" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: map with sig0 -> 0/1 rows
  });

  it("data:null with no error on a later page -> null", async () => {
    const sigs = seed([120], 50);
    db.fault = { page: 1, mode: "nullNoError" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: sig0 -> 50/120 rows
  });

  it("an empty page in the middle of the read (lagging replica) -> null", async () => {
    const sigs = seed([120], 50);
    db.fault = { page: 1, mode: "emptyPage" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: sig0 -> 50/120 rows
  });

  it("an empty first page whose count says 1 row exists, legs=[1] -> null", async () => {
    const sigs = seed([1], 50);
    db.fault = { page: 0, mode: "emptyPage" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: sig0 -> 0/1 rows
  });

  it("a row without id, legs=[17,34] max_rows 50 -> null", async () => {
    const sigs = seed([17, 34], 50);
    db.fault = { page: 0, mode: "noId" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: sig1 -> 33/34 rows
  });

  it("a response with no row count -> null", async () => {
    const sigs = seed([120], 50);
    db.fault = { page: 0, mode: "noCount" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: no count asked; the mid-read empty case could not be caught
  });

  it("a row count of * (NaN) -> null", async () => {
    const sigs = seed([120], 50);
    db.fault = { page: 0, mode: "starCount" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull();
  });

  it("a page that ignores gt(id) and repeats the previous page -> null", async () => {
    const sigs = seed([40], 20);
    db.fault = { page: 1, mode: "ignoreGt" };
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: the repeat page was added again, then a stop at 40 rows with 20 duplicates
  });

  it("a fault in the second chunk fails the whole map", async () => {
    const sigs = seed(Array.from({ length: 150 }, () => 1), 1000);
    db.fault = { page: 2, mode: "emptyPage" }; // chunk 1 = queries 0,1; chunk 2 starts at 2
    expect(await fetchStoredLegsMany(sigs)).toBeNull(); // before: sigs 100..149 -> 0/1 rows
  });
});

describe("fetchStoredLegs (one tx) pages past max_rows", () => {
  for (const [cap, n] of [[50, 51], [100, 250], [1000, 1001]] as const) {
    it(`max_rows ${cap}, ${n} legs -> all ${n} rows`, async () => {
      const [sig] = seed([n], cap);
      const got = await fetchStoredLegs(sig);
      expect(got?.length).toBe(n); // before: cap rows
      expect(new Set(got!.map((r) => r.leg_index)).size).toBe(n);
    });
  }

  it("only this tx's rows on this network", async () => {
    const [a] = seed([3, 2], 1000);
    db.rows.push({ ...db.rows[0], id: id(999), network: "testnet" });
    expect((await fetchStoredLegs(a))?.length).toBe(3);
  });

  for (const mode of ["nullNoError", "emptyPage", "noId", "noCount"] as const) {
    it(`${mode} mid-read -> null`, async () => {
      const [sig] = seed([120], 50);
      db.fault = { page: mode === "noCount" ? 0 : 1, mode };
      expect(await fetchStoredLegs(sig)).toBeNull();
    });
  }
});
