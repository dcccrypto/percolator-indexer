/**
 * Behaviour tests for cleanup-corrupted-stats main(): paged reads past
 * PostgREST max_rows, and failure propagation (throws / non-zero exit).
 * Supabase is fully mocked; no real database is touched.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from "vitest";

const SERVER_MAX_ROWS = 1000;
interface Row { slab_address: string; decimals: number; total_open_interest: number; mint_address: string; insurance_balance: number; c_tot: number }
let rows: Row[] = [];
let updates: { patch: any; slab: string }[] = [];
let failUpdateFor: Set<string> = new Set();
let selectError: any = null;
let selectCalls = 0;

const isBadDec = (r: Row) => r.decimals > 18 || r.decimals < 0;
const isBadStats = (r: Row) => r.total_open_interest > 5e17 || r.insurance_balance > 5e17 || r.c_tot > 5e17;

// O(1) lookup for the mock UPDATE: a linear rows.find() per update made the
// 2,001-row spec quadratic. Rebuilt only when `rows` is reassigned (not on in-place push).
let indexedRows: Row[] | null = null;
let indexed = new Map<string, Row>();
function rowIndex(): Map<string, Row> {
  if (indexedRows !== rows) { indexedRows = rows; indexed = new Map(rows.map((r) => [r.slab_address, r])); }
  return indexed;
}

function makeSupabase() {
  return {
    from: () => ({
      select: () => {
        selectCalls++;
        let pred: (r: Row) => boolean = () => true;
        let after: string | null = null;
        let lim = Infinity;
        const q: any = {
          eq: () => q,
          or: (s: string) => { pred = s.startsWith("decimals") ? isBadDec : isBadStats; return q; },
          order: () => q,
          gt: (_c: string, v: string) => { after = v; return q; },
          limit: (n: number) => { lim = n; return q; },
          then: (res: any, rej: any) => {
            if (selectError) return Promise.resolve({ data: null, error: selectError }).then(res, rej);
            const data = rows
              .filter(pred)
              .filter((r) => after === null || r.slab_address > after)
              .sort((a, b) => (a.slab_address < b.slab_address ? -1 : 1))
              .slice(0, Math.min(lim, SERVER_MAX_ROWS));
            return Promise.resolve({ data, error: null }).then(res, rej);
          },
        };
        return q;
      },
      update: (patch: any) => {
        let slab = "";
        const q: any = {
          eq: (c: string, v: string) => {
            if (c === "slab_address") slab = v;
            if (c !== "network") return q;
            return q;
          },
          then: (res: any, rej: any) => {
            updates.push({ patch, slab });
            if (failUpdateFor.has(slab)) return Promise.resolve({ error: { message: "boom " + slab } }).then(res, rej);
            const r = rowIndex().get(slab)!;
            if ("decimals" in patch) r.decimals = patch.decimals;
            if ("c_tot" in patch) { r.c_tot = 0; r.total_open_interest = 0; r.insurance_balance = 0; }
            return Promise.resolve({ error: null }).then(res, rej);
          },
        };
        return q;
      },
    }),
  };
}

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => makeSupabase(),
  getNetwork: () => "devnet",
}));

const mk = (i: number, over: Partial<Row> = {}): Row => ({
  slab_address: "S" + String(i).padStart(6, "0"),
  decimals: 6, total_open_interest: 0, mint_address: "M", insurance_balance: 0, c_tot: 0, ...over,
});

// Warm the module graph once, with its own hook budget, so the first spec's
// timeout does not pay the cold import. 2,001 rows = two full 1000-row pages
// plus a one-row third page, so paging past max_rows is still proven.
beforeAll(async () => {
  await import("../../src/scripts/cleanup-corrupted-stats.js");
}, 120_000);

beforeEach(() => {
  rows = []; updates = []; failUpdateFor = new Set(); selectError = null; selectCalls = 0;
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
});

describe("cleanup-corrupted-stats main()", () => {
  it("fixes every corrupted row even past the 1000-row page limit", async () => {
    rows = Array.from({ length: 2001 }, (_, i) => mk(i, { decimals: 99, c_tot: 1e30 }));
    const { main } = await import("../../src/scripts/cleanup-corrupted-stats.js");
    await main();
    expect(rows.filter(isBadDec)).toHaveLength(0);
    expect(rows.filter(isBadStats)).toHaveLength(0);
    expect(updates.filter((u) => "decimals" in u.patch)).toHaveLength(2001);
    expect(updates.filter((u) => "c_tot" in u.patch)).toHaveLength(2001);
  });

  it("does not loop forever when updates fail, and still visits all rows", async () => {
    rows = Array.from({ length: 1200 }, (_, i) => mk(i, { decimals: 99 }));
    failUpdateFor = new Set(["S000005"]);
    const { main } = await import("../../src/scripts/cleanup-corrupted-stats.js");
    await expect(main()).rejects.toThrow(/1 .*failed|failed/i);
    expect(updates.filter((u) => "decimals" in u.patch)).toHaveLength(1200);
  });

  it("rejects (non-zero exit path) when a stats update fails, after attempting all rows", async () => {
    rows = [mk(1, { c_tot: 1e30 }), mk(2, { c_tot: 1e30 })];
    failUpdateFor = new Set(["S000001"]);
    const { main } = await import("../../src/scripts/cleanup-corrupted-stats.js");
    await expect(main()).rejects.toThrow();
    expect(updates.filter((u) => "c_tot" in u.patch)).toHaveLength(2);
  });

  it("rejects on a select error", async () => {
    selectError = { message: "db down" };
    const { main } = await import("../../src/scripts/cleanup-corrupted-stats.js");
    await expect(main()).rejects.toBeTruthy();
  });

  it("resolves and prints success only when nothing failed", async () => {
    rows = [mk(1, { decimals: 40 })];
    const { main } = await import("../../src/scripts/cleanup-corrupted-stats.js");
    await expect(main()).resolves.toBeUndefined();
    expect(rows[0].decimals).toBe(6);
  });
});
