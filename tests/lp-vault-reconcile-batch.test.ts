/**
 * LP-vault full reconcile: DB requests and write-failure safety.
 *
 * The real LpVaultIndexer and the real SupabaseLpVaultStore run over an in-memory stand-in for PostgREST (`fakePostgrest`)
 * that counts requests (one awaited query = one HTTP request) and can fail any of them. An upsert is one statement, so a
 * failed upsert applies none of its rows, exactly like PostgreSQL.
 *
 * Measured with the real PostgREST harness (J-20261122-IDX-REPORT-DBCALLSPERDELIVERY): the per-vault reconcile issued
 * 320 requests per 10-min pass over 55 vaults (2 per vault + 4 per open position, one after another).
 *
 * On the per-vault code these fail: the request counts, and "one poison row does not block its siblings" (a vault's
 * reconcile stopped at its first failed write). Every other safety case here holds on both.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const { captured, logs } = vi.hoisted(() => ({
  captured: vi.fn(),
  logs: { error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() },
}));
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: async () => {} }));
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, createLogger: () => logs, captureException: (...a: unknown[]) => captured(...a) };
});

import type { SupabaseClient } from "@supabase/supabase-js";
import { LpVaultIndexer } from "../src/services/LpVaultIndexer.js";
import { SupabaseLpVaultStore, type LpVaultStore } from "../src/lpVault/store.js";
import type { LpVaultChain, OnchainClaim, RegistryInfo, SigInfo } from "../src/lpVault/chain.js";

type Row = Record<string, unknown>;
interface Req {
  table: string;
  verb: "select" | "upsert" | "maybeSingle";
  filters: { col: string; op: "eq" | "in" | "or"; val: unknown }[];
  rows?: Row[];
}
const PK: Record<string, string[]> = {
  lp_vault_positions: ["network", "registry", "user_wallet"],
  lp_vault_events: ["signature", "ix_index"],
  lp_vault_sync_cursor: ["network", "registry"],
};

/** Enough of PostgREST for SupabaseLpVaultStore: eq / in / or(col.gt.0,...) / order / range / maybeSingle / upsert. */
function fakePostgrest(opts: { maxRows?: number; shuffleTies?: boolean } = {}) {
  let reqNo = 0;
  // Deterministic per-request tie order (FNV-1a of request number + row), standing in for PostgreSQL's unspecified one.
  const tieKey = (row: Row, n: number) => {
    let h = 2166136261;
    for (const ch of `${n}|${row.registry}|${row.user_wallet}|${row.signature}|${row.ix_index}`) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
    return h >>> 0;
  };
  const tables: Record<string, Row[]> = { lp_vault_positions: [], lp_vault_events: [], lp_vault_sync_cursor: [] };
  const log: Req[] = [];
  let fail: ((r: Req) => string | null) | null = null;
  const num = (v: unknown) => (v === null || v === undefined ? null : BigInt(String(v)));
  const matches = (row: Row, f: Req["filters"][number]) => {
    if (f.op === "eq") return row[f.col] === f.val;
    if (f.op === "in") return (f.val as unknown[]).includes(row[f.col]);
    return String(f.val).split(",").some((cond) => {
      const [col, op, v] = cond.split(".");
      if (op !== "gt") throw new Error(`fake: unsupported or() ${cond}`);
      const x = num(row[col!]);
      return x !== null && x > BigInt(v!);
    });
  };
  const cmp = (a: unknown, b: unknown) => {
    if (typeof a === "number" && typeof b === "number") return a - b;
    return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
  };
  const project = (row: Row, select: string) => {
    const out: Row = {};
    for (const c of select.split(",")) {
      const [name, cast] = c.trim().split("::");
      const v = row[name!];
      out[name!] = cast === "text" && v !== null && v !== undefined ? String(v) : v ?? null;
    }
    return out;
  };
  const db = {
    from(table: string) {
      const req: Req = { table, verb: "select", filters: [] };
      let select = "*";
      const orders: { col: string; asc: boolean }[] = [];
      const run = (range?: [number, number], single = false) => {
        log.push(req);
        const msg = fail?.(req) ?? null;
        if (msg) return Promise.resolve({ data: null, error: { message: msg } });
        if (req.verb === "upsert") {
          const key = PK[table]!;
          // One statement: every row applies, or (on the error above) none does.
          for (const r of req.rows!) {
            const i = tables[table]!.findIndex((x) => key.every((k) => x[k] === r[k]));
            if (i >= 0) tables[table]![i] = { ...tables[table]![i], ...r };
            else tables[table]!.push({ ...r });
          }
          return Promise.resolve({ data: null, error: null });
        }
        let rows = tables[table]!.filter((r) => req.filters.every((f) => matches(r, f)));
        const n = reqNo++;
        rows = [...rows].sort((a, b) => {
          for (const o of orders) {
            const c = cmp(a[o.col], b[o.col]);
            if (c !== 0) return o.asc ? c : -c;
          }
          return opts.shuffleTies ? tieKey(a, n) - tieKey(b, n) : 0;
        });
        if (single) return Promise.resolve({ data: rows[0] ? project(rows[0], select) : null, error: null });
        if (range) rows = rows.slice(range[0], range[1] + 1);
        if (opts.maxRows !== undefined) rows = rows.slice(0, opts.maxRows);
        return Promise.resolve({ data: rows.map((r) => project(r, select)), error: null });
      };
      const q = {
        select(s: string) { select = s; return q; },
        eq(col: string, val: unknown) { req.filters.push({ col, op: "eq", val }); return q; },
        in(col: string, val: unknown[]) { req.filters.push({ col, op: "in", val: [...val] }); return q; },
        or(val: string) { req.filters.push({ col: "", op: "or", val }); return q; },
        order(col: string, o?: { ascending?: boolean }) { orders.push({ col, asc: o?.ascending !== false }); return q; },
        range(from: number, to: number) { return run([from, to]); },
        maybeSingle() { req.verb = "maybeSingle"; return run(undefined, true); },
        upsert(rows: Row | Row[]) { req.verb = "upsert"; req.rows = Array.isArray(rows) ? rows : [rows]; return run(); },
      };
      return q;
    },
  };
  return {
    db: db as unknown as SupabaseClient,
    tables,
    log,
    failWhen(f: ((r: Req) => string | null) | null) { fail = f; },
  };
}

const NET = "devnet";
const CURSOR = "sig_head";
const regName = (i: number) => `REG${String(i).padStart(3, "0")}`;
const userName = (i: number) => `user${String(i).padStart(2, "0")}`;

/** Seed `nReg` vaults with `nUser` open positions each (2 deposits each, folded: 300 shares), cursor at CURSOR. */
function seed(pg: ReturnType<typeof fakePostgrest>, nReg: number, nUser: number, eventsPerUser = 2) {
  const regs: RegistryInfo[] = [];
  for (let r = 0; r < nReg; r++) {
    const registry = regName(r);
    regs.push({ registry, programId: "PROG", market: `MKT${r}`, lpMint: `MINT${r}` });
    pg.tables.lp_vault_sync_cursor!.push({ network: NET, registry, signature: CURSOR, slot: 1_000 });
    for (let u = 0; u < nUser; u++) {
      const user = userName(u);
      for (let e = 0; e < eventsPerUser; e++) {
        pg.tables.lp_vault_events!.push({
          signature: `${registry}-${user}-${e}`, ix_index: 0, network: NET, kind: "deposit", program_id: "PROG",
          market_slab: `MKT${r}`, registry, user_wallet: user, lp_mint: `MINT${r}`, collateral_atoms: e === 0 ? "100" : "200",
          lp_amount: e === 0 ? "100" : "200", domain: null, slot: 10 + e, block_time: null,
        });
      }
      pg.tables.lp_vault_positions!.push({
        network: NET, registry, user_wallet: user, market_slab: `MKT${r}`, lp_mint: `MINT${r}`, lp_shares: "300",
        pending_redeem_shares: "0", cost_basis_atoms: "300", realized_pnl_atoms: "0", total_deposited_atoms: "300",
        total_redeemed_atoms: "0", basis_known: true, inconsistency: null, transfer_detected_slot: null,
        onchain_lp_shares: null, reconciled_slot: null, event_count: eventsPerUser, updated_slot: 11, updated_at: "seed",
      });
    }
  }
  return regs;
}

/** Chain with nothing new since CURSOR. user01 of every vault holds 50 more shares than indexed (a transfer). */
function chainFor(regs: RegistryInfo[]): LpVaultChain & { claimErr: Map<string, Error> } {
  const claimErr = new Map<string, Error>();
  return {
    claimErr,
    async listRegistries() { return regs; },
    async getSignatures(_a: string, o: { until?: string }): Promise<SigInfo[]> {
      return o.until === CURSOR ? [] : [{ signature: CURSOR, slot: 1_000, failed: false }];
    },
    async getTransaction() { throw new Error("no new transactions in this test"); },
    async getClaim(user: string, _mint: string, registry: string): Promise<OnchainClaim> {
      const e = claimErr.get(`${registry}|${user}`);
      if (e) throw e;
      return { heldShares: user === userName(1) ? 350n : 300n, pendingShares: 0n, contextSlot: 2_000 };
    },
  };
}

/** The store minus its batch methods: the per-vault reconcile path. */
function perVault(store: LpVaultStore): LpVaultStore {
  const hide = new Set(["listOpenPositionsMany", "loadEventsMany", "writePositions"]);
  return new Proxy(store, {
    get(t, p, r) {
      if (hide.has(String(p))) return undefined;
      const v = Reflect.get(t, p, r);
      return typeof v === "function" ? v.bind(t) : v;
    },
  });
}

/**
 * Seed, load every vault's cursor (outside the count), then count ONE tick: discovery, a sync with nothing new
 * (no DB request) and the full reconcile.
 */
async function reconcileTick(pg: ReturnType<typeof fakePostgrest>, regs: RegistryInfo[], wrap: (s: LpVaultStore) => LpVaultStore = (s) => s) {
  const chain = chainFor(regs);
  const store = wrap(new SupabaseLpVaultStore(pg.db));
  const ix = new LpVaultIndexer({ chain, store, network: NET, programIds: ["PROG"], rpcCallTimeoutMs: 60_000 });
  for (const reg of regs) await ix.syncRegistry(reg);
  pg.log.length = 0;
  return { ix, chain, tick: () => ix.tick() };
}

/** Positions table without the write timestamp, keyed and sorted, for before/after and path-vs-path comparisons. */
const snapshot = (pg: ReturnType<typeof fakePostgrest>) =>
  pg.tables.lp_vault_positions!
    .map(({ updated_at: _u, ...r }) => r)
    .sort((a, b) => `${a.registry}|${a.user_wallet}`.localeCompare(`${b.registry}|${b.user_wallet}`));
const row = (pg: ReturnType<typeof fakePostgrest>, registry: string, user: string) =>
  pg.tables.lp_vault_positions!.find((r) => r.registry === registry && r.user_wallet === user)!;
const reconciled = (r: Row) => r.reconciled_slot === 2_000;

/** The result every case is checked against: the per-vault path, no faults. */
async function referenceSnapshot(nReg: number, nUser: number, maxRows?: number) {
  const pg = fakePostgrest({ maxRows });
  const regs = seed(pg, nReg, nUser);
  const { tick } = await reconcileTick(pg, regs, perVault);
  await tick();
  return snapshot(pg);
}

beforeEach(() => {
  captured.mockClear();
  Object.values(logs).forEach((f) => f.mockClear());
});

describe("LP-vault full reconcile: DB requests per pass", () => {
  it("10 vaults x 3 open positions: 5 requests (was 2 per vault + 4 per position = 140), same rows as the per-vault path", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 10, 3);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    const byKind = pg.log.map((r) => `${r.table}.${r.verb}`);
    // positions read (1 page + the empty page that ends it), events read (same), one bulk upsert of 30 rows
    expect.soft(byKind).toEqual([
      "lp_vault_positions.select", "lp_vault_positions.select",
      "lp_vault_events.select", "lp_vault_events.select",
      "lp_vault_positions.upsert",
    ]);
    expect.soft(pg.log.at(-1)?.rows?.length).toBe(30);
    expect(snapshot(pg)).toEqual(await referenceSnapshot(10, 3));
    expect(snapshot(pg).filter(reconciled)).toHaveLength(30);
    expect(row(pg, "REG004", "user01").transfer_detected_slot).toBe(2_000); // the 50-share mismatch is recorded
  });

  it("150 vaults (more than one `in` chunk): 9 requests (was 900), same rows", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 150, 1);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    // 2 registry chunks x 2 pages + 2 position chunks x 2 pages + 1 upsert
    expect.soft(pg.log).toHaveLength(9);
    expect(snapshot(pg)).toEqual(await referenceSnapshot(150, 1));
  });

  it("pages correctly when PostgREST caps every response (max_rows 7 < page size)", async () => {
    const pg = fakePostgrest({ maxRows: 7 });
    const regs = seed(pg, 6, 4);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    expect(snapshot(pg)).toEqual(await referenceSnapshot(6, 4, 7));
    expect(snapshot(pg).filter(reconciled)).toHaveLength(24);
  });
});

describe("LP-vault full reconcile: write failures are at least as safe as per-row writes", () => {
  it("a transiently failing bulk write: nothing half-written, every row lands row by row, the failure is logged", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 4, 3);
    const { tick } = await reconcileTick(pg, regs);
    let failed = 0;
    pg.failWhen((r) => (r.verb === "upsert" && r.table === "lp_vault_positions" && r.rows!.length > 1 && failed++ === 0 ? "connection reset" : null));
    await tick();
    expect(snapshot(pg)).toEqual(await referenceSnapshot(4, 3));
    if (failed > 0) expect(logs.warn).toHaveBeenCalledWith(expect.stringContaining("write failed"), expect.objectContaining({ error: expect.stringContaining("connection reset") }));
    expect(captured).not.toHaveBeenCalled();
  });

  it("one poison row: only that row stays at its previous state (whole), its siblings are reconciled, the error is reported", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 4, 3);
    const before = snapshot(pg);
    const { tick } = await reconcileTick(pg, regs);
    const poison = (r: Row) => r.registry === "REG002" && r.user_wallet === "user01";
    // A row the DB refuses (think CHECK violation): any statement containing it fails as a whole.
    pg.failWhen((r) => (r.verb === "upsert" && r.table === "lp_vault_positions" && r.rows!.some(poison) ? "new row violates check constraint" : null));
    await tick();
    const ref = await referenceSnapshot(4, 3);
    const after = snapshot(pg);
    expect.soft(after.filter(poison)).toEqual(before.filter(poison)); // not half-written: exactly the old row
    expect.soft(after.filter((r) => !poison(r))).toEqual(ref.filter((r) => !poison(r)));
    // The per-vault path stopped REG002 at user01, so user02 of the same vault was never reconciled.
    expect.soft(reconciled(row(pg, "REG002", "user02"))).toBe(true);
    expect(captured).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("violates check constraint") }),
      expect.objectContaining({ tags: { context: "lp-vault-sync" }, extra: expect.objectContaining({ registry: "REG002" }) }),
    );
    expect(logs.error).toHaveBeenCalledWith("LP-vault sync failed", expect.objectContaining({ registry: "REG002" }));
    expect(captured.mock.calls.filter((c) => (c[1] as { extra: { registry: string } }).extra.registry !== "REG002")).toEqual([]);
  });

  it("every position write fails: no row changes at all, every vault's failure is reported, the tick does not throw", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 4, 3);
    const before = snapshot(pg);
    const { tick } = await reconcileTick(pg, regs);
    pg.failWhen((r) => (r.verb === "upsert" && r.table === "lp_vault_positions" ? "permission denied for table lp_vault_positions" : null));
    await expect(tick()).resolves.toBeUndefined();
    expect(snapshot(pg)).toEqual(before);
    const reported = new Set(captured.mock.calls.map((c) => (c[1] as { extra: { registry: string } }).extra.registry));
    expect(reported).toEqual(new Set(regs.map((r) => r.registry)));
    for (const c of captured.mock.calls) expect((c[0] as Error).message).toContain("permission denied");
    expect(pg.tables.lp_vault_sync_cursor!.every((c) => c.signature === CURSOR)).toBe(true);
  });

  it("the next pass after a failed write reconciles everything (nothing is lost for good)", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 3, 2);
    const chain = chainFor(regs);
    const ix = new LpVaultIndexer({ chain, store: new SupabaseLpVaultStore(pg.db), network: NET, programIds: ["PROG"], reconcileIntervalMs: -1 });
    pg.failWhen((r) => (r.verb === "upsert" && r.table === "lp_vault_positions" ? "statement timeout" : null));
    await ix.tick();
    expect(snapshot(pg).filter(reconciled)).toHaveLength(0);
    pg.failWhen(null);
    await ix.tick();
    expect(snapshot(pg)).toEqual(await referenceSnapshot(3, 2));
  });
});

describe("LP-vault full reconcile: read and chain failures", () => {
  it("a failed batched read falls back vault by vault: everything is still reconciled", async () => {
    for (const table of ["lp_vault_positions", "lp_vault_events"]) {
      const pg = fakePostgrest();
      const regs = seed(pg, 4, 2);
      const { tick } = await reconcileTick(pg, regs);
      // Only reads spanning several vaults fail (the per-vault reads filter registry with eq, not in).
      pg.failWhen((r) => (r.table === table && r.filters.some((f) => f.op === "in" && f.col === "registry") ? "upstream timeout" : null));
      await tick();
      expect(snapshot(pg), table).toEqual(await referenceSnapshot(4, 2));
      expect(captured).not.toHaveBeenCalled();
    }
  });

  it("one vault's reads fail: that vault is reported, the others are reconciled", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 4, 2);
    const before = snapshot(pg);
    const { tick } = await reconcileTick(pg, regs);
    pg.failWhen((r) => {
      if (r.table !== "lp_vault_events") return null;
      const reg = r.filters.find((f) => f.col === "registry");
      const has = reg && (reg.op === "eq" ? reg.val === "REG001" : (reg.val as string[]).includes("REG001"));
      return has ? "upstream timeout" : null;
    });
    await tick();
    const ref = await referenceSnapshot(4, 2);
    const isBad = (r: Row) => r.registry === "REG001";
    expect.soft(snapshot(pg).filter(isBad)).toEqual(before.filter(isBad));
    expect.soft(snapshot(pg).filter((r) => !isBad(r))).toEqual(ref.filter((r) => !isBad(r)));
    expect(new Set(captured.mock.calls.map((c) => (c[1] as { extra: { registry: string } }).extra.registry))).toEqual(new Set(["REG001"]));
  });

  it("a position over the fold limit is refused for its vault only", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 3, 1);
    for (let e = 2; e < 10_000; e++) {
      pg.tables.lp_vault_events!.push({
        signature: `REG001-user00-${e}`, ix_index: 0, network: NET, kind: "deposit", program_id: "PROG", market_slab: "MKT1",
        registry: "REG001", user_wallet: "user00", lp_mint: "MINT1", collateral_atoms: "1", lp_amount: "1", domain: null,
        slot: 10 + e, block_time: null,
      });
    }
    const before = snapshot(pg);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    const isBad = (r: Row) => r.registry === "REG001";
    expect(snapshot(pg).filter(isBad)).toEqual(before.filter(isBad));
    expect(snapshot(pg).filter((r) => !isBad(r)).every(reconciled)).toBe(true);
    expect(captured).toHaveBeenCalledWith(expect.objectContaining({ message: expect.stringContaining("fold refused") }), expect.objectContaining({ extra: expect.objectContaining({ registry: "REG001" }) }));
  });

  it("a store that returns no history for a position: that vault is reported and nothing of it is folded as empty", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 3, 2);
    const before = snapshot(pg);
    const dropKey = (store: LpVaultStore): LpVaultStore => new Proxy(store, {
      get(t, p, r) {
        const v = Reflect.get(t, p, r);
        if (p !== "loadEventsMany" || typeof v !== "function") return typeof v === "function" ? v.bind(t) : v;
        return async (...a: unknown[]) => {
          const m = (await v.apply(t, a)) as Map<string, unknown>;
          m.delete("REG001|user00");
          return m;
        };
      },
    });
    const { tick } = await reconcileTick(pg, regs, dropKey);
    await tick();
    const isBad = (r: Row) => r.registry === "REG001";
    if (typeof (SupabaseLpVaultStore.prototype as { loadEventsMany?: unknown }).loadEventsMany === "function") {
      expect(snapshot(pg).filter(isBad)).toEqual(before.filter(isBad));
      expect(captured).toHaveBeenCalledWith(expect.objectContaining({ message: expect.stringContaining("no history") }), expect.objectContaining({ extra: expect.objectContaining({ registry: "REG001" }) }));
    }
    expect(snapshot(pg).filter((r) => !isBad(r)).every(reconciled)).toBe(true);
  });

  it("a failed chain read stops that vault only; its earlier users and every other vault are reconciled", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 3, 3);
    const { tick, chain } = await reconcileTick(pg, regs);
    chain.claimErr.set("REG001|user01", new Error("rpc 503"));
    await tick();
    expect(reconciled(row(pg, "REG001", "user00"))).toBe(true);
    expect(reconciled(row(pg, "REG001", "user01"))).toBe(false);
    for (const r of snapshot(pg).filter((x) => x.registry !== "REG001")) expect(reconciled(r)).toBe(true);
    expect(captured).toHaveBeenCalledWith(expect.objectContaining({ message: "rpc 503" }), expect.objectContaining({ extra: expect.objectContaining({ registry: "REG001" }) }));
  });
});

// ── Mutation killers (J-20261123-IDX-REPORT-LPRECONCILEBATCHSAFETYMUT: 28/44 -> 42/44 mutants of the batched reads,
// writes and reconcile killed; S01 and S17 are equivalent). Mutant ids are in the comments. `shuffleTies` on the fake
// returns rows the query's ORDER BY leaves tied in a per-request order (as PostgreSQL may), so a read whose order is
// not total pages wrong.

const upserts = (pg: ReturnType<typeof fakePostgrest>) =>
  pg.log.filter((r) => r.table === "lp_vault_positions" && r.verb === "upsert").map((r) => r.rows!.length);

describe("LP-vault full reconcile: write and read boundaries (1 row, 101 positions, 501 rows)", () => {
  // S06, S22, I03, I12: a pass with exactly one open position must still read its history and write its row.
  it("1 vault x 1 open position: the row is reconciled in one upsert, nothing is reported", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 1, 1);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    expect.soft(upserts(pg)).toEqual([1]);
    expect(snapshot(pg)).toEqual(await referenceSnapshot(1, 1));
    expect(reconciled(row(pg, "REG000", "user00"))).toBe(true);
    expect(captured).not.toHaveBeenCalled();
  });

  // S06: 101 positions = one full `in` chunk + a chunk of 1.
  it("101 open positions (100 + 1 in the events chunks): every one is reconciled", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 101, 1);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    // loadEventsMany pre-fills every wanted key with [], so a chunk it never reads is folded as EMPTY (row zeroed:
    // lp_shares 0, cost basis 0, sticky transfer flag), not caught by the indexer's "no history" guard.
    expect.soft(row(pg, "REG100", "user00")).toMatchObject({ lp_shares: "300", cost_basis_atoms: "300", transfer_detected_slot: null });
    expect(snapshot(pg)).toEqual(await referenceSnapshot(101, 1));
    expect(captured).not.toHaveBeenCalled();
  });

  // I02, I03, I04: 501 rows = a full write chunk of 500 + a chunk of 1; each row in exactly one statement.
  it("501 rows: two upserts of 500 and 1, every row reconciled", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 167, 3);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    expect.soft(upserts(pg)).toEqual([500, 1]);
    const written = pg.log.filter((r) => r.verb === "upsert").flatMap((r) => r.rows!.map((x) => `${x.registry}|${x.user_wallet}`));
    expect.soft(new Set(written).size).toBe(written.length);
    expect(snapshot(pg).filter(reconciled)).toHaveLength(501);
  });
});

describe("LP-vault full reconcile: which rows the batched read lists and folds", () => {
  // S15: a row with 0 indexed shares but on-chain shares (LP transferred in) is still open and must be re-reconciled.
  it("a position open only on chain (lp_shares 0, onchain 50) is reconciled; its on-chain balance is refreshed", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 2, 1);
    pg.tables.lp_vault_positions!.push({
      ...row(pg, "REG000", "user00"), user_wallet: "userT", lp_shares: "0", cost_basis_atoms: "0", total_deposited_atoms: "0",
      basis_known: false, transfer_detected_slot: 1_500, onchain_lp_shares: "50", reconciled_slot: 1_500, event_count: 0, updated_slot: null,
    });
    const { tick, chain } = await reconcileTick(pg, regs);
    const getClaim = chain.getClaim.bind(chain);
    chain.getClaim = async (user, mint, registry, programId) =>
      user === "userT" ? { heldShares: 0n, pendingShares: 0n, contextSlot: 2_000 } : getClaim(user, mint, registry, programId);
    await tick();
    const t = row(pg, "REG000", "userT");
    expect(t.onchain_lp_shares).toBe("0");
    expect(t.reconciled_slot).toBe(2_000);
    expect(t.transfer_detected_slot).toBeNull(); // 0 on both sides = full exit
  });

  // S16: a closed position (0 indexed, 0 on chain) is not open; the full reconcile must leave it alone.
  it("a closed position is not reconciled (no claim read, row untouched)", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 2, 1);
    pg.tables.lp_vault_positions!.push({
      ...row(pg, "REG001", "user00"), user_wallet: "userC", lp_shares: "0", onchain_lp_shares: "0", reconciled_slot: 900,
      event_count: 0, updated_at: "closed",
    });
    const { tick, chain } = await reconcileTick(pg, regs);
    const asked: string[] = [];
    const getClaim = chain.getClaim.bind(chain);
    chain.getClaim = async (user, mint, registry, programId) => (asked.push(user), getClaim(user, mint, registry, programId));
    await tick();
    expect.soft(asked).not.toContain("userC");
    expect(row(pg, "REG001", "userC")).toMatchObject({ reconciled_slot: 900, updated_at: "closed" });
  });

  // S18: lp_vault_events holds every network; another network's events of the same registry/user must not be folded.
  it("another network's events for the same registry and user are not folded into this network's row", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 2, 1);
    pg.tables.lp_vault_events!.push({
      ...pg.tables.lp_vault_events!.find((e) => e.registry === "REG000" && e.user_wallet === "user00")!,
      signature: "testnet-sig", network: "testnet", collateral_atoms: "999", lp_amount: "999", slot: 50,
    });
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    expect(row(pg, "REG000", "user00")).toMatchObject({ lp_shares: "300", event_count: 2, reconciled_slot: 2_000 });
  });

  // S14: page under a total order. With ties shuffled per request, an order on user_wallet alone skips/repeats rows.
  it("pages correctly when PostgREST caps responses and returns tied rows in any order", async () => {
    const pg = fakePostgrest({ maxRows: 7, shuffleTies: true });
    const regs = seed(pg, 6, 4);
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    const written = pg.log.filter((r) => r.verb === "upsert").flatMap((r) => r.rows!.map((x) => `${x.registry}|${x.user_wallet}`));
    expect.soft(new Set(written).size).toBe(written.length);
    expect(snapshot(pg).filter(reconciled)).toHaveLength(24);
  });
});

describe("LP-vault full reconcile: the batched path keeps the per-vault skip and sticky flags", () => {
  // I15, I21: a vault tx that landed after this pass's sync: that vault's rows must not be reconciled (the claim may
  // include it, so a mismatch would set a sticky transfer flag on a good row).
  it("a vault whose newest signature moved after the sync is skipped; the other vaults are reconciled", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 3, 2);
    const before = snapshot(pg);
    const { tick, chain } = await reconcileTick(pg, regs);
    const getSignatures = chain.getSignatures.bind(chain);
    chain.getSignatures = async (a, o) =>
      a === "REG002" && !o.until ? [{ signature: "landed-after-sync", slot: 1_500, failed: false }] : getSignatures(a, o);
    await tick();
    const isBad = (r: Row) => r.registry === "REG002";
    expect(snapshot(pg).filter(isBad)).toEqual(before.filter(isBad));
    expect(snapshot(pg).filter((r) => !isBad(r)).every(reconciled)).toBe(true);
    expect(row(pg, "REG002", "user01").transfer_detected_slot).toBeNull();
  });

  // I16: transfer_detected_slot is sticky: once set, a later matching claim does not clear it (basis stays unknown).
  it("a previously flagged position keeps its transfer flag when the claim now matches", async () => {
    const pg = fakePostgrest();
    const regs = seed(pg, 2, 1);
    Object.assign(row(pg, "REG001", "user00"), { transfer_detected_slot: 1_500, basis_known: false, onchain_lp_shares: "250" });
    const { tick } = await reconcileTick(pg, regs);
    await tick();
    expect(row(pg, "REG001", "user00")).toMatchObject({ transfer_detected_slot: 1_500, basis_known: false, reconciled_slot: 2_000, onchain_lp_shares: "300" });
  });
});
