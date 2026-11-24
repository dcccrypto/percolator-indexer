/**
 * Real-path variant of tests/trade-dedup-195.test.ts (GH#195).
 *
 * The existing file only regex-matches the migration SQL and the source TEXT of insertTradeRow.ts; it never
 * runs the writer and never looks at a stored row. This variant:
 *   - starts a throwaway Postgres 16 cluster (k02-pg-runner pattern: initdb in /tmp, unix socket, free port),
 *   - builds the pre-#195 trades shape (baseline columns + an existing uq_trades_sig_asset_leg), seeds duplicates,
 *   - applies the REAL supabase/migrations/20260903120000 file,
 *   - drives the REAL insertTradeRow / insertTradeRows through a getSupabase() stand-in that turns each call
 *     into the statement PostgREST emits (json_populate_recordset INSERT; upsert -> ON CONFLICT (on_conflict)
 *     DO NOTHING | DO UPDATE; no on_conflict -> the primary key; .select(cols) -> RETURNING cols),
 *   - asserts on the rows actually stored.
 * Skips itself when no Postgres binaries are found (TRADE_DEDUP_PG_BIN overrides the path), so CI without
 * Postgres stays green.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const BIN = process.env.TRADE_DEDUP_PG_BIN || "/opt/homebrew/opt/postgresql@16/bin";
const HAVE_PG = fs.existsSync(path.join(BIN, "initdb"));
const MIG = path.join(__dirname, "..", "supabase", "migrations", "20260903120000_trades_dedup_nulls_and_backfill.sql");

const pg = vi.hoisted(() => ({ sql: (_q: string): { out: string; err: string; ok: boolean } => ({ out: "", err: "", ok: false }) }));

type PgErr = { code: string; message: string };
const parseErr = (err: string): PgErr => {
  const m = err.match(/ERROR:\s+([0-9A-Z]{5}):\s*([^\n]*)/);
  return m ? { code: m[1], message: m[2] } : { code: "XX000", message: err.trim() };
};
const lit = (v: unknown) => "$j$" + JSON.stringify(v) + "$j$";

vi.mock("@percolatorct/shared", () => {
  const exec = (q: string) => {
    const r = pg.sql(q);
    return r.ok ? { data: r.out.trim() ? JSON.parse(r.out.trim()) : null, error: null } : { data: null, error: parseErr(r.err) };
  };
  const insertSql = (rows: Record<string, unknown>[], tail: string, returning: string | null) => {
    const cols = [...new Set(rows.flatMap((r) => Object.keys(r)))];
    const c = cols.join(", ");
    const ins = `INSERT INTO trades (${c}) SELECT ${c} FROM json_populate_recordset(NULL::trades, ${lit(rows)}) ${tail}` +
      (returning ? ` RETURNING ${returning}` : "");
    return returning ? `WITH w AS (${ins}) SELECT coalesce(json_agg(w), '[]'::json) FROM w` : ins;
  };
  const from = (t: string) => {
    if (t !== "trades") throw new Error(`unexpected table ${t}`);
    return {
      insert: (row: Record<string, unknown>) => {
        const q = insertSql([row], "", null);
        return { then: (res: (v: unknown) => unknown, rej?: (e: unknown) => unknown) => Promise.resolve(exec(q)).then(res, rej) };
      },
      upsert: (rows: Record<string, unknown>[], opts: { onConflict?: string; ignoreDuplicates?: boolean } = {}) => {
        const cols = [...new Set(rows.flatMap((r) => Object.keys(r)))];
        const target = opts.onConflict ?? "id"; // PostgREST: no on_conflict = the primary key
        const action = opts.ignoreDuplicates
          ? "DO NOTHING"
          : `DO UPDATE SET ${cols.map((k) => `${k} = EXCLUDED.${k}`).join(", ")}`;
        const tail = `ON CONFLICT (${target}) ${action}`;
        return {
          select: (sel: string) => {
            const q = insertSql(rows, tail, sel);
            return { then: (res: (v: unknown) => unknown, rej?: (e: unknown) => unknown) => Promise.resolve(exec(q)).then(res, rej) };
          },
        };
      },
    };
  };
  return {
    getSupabase: () => ({ from }),
    getNetwork: () => "devnet",
    createLogger: () => ({ warn: () => {}, error: () => {}, info: () => {}, debug: () => {} }),
  };
});

import { insertTradeRow, insertTradeRows, tradeKey, type IndexerTradeRow } from "../src/db/insertTradeRow.js";

let data = "", sock = "", port = 0;
const run = (cmd: string, args: string[], input?: string) =>
  spawnSync(path.join(BIN, cmd), args, { input, encoding: "utf8", env: { ...process.env, LC_ALL: "C", LANG: "C" } });
const psql = (q: string) => {
  const r = run("psql", ["-X", "-q", "-At", "-h", sock, "-p", String(port), "-U", "td", "-d", "td", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"],
    "\\set VERBOSITY verbose\n" + q + ";\n");
  return { out: r.stdout ?? "", err: r.stderr ?? "", ok: r.status === 0 };
};
const must = (q: string) => { const r = psql(q); if (!r.ok) throw new Error(`${q.slice(0, 80)}: ${r.err}`); return r.out.trim(); };
const stored = (where = "true") =>
  JSON.parse(must(`SELECT coalesce(json_agg(t ORDER BY tx_signature, asset_index, leg_index), '[]') FROM (SELECT id, tx_signature, asset_index, leg_index, trader, size::text AS size FROM trades WHERE ${where}) t`)) as
    { id: string; tx_signature: string | null; asset_index: number; leg_index: number; trader: string; size: string }[];

const SIG = (c: string) => c.repeat(64) + "AbCdEfGhJkLmNpQrStUvWxYz"; // 88 chars, base58 alphabet
const row = (o: Partial<IndexerTradeRow> = {}): IndexerTradeRow => ({
  slab_address: "Slab1111", trader: "Trader", side: "long", size: "10", price: 1.5, fee: 0.01,
  tx_signature: SIG("a"), asset_index: 0, leg_index: 0, ...o,
});
// Pre-#195 rows (explicit ids so "first" is well-defined): 3 copies of one key, 2 distinct legs + 1 distinct
// asset on another sig, 2 NULL-signature copies, 2 sigs that differ only in case.
const SEED: [string, string | null, number, number, string][] = [
  ["00000000-0000-0000-0000-000000000001", "seedA", 0, 0, "first"],
  ["00000000-0000-0000-0000-000000000002", "seedA", 0, 0, "second"],
  ["00000000-0000-0000-0000-000000000003", "seedA", 0, 0, "third"],
  ["00000000-0000-0000-0000-000000000004", "seedB", 0, 0, "legA"],
  ["00000000-0000-0000-0000-000000000005", "seedB", 0, 1, "legB"],
  ["00000000-0000-0000-0000-000000000006", "seedB", 1, 0, "asset1"],
  ["00000000-0000-0000-0000-000000000007", null, 0, 0, "null1"],
  ["00000000-0000-0000-0000-000000000008", null, 0, 0, "null2"],
  ["00000000-0000-0000-0000-000000000009", "seedC", 0, 0, "upper"],
  ["00000000-0000-0000-0000-00000000000a", "SEEDC", 0, 0, "lower"],
  ["00000000-0000-0000-0000-00000000000b", "seedD", 0, 0, "slab1"],
  ["00000000-0000-0000-0000-00000000000c", "seedD", 0, 0, "slab2"], // same key, other slab: still one fill
];
let migErr = "";

describe.skipIf(!HAVE_PG)("GH#195 dedup against a real Postgres (stored rows, real writer, real migration)", () => {
  beforeAll(async () => {
    data = fs.mkdtempSync(path.join(os.tmpdir(), "td195pg-"));
    sock = fs.mkdtempSync("/tmp/td195s-");
    port = await new Promise<number>((r) => { const s = net.createServer(); s.listen(0, "127.0.0.1", () => { const p = (s.address() as net.AddressInfo).port; s.close(() => r(p)); }); });
    for (const [c, a] of [
      ["initdb", ["-D", data, "-U", "td", "-A", "trust", "--no-sync"]],
      ["pg_ctl", ["-D", data, "-w", "-l", path.join(data, "log"), "-o", `-p ${port} -k ${sock} -c listen_addresses=''`, "start"]],
      ["createdb", ["-h", sock, "-p", String(port), "-U", "td", "td"]],
    ] as [string, string[]][]) { const r = run(c, a); if (r.status !== 0) throw new Error(`${c}: ${r.stderr}`); }
    pg.sql = psql;
    // Baseline trades + later columns.
    must(`CREATE TABLE trades (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), slab_address TEXT NOT NULL, trader TEXT NOT NULL,
      side TEXT CHECK (side IS NULL OR side IN ('long','short')), size NUMERIC, price NUMERIC, fee NUMERIC DEFAULT 0,
      tx_signature TEXT, asset_index INTEGER NOT NULL DEFAULT 0, leg_index SMALLINT NOT NULL DEFAULT 0,
      is_liquidation BOOLEAN NOT NULL DEFAULT false, network TEXT NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
      size_provenance TEXT, price_lot_exp SMALLINT, created_at TIMESTAMPTZ DEFAULT now())`);
    // Seed BEFORE any unique key exists: production reached #195 holding such rows (NULL-sig dups under the
    // NULLS DISTINCT key, older rows from before the composite key); the migration must clean them up.
    // An index named uq_trades_sig_asset_leg already exists in production (20260726181000), so the migration
    // must DROP it before re-creating it. It is non-unique here only because the seeded duplicates exist.
    must(`INSERT INTO trades (id, slab_address, trader, tx_signature, asset_index, leg_index) VALUES ` +
      SEED.map(([id, s, a, l, t]) => `('${id}', '${t === "slab2" ? "Slab2222" : "Slab1111"}', '${t}', ${s === null ? "NULL" : `'${s}'`}, ${a}, ${l})`).join(", "));
    must(`CREATE INDEX uq_trades_sig_asset_leg ON trades (tx_signature, asset_index, leg_index)`);
    const m = psql(fs.readFileSync(MIG, "utf8").replace(/;\s*$/, ""));
    migErr = m.ok ? "" : m.err;
  }, 60_000);
  afterAll(() => {
    if (data) run("pg_ctl", ["-D", data, "-w", "-m", "fast", "stop"]);
    fs.rmSync(data, { recursive: true, force: true });
    fs.rmSync(sock, { recursive: true, force: true });
  });

  it("the migration applies over pre-existing duplicates", () => {
    expect(migErr).toBe("");
  });

  it("backfill keeps exactly one row per key, the lowest id, and never a distinct leg/asset/case", () => {
    const seeded = stored("tx_signature LIKE 'seed%' OR tx_signature LIKE 'SEED%' OR tx_signature IS NULL");
    expect(seeded.map((r) => [r.tx_signature, r.asset_index, r.leg_index, r.trader])).toEqual([
      ["SEEDC", 0, 0, "lower"],
      ["seedA", 0, 0, "first"],
      ["seedB", 0, 0, "legA"],
      ["seedB", 0, 1, "legB"],
      ["seedB", 1, 0, "asset1"],
      ["seedC", 0, 0, "upper"],
      ["seedD", 0, 0, "slab1"],
      [null, 0, 0, "null1"],
    ]);
  });

  it("the built index is exactly (tx_signature, asset_index, leg_index), unique, non-partial, NULLS NOT DISTINCT", () => {
    const def = must(`SELECT pg_get_indexdef('uq_trades_sig_asset_leg'::regclass)`);
    expect(def).toBe("CREATE UNIQUE INDEX uq_trades_sig_asset_leg ON public.trades USING btree (tx_signature, asset_index, leg_index) NULLS NOT DISTINCT");
  });

  it("batch: two legs and two assets of one tx are all stored; re-delivery stores nothing and returns no keys", async () => {
    const legs = [row({ leg_index: 0 }), row({ leg_index: 1, size: "20" }), row({ asset_index: 1, size: "30" })];
    const first = await insertTradeRows(legs);
    expect(new Set(first.map(tradeKey))).toEqual(new Set(legs.map(tradeKey)));
    expect(stored(`tx_signature = '${SIG("a")}'`).map((r) => [r.asset_index, r.leg_index, r.size])).toEqual([[0, 0, "10"], [0, 1, "20"], [1, 0, "30"]]);
    const again = await insertTradeRows(legs.map((l) => ({ ...l, size: "999" })));
    expect(again).toEqual([]);
    expect(stored(`tx_signature = '${SIG("a")}'`).map((r) => r.size)).toEqual(["10", "20", "30"]); // not overwritten
  });

  it("batch: a re-delivery mixing one stored leg with one new leg stores and returns only the new one", async () => {
    const out = await insertTradeRows([row({ leg_index: 0 }), row({ leg_index: 2, size: "40" })]);
    expect(out.map(tradeKey)).toEqual([tradeKey(row({ leg_index: 2 }))]);
    expect(stored(`tx_signature = '${SIG("a")}'`)).toHaveLength(4);
  });

  it("signatures are stored byte-for-byte: case-only and long-shared-prefix sigs are distinct trades", async () => {
    const upper = SIG("B"), lower = SIG("b"), prefixTwin = SIG("B").slice(0, 87) + "Z";
    const rows = [row({ tx_signature: upper }), row({ tx_signature: lower }), row({ tx_signature: prefixTwin })];
    const out = await insertTradeRows(rows);
    expect(out).toHaveLength(3);
    const got = stored(`tx_signature IN ('${upper}', '${lower}', '${prefixTwin}')`).map((r) => r.tx_signature).sort();
    expect(got).toEqual([upper, lower, prefixTwin].sort());
    expect(await insertTradeRow(row({ tx_signature: lower, leg_index: 5 }))).toBe(true);
    expect(stored(`tx_signature = '${lower}'`)).toHaveLength(2);
  });

  it("single-row: a duplicate leg returns false and stores nothing; a real DB error throws", async () => {
    const r = row({ tx_signature: SIG("c") });
    expect(await insertTradeRow(r)).toBe(true);
    expect(await insertTradeRow({ ...r, size: "1" })).toBe(false);
    expect(stored(`tx_signature = '${SIG("c")}'`).map((x) => x.size)).toEqual(["10"]);
    await expect(insertTradeRow(row({ tx_signature: SIG("d"), trader: null as unknown as string }))).rejects.toThrow(/insertTradeRow failed/);
    await expect(insertTradeRows([row({ tx_signature: SIG("d"), trader: null as unknown as string })])).rejects.toThrow(/insertTradeRows failed/);
  });

  it("a missing or empty signature is refused before the DB and nothing is stored", async () => {
    const before = Number(must(`SELECT count(*) FROM trades`));
    for (const s of [null, ""]) {
      await expect(insertTradeRow(row({ tx_signature: s }))).rejects.toThrow(/GH#195/);
      await expect(insertTradeRows([row({ tx_signature: s, leg_index: 9 })])).rejects.toThrow(/GH#195/);
    }
    expect(Number(must(`SELECT count(*) FROM trades`))).toBe(before);
  });

  it("tradeKey never collides across signature case, separators, leg or asset", () => {
    expect(tradeKey({ tx_signature: "aB", asset_index: 0, leg_index: 0 })).not.toBe(tradeKey({ tx_signature: "ab", asset_index: 0, leg_index: 0 }));
    expect(tradeKey({ tx_signature: "x", asset_index: 1, leg_index: 23 })).not.toBe(tradeKey({ tx_signature: "x", asset_index: 12, leg_index: 3 }));
    expect(tradeKey({ tx_signature: "x", asset_index: 0, leg_index: 1 })).not.toBe(tradeKey({ tx_signature: "x", asset_index: 1, leg_index: 0 }));
    expect(tradeKey({ tx_signature: "x", asset_index: 0, leg_index: 0 })).not.toBe(tradeKey({ tx_signature: "x", asset_index: 0, leg_index: 1 }));
    expect(tradeKey({ tx_signature: "x", asset_index: 0, leg_index: 1 })).not.toBe(tradeKey({ tx_signature: "x", asset_index: 1, leg_index: 1 }));
  });
});
