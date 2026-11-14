/**
 * DB round trips to ingest 1000 fixture trades through the poll path (TradeIndexerPolling), counting every call that
 * reaches the Supabase client. RPC is faked; the DB is an in-memory fake that implements just the queries the indexer
 * issues (insert / upsert+select / select with eq,neq,in,order,range) and counts each awaited query as ONE call.
 *
 * Run:  npx vitest run --config bench/vitest.bench.config.ts bench/dbcalls.bench.test.ts
 * Prints: DBCALLS {"trades":1000,"dbCalls":N,"perTable":{...},"rowsSha256":"..."}
 * rowsSha256 = sha256 of the final tables (created_at/id dropped), rows sorted by their JSON.
 *
 * Fixture: tests/fixtures/tradecpi/full.json (one real TradeCpi fill) replayed under 1000 distinct signatures, in
 * 100 txs windows; every 10th tx carries a keeper-crank-only twin so windows are mixed. No position/price tables
 * exist in this indexer's write path (StatsCollector no longer writes them), so trades, cursor and v2.2 events are
 * everything that is written.
 */
import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { PublicKey } from "@solana/web3.js";
import { encodeBase58 } from "../src/lib/base58.js";

const N = 1000;
const full = JSON.parse(readFileSync(new URL("../tests/fixtures/tradecpi/full.json", import.meta.url), "utf8"));
const PROGRAM: string = full.tx.transaction.message.instructions.find((i: any) => i.data?.length > 100).programId;
const pk = (s: string) => new PublicKey(s);
const sigOf = (n: number) => encodeBase58(Uint8Array.from({ length: 64 }, (_, k) => (k === 0 ? 1 : k === 1 ? (n >> 8) + 1 : k === 2 ? (n & 255) + 1 : ((k * 13) % 255) + 1)));
const hydrate = (ix: any) => (ix.parsed ? ix : { ...ix, programId: pk(ix.programId), accounts: (ix.accounts ?? []).map(pk) });
const mkTx = (n: number) => ({
  slot: full.tx.slot + n,
  blockTime: full.tx.blockTime + n,
  meta: { err: null, logMessages: [], innerInstructions: full.tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map(hydrate) })) },
  transaction: { message: { instructions: full.tx.transaction.message.instructions.map(hydrate) } },
});
const tradeIx = full.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
const MARKET: string = tradeIx.accounts[1];
const sigs = Array.from({ length: N }, (_, i) => sigOf(i + 1));
const txs = new Map(sigs.map((s, i) => [s, mkTx(i)]));
const cursorStart = sigOf(0);

// ---- counting fake DB ----
type Row = Record<string, any>;
const tables: Record<string, Row[]> = { trades: [], trade_indexer_cursor: [], v22_events: [], skipped_signatures: [] };
let calls = 0;
const perTable: Record<string, number> = {};
const keyOf: Record<string, string[]> = {
  trades: ["tx_signature", "asset_index", "leg_index"],
  trade_indexer_cursor: ["network", "slab_address"],
  v22_events: ["signature", "ix_index", "inner_index", "network"],
  skipped_signatures: ["signature", "slab"],
};
function builder(table: string) {
  let op: "select" | "insert" | "upsert" = "select";
  let payload: Row[] = [];
  let ignore = false;
  let returning = false;
  const filters: ((r: Row) => boolean)[] = [];
  let range: [number, number] | null = null;
  let limit = Infinity;
  const b: any = {
    select() { returning = true; return b; },
    insert(p: any) { op = "insert"; payload = [].concat(p); return b; },
    upsert(p: any, o: any = {}) { op = "upsert"; payload = [].concat(p); ignore = !!o.ignoreDuplicates; return b; },
    eq(c: string, v: any) { filters.push((r) => r[c] === v); return b; },
    neq(c: string, v: any) { filters.push((r) => r[c] !== v); return b; },
    in(c: string, v: any[]) { filters.push((r) => v.includes(r[c])); return b; },
    order() { return b; },
    range(a: number, z: number) { range = [a, z]; return b; },
    limit(n: number) { limit = n; return b; },
    then(res: (v: any) => any, rej?: (e: any) => any) {
      calls++;
      perTable[table] = (perTable[table] ?? 0) + 1;
      let out: any;
      if (op === "select") {
        let rows = tables[table].filter((r) => filters.every((f) => f(r)));
        if (range) rows = rows.slice(range[0], range[1] + 1);
        out = { data: rows.slice(0, limit).map((r) => ({ ...r })), error: null };
      } else {
        const k = keyOf[table];
        const written: Row[] = [];
        for (const p of payload) {
          const i = tables[table].findIndex((r) => k.every((c) => r[c] === p[c]));
          if (i >= 0) {
            if (op === "insert") { out = { data: null, error: { code: "23505", message: "dup" } }; break; }
            if (!ignore) tables[table][i] = { ...tables[table][i], ...p };
            continue;
          }
          tables[table].push({ id: tables[table].length + 1, created_at: new Date(0).toISOString(), ...p });
          written.push(p);
        }
        out ??= { data: returning ? written.map((r) => ({ ...r })) : null, error: null };
      }
      return Promise.resolve(out).then(res, rej);
    },
  };
  return b;
}

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] }, // = PROGRAM (vi.mock is hoisted)
    getSupabase: () => ({ from: (t: string) => builder(t) }),
    getNetwork: () => "devnet",
    getConnection: () => fakeConnection,
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    createLogger: () => ({ info: noop, warn: (...a: any[]) => { if (process.env.BENCH_DEBUG) console.log("WARN", ...a.map((x) => JSON.stringify(x))); }, error: noop, debug: noop }),
    captureException: noop,
    eventBus: { on: noop, off: noop, publish: noop },
  };
});
vi.mock("../src/db/marketRows.js", () => ({ getMarketRows: async () => [], getMarketRowsCached: async () => [] }));

const fakeConnection = {
  // newest-first, `before` / `until` exclusive
  async getSignaturesForAddress(_a: PublicKey, o: { limit?: number; before?: string; until?: string } = {}) {
    let all = [...sigs].reverse().map((signature) => ({ signature, err: null }));
    if (o.before) { const b = all.findIndex((s) => s.signature === o.before); all = b >= 0 ? all.slice(b + 1) : []; }
    const cut = o.until ? all.findIndex((s) => s.signature === o.until) : -1;
    return (cut >= 0 ? all.slice(0, cut) : all).slice(0, o.limit ?? 1000);
  },
  async getParsedTransactions(list: string[]) { return list.map((s) => txs.get(s) ?? null); },
  async getParsedTransaction(s: string) { return txs.get(s) ?? null; },
  async getAccountInfo() { return { data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") }; },
  async getMultipleAccountsInfo(keys: PublicKey[]) { return keys.map(() => null); },
};

import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";

describe("bench: DB round trips per 1000 trades (poll path)", () => {
  it("counts", async () => {
    const idx: any = new TradeIndexerPolling();
    idx._running = true;
    idx.hasBackfilled = true;
    idx.cursorsLoaded = Promise.resolve();
    idx.lastSignature.set(MARKET, cursorStart);
    await idx.indexTradesForSlab(MARKET, 1000);
    idx._running = false;
    const dump = Object.keys(tables).sort().map((t) =>
      t + "\n" + tables[t].map(({ id: _i, created_at: _c, updated_at: _u, ...r }) => JSON.stringify(r, Object.keys(r).sort())).sort().join("\n"),
    ).join("\n");
    const out = { trades: tables.trades.length, dbCalls: calls, callsPer1000: Math.round((calls / Math.max(1, tables.trades.length)) * 1000), perTable, rowsSha256: createHash("sha256").update(dump).digest("hex").slice(0, 16) };
    console.log("DBCALLS " + JSON.stringify(out));
    expect(tables.trades.length).toBe(N);
  }, 120_000);
});
