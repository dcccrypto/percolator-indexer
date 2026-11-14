/**
 * Replay bench: TradeIndexerPolling cold backfill over a RECORDED fixture (no network, no DB).
 *
 * Fixture: bench/fixtures/v22-6markets-20261113.json.gz = getSignaturesForAddress(limit 100) for the 6 markets of the
 * live v2.2 keeper registry plus every unique parsed transaction, recorded read-only from public devnet on 2026-11-13.
 * RPC and DB are fakes with a fixed latency (BENCH_RPC_MS, BENCH_DB_MS) and call counters. The trade-indexer code under
 * test is the real one (decode, dedup, v2.2 log events, cursor logic); only the network and the DB are replaced.
 *
 * Run:  npx vitest run --config bench/vitest.bench.config.ts
 * Prints one line: BENCH {...} with RPC calls by method, getTransaction count, DB calls, wall ms.
 */
import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { PublicKey } from "@solana/web3.js";
import { createHash } from "node:crypto";

const RPC_MS = Number(process.env.BENCH_RPC_MS ?? 60);
const DB_MS = Number(process.env.BENCH_DB_MS ?? 15);
const WRAPPER = "5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe";

const fx = JSON.parse(gunzipSync(readFileSync(new URL("./fixtures/v22-6markets-20261113.json.gz", import.meta.url))).toString("utf8")) as {
  slabs: string[];
  sigs: Record<string, { signature: string; err: unknown }[]>;
  txs: Record<string, any>;
};

const pk = (s: string) => new PublicKey(s);
function rehydrateIx(ix: any) {
  if (ix.parsed) return { ...ix, programId: pk(ix.programId) };
  return { ...ix, programId: pk(ix.programId), accounts: (ix.accounts ?? []).map(pk) };
}
function rehydrate(tx: any) {
  if (!tx) return null;
  return {
    ...tx,
    meta: tx.meta && {
      ...tx.meta,
      innerInstructions: (tx.meta.innerInstructions ?? []).map((g: any) => ({ ...g, instructions: g.instructions.map(rehydrateIx) })),
    },
    transaction: {
      ...tx.transaction,
      message: {
        ...tx.transaction.message,
        accountKeys: tx.transaction.message.accountKeys.map((k: any) => ({ ...k, pubkey: pk(k.pubkey) })),
        instructions: tx.transaction.message.instructions.map(rehydrateIx),
      },
    },
  };
}

const counts: Record<string, number> = {};
let txFetched = 0;
let dbCalls = 0;
const writes: string[] = []; // every DB write payload, in order (compared as a hash between runs)
const seen = new Set<string>(); // "slab signature" pairs returned by getSignaturesForAddress
const jsonSafe = (v: unknown) => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x));
const bump = (k: string, n = 1) => { counts[k] = (counts[k] ?? 0) + n; };
// Provider budget: more than BENCH_RPS_LIMIT history calls inside any sliding 1 s window counts one "429" (counted only,
// the call still succeeds, so the replay and its write hash stay comparable between runs).
const RPS_LIMIT = Number(process.env.BENCH_RPS_LIMIT ?? 10);
const callTimes: number[] = [];
let would429 = 0;
const meter = () => {
  const t = performance.now();
  while (callTimes.length && t - callTimes[0] >= 1000) callTimes.shift();
  callTimes.push(t);
  if (callTimes.length > RPS_LIMIT) would429++;
};
const rpcSleep = () => new Promise((r) => setTimeout(r, RPC_MS));
const dbSleep = async () => { dbCalls++; await new Promise((r) => setTimeout(r, DB_MS)); };

const fakeConnection = {
  // Same contract as the RPC: newest-first, `before` and `until` exclusive, at most `limit` (default 1000).
  async getSignaturesForAddress(addr: PublicKey, opts: { limit?: number; before?: string; until?: string } = {}) {
    bump("getSignaturesForAddress");
    meter();
    await rpcSleep();
    let all = fx.sigs[addr.toBase58()] ?? [];
    if (opts.before) {
      const b = all.findIndex((s) => s.signature === opts.before);
      all = b >= 0 ? all.slice(b + 1) : [];
    }
    const cut = opts.until ? all.findIndex((s) => s.signature === opts.until) : -1;
    const page = (cut >= 0 ? all.slice(0, cut) : all).slice(0, opts.limit ?? 1000);
    for (const s of page) seen.add(addr.toBase58() + " " + s.signature);
    return page;
  },
  // web3.js sends a JSON-RPC batch: one HTTP round trip, but one getTransaction per signature (what RPC providers bill).
  async getParsedTransactions(sigs: string[]) {
    bump("getParsedTransactions");
    meter();
    txFetched += sigs.length;
    await rpcSleep();
    return sigs.map((s) => rehydrate(fx.txs[s] ?? null));
  },
  async getParsedTransaction(sig: string) {
    bump("getParsedTransaction");
    meter();
    txFetched += 1;
    await rpcSleep();
    return rehydrate(fx.txs[sig] ?? null);
  },
  async getAccountInfo() {
    bump("getAccountInfo");
    await rpcSleep();
    return null;
  },
  async getMultipleAccountsInfo(keys: PublicKey[]) {
    bump("getMultipleAccountsInfo");
    await rpcSleep();
    return keys.map(() => null);
  },
};

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["5NGgnU2j315Ci2tso8VJDEthaVExuiKG3tn4xnur28xe"] }, // = WRAPPER (vi.mock is hoisted)
    getConnection: () => fakeConnection,
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: noop,
    eventBus: { on: noop, off: noop, publish: noop },
  };
});
vi.mock("../src/db/marketRows.js", () => ({
  getMarketRows: async () => fx.slabs.map((s) => ({ slab_address: s, indexer_excluded: false, keeper_status: "active" })),
  getMarketRowsCached: async () => fx.slabs.map((s) => ({ slab_address: s, indexer_excluded: false, keeper_status: "active" })),
}));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: async (row: unknown) => { writes.push("trade " + jsonSafe(row)); await dbSleep(); return true; } }));
vi.mock("../src/db/insertV22Events.js", () => ({ insertV22Events: async (rows: unknown) => { writes.push("v22 " + jsonSafe(rows)); await dbSleep(); return true; } }));
vi.mock("../src/db/traderNetPosition.js", () => ({ resolveRebalanceReduce: async () => { await dbSleep(); return { ok: false, reason: "bench" }; } }));
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: async (rows: unknown) => { writes.push("skip " + jsonSafe(rows)); await dbSleep(); }, assertSkippedSignatureSinkReady: () => undefined }));
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: async () => { await dbSleep(); return []; },
  fetchStoredLegsMany: async () => { await dbSleep(); return new Map(); },
}));

import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";

describe("bench: cold trade backfill, 6 live v2.2 markets (recorded)", () => {
  it("replays", async () => {
    const idx: any = new TradeIndexerPolling();
    idx._running = true;
    const t0 = performance.now();
    await idx.backfill();
    const wallMs = Math.round(performance.now() - t0);
    idx._running = false;
    const signatures = Object.values(fx.sigs).reduce((n, l) => n + l.length, 0);
    const unique = new Set(Object.values(fx.sigs).flat().map((s) => s.signature)).size;
    const out = { markets: fx.slabs.length, signatures, uniqueTxs: unique, rpcMs: RPC_MS, dbMs: DB_MS, txFetched, rpc: counts, rpsLimit: RPS_LIMIT, would429, dbCalls, writes: writes.length, writesSha256: createHash("sha256").update(writes.join("\n")).digest("hex").slice(0, 16), wallMs };
    console.log("BENCH " + JSON.stringify(out));
    expect(idx.hasBackfilled).toBe(true);
    for (const s of fx.slabs) expect(idx.lastSignature.get(s)).toBe(fx.sigs[s][0].signature); // every cursor committed
  }, 600_000);
});

/**
 * Steady-state poll whose window is longer than one getSignaturesForAddress page. Each slab's cursor sits on its
 * OLDEST recorded signature, so the window is the 99 newer ones (about 4 minutes at ~25 signatures/min, less than
 * one 5-minute poll interval). A signature the poll never sees is a missed trade: the cursor moves to the newest one
 * at the end of the pass and the skipped ones are never polled again.
 * Reference (full, untruncated window): TRADE_MAX_SIGNATURES=100 fetches all 99 in one call.
 */
describe("bench: steady-state poll, cursor 99 signatures back, 6 live v2.2 markets (recorded)", () => {
  it("replays", async () => {
    for (const k of Object.keys(counts)) delete counts[k];
    txFetched = 0;
    dbCalls = 0;
    would429 = 0;
    callTimes.length = 0;
    writes.length = 0;
    seen.clear();
    const idx: any = new TradeIndexerPolling();
    idx._running = true;
    idx.hasBackfilled = true;
    for (const s of fx.slabs) idx.lastSignature.set(s, fx.sigs[s].at(-1)!.signature);
    const window = fx.slabs.flatMap((s) => fx.sigs[s].slice(0, -1).map((x) => s + " " + x.signature));
    const t0 = performance.now();
    await idx.pollAllMarketsOnce();
    const wallMs = Math.round(performance.now() - t0);
    idx._running = false;
    const missed = window.filter((k) => !seen.has(k)).length;
    const windowSec = Math.max(...fx.slabs.map((s) => (fx.sigs[s][0] as any).blockTime - (fx.sigs[s].at(-1) as any).blockTime));
    const out = {
      markets: fx.slabs.length,
      maxSignatures: Number(process.env.TRADE_MAX_SIGNATURES ?? 50),
      windowSec,
      windowSigs: window.length,
      windowUnique: new Set(window.map((k) => k.split(" ")[1])).size,
      seenSigs: seen.size,
      missed,
      txFetched,
      rpc: counts,
      rpsLimit: RPS_LIMIT,
      would429,
      dbCalls,
      writes: writes.length,
      writesSha256: createHash("sha256").update(writes.join("\n")).digest("hex").slice(0, 16),
      wallMs,
    };
    console.log("BENCH-POLL " + JSON.stringify(out));
    for (const s of fx.slabs) expect(idx.lastSignature.get(s)).toBe(fx.sigs[s][0].signature);
  }, 600_000);
});
