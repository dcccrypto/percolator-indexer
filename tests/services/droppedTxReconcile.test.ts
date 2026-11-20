/**
 * A transaction ingested at `confirmed` and then dropped by a fork (J-20261120-IDX-REPORT-CONFIRMEDTXDROPPED).
 *
 * Port of the report's draft assertions (fork.test.ts, CTD_ASSERT=1) to the mocked-supabase style. Each of the four
 * ingest paths runs for real (webhook route, TradeIndexer poll, Atlas stream, LpVaultIndexer with the Supabase store) and
 * writes into one in-memory set of tables (tests/helpers/fakeSupabase.ts). Then the fork resolves: the victim's
 * getSignatureStatuses answer is null (drop), and in the "replace" case a re-signed copy Y lands in its place. Time passes
 * well past finality, the finalized reconcile pass runs (when the build has one) and the market_stats rollup runs.
 *
 * Assertions, as in the draft: the dropped signature has no rows left in trades / v22_events / lp_vault_events, and
 * market_stats equals the rollup without it (in the replace case the fill is counted ONCE, not twice).
 *
 * The reconciler is imported dynamically so the same file runs on a build without it: there it skips the pass and the
 * assertions fail with the rows the report measured (trades 1, v22_events 1, lp_vault_events 1, market_stats +X).
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { Message, PublicKey, type VersionedTransactionResponse } from "@solana/web3.js";

const H = vi.hoisted(() => ({
  db: null as any,
  getAccountInfo: (async () => null) as (...a: unknown[]) => Promise<unknown>,
  PROGRAM: "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB",
}));

vi.mock("../../src/parsers/lotScale.js", async (orig) => {
  const actual = await orig<typeof import("../../src/parsers/lotScale.js")>();
  return { ...actual, resolveLotExp: async (s: string, a: number) => actual.knownLotExp(s, a) ?? 0 };
});
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const quiet = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} };
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: [H.PROGRAM], programId: H.PROGRAM, webhookSecret: "s3cret" },
    eventBus: { publish: () => {}, emit: () => {}, on: () => {} },
    createLogger: () => quiet,
    getSupabase: () => H.db,
    getNetwork: () => "devnet",
    getConnection: () => ({ getAccountInfo: (...a: unknown[]) => H.getAccountInfo(...a) }),
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    captureException: () => {},
    sendCriticalAlert: async () => {},
  };
});
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: async () => undefined, assertSkippedSignatureSinkReady: () => {} }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../../src/db/storedLegs.js")>()),
  fetchStoredLegs: async () => [],
  fetchStoredLegsMany: async () => null,
}));

import { createFakeSupabase } from "../helpers/fakeSupabase.js";
import { webhookRoutes } from "../../src/routes/webhook.js";
import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";
import { EventStreamService } from "../../src/services/EventStreamService.js";
import { StatsCollector } from "../../src/services/StatsCollector.js";
import { LpVaultIndexer } from "../../src/services/LpVaultIndexer.js";
import { SupabaseLpVaultStore, recomputePosition, MemoryLpVaultStore } from "../../src/lpVault/store.js";
import { normalizeRpcTransaction } from "../../src/lpVault/decoder.js";
import type { LpVaultChain, OnchainClaim, RegistryInfo, SigInfo } from "../../src/lpVault/chain.js";
import { encodeBase58 } from "../../src/lib/base58.js";
import { resetMarketLayoutNotes } from "../../src/layout/marketVersions.js";
import { resetLogEventStats } from "../../src/parsers/v22FillEvents.js";

// Built from a variable so a build without the module still loads this file (and fails the assertions instead).
const RECONCILER = "../../src/services/DroppedTxReconciler.js";
const Reconciler: any = await import(/* @vite-ignore */ RECONCILER).then((m) => m.DroppedTxReconciler, () => null);

const PROGRAM = H.PROGRAM;
const FX = join(__dirname, "..", "fixtures");
const fx = (n: string) => JSON.parse(readFileSync(join(FX, "tradecpi", `${n}.json`), "utf8"));
const ENHANCED = JSON.parse(readFileSync(join(FX, "tradecpi", "helius-enhanced.json"), "utf8")).txs as Record<string, any>;
const EX = JSON.parse(readFileSync(join(FX, "v22-fill-events-doc-examples.json"), "utf8")).examples as Record<string, { b64: string }>;
const pk = (s: string) => new PublicKey(s);
const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
/** A different, valid base58 signature of the same length (the same tx re-signed). */
const resign = (sig: string, k = 1) => sig.slice(0, -2) + B58[(B58.indexOf(sig.at(-2)!) + k) % 58] + sig.at(-1);

// Trade victim: the "partial" TradeCpi fill (483 @ 13.861751) on market Ar6khqrJ…; PRE is the same fill re-signed (a
// live trade on the same market, so market_stats has a truth to compare against).
const PARTIAL = fx("partial");
const MKT = PARTIAL.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1] as string;
const X = PARTIAL.sig as string;
const PRE = resign(X, 1);
const Y = resign(X, 2);
const ctxData = { data: Buffer.from(PARTIAL.ctxReturnHex.padEnd(640, "0"), "hex") };

// v2.2 log-event victim (poll + stream): a wrapper ix on market V22M whose logs carry a fill_event.
const V22M = new PublicKey(new Uint8Array(32).fill(1)).toBase58();
const TAKER = new PublicKey(new Uint8Array(32).fill(2)).toBase58();
const XV = resign(X, 3);
const YV = resign(X, 4);
const v22Logs = [`Program ${PROGRAM} invoke [1]`, `Program data: ${EX.fill_clipped_tradecpi.b64}`, `Program ${PROGRAM} consumed 1 of 2 compute units`, `Program ${PROGRAM} success`];
const v22Data = encodeBase58(new Uint8Array([10, 0, 0, 0]));

type Path = "W" | "P" | "S";
type Case = "drop" | "replace";

const dropped = new Set<string>();
const statusCalls: number[] = [];
/** When set, answers instead of the fork model (an RPC fault, a lagging node). */
let statusOverride: ((sigs: string[]) => Promise<unknown>) | null = null;
const statusConn = {
  async getSignatureStatuses(sigs: string[], _cfg?: unknown): Promise<any> {
    if (statusOverride) return statusOverride(sigs);
    statusCalls.push(sigs.length);
    return { context: { slot: 1 }, value: sigs.map((s) => (dropped.has(s) ? null : { slot: 1, confirmations: null, err: null, confirmationStatus: "finalized" })) };
  },
};

beforeEach(() => {
  H.db = createFakeSupabase();
  H.getAccountInfo = async () => ctxData;
  dropped.clear();
  statusCalls.length = 0;
  statusOverride = null;
  resetMarketLayoutNotes();
  resetLogEventStats();
});

// ── the three trade paths ───────────────────────────────────────────────────

async function ingestWebhook(sig: string): Promise<void> {
  const tx = { ...structuredClone(ENHANCED[X]), signature: sig };
  const res = await webhookRoutes().fetch(new Request("http://x/webhook/trades", {
    method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify([tx]),
  }));
  expect(res.status).toBe(200);
}

function pollTx(f: any) {
  const tx = f.tx;
  return {
    slot: tx.slot, blockTime: tx.blockTime,
    meta: { err: null, logMessages: [], innerInstructions: tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map((i: any) => ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) })) },
    transaction: { message: { instructions: tx.transaction.message.instructions.map((i: any) => (i.parsed ? i : { ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) } },
  };
}
const pollV22Tx = () => ({
  slot: 99, blockTime: 1_790_000_000,
  meta: { err: null, logMessages: v22Logs, innerInstructions: [] },
  transaction: { message: { instructions: [{ programId: pk(PROGRAM), accounts: [pk(TAKER), pk(V22M)], data: v22Data }] } },
});
async function ingestPoll(sig: string, v22 = false): Promise<void> {
  const idx: any = new TradeIndexerPolling();
  await idx.processTransaction(v22 ? pollV22Tx() : pollTx(PARTIAL), sig, v22 ? V22M : MKT, new Set([PROGRAM]));
}

async function ingestStream(sig: string, v22 = false): Promise<void> {
  let cb: (m: any) => unknown = () => {};
  const ws: any = { sub: () => {}, onNotification: (c: any) => { cb = c; }, close: () => {}, isOpen: true };
  const svc = new EventStreamService({ ws, programId: PROGRAM, connection: { getAccountInfo: async () => ctxData } as any, autoIndex: true, knownSlabs: [MKT, V22M] });
  await svc.start();
  const tx = v22
    ? { signature: sig, slot: 99, blockTime: 1_790_000_000, transaction: { signatures: [sig], message: { instructions: [{ programId: PROGRAM, accounts: [TAKER, V22M], data: v22Data }] } }, meta: { err: null, logMessages: v22Logs, innerInstructions: [] } }
    : { ...structuredClone(PARTIAL.tx), signature: sig };
  await cb({ method: "transactionNotification", params: { result: tx } });
  await new Promise((r) => setTimeout(r, 30));
}

const ingest = (path: Path, sig: string, v22 = false) =>
  path === "W" ? ingestWebhook(sig) : path === "P" ? ingestPoll(sig, v22) : ingestStream(sig, v22);

// ── helpers ─────────────────────────────────────────────────────────────────

function rowsOf(sigs: string[]) {
  const s = new Set(sigs);
  return {
    trades: H.db.rows("trades").filter((r: any) => s.has(r.tx_signature)).length,
    v22_events: H.db.rows("v22_events").filter((r: any) => s.has(r.signature)).length,
    lp_vault_events: H.db.rows("lp_vault_events").filter((r: any) => s.has(r.signature)).length,
  };
}
const stats = (slab: string) => {
  const r = H.db.rows("market_stats").find((x: any) => x.slab_address === slab);
  return r ? { volume_24h: Number(r.volume_24h), trade_count_24h: r.trade_count_24h } : null;
};
function collector(): any {
  const sc: any = new StatsCollector({ getMarkets: () => new Map() } as any);
  sc._running = true; // the volume rollup only runs while started; start() would also arm its timers
  return sc;
}
function reconciler(sc: any, lpStore?: unknown) {
  if (!Reconciler) return null;
  return new Reconciler({
    connection: statusConn, network: "devnet", db: () => H.db, lpStore,
    now: () => H.db.clock.t, onTradesRemoved: (slabs: string[]) => sc.resyncVolume(slabs),
  });
}
/** Time passes past finality + blockhash lifetime; the reconcile pass runs on its 60 s timer (twice: a null must repeat). */
async function afterFinality(r: any): Promise<void> {
  H.db.clock.t += 3 * 60_000;
  if (r) await r.runOnce();
  H.db.clock.t += 60_000;
  if (r) await r.runOnce();
}

// ── the draft assertions, per path ──────────────────────────────────────────

describe.each<[Path, Case]>([
  ["W", "drop"], ["W", "replace"], ["P", "drop"], ["P", "replace"], ["S", "drop"], ["S", "replace"],
])("%s path, %s: a tx written at confirmed and then dropped by a fork leaves no rows", (path, kase) => {
  it("X has no rows in trades / v22_events, and market_stats equals the rollup without X", async () => {
    const sc = collector();
    const r = reconciler(sc);
    const withV22 = path !== "W"; // the Helius enhanced payload carries no logs: the webhook writes no v22 log events
    await ingest(path, PRE);
    await ingest(path, X);
    if (withV22) await ingest(path, XV, true);
    // Ingested at confirmed: one trade row for X (and one v22 row for XV).
    expect(rowsOf([X, XV])).toEqual({ trades: 1, v22_events: withV22 ? 1 : 0, lp_vault_events: 0 });
    await sc.syncVolumeForAllDBMarkets();
    expect(stats(MKT)).toEqual({ volume_24h: 966, trade_count_24h: 2 });

    // The fork resolves: X (and XV) are gone at every commitment; in "replace" Y takes the slot and is ingested.
    dropped.add(X).add(XV);
    if (kase === "replace") {
      await ingest(path, Y);
      if (withV22) await ingest(path, YV, true);
    }
    await afterFinality(r);
    await sc.syncVolumeForAllDBMarkets(); // the periodic rollup

    expect.soft(rowsOf([X, XV]), `rows of the dropped tx ${X.slice(0, 8)}…`).toEqual({ trades: 0, v22_events: 0, lp_vault_events: 0 });
    if (kase === "replace") expect.soft(rowsOf([Y, YV])).toEqual({ trades: 1, v22_events: withV22 ? 1 : 0, lp_vault_events: 0 });
    // Truth: PRE (+ Y), each 483. Without the fix: 966 / 2 (drop) and 1449 / 3 (replace: the fill counted twice).
    expect.soft(stats(MKT), "market_stats of the market").toEqual(kase === "drop" ? { volume_24h: 483, trade_count_24h: 1 } : { volume_24h: 966, trade_count_24h: 2 });
  });
});

// ── LP vault ────────────────────────────────────────────────────────────────

const LP_WRAPPER = "GnwdeQrAh4qzChJeVLrM21CXXWC1akjLH3DiijwzEEYZ";
const LP_X = "626qJJquVopCrgdwNWpfmcn5NLmWfbJrg6tYZZYTSrK3ihHHGZ7r616BQ72uEVGKH1go5EMbKZm166GkPcGtAnP2";
const LP_Y = resign(LP_X, 1);
const FXUC: RegistryInfo = {
  registry: "FXUCNnxbkZBed2cax5waBWCaGiHg7BRzxEjrcZie7iDC",
  programId: LP_WRAPPER,
  market: "7mgX3bkzEivRrCffCJ7XzqfAp3gpjm63RNinwDhr7b41",
  lpMint: "Fgn5f3Sg7xPbGZeXVMZ4GBwQbQBChFovoGU5eHGpnPoU",
};
const LP_USER = "9sM73A4MvS2ye2Fuvpr1tmkj68iA61eebuRKz1rnGUWa";

function loadLp(sig: string): VersionedTransactionResponse {
  const raw = JSON.parse(readFileSync(join(FX, "lp-vault", `${LP_X}.json`), "utf8"));
  return {
    slot: raw.slot, blockTime: raw.blockTime,
    transaction: { signatures: [sig], message: new Message(raw.transaction.message) },
    meta: { err: raw.meta.err, innerInstructions: raw.meta.innerInstructions, loadedAddresses: { writable: [], readonly: [] } },
  } as unknown as VersionedTransactionResponse;
}

class ForkLpChain implements LpVaultChain {
  sigs: SigInfo[] = []; // oldest -> newest
  claim: OnchainClaim = { heldShares: 0n, pendingShares: 0n, contextSlot: 1 };
  async listRegistries() { return [FXUC]; }
  async getSignatures(_a: string, o: { until?: string; before?: string; limit: number }) {
    const newest = [...this.sigs].reverse();
    if (o.until) { const i = newest.findIndex((s) => s.signature === o.until); if (i >= 0) newest.splice(i); }
    if (o.before) { const i = newest.findIndex((s) => s.signature === o.before); newest.splice(0, i + 1); }
    return newest.slice(0, o.limit);
  }
  async getTransaction(sig: string) { return this.sigs.some((s) => s.signature === sig) ? normalizeRpcTransaction(sig, loadLp(sig)) : null; }
  async getClaim() { return this.claim; }
}

describe.each<Case>(["drop", "replace"])("L path, %s: an LP deposit dropped by a fork", (kase) => {
  it("X has no lp_vault_events row, and the position is the fold without X with no phantom transfer flag", async () => {
    const chain = new ForkLpChain();
    const store = new SupabaseLpVaultStore(H.db);
    const lp = new LpVaultIndexer({ chain, store, network: "devnet", programIds: [LP_WRAPPER], reconcileIntervalMs: 0, discoveryIntervalMs: 1e12 });
    const r = reconciler(collector(), store);

    // Confirmed: the deposit is listed and the chain shows its shares.
    chain.sigs = [{ signature: LP_X, slot: 505300414, failed: false }];
    chain.claim = { heldShares: 4_999_999_000n, pendingShares: 0n, contextSlot: 505300414 };
    await lp.tick();
    expect(rowsOf([LP_X]).lp_vault_events).toBe(1);

    // The fork resolves: X is gone; in "replace" the re-signed Y lands. The chain's claim is the post-fork truth.
    dropped.add(LP_X);
    chain.sigs = kase === "replace" ? [{ signature: LP_Y, slot: 505300414, failed: false }] : [];
    chain.claim = { heldShares: kase === "replace" ? 4_999_999_000n : 0n, pendingShares: 0n, contextSlot: 505300500 };
    await lp.tick();
    await lp.tick();
    await afterFinality(r);

    expect.soft(rowsOf([LP_X]), `rows of the dropped tx ${LP_X.slice(0, 8)}…`).toEqual({ trades: 0, v22_events: 0, lp_vault_events: 0 });
    const pos = await store.loadPosition("devnet", FXUC.registry, LP_USER);
    expect(pos).toMatchObject(kase === "drop"
      ? { lp_shares: "0", cost_basis_atoms: "0", total_deposited_atoms: "0", basis_known: true, inconsistency: null }
      : { lp_shares: "4999999000", cost_basis_atoms: "5000000000", total_deposited_atoms: "5000000000", basis_known: true, inconsistency: null });
  });
});

// ── the reconcile pass itself (needs the module) ────────────────────────────

const describeFix = Reconciler ? describe : describe.skip;

describeFix("DroppedTxReconciler", () => {
  const seedTrade = (sig: string, slab = MKT, size = "483") =>
    H.db.from("trades").insert({ slab_address: slab, trader: TAKER, side: "long", size, price: 1, tx_signature: sig, network: "devnet" });

  it("RPC cost: 279 signatures cost 2 getSignatureStatuses calls in a pass; once finalized they cost nothing more", async () => {
    const sigs = Array.from({ length: 279 }, (_, i) => `S${i}`);
    for (const s of sigs) await seedTrade(s);
    const r = reconciler(collector());
    H.db.clock.t += 3 * 60_000;
    const first = await r.runOnce();
    expect(statusCalls).toEqual([256, 23]);
    expect(first).toMatchObject({ candidates: 279, checked: 279, rpcCalls: 2, finalized: 279, pending: 0, dropped: [] });
    H.db.clock.t += 60_000;
    const second = await r.runOnce();
    expect(second).toMatchObject({ candidates: 0, rpcCalls: 0 });
    expect(statusCalls).toEqual([256, 23]);
  });

  it("one null is not enough: the rows go only after a second pass also answers null", async () => {
    await seedTrade(X);
    await seedTrade(PRE);
    dropped.add(X);
    const r = reconciler(collector());
    H.db.clock.t += 3 * 60_000;
    expect((await r.runOnce()).dropped).toEqual([]);
    expect(rowsOf([X]).trades).toBe(1);
    H.db.clock.t += 60_000;
    expect((await r.runOnce()).dropped).toEqual([X]);
    expect(rowsOf([X, PRE]).trades).toBe(1);
    // the removal is recorded for audit
    expect(H.db.rows("skipped_signatures")).toEqual([expect.objectContaining({ signature: X, source: "fork-dropped", slab: MKT, network: "devnet" })]);
  });

  it("a signature that answers again (a lagging node) loses its strike", async () => {
    await seedTrade(X);
    const r = reconciler(collector());
    H.db.clock.t += 3 * 60_000;
    dropped.add(X);
    await r.runOnce(); // null #1
    dropped.delete(X);
    statusOverride = async (sigs) => ({ value: sigs.map(() => ({ confirmationStatus: "confirmed" })) });
    H.db.clock.t += 60_000;
    await r.runOnce(); // answers again (still confirmed): strike reset
    statusOverride = null;
    dropped.add(X);
    H.db.clock.t += 60_000;
    expect((await r.runOnce()).dropped).toEqual([]); // null #1 again
    expect(rowsOf([X]).trades).toBe(1);
  });

  it("a pass where every status is null is an RPC fault: nothing is removed", async () => {
    for (const s of ["A1", "A2", "A3", "A4"]) { await seedTrade(s); dropped.add(s); }
    const r = reconciler(collector());
    for (let i = 0; i < 3; i++) {
      H.db.clock.t += 2 * 60_000;
      const res = await r.runOnce();
      expect(res).toMatchObject({ suspect: true, dropped: [] });
    }
    expect(H.db.rows("trades")).toHaveLength(4);
  });

  it("an RPC error or a short status list aborts the pass with no strike", async () => {
    await seedTrade(X);
    dropped.add(X);
    const r = reconciler(collector());
    statusOverride = async () => { throw new Error("429"); };
    H.db.clock.t += 3 * 60_000;
    await expect(r.runOnce()).rejects.toThrow("429");
    statusOverride = async () => ({ value: [] });
    H.db.clock.t += 60_000;
    await expect(r.runOnce()).rejects.toThrow(/0 statuses for 1/);
    statusOverride = null;
    H.db.clock.t += 60_000;
    expect((await r.runOnce()).dropped).toEqual([]); // the first real null
    expect(rowsOf([X]).trades).toBe(1);
  });

  it("rows younger than the finality margin are not checked yet", async () => {
    await seedTrade(X);
    const r = reconciler(collector());
    H.db.clock.t += 60_000; // < minAgeMs (120 s)
    expect(await r.runOnce()).toMatchObject({ candidates: 0, rpcCalls: 0 });
  });

  it("market_stats: a market whose only trade was dropped goes to 0 (the rollup alone would keep the dropped fill)", async () => {
    await seedTrade(X);
    const sc = collector();
    await sc.syncVolumeForAllDBMarkets();
    expect(stats(MKT)).toEqual({ volume_24h: 483, trade_count_24h: 1 });
    dropped.add(X);
    await afterFinality(reconciler(sc));
    await sc.syncVolumeForAllDBMarkets();
    expect(stats(MKT)).toEqual({ volume_24h: 0, trade_count_24h: 0 });
  });

  it("a delete failure keeps the signature pending; the next pass removes it", async () => {
    await seedTrade(X);
    dropped.add(X);
    const r = reconciler(collector());
    H.db.clock.t += 3 * 60_000;
    await r.runOnce();
    const realFrom = H.db.from;
    H.db.from = (t: string) => {
      const q = realFrom(t);
      if (t !== "v22_events") return q;
      return new Proxy(q, { get: (o, k) => (k === "delete" ? () => ({ in: () => ({ eq: () => ({ select: async () => ({ data: null, error: { message: "timeout" } }) }) }) }) : o[k]) });
    };
    H.db.clock.t += 60_000;
    await expect(r.runOnce()).rejects.toThrow(/v22_events delete: timeout/);
    H.db.from = realFrom;
    H.db.clock.t += 60_000;
    expect((await r.runOnce()).dropped).toEqual([X]);
    expect(rowsOf([X]).trades).toBe(0);
  });
});

describeFix("recomputePosition forkRepair", () => {
  const ev = (sig: string, lp: bigint) => ({
    signature: sig, ixIndex: 0, slot: 10, blockTime: null, kind: "deposit" as const, programId: "P", market: "M", registry: "R",
    user: "U", lpMint: "L", collateralAtoms: lp, lpAmount: lp, domain: null,
  });
  it("keeps a real transfer flag: the last on-chain reading still disagrees with the repaired fold", async () => {
    const store = new MemoryLpVaultStore();
    await store.upsertEvents("devnet", [ev("a", 100n), ev("phantom", 50n)]);
    await recomputePosition(store, "devnet", "R", "U", { reconcile: { onchainShares: 70n, contextSlot: 20 } }); // 150 indexed, 70 held
    store.events.delete("phantom|0");
    const row = await recomputePosition(store, "devnet", "R", "U", { forkRepair: true });
    expect(row).toMatchObject({ lp_shares: "100", transfer_detected_slot: 20, basis_known: false });
  });
});
