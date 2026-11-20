/**
 * Data-loss regression (IDX-FILLLOSS): a failed fallback fill insert was log-only and the cursor still
 * committed, so the fill was never stored. The cursor is now held (capped) so the signatures are retried.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";
const captureSpy = vi.hoisted(() => vi.fn());
const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const txBySig = new Map<string, unknown>();
const sigLists = new Map<string, { signature: string; err: null }[]>();
const connection = {
  getSignaturesForAddress: vi.fn(async (pk: PublicKey, o?: any) => {
    const all = sigLists.get(pk.toBase58()) ?? [];
    if (o?.until) { const i = all.findIndex((s) => s.signature === o.until); return i < 0 ? all : all.slice(0, i); }
    return o?.limit ? all.slice(0, o.limit) : all;
  }),
  getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map((s) => txBySig.get(s) ?? null)),
  getParsedTransaction: vi.fn(async (s: string) => txBySig.get(s) ?? null),
  getAccountInfo: vi.fn(async () => null),
};
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  const noop = () => undefined;
  return { ...actual, config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] }, getConnection: () => connection,
    withRetry: async (fn: () => Promise<unknown>) => fn(), createLogger: () => ({ info: noop, warn: noop, error: noop, debug: noop }),
    captureException: captureSpy, eventBus: { on: noop, off: noop, publish: noop } };
});
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../../src/db/tradeCursor.js", () => ({ loadTradeCursors: vi.fn(async () => new Map()), saveTradeCursor: vi.fn(async () => undefined) }));
vi.mock("../../src/db/storedLegs.js", async (orig) => ({ ...(await orig<any>()),
  fetchStoredLegs: vi.fn(async (sig: string) => storedOf(sig)), fetchStoredLegsMany: vi.fn(async (s: string[]) => new Map(s.map((x) => [x, storedOf(x)]))) }));
const store = new Set<string>();
const rows = new Map<string, any>();
// Like the real table: rows already written come back as stored legs, so a retry pass skips them (writes nothing).
function storedOf(sig: string) { const r = rows.get(sig); return r ? [{ slab_address: r.slab_address, asset_index: r.asset_index, leg_index: r.leg_index, trader: r.trader, side: r.side, size: r.size }] : []; }
const put = (r: any) => { store.add(r.tx_signature); rows.set(r.tx_signature, r); return true; };
vi.mock("../../src/db/insertTradeRow.js", async (orig) => ({ ...(await orig<any>()), insertTradeRow: vi.fn(), insertTradeRows: vi.fn() }));
import { insertTradeRow, insertTradeRows } from "../../src/db/insertTradeRow.js";
import { recordSkippedSignatures } from "../../src/lib/skippedSignatures.js";
import { TradeIndexerPolling, MAX_WRITE_HOLDS } from "../../src/services/TradeIndexer.js";
import { encodeBase58 } from "../../src/lib/base58.js";
const pk = (s: string) => new PublicKey(s);
const full = JSON.parse(readFileSync(new URL("../fixtures/tradecpi/full.json", import.meta.url), "utf8"));
const hydrate = (i: any) => (i.parsed ? i : { ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) });
const mkTx = (n: number) => ({ slot: 1 + n, blockTime: 1 + n,
  meta: { err: null, logMessages: [], innerInstructions: full.tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map(hydrate) })) },
  transaction: { message: { instructions: full.tx.transaction.message.instructions.map(hydrate) } } });
const MARKET: string = full.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1];
const sigOf = (n: number) => encodeBase58(Uint8Array.from({ length: 64 }, (_, k) => (k === 0 ? 1 : k === 1 ? n + 1 : (k * 13) % 255 + 1)));
const key = (r: any) => r.tx_signature;

const setup = () => {
  const sigs = [1, 2, 3].map(sigOf);
  sigs.forEach((s, i) => txBySig.set(s, mkTx(i)));
  sigLists.set(MARKET, [...sigs].reverse().map((signature) => ({ signature, err: null })));
  connection.getAccountInfo.mockResolvedValue({ data: Buffer.from(full.ctxReturnHex.padEnd(640, "0"), "hex") } as never);
  vi.mocked(insertTradeRows).mockRejectedValue(new Error("batch down"));
  const idx: any = new TradeIndexerPolling(); idx._running = true;
  return { sigs, idx };
};

describe("poll path: a failed fill write must not lose the fill", () => {
  beforeEach(() => { store.clear(); rows.clear(); vi.clearAllMocks(); captureSpy.mockClear(); });

  it("fill 2 fails once: cursor is held, next poll stores it", async () => {
    const { sigs, idx } = setup();
    let failed = false;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1] && !failed) { failed = true; throw new Error("transient db"); }
      return put(r); }) as never);
    await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).not.toBe(sigs[2]);
    await idx.indexTradesForSlab(MARKET, 100);
    expect(sigs.map((s) => store.has(s))).toEqual([true, true, true]);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });

  it("hold cap: a poison row (other writes succeed) holds the cursor MAX_WRITE_HOLDS polls, then only it is skipped, by name", async () => {
    const { sigs, idx } = setup();
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1]) throw new Error("poison row");
      return put(r); }) as never);
    // a busy slab: each poll brings a new fill whose write succeeds (that is what proves the database is up)
    const all = [...sigs];
    const addFresh = (n: number) => { const x = sigOf(20 + n); txBySig.set(x, mkTx(20 + n)); all.push(x); sigLists.set(MARKET, [...all].reverse().map((signature) => ({ signature, err: null }))); return x; };
    for (let p = 1; p <= MAX_WRITE_HOLDS; p++) {
      await idx.indexTradesForSlab(MARKET, 100);
      expect(idx.lastSignature.get(MARKET)).not.toBe(all[all.length - 1]);
      addFresh(p);
    }
    await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).toBe(all[all.length - 1]);
    expect(captureSpy).toHaveBeenCalledTimes(1);
    expect(String(captureSpy.mock.calls[0][0].message)).toContain(sigs[1]);
    expect(captureSpy.mock.calls[0][1].extra.failedSigs).toEqual([sigs[1]]);
    expect(store.has(sigs[1])).toBe(false);
    expect(store.has(sigs[0]) && store.has(sigs[2])).toBe(true);
    expect(vi.mocked(recordSkippedSignatures).mock.calls.some(([r]) => r.some((x: any) => x.signature === sigs[1]))).toBe(true);
  });

  it("quiet slab: a lone failing row with no other write in a pass is held, never skipped (no data loss)", async () => {
    const { sigs, idx } = setup();
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1]) throw new Error("bad row");
      return put(r); }) as never);
    for (let p = 0; p < MAX_WRITE_HOLDS * 3; p++) await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
    expect(captureSpy.mock.calls.some(([e]) => /skipped/.test(String(e.message)))).toBe(false);
  });

  it("a clean poll resets the hold count (no alert after fail, succeed, fail)", async () => {
    const { sigs, idx } = setup();
    let bad = true;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1] && bad) throw new Error("db");
      return put(r); }) as never);
    for (let p = 0; p < MAX_WRITE_HOLDS; p++) await idx.indexTradesForSlab(MARKET, 100);
    bad = false;
    await idx.indexTradesForSlab(MARKET, 100);
    bad = true; store.clear(); rows.clear();
    idx.lastSignature.delete(MARKET);
    for (let p = 0; p < MAX_WRITE_HOLDS; p++) await idx.indexTradesForSlab(MARKET, 100);
    expect(captureSpy).not.toHaveBeenCalled();
    expect(idx.lastSignature.get(MARKET)).not.toBe(sigs[2]);
  });

  it("no saved cursor: signatures arriving during a hold, beyond the newest-limit window, are still fetched and written", async () => {
    const { sigs, idx } = setup();
    let bad = true;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[1] && bad) throw new Error("db");
      return put(r); }) as never);
    await idx.indexTradesForSlab(MARKET, 3); // held: no cursor
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
    const all = [...sigs];
    for (let n = 0; n < 6; n++) { const x = sigOf(40 + n); txBySig.set(x, mkTx(40 + n)); all.push(x); }
    sigLists.set(MARKET, [...all].reverse().map((signature) => ({ signature, err: null })));
    bad = false;
    await idx.indexTradesForSlab(MARKET, 3); // limit 3 < 6 new signatures
    expect(all.map((x) => store.has(x))).toEqual(all.map(() => true));
    expect(idx.lastSignature.get(MARKET)).toBe(all[all.length - 1]);
  });

  it("no saved cursor, tx fetch fails on the first poll: the oldest signature is still processed after recovery (anchor is exclusive)", async () => {
    const { sigs, idx } = setup();
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => put(r)) as never);
    vi.mocked(insertTradeRows).mockImplementation((async (rs: any[]) => { rs.forEach(put); return rs.length; }) as never);
    connection.getParsedTransactions.mockRejectedValueOnce(new Error("rpc down"));
    await idx.indexTradesForSlab(MARKET, 3);
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
    const all = [...sigs];
    for (let n = 0; n < 4; n++) { const x = sigOf(60 + n); txBySig.set(x, mkTx(60 + n)); all.push(x); }
    sigLists.set(MARKET, [...all].reverse().map((signature) => ({ signature, err: null })));
    await idx.indexTradesForSlab(MARKET, 3);
    expect(all.map((x) => store.has(x))).toEqual(all.map(() => true));
    expect(idx.lastSignature.get(MARKET)).toBe(all[all.length - 1]);
  });

  it("a DB outage far longer than MAX_WRITE_HOLDS polls drops nothing: every fill is written after recovery", async () => {
    const { sigs, idx } = setup();
    let down = true;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (down) throw new Error("db down");
      return put(r); }) as never);
    let now = 1_000_000;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    for (let p = 0; p < MAX_WRITE_HOLDS * 3; p++) {
      await idx.indexTradesForSlab(MARKET, 100);
      expect(idx.lastSignature.get(MARKET)).toBeUndefined();
      now += 30_000;
    }
    expect(store.size).toBe(0);
    expect(captureSpy.mock.calls.every(([e]) => /outage|failing for/.test(String(e.message)))).toBe(true); // outage alert only, never a skip
    expect(captureSpy).toHaveBeenCalled();
    down = false;
    await idx.indexTradesForSlab(MARKET, 100);
    clock.mockRestore();
    expect(sigs.map((s) => store.has(s))).toEqual([true, true, true]);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });

  it("writeHolds is per instance: another indexer's holds do not count toward this one's cap", async () => {
    const a = setup();
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === a.sigs[1]) throw new Error("poison row");
      return put(r); }) as never);
    for (let p = 0; p < MAX_WRITE_HOLDS; p++) await a.idx.indexTradesForSlab(MARKET, 100);
    const b: any = new TradeIndexerPolling(); b._running = true;
    await b.indexTradesForSlab(MARKET, 100);
    expect(b.lastSignature.get(MARKET)).toBeUndefined();
    expect(captureSpy).not.toHaveBeenCalled();
  });

  it("first poll without a cursor: a held failed signature that fell out of the newest-limit window is still retried", async () => {
    const { sigs, idx } = setup();
    let bad = true;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if (key(r) === sigs[0] && bad) throw new Error("transient db");
      return put(r); }) as never);
    await idx.indexTradesForSlab(MARKET, 3);
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
    const more = [4, 5, 6].map(sigOf);
    more.forEach((s, i) => txBySig.set(s, mkTx(10 + i)));
    sigLists.set(MARKET, [...[...sigs, ...more]].reverse().map((signature) => ({ signature, err: null })));
    bad = false;
    await idx.indexTradesForSlab(MARKET, 3); // newest 3 = more only; sigs[0] is outside it
    expect(store.has(sigs[0])).toBe(true);
    expect(idx.lastSignature.get(MARKET)).toBe(more[2]);
  });

  it("a failed direct write (no pending queue, e.g. a tag 44 row) marks the pass failed (the cursor will hold)", async () => {
    const { sigs, idx } = setup();
    vi.mocked(insertTradeRow).mockRejectedValue(new Error("db down") as never);
    const pass: any = { earlier: new Set(), incomplete: false };
    await expect(idx.processTransaction(txBySig.get(sigs[0]), sigs[0], MARKET, new Set([PROGRAM]), pass)).rejects.toThrow("db down");
    expect(pass.writeFailed).toBe(true);
    expect(pass.incomplete).toBe(true);
    expect([...pass.failedSigs]).toEqual([sigs[0]]);
  });

  it("an unexpected throw inside processTransaction holds the cursor and is retried", async () => {
    const { sigs, idx } = setup();
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => { return put(r); }) as never);
    const orig = idx.processTransaction.bind(idx);
    let boom = true;
    idx.processTransaction = async (tx: any, sig: string, ...rest: any[]) => {
      if (sig === sigs[1] && boom) throw new Error("db read failed");
      return orig(tx, sig, ...rest);
    };
    await idx.indexTradesForSlab(MARKET, 100);
    expect(idx.lastSignature.get(MARKET)).toBeUndefined();
    boom = false;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(sigs.map((s) => store.has(s))).toEqual([true, true, true]);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });

  it("a write-only outage that starts after a successful write (reads keep answering) is never counted as poison", async () => {
    const { sigs, idx } = setup();
    let down = false;
    let bad = true;
    vi.mocked(insertTradeRow).mockImplementation((async (r: any) => {
      if ((key(r) === sigs[1] && bad) || down) throw new Error("db write failed");
      return put(r); }) as never);
    await idx.indexTradesForSlab(MARKET, 100); // sigs[1] fails, 0 and 2 written: a hold with sawWrite
    down = true;
    for (let p = 0; p < MAX_WRITE_HOLDS * 3; p++) {
      await idx.indexTradesForSlab(MARKET, 100);
      expect(idx.lastSignature.get(MARKET)).toBeUndefined();
    }
    expect(captureSpy.mock.calls.some(([e]) => /skipped/.test(String(e.message)))).toBe(false);
    down = false; bad = false;
    await idx.indexTradesForSlab(MARKET, 100);
    expect(sigs.map((s) => store.has(s))).toEqual([true, true, true]);
    expect(idx.lastSignature.get(MARKET)).toBe(sigs[2]);
  });
});

describe("write-outage alert under a wall-clock step back", () => {
  it("still alerts when the clock steps back during an outage (negative age is due)", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      vi.setSystemTime(new Date("2026-11-16T00:00:00Z"));
      captureSpy.mockClear();
      const idx: any = new TradeIndexerPolling();
      const pass = { failedSigs: ["sigA"], writesOk: 0 };
      await idx.holdAfterWriteFailure(MARKET, pass);
      vi.setSystemTime(new Date("2026-11-16T00:02:00Z"));
      await idx.holdAfterWriteFailure(MARKET, pass);
      expect(captureSpy).toHaveBeenCalledTimes(1); // normal alert after >= 60s
      vi.setSystemTime(new Date("2026-11-15T00:00:00Z")); // step back 1 day
      await idx.holdAfterWriteFailure(MARKET, pass);
      expect(captureSpy).toHaveBeenCalledTimes(2); // fails today: silenced until the clock catches up
    } finally { vi.useRealTimers(); }
  });

  it("a step back before any alert restarts the outage clock instead of alerting early, then alerts after the threshold; no spam", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      vi.setSystemTime(new Date("2026-11-16T00:00:10Z"));
      captureSpy.mockClear();
      const idx: any = new TradeIndexerPolling();
      const pass = { failedSigs: ["sigA"], writesOk: 0 };
      await idx.holdAfterWriteFailure(MARKET, pass);
      vi.setSystemTime(new Date("2026-11-16T00:00:00Z")); // 10s back, outage younger than 60s
      await idx.holdAfterWriteFailure(MARKET, pass);
      expect(captureSpy).toHaveBeenCalledTimes(0);
      vi.setSystemTime(new Date("2026-11-16T00:01:05Z"));
      await idx.holdAfterWriteFailure(MARKET, pass);
      expect(captureSpy).toHaveBeenCalledTimes(1);
      await idx.holdAfterWriteFailure(MARKET, pass);
      expect(captureSpy).toHaveBeenCalledTimes(1);
    } finally { vi.useRealTimers(); }
  });
});
