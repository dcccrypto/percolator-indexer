import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { recordSkipped, captured, logs } = vi.hoisted(() => ({
  recordSkipped: vi.fn(async (..._a: unknown[]) => {}),
  captured: vi.fn(),
  logs: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
}));

vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...a) }));
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    createLogger: () => ({ ...logs, debug: vi.fn() }),
    captureException: (...a: unknown[]) => captured(...a),
  };
});
// One synthetic event per transaction: the transaction's `user` field carries the user.
vi.mock("../src/lpVault/decoder.js", () => ({
  decodeLpVaultEvents: (tx: { signature: string; user?: string; registry?: string; market?: string | null; lpMint?: string | null }) =>
    tx.user
      ? [{
          signature: tx.signature, ixIndex: 0, slot: 1, blockTime: null, kind: "deposit", programId: "PROG",
          market: tx.market ?? null, registry: tx.registry ?? "REG", user: tx.user, lpMint: tx.lpMint ?? null,
          collateralAtoms: 100n, lpAmount: 100n, domain: null,
        }]
      : [],
}));

import { LpVaultIndexer } from "../src/services/LpVaultIndexer.js";
import { MemoryLpVaultStore } from "../src/lpVault/store.js";
import type { LpVaultChain, OnchainClaim, RegistryInfo, SigInfo } from "../src/lpVault/chain.js";

const REG: RegistryInfo = { registry: "REG", programId: "PROG", market: "MKT", lpMint: "MINT" };
const poison = Object.assign(new Error("Transaction version (2) is not supported"), { code: -32015 });

class Chain implements LpVaultChain {
  /** oldest -> newest */
  sigs: SigInfo[] = [];
  txs = new Map<string, unknown>();
  txErr = new Map<string, Error>();
  registries: RegistryInfo[] = [REG];
  claim: OnchainClaim = { heldShares: 100n, pendingShares: 0n, contextSlot: 9 };
  listCalls = 0;
  sigCalls: { until?: string; before?: string; limit: number }[] = [];
  failList = false;
  async listRegistries() { this.listCalls++; if (this.failList) throw new Error("list down"); return this.registries; }
  async getSignatures(_a: string, o: { until?: string; before?: string; limit: number }) {
    this.sigCalls.push(o);
    const newestFirst = [...this.sigs].reverse();
    if (o.until) {
      const i = newestFirst.findIndex((s) => s.signature === o.until);
      if (i >= 0) newestFirst.splice(i);
    }
    if (o.before) {
      const i = newestFirst.findIndex((s) => s.signature === o.before);
      newestFirst.splice(0, i + 1);
    }
    return newestFirst.slice(0, o.limit);
  }
  async getTransaction(sig: string) {
    const e = this.txErr.get(sig);
    if (e) throw e;
    return (this.txs.has(sig) ? this.txs.get(sig) : null) as never;
  }
  async getClaim() { return this.claim; }
  add(sig: string, user?: string, extra: Record<string, unknown> = {}, failed = false) {
    this.sigs.push({ signature: sig, slot: this.sigs.length + 1, failed });
    this.txs.set(sig, { signature: sig, user, ...extra });
  }
}

const mk = (chain: Chain, store = new MemoryLpVaultStore(), extra: object = {}) =>
  ({ store, ix: new LpVaultIndexer({ chain, store, network: "devnet", programIds: ["PROG"], ...extra }) });

beforeEach(() => {
  recordSkipped.mockClear();
  captured.mockClear();
  Object.values(logs).forEach((f) => f.mockClear());
});
afterEach(() => vi.useRealTimers());

describe("LpVaultIndexer syncRegistry", () => {
  it("returns an empty result with no signatures and does not move the cursor", async () => {
    const chain = new Chain();
    const { ix, store } = mk(chain);
    const r = await ix.syncRegistry(REG);
    expect(r).toMatchObject({ signatures: 0, backlog: 0, events: 0, users: [] });
    expect(store.syncCursors.size).toBe(0);
  });

  it("ignores failed transactions, folds events and defaults market/lpMint from the registry", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.add("s2", "bob", {}, true); // failed: never fetched
    chain.add("s3", undefined); // no events
    chain.add("s4", "carol", { registry: "OTHER" }); // other vault: filtered
    const { ix, store } = mk(chain);
    const r = await ix.syncRegistry(REG);
    expect(r.events).toBe(1);
    expect(r.users).toEqual(["alice"]);
    const ev = [...store.events.values()][0]!.ev;
    expect(ev.market).toBe("MKT");
    expect(ev.lpMint).toBe("MINT");
    expect(store.syncCursors.get("devnet|REG")?.signature).toBe("s4");
  });

  it("keeps an event's own market and lpMint over the registry's", async () => {
    const chain = new Chain();
    chain.add("s1", "alice", { market: "OWN_M", lpMint: "OWN_L" });
    const { ix, store } = mk(chain);
    await ix.syncRegistry(REG);
    const ev = [...store.events.values()][0]!.ev;
    expect([ev.market, ev.lpMint]).toEqual(["OWN_M", "OWN_L"]);
  });

  it("throws when getTransaction returns null, leaving the cursor untouched", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.txs.delete("s1");
    const { ix, store } = mk(chain);
    await expect(ix.syncRegistry(REG)).rejects.toThrow(/returned null for s1/);
    expect(store.syncCursors.size).toBe(0);
  });

  it("rethrows a non-poison getTransaction error", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.txErr.set("s1", new Error("429 too many requests"));
    const { ix } = mk(chain);
    await expect(ix.syncRegistry(REG)).rejects.toThrow(/429/);
  });

  it("throws when enumeration reaches maxBacklogSignatures", async () => {
    const chain = new Chain();
    for (let i = 0; i < 1000; i++) chain.add(`s${i}`);
    const { ix } = mk(chain, undefined, { maxBacklogSignatures: 1000 });
    await expect(ix.syncRegistry(REG)).rejects.toThrow(/backlog exceeds 1000/);
  });

  it("pages across a full page of 1000 signatures using `before`", async () => {
    const chain = new Chain();
    for (let i = 0; i < 1001; i++) chain.add(`s${i}`);
    const { ix } = mk(chain);
    const r = await ix.syncRegistry(REG);
    expect(r.backlog).toBe(1001);
    expect(chain.sigCalls[1]!.before).toBe("s1"); // oldest of the first (newest-first) page
  });

  it("skips a poison transaction, records it, and reconciles every open position", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.add("s2", "bob");
    chain.add("s3", "alice");
    chain.txErr.set("s2", poison);
    const { ix, store } = mk(chain);
    const r = await ix.syncRegistry(REG);
    expect(recordSkipped).toHaveBeenCalledTimes(1);
    expect((recordSkipped.mock.calls[0]![0] as { signature: string }[])[0]!.signature).toBe("s2");
    expect(logs.error).toHaveBeenCalledWith(expect.stringContaining("advanced past skipped"), expect.anything());
    // alice reconciled twice (open-positions pass + touched pass)
    expect(r.reconciled).toBe(2);
    expect(store.positions.size).toBe(1);
  });

  it("records a non-Error poison rejection by its string form", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.add("s2", "bob");
    chain.txErr.set("s2", "Transaction version (2) is not supported" as unknown as Error);
    const { ix } = mk(chain);
    await ix.syncRegistry(REG);
    expect((recordSkipped.mock.calls[0]![0] as { error: string }[])[0]!.error).toMatch(/version \(2\)/);
  });

  it("refuses a window where every transaction is unreadable and holds the cursor", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.txErr.set("s1", poison);
    const { ix, store } = mk(chain);
    await expect(ix.syncRegistry(REG)).rejects.toThrow(/mass skip refused/);
    expect(store.syncCursors.size).toBe(0);
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("raises the breaker alert after consecutive held polls", async () => {
    const chain = new Chain();
    chain.registries = [{ ...REG, registry: "BRK" }];
    chain.add("s1", "alice", { registry: "BRK" });
    chain.txErr.set("s1", poison);
    const { ix } = mk(chain);
    const reg = chain.registries[0]!;
    for (let i = 0; i < 10; i++) await expect(ix.syncRegistry(reg)).rejects.toThrow(/mass skip/);
    expect(logs.error).toHaveBeenCalledWith(expect.stringContaining("ALERT"), expect.objectContaining({ registry: "BRK", consecutivePolls: 10 }));
    expect(captured).toHaveBeenCalledWith(expect.any(Error), expect.objectContaining({ tags: { context: "lp-vault-breaker-held" } }));
  });

  it("drains a backlog over several syncs and defers reconcile while draining", async () => {
    const chain = new Chain();
    for (let i = 1; i <= 5; i++) chain.add(`s${i}`, `u${i}`);
    const { ix, store } = mk(chain, undefined, { maxSignaturesPerSync: 2 });
    const a = await ix.syncRegistry(REG);
    expect([a.signatures, a.backlog, a.reconciled, a.reconcileSkipped]).toEqual([2, 5, 0, 2]);
    const b = await ix.syncRegistry(REG);
    expect([b.signatures, b.backlog]).toEqual([2, 3]);
    const c = await ix.syncRegistry(REG);
    expect([c.signatures, c.backlog, c.reconciled]).toEqual([1, 1, 1]);
    expect([...store.syncCursors.values()][0]!.signature).toBe("s5");
  });

  it("skips reconcile for a user when a vault tx landed after the claim read", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    const orig = chain.getClaim.bind(chain);
    chain.getClaim = async () => { chain.add("s2"); return orig(); };
    const { ix } = mk(chain);
    const r = await ix.syncRegistry(REG);
    expect([r.reconciled, r.reconcileSkipped]).toEqual([0, 1]);
  });
});

describe("LpVaultIndexer cursors", () => {
  it("seeds from the durable cursor once and reuses the in-memory one", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    const store = new MemoryLpVaultStore();
    await store.saveCursor("devnet", "REG", "s1", 1);
    const load = vi.spyOn(store, "loadCursor");
    const { ix } = mk(chain, store);
    expect((await ix.syncRegistry(REG)).signatures).toBe(0);
    await ix.syncRegistry(REG);
    expect(load).toHaveBeenCalledTimes(1);
    expect(chain.sigCalls[0]!.until).toBe("s1");
  });

  it("asks the store only once for a vault without a stored cursor", async () => {
    const chain = new Chain();
    const store = new MemoryLpVaultStore();
    const load = vi.spyOn(store, "loadCursor");
    const { ix } = mk(chain, store);
    await ix.syncRegistry(REG);
    await ix.syncRegistry(REG);
    expect(load).toHaveBeenCalledTimes(1);
  });

  it("ephemeralCursor never reads or writes the durable cursor but keeps its own", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    const store = new MemoryLpVaultStore();
    await store.saveCursor("devnet", "REG", "zzz", 1);
    const save = vi.spyOn(store, "saveCursor");
    const load = vi.spyOn(store, "loadCursor");
    const { ix } = mk(chain, store, { ephemeralCursor: true });
    expect((await ix.syncRegistry(REG)).signatures).toBe(1);
    expect(save).not.toHaveBeenCalled();
    expect(load).not.toHaveBeenCalled();
    expect((await ix.syncRegistry(REG)).signatures).toBe(0);
  });
});

describe("LpVaultIndexer reconcileOpenPositions", () => {
  it("is a no-op before any cursor exists", async () => {
    const { ix } = mk(new Chain());
    expect(await ix.reconcileOpenPositions(REG)).toEqual({ reconciled: 0, skipped: 0 });
  });

  it("reconciles every open position once a cursor exists", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.add("s2", "bob");
    const { ix } = mk(chain);
    await ix.syncRegistry(REG);
    expect(await ix.reconcileOpenPositions(REG)).toEqual({ reconciled: 2, skipped: 0 });
  });
});

describe("LpVaultIndexer registries, tick, start/stop", () => {
  it("refreshRegistries stores and returns the discovered vaults", async () => {
    const chain = new Chain();
    const { ix } = mk(chain);
    expect(ix.getRegistries()).toEqual([]);
    expect(await ix.refreshRegistries()).toEqual([REG]);
    expect(ix.getRegistries()).toEqual([REG]);
  });

  it("tick discovers, syncs, warns about a backlog and runs the full reconcile", async () => {
    const chain = new Chain();
    for (let i = 1; i <= 3; i++) chain.add(`s${i}`, `u${i}`);
    const { ix, store } = mk(chain, undefined, { maxSignaturesPerSync: 2 });
    const rec = vi.spyOn(ix, "reconcileOpenPositions");
    await ix.tick();
    expect(chain.listCalls).toBe(1);
    expect(logs.warn).toHaveBeenCalledWith(expect.stringContaining("draining a backlog"), expect.objectContaining({ backlog: 3 }));
    expect(rec).toHaveBeenCalledTimes(1);
    // second tick inside both intervals: no discovery, no full reconcile
    await ix.tick();
    expect(chain.listCalls).toBe(1);
    expect(rec).toHaveBeenCalledTimes(1);
    expect(store.positions.size).toBeGreaterThan(0);
  });

  it("tick honours custom discovery/reconcile intervals", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    const { ix } = mk(chain, undefined, { discoveryIntervalMs: -1, reconcileIntervalMs: -1 });
    const rec = vi.spyOn(ix, "reconcileOpenPositions");
    await ix.tick();
    await ix.tick();
    expect(chain.listCalls).toBe(2);
    expect(rec).toHaveBeenCalledTimes(2);
  });

  it("tick isolates a failing vault: logs, captures, and carries on", async () => {
    const chain = new Chain();
    chain.registries = [{ ...REG, registry: "BAD" }, REG];
    chain.add("s1", "alice");
    chain.getSignatures = async (a: string) => {
      if (a === "BAD") throw "plain string failure";
      return [{ signature: "s1", slot: 1, failed: false }];
    };
    const { ix, store } = mk(chain);
    await ix.tick();
    expect(logs.error).toHaveBeenCalledWith("LP-vault sync failed", expect.objectContaining({ registry: "BAD", error: "plain string failure" }));
    expect(captured).toHaveBeenCalledWith(expect.any(Error), expect.objectContaining({ tags: { context: "lp-vault-sync" } }));
    expect(store.syncCursors.get("devnet|REG")?.signature).toBe("s1"); // the healthy vault was still synced
  });

  it("tick logs Error messages from a failing vault", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    chain.txErr.set("s1", new Error("boom"));
    const { ix } = mk(chain);
    await ix.tick();
    expect(logs.error).toHaveBeenCalledWith("LP-vault sync failed", expect.objectContaining({ error: "boom" }));
  });

  it("tick never throws when discovery fails, and does not set the busy flag forever", async () => {
    const chain = new Chain();
    chain.failList = true;
    const { ix } = mk(chain);
    await expect(ix.tick()).resolves.toBeUndefined();
    expect(logs.error).toHaveBeenCalledWith("LP-vault tick failed", { error: "list down" });
    chain.failList = false;
    await ix.tick();
    expect(chain.listCalls).toBe(2);
    const other = mk(chain).ix; // fresh indexer: discovery is due again
    (chain as { listRegistries: unknown }).listRegistries = async () => { throw "str"; };
    await other.tick();
    expect(logs.error).toHaveBeenCalledWith("LP-vault tick failed", { error: "str" });
  });

  it("tick is re-entrancy safe while a tick is in flight", async () => {
    const chain = new Chain();
    let release!: () => void;
    chain.listRegistries = async () => { chain.listCalls++; await new Promise<void>((r) => (release = r)); return []; };
    const { ix } = mk(chain);
    const first = ix.tick();
    await ix.tick(); // returns immediately
    expect(chain.listCalls).toBe(1);
    release();
    await first;
  });

  it("start schedules the interval and a first tick, is idempotent, and stop cancels", async () => {
    vi.useFakeTimers();
    const chain = new Chain();
    const { ix } = mk(chain, undefined, { pollIntervalMs: 1000, discoveryIntervalMs: -1 });
    ix.start();
    ix.start();
    expect(logs.info).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5_000);
    const running = chain.listCalls; // 5 interval ticks + the delayed first tick
    expect(running).toBeGreaterThanOrEqual(5);
    ix.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(chain.listCalls).toBe(running);
    ix.stop(); // safe twice
  });

  it("start uses the 30s default poll interval", async () => {
    vi.useFakeTimers();
    const { ix } = mk(new Chain());
    ix.start();
    expect(logs.info).toHaveBeenCalledWith("LpVaultIndexer started", expect.objectContaining({ intervalMs: 30_000 }));
    ix.stop();
  });
});

// J-20261117-IDX-BUG-LPVAULTFAILEDWINDOW: three suspected stall leads, pinned. None is a defect (see each comment).
describe("LpVaultIndexer failed-signature leads", () => {
  it("lead 1: a poison tx beside only on-chain-failed txs holds the cursor, and one readable tx releases it", async () => {
    const chain = new Chain();
    chain.add("f1", undefined, {}, true);
    chain.add("p1", "alice");
    chain.txErr.set("p1", poison);
    chain.add("f2", undefined, {}, true);
    const { ix, store } = mk(chain);
    // A failed tx comes from getSignatures, not a getTransaction read, so it is no evidence the reader works.
    await expect(ix.syncRegistry(REG)).rejects.toThrow(/1 unreadable, 0 read ok/);
    expect(store.syncCursors.size).toBe(0);
    // Any readable sibling proves the reader is healthy: the poison tx is skipped and the cursor moves past all of it.
    chain.add("ok", "bob");
    const r = await ix.syncRegistry(REG);
    expect(r.signatures).toBe(4);
    expect([...store.syncCursors.values()][0]!.signature).toBe("ok");
    expect(recordSkipped).toHaveBeenCalledTimes(1);
  });

  it("lead 2: a registry missing from a later discovery stays registered, and its poll is an empty no-op", async () => {
    const chain = new Chain();
    const { ix } = mk(chain);
    await ix.refreshRegistries();
    chain.registries = [];
    await ix.refreshRegistries();
    expect(ix.getRegistries()).toEqual([REG]);
    // Cost of keeping it: one empty getSignatures per poll, no events, no cursor, no error.
    chain.sigCalls.length = 0;
    const r = await ix.syncRegistry(REG);
    expect([r.signatures, r.events]).toEqual([0, 0]);
    expect(chain.sigCalls).toHaveLength(1);
  });

  it("lead 3: a failed tx landing after the sync defers one reconcile; the next sync's cursor covers it and reconcile runs", async () => {
    const chain = new Chain();
    chain.add("s1", "alice");
    const orig = chain.getClaim.bind(chain);
    let spammed = false;
    chain.getClaim = async () => { if (!spammed) { spammed = true; chain.add("spam", undefined, {}, true); } return orig(); };
    const { ix, store } = mk(chain);
    const a = await ix.syncRegistry(REG);
    expect([a.reconciled, a.reconcileSkipped]).toEqual([0, 1]);
    // The cursor window includes failed signatures, so the next pass compares against "spam" and the basis is checked.
    const b = await ix.syncRegistry(REG);
    expect([b.signatures, b.reconciled, b.reconcileSkipped]).toEqual([1, 0, 0]);
    expect([...store.syncCursors.values()][0]!.signature).toBe("spam");
    const rec = await ix.reconcileOpenPositions(REG);
    expect(rec).toEqual({ reconciled: 1, skipped: 0 });
  });
});
