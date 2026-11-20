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

const mk = (chain: Chain, store = new MemoryLpVaultStore()) =>
  ({ store, ix: new LpVaultIndexer({ chain, store, network: "devnet", programIds: ["PROG"] }) });

const U = 10;
const users = Array.from({ length: U }, (_, i) => `user${i}`);
/** Chain with U depositors; each user's on-chain claim differs from the indexed fold so a reconcile writes. */
function setup() {
  const chain = new Chain();
  users.forEach((u, i) => chain.add(`s${i}`, u));
  const claims = new Map(users.map((u, i) => [u, { heldShares: 1000n + BigInt(i), pendingShares: BigInt(i), contextSlot: 50 + i }]));
  chain.getClaim = (async (user: string) => claims.get(user)!) as never;
  return chain;
}
const latestChecks = (chain: Chain) => chain.sigCalls.filter((o) => o.limit === 1 && !o.until && !o.before).length;
const snapshot = (store: MemoryLpVaultStore) => [...store.positions.entries()].sort(([a], [b]) => (a < b ? -1 : 1));

describe("LpVaultIndexer D3: one latest-signature check per reconcile, not one per user", () => {
  beforeEach(() => {
    recordSkipped.mockClear();
    captured.mockClear();
    Object.values(logs).forEach((f) => f.mockClear());
  });

  it(`${U} open positions: 1 latest-signature read (was ${U}); stored positions equal the per-user reconcile`, async () => {
    const chain = setup();
    const { store, ix } = mk(chain);
    await ix.syncRegistry(REG);
    chain.sigCalls = [];
    expect(await ix.reconcileOpenPositions(REG)).toEqual({ reconciled: U, skipped: 0 });
    expect(latestChecks(chain)).toBe(1);

    // Reference: the old loop body, one user at a time (claim, latest check, recompute).
    const ref = setup();
    const r = mk(ref);
    await r.ix.syncRegistry(REG);
    for (const u of users) {
      expect(await (r.ix as any).reconcileUsers(REG, [u], "s" + (U - 1))).toEqual({ reconciled: 1, skipped: 0 });
    }
    expect(snapshot(store)).toEqual(snapshot(r.store));
    expect(users.map((u) => store.positions.get(`devnet|REG|${u}`)?.reconciled_slot)).toEqual(users.map((_, i) => 50 + i));
  });

  it("a vault tx landing during the claim reads skips every user (no claim may include it)", async () => {
    const chain = setup();
    const { store, ix } = mk(chain);
    await ix.syncRegistry(REG);
    const before = snapshot(store);
    const orig = chain.getClaim;
    let n = 0;
    chain.getClaim = (async (...a: unknown[]) => { if (++n === 3) chain.add("late"); return (orig as any)(...a); }) as never;
    expect(await ix.reconcileOpenPositions(REG)).toEqual({ reconciled: 0, skipped: U });
    expect(snapshot(store)).toEqual(before);
  });

  it("a claim read that throws still reconciles the users read before it, then rethrows", async () => {
    const chain = setup();
    const { store, ix } = mk(chain);
    await ix.syncRegistry(REG);
    const orig = chain.getClaim;
    chain.getClaim = (async (user: string, ...a: unknown[]) => {
      if (user === "user4") throw new Error("rpc down");
      const c = await (orig as any)(user, ...a);
      return { ...c, contextSlot: c.contextSlot + 100 };
    }) as never;
    chain.sigCalls = [];
    await expect(ix.reconcileOpenPositions(REG)).rejects.toThrow("rpc down");
    expect(latestChecks(chain)).toBe(1);
    // users 0..3 were read before the failure and reconciled (new slot), as the per-user loop did; 4..9 keep the sync's
    expect(users.map((u) => store.positions.get(`devnet|REG|${u}`)?.reconciled_slot ?? null)).toEqual(users.map((_, i) => (i < 4 ? 150 + i : 50 + i)));
  });

  it("no open positions: no RPC at all", async () => {
    const chain = new Chain();
    chain.add("s0");
    const { ix } = mk(chain);
    await ix.syncRegistry(REG);
    chain.sigCalls = [];
    expect(await ix.reconcileOpenPositions(REG)).toEqual({ reconciled: 0, skipped: 0 });
    expect(chain.sigCalls).toEqual([]);
  });
});
