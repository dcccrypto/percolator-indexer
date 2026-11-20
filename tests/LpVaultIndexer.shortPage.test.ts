/**
 * DRAFT failing test (J-20261120-IDX-REPORT-RPCFAULTINJECTREPLAY): permanent LP-vault event loss the RPC fault replay
 * measured on feat/v22-indexer 9e8e192 (fault "shortpage"). Drop into tests/ to run:
 *   vitest run tests/LpVaultIndexer.shortPage.draft.test.ts --maxWorkers=4
 *
 * LpVaultIndexer.syncRegistry pages getSignatures(registry, { until: cursor }) and stops at the first page shorter than
 * PAGE (1000), taking that as "the cursor was reached". A node that cuts the page short (here: newest half) ends the
 * walk early; the window is folded and the cursor commits to its newest signature, so the older signatures between
 * the old cursor and the short page are never fetched: their deposits / redemptions are missing from
 * lp_vault_events and the folded position (shares, cost basis) is wrong for good.
 */
import { describe, expect, it, vi } from "vitest";

vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: async () => undefined }));
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, createLogger: () => ({ error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() }), captureException: vi.fn() };
});
// One deposit event per transaction (the decoder is not under test here).
vi.mock("../src/lpVault/decoder.js", () => ({
  decodeLpVaultEvents: (tx: { signature: string; user?: string }) =>
    tx.user
      ? [{ signature: tx.signature, ixIndex: 0, slot: 1, blockTime: null, kind: "deposit", programId: "PROG", market: null, registry: "REG", user: tx.user, lpMint: null, collateralAtoms: 100n, lpAmount: 100n, domain: null }]
      : [],
}));

import { LpVaultIndexer } from "../src/services/LpVaultIndexer.js";
import { MemoryLpVaultStore } from "../src/lpVault/store.js";
import type { LpVaultChain, RegistryInfo, SigInfo } from "../src/lpVault/chain.js";

const REG: RegistryInfo = { registry: "REG", programId: "PROG", market: "MKT", lpMint: "MINT" };

class ShortPageChain implements LpVaultChain {
  sigs: SigInfo[] = []; // oldest -> newest
  shortNext = false;
  async listRegistries() { return [REG]; }
  async getSignatures(_a: string, o: { until?: string; before?: string; limit: number }) {
    let list = [...this.sigs].reverse();
    if (o.before) { const i = list.findIndex((s) => s.signature === o.before); list = i < 0 ? [] : list.slice(i + 1); }
    if (o.until) { const i = list.findIndex((s) => s.signature === o.until); if (i >= 0) list = list.slice(0, i); }
    list = list.slice(0, o.limit);
    if (this.shortNext && !o.before && list.length >= 2) { this.shortNext = false; list = list.slice(0, Math.floor(list.length / 2)); }
    return list;
  }
  async getTransaction(sig: string) { return { signature: sig, user: "U" } as never; }
  async getClaim() { return { heldShares: 0n, pendingShares: 0n, contextSlot: 1 }; }
  add(sig: string) { this.sigs.push({ signature: sig, slot: this.sigs.length + 1, failed: false }); }
}

describe("LpVaultIndexer: a short signature page must not lose LP events for good", () => {
  it("folds every deposit when the node cuts one page short and answers in full later", async () => {
    const chain = new ShortPageChain();
    const store = new MemoryLpVaultStore();
    const ix = new LpVaultIndexer({ chain, store, network: "devnet", programIds: ["PROG"] });
    chain.add("s1");
    await ix.syncRegistry(REG); // cursor at s1
    for (const s of ["s2", "s3", "s4", "s5"]) chain.add(s);
    chain.shortNext = true; // the node returns [s5, s4] only
    await ix.syncRegistry(REG);
    await ix.syncRegistry(REG); // a later poll with a healthy node
    const events = await store.loadEvents("devnet", "REG", "U");
    // 9e8e192: ["s1", "s4", "s5"] (s2 and s3 were never fetched; the cursor moved to s5)
    expect(events.map((e: { signature: string }) => e.signature).sort()).toEqual(["s1", "s2", "s3", "s4", "s5"]);
  });
});
