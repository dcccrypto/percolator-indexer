import { describe, it, expect, beforeEach, vi } from "vitest";

const { errorLog } = vi.hoisted(() => ({ errorLog: vi.fn() }));
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ error: errorLog, warn: vi.fn(), info: vi.fn(), debug: vi.fn() }),
}));
import type { SupabaseClient } from "@supabase/supabase-js";
import type { LpVaultEvent } from "../../src/lpVault/decoder.js";
import {
  MemoryLpVaultStore,
  SupabaseLpVaultStore,
  recomputePosition,
  resetCursorWarnings,
} from "../../src/lpVault/store.js";

/** A chainable stand-in for the supabase query builder. Records every call and resolves to `result`. */
type Result = { data?: unknown; error?: { message: string } | null };
function fakeDb(result: Result) {
  const log: Array<{ table: string; op: string; args: unknown[] }> = [];
  const db = {
    from(table: string) {
      const q: Record<string, unknown> = {};
      const chain = (op: string) => (...args: unknown[]) => {
        log.push({ table, op, args });
        return q;
      };
      for (const op of ["select", "eq", "order", "or"]) q[op] = chain(op);
      const done = (op: string) => (...args: unknown[]) => {
        log.push({ table, op, args });
        return Promise.resolve({ data: null, error: null, ...result });
      };
      q.limit = done("limit");
      q.maybeSingle = done("maybeSingle");
      q.upsert = done("upsert");
      return q;
    },
  };
  return { db: db as unknown as SupabaseClient, log };
}

const REG = "REG";
const USER = "USER";
const ev = (over: Partial<LpVaultEvent>): LpVaultEvent => ({
  signature: "sig1",
  ixIndex: 0,
  slot: 100,
  blockTime: null,
  kind: "deposit",
  programId: "PROG",
  market: "SLAB",
  registry: REG,
  user: USER,
  lpMint: "MINT",
  collateralAtoms: 1_000n,
  lpAmount: 1_000n,
  domain: null,
  ...over,
});

describe("SupabaseLpVaultStore", () => {
  beforeEach(() => resetCursorWarnings());

  it("upsertEvents sends nothing for an empty batch", async () => {
    const { db, log } = fakeDb({});
    await new SupabaseLpVaultStore(db).upsertEvents("devnet", []);
    expect(log).toEqual([]);
  });

  it("upsertEvents writes u64 amounts as strings and is idempotent on (signature, ix_index)", async () => {
    const { db, log } = fakeDb({});
    const big = 2n ** 64n - 1n;
    await new SupabaseLpVaultStore(db).upsertEvents("devnet", [
      ev({ collateralAtoms: big, lpAmount: null, blockTime: 1_700_000_000 }),
      ev({ signature: "sig2", lpAmount: 5n }),
    ]);
    const [rows, opts] = log[0].args as [Array<Record<string, unknown>>, Record<string, unknown>];
    expect(rows[0]).toMatchObject({
      network: "devnet",
      collateral_atoms: big.toString(),
      lp_amount: null,
      block_time: "2023-11-14T22:13:20.000Z",
      user_wallet: USER,
    });
    expect(rows[1]).toMatchObject({ lp_amount: "5", block_time: null });
    expect(opts).toEqual({ onConflict: "signature,ix_index", ignoreDuplicates: true });
  });

  it("upsertEvents throws on a database error", async () => {
    const { db } = fakeDb({ error: { message: "boom" } });
    await expect(new SupabaseLpVaultStore(db).upsertEvents("devnet", [ev({})])).rejects.toThrow("lp_vault_events upsert: boom");
  });

  it("loadEvents rebuilds bigints from text columns, scoped to network/registry/user", async () => {
    const { db, log } = fakeDb({
      data: [
        { signature: "a", ix_index: 2, kind: "deposit", slot: "7", market_slab: "S", lp_mint: "M", collateral_atoms: "18446744073709551615", lp_amount: "9" },
        { signature: "b", ix_index: 0, kind: "request_redeem", slot: 8, market_slab: null, lp_mint: null, collateral_atoms: "0", lp_amount: null },
      ],
    });
    const out = await new SupabaseLpVaultStore(db).loadEvents("devnet", REG, USER);
    expect(out[0]).toEqual({
      kind: "deposit", slot: 7, signature: "a", ixIndex: 2,
      collateralAtoms: 2n ** 64n - 1n, lpAmount: 9n, marketSlab: "S", lpMint: "M",
    });
    expect(out[1].lpAmount).toBeNull();
    const eqs = log.filter((l) => l.op === "eq").map((l) => l.args);
    expect(eqs).toEqual([["network", "devnet"], ["registry", REG], ["user_wallet", USER]]);
  });

  it("loadEvents treats null data as no events", async () => {
    const { db } = fakeDb({ data: null });
    expect(await new SupabaseLpVaultStore(db).loadEvents("devnet", REG, USER)).toEqual([]);
  });

  it("loadEvents refuses a history that hit the row cap rather than fold a truncated one", async () => {
    const row = { signature: "x", ix_index: 0, kind: "deposit", slot: 1, market_slab: null, lp_mint: null, collateral_atoms: "1", lp_amount: "1" };
    const { db } = fakeDb({ data: Array.from({ length: 10_000 }, () => row) });
    await expect(new SupabaseLpVaultStore(db).loadEvents("devnet", REG, USER)).rejects.toThrow("fold refused");
  });

  it("loadEvents throws on a database error", async () => {
    const { db } = fakeDb({ error: { message: "nope" } });
    await expect(new SupabaseLpVaultStore(db).loadEvents("devnet", REG, USER)).rejects.toThrow("lp_vault_events select: nope");
  });

  it("loadPosition returns the row, null when absent, and throws on error", async () => {
    const row = { registry: REG };
    expect(await new SupabaseLpVaultStore(fakeDb({ data: row }).db).loadPosition("devnet", REG, USER)).toBe(row);
    expect(await new SupabaseLpVaultStore(fakeDb({ data: null }).db).loadPosition("devnet", REG, USER)).toBeNull();
    await expect(new SupabaseLpVaultStore(fakeDb({ error: { message: "x" } }).db).loadPosition("devnet", REG, USER)).rejects.toThrow("lp_vault_positions select: x");
  });

  it("loadPosition selects numeric columns as text so u128 never passes through a JS number", async () => {
    const { db, log } = fakeDb({ data: null });
    await new SupabaseLpVaultStore(db).loadPosition("devnet", REG, USER);
    const cols = log.find((l) => l.op === "select")!.args[0] as string;
    expect(cols).toContain("lp_shares::text");
    expect(cols).toContain("cost_basis_atoms::text");
    expect(cols).toContain("realized_pnl_atoms::text");
  });

  it("writePosition upserts on the position key and throws on error", async () => {
    const mem = new MemoryLpVaultStore();
    const row = await recomputePosition(mem, "devnet", REG, USER);
    const { db, log } = fakeDb({});
    await new SupabaseLpVaultStore(db).writePosition(row);
    expect(log[0].args[1]).toEqual({ onConflict: "network,registry,user_wallet" });
    expect(log[0].args[0]).toMatchObject({ registry: REG, user_wallet: USER });
    await expect(new SupabaseLpVaultStore(fakeDb({ error: { message: "bad" } }).db).writePosition(row)).rejects.toThrow("lp_vault_positions upsert: bad");
  });

  it("listOpenPositions filters to positions that may hold shares and throws on error", async () => {
    const { db, log } = fakeDb({ data: [{ registry: REG }] });
    expect(await new SupabaseLpVaultStore(db).listOpenPositions("devnet", REG)).toEqual([{ registry: REG }]);
    expect(log.find((l) => l.op === "or")!.args[0]).toBe("lp_shares.gt.0,onchain_lp_shares.gt.0");
    expect(await new SupabaseLpVaultStore(fakeDb({ data: null }).db).listOpenPositions("devnet", REG)).toEqual([]);
    await expect(new SupabaseLpVaultStore(fakeDb({ error: { message: "q" } }).db).listOpenPositions("devnet", REG)).rejects.toThrow("lp_vault_positions list: q");
  });

  describe("sync cursor (best effort)", () => {
    it("loads the stored signature, or null when none", async () => {
      expect(await new SupabaseLpVaultStore(fakeDb({ data: { signature: "S1" } }).db).loadCursor("devnet", REG)).toBe("S1");
      expect(await new SupabaseLpVaultStore(fakeDb({ data: null }).db).loadCursor("devnet", REG)).toBeNull();
    });

    it("a missing table degrades to null instead of throwing", async () => {
      const store = new SupabaseLpVaultStore(fakeDb({ error: { message: "relation does not exist" } }).db);
      await expect(store.loadCursor("devnet", REG)).resolves.toBeNull();
    });

    it("saveCursor upserts per (network, registry) and swallows a missing table", async () => {
      const { db, log } = fakeDb({});
      await new SupabaseLpVaultStore(db).saveCursor("devnet", REG, "S2", 42);
      expect(log[0].args[0]).toMatchObject({ network: "devnet", registry: REG, signature: "S2", slot: 42 });
      expect(log[0].args[1]).toEqual({ onConflict: "network,registry" });
      await expect(new SupabaseLpVaultStore(fakeDb({ error: { message: "gone" } }).db).saveCursor("devnet", REG, "S", 1)).resolves.toBeUndefined();
    });

    it("logs the unavailable table once per op and registry, not every poll", async () => {
      const store = new SupabaseLpVaultStore(fakeDb({ error: { message: "gone" } }).db);
      errorLog.mockClear();
      await store.loadCursor("devnet", REG);
      await store.loadCursor("devnet", REG);
      await store.saveCursor("devnet", REG, "S", 1);
      await store.saveCursor("devnet", REG, "S", 1);
      await store.loadCursor("devnet", "OTHER");
      expect(errorLog).toHaveBeenCalledTimes(3); // load/REG, save/REG, load/OTHER
    });
  });
});

describe("MemoryLpVaultStore.listOpenPositions", () => {
  it("lists positions with indexed or on-chain shares and drops closed ones", async () => {
    const mem = new MemoryLpVaultStore();
    const base = await recomputePosition(mem, "devnet", REG, "closed");
    await mem.writePosition({ ...base, user_wallet: "indexed", lp_shares: "5" });
    await mem.writePosition({ ...base, user_wallet: "onchain", lp_shares: "0", onchain_lp_shares: "3" });
    await mem.writePosition({ ...base, user_wallet: "onchain-zero", lp_shares: "0", onchain_lp_shares: "0" });
    await mem.writePosition({ ...base, user_wallet: "other-registry", registry: "R2", lp_shares: "9" });
    const open = (await mem.listOpenPositions("devnet", REG)).map((p) => p.user_wallet).sort();
    expect(open).toEqual(["indexed", "onchain"]);
  });
});

describe("recomputePosition reconciliation", () => {
  const deposit = ev({ kind: "deposit", collateralAtoms: 1_000n, lpAmount: 1_000n, slot: 10 });

  it("records the slot when the on-chain claim differs from the indexed shares", async () => {
    const mem = new MemoryLpVaultStore();
    await mem.upsertEvents("devnet", [deposit]);
    const row = await recomputePosition(mem, "devnet", REG, USER, { reconcile: { onchainShares: 400n, contextSlot: 55 } });
    expect(row.transfer_detected_slot).toBe(55);
    expect(row.onchain_lp_shares).toBe("400");
    expect(row.reconciled_slot).toBe(55);
    expect(row.basis_known).toBe(false);
    expect(row.inconsistency).toContain("slot 55");
  });

  it("keeps the transfer flag sticky on a later matching reconcile while shares remain", async () => {
    const mem = new MemoryLpVaultStore();
    await mem.upsertEvents("devnet", [deposit]);
    await recomputePosition(mem, "devnet", REG, USER, { reconcile: { onchainShares: 400n, contextSlot: 55 } });
    const row = await recomputePosition(mem, "devnet", REG, USER, { reconcile: { onchainShares: 1_000n, contextSlot: 60 } });
    expect(row.transfer_detected_slot).toBe(55);
    expect(row.reconciled_slot).toBe(60);
    expect(row.basis_known).toBe(false);
  });

  it("clears the flag on a match at zero on both sides (full exit)", async () => {
    const mem = new MemoryLpVaultStore();
    await mem.upsertEvents("devnet", [
      deposit,
      ev({ signature: "sig2", slot: 20, kind: "request_redeem", collateralAtoms: 0n, lpAmount: 1_000n }),
      ev({ signature: "sig3", slot: 30, kind: "execute_redeem", collateralAtoms: 1_200n, lpAmount: 1_000n }),
    ]);
    await recomputePosition(mem, "devnet", REG, USER, { reconcile: { onchainShares: 5n, contextSlot: 25 } });
    const row = await recomputePosition(mem, "devnet", REG, USER, { reconcile: { onchainShares: 0n, contextSlot: 40 } });
    expect(row.lp_shares).toBe("0");
    expect(row.transfer_detected_slot).toBeNull();
  });

  it("prefers an explicit market slab, then the stored one, and keeps a previously seen lp mint", async () => {
    const mem = new MemoryLpVaultStore();
    await mem.upsertEvents("devnet", [deposit]);
    const a = await recomputePosition(mem, "devnet", REG, USER, { marketSlab: "EXPLICIT" });
    expect(a.market_slab).toBe("EXPLICIT");
    const b = await recomputePosition(mem, "devnet", REG, USER);
    expect(b.market_slab).toBe("EXPLICIT");
    expect(b.lp_mint).toBe("MINT");
  });

  it("an empty history is a zero position with no market or mint", async () => {
    const row = await recomputePosition(new MemoryLpVaultStore(), "devnet", REG, USER);
    expect(row).toMatchObject({ lp_shares: "0", market_slab: null, lp_mint: null, event_count: 0 });
  });
});
